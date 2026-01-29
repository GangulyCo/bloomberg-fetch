import blpapi  # Bloomberg BLPAPI, this and json are all that is needed for CMP.
from blpapi import CorrelationId  # For keeping track of multiple requests
import json  # CMP uses JSON for requests and response
import time
import re
from datetime import date, datetime
import pandas as pd
import numpy as np
from collections import OrderedDict
import logging  # For logging errors and debug information
from IPython.display import display, HTML  # For displaying HTML in Jupyter Notebook


def connect_to_cmp(host="localhost", port=8194):
    """
    Establishes a session with Bloomberg CMP service.

    Parameters:
        host (str): Server host (default is 'localhost').
        port (int): Server port (default is 8194).

    Returns:
        service (blpapi.Service): Connected CMP service.
        session (blpapi.Session): Active Bloomberg session.
    """
    options = blpapi.SessionOptions()
    options.setServerHost(host)
    options.setServerPort(port)

    session = blpapi.Session(options)
    if not session.start():
        print("Failed to start Bloomberg session.")
        raise ConnectionError("Failed to start Bloomberg session.")

    if not session.openService("//blp/cmp"):
        print("Failed to open CMP service.")
        raise ConnectionError("Failed to open CMP service.")

    service = session.getService("//blp/cmp")
    print("Connected to Bloomberg CMP service.")
    return service, session


def make_request(request_json, service, session, parse_response=True):
    """
    Formats and sends a single request to the CMP service. Waits and returns response

    Parameters:
        request_json (dict): Request parameters.
        service (blpapi.Service): Connected CMP service.
        session (blpapi.Session): Active Bloomberg session.
        parse_response (bool): If True, parses the response using parse_value.
                               If False, returns raw response values as strings.

    Returns:
        dict: Parsed response from the CMP service if parse_response is True,
              otherwise the raw JSON response.
    """
    request_body = {
        "cmpExcelRequest": {
            "parameters": [
                {"name": x, "value": str(request_json[x])} for x in request_json
            ],
        }
    }

    request = service.createRequest("request")
    request.getElement("cmpJsonRequest").setElement(
        "requestData", json.dumps(request_body, default=str)
    )

    retries = 3
    for attempt in range(retries):
        response = get_response(request, session)
        if response:
            if response.hasElement("errorResponse"):
                msg = (
                    response.getElement("errorResponse")
                    .getElement("message")
                    .getValueAsString()
                )
                if "Limit reached" in msg:
                    print("Throttling detected, retrying after 1 second...")
                    time.sleep(1)
                else:
                    print(f"Error from CMP: {msg}")
                    raise Exception(msg)
            else:
                data = json.loads(
                    response.getElement("cmpJsonResponse")
                    .getElement("responseData")
                    .getValueAsString()
                )
                if "cmpExcelResponse" in data:
                    if parse_response:
                        output = {
                            x["name"]: parse_value(x["value"])
                            for results in data["cmpExcelResponse"]["results"]
                            for x in results["fields"]
                        }
                        return output
                    else:
                        output = {
                            x["name"]: x["value"]
                            for results in data["cmpExcelResponse"]["results"]
                            for x in results["fields"]
                        }
                        return output
                else:
                    # print(response)
                    raise ValueError(f"Invalid response: {response}")
        else:
            print("No response received, retrying...")

    raise TimeoutError("Failed to receive a valid response after multiple attempts.")


def make_multiple_requests(
    requests_json, service, session, timeout=1200, max_retries=3, retry_delay=30
):
    """
    Send multiple requests concurrently to the Bloomberg CMP service and collect their responses.

    For each dictionary in `requests_json` (representing one request's parameters), this function:
      1. Wraps the parameters in the required JSON structure and creates a Bloomberg request.
      2. Sends the request with a unique correlation ID (using the index as the identifier).
      3. Stores the Bloomberg Request object (for potential retries) in a mapping.
      4. Waits for responses and matches each to its originating request via the correlation ID.
         - If a response contains an error (e.g., "Limit reached") and retries remain, the request is resent.
         - For a successful response, the JSON is parsed and a new field `_request_order` (the original index)
           is added.
      5. Once all responses are processed, the function returns the responses in the same order as the original requests.

    Args:
        requests_json (list): A list of dictionaries, each containing parameters for one request.
        service (blpapi.Service): The Bloomberg CMP service.
        session (blpapi.Session): The active Bloomberg session.
        timeout (int): Maximum time (in seconds) to wait for all responses.
        max_retries (int): Maximum number of retries for throttled requests.
        retry_delay (int): Seconds to wait before retrying a throttled request.

    Returns:
        list: A list of responses (or Exception objects) corresponding to the original request order.
    """
    # Mapping: request index -> Bloomberg Request object (for potential retries)
    requests_map = {}
    # Mapping: correlation ID value (an integer) -> request index.
    corr_mapping = {}
    # Mapping: request index -> response (or Exception) once received.
    responses = {}
    # Mapping: request index -> current retry count.
    retry_counts = {}

    logging.info("Sending %d requests...", len(requests_json))

    # Build and send each request.
    for i, req_json in enumerate(requests_json):
        # Construct the JSON body expected by Bloomberg.
        request_body = {
            "cmpExcelRequest": {
                "parameters": [
                    {"name": key, "value": str(req_json[key])} for key in req_json
                ],
            }
        }
        # Create the Bloomberg request object.
        req = service.createRequest("request")
        req.getElement("cmpJsonRequest").setElement(
            "requestData", json.dumps(request_body, default=str)
        )
        # Create a unique correlation ID (using the index).
        corr = CorrelationId(i)
        session.sendRequest(req, None, corr)

        # Store the request object for potential retries.
        requests_map[i] = req
        # Map the correlation ID's integer value to this request index.
        corr_mapping[corr.value()] = i
        # Initialize the retry counter for this request.
        retry_counts[i] = 0

        logging.debug("Sent request %d: %s", i, req_json)

    # Set of indices for requests still waiting for a final response.
    pending = set(range(len(requests_json)))
    start_time = time.time()

    logging.info("Waiting for responses... Pending requests: %s", pending)

    # Process events until all pending requests have been handled.
    while pending:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            logging.error(
                "Request timed out after %d seconds with pending: %s", timeout, pending
            )
            raise Exception("Request timed out")

        event = session.nextEvent(1000)
        event_type = event.eventType()
        logging.debug(
            "Event received: %s with %d messages; Pending: %s",
            str(event_type),
            len(list(event)),
            pending,
        )

        for msg in event:
            # Skip messages that don't have a correlation ID.
            if not msg.correlationIds():
                logging.debug("Message without correlationIds: %s", msg.toString())
                continue

            cid = msg.correlationIds()[0]
            if cid.value() not in corr_mapping:
                logging.debug("Received unknown correlation id: %s", cid)
                continue

            corr_id_int = corr_mapping[cid.value()]
            if corr_id_int not in pending:
                continue

            logging.debug(
                "Processing message for request %d: %s", corr_id_int, msg.messageType()
            )

            # Handle a direct failure response.
            if msg.messageType() == "RequestFailure":
                error_info = "RequestFailure: " + msg.toString()
                responses[corr_id_int] = Exception(error_info)
                pending.remove(corr_id_int)
                logging.error("Request %d failed: %s", corr_id_int, error_info)
            # Process partial or full responses.
            elif event_type in [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]:
                if msg.hasElement("errorResponse"):
                    error_msg = (
                        msg.getElement("errorResponse")
                        .getElement("message")
                        .getValueAsString()
                    )
                    # If throttling is detected, retry if allowed.
                    if "Limit reached" in error_msg:
                        if retry_counts[corr_id_int] < max_retries:
                            retry_counts[corr_id_int] += 1
                            logging.warning(
                                "Throttle reached for request %d, retrying (%d/%d) after %d seconds...",
                                corr_id_int,
                                retry_counts[corr_id_int],
                                max_retries,
                                retry_delay,
                            )
                            time.sleep(retry_delay)
                            session.sendRequest(
                                requests_map[corr_id_int],
                                None,
                                CorrelationId(corr_id_int),
                            )
                        else:
                            responses[corr_id_int] = Exception(
                                f"Throttle limit reached after {max_retries} retries."
                            )
                            pending.remove(corr_id_int)
                            logging.error(
                                "Request %d exceeded max retries.", corr_id_int
                            )
                    else:
                        responses[corr_id_int] = Exception(error_msg)
                        pending.remove(corr_id_int)
                        logging.error(
                            "Error in response for request %d: %s",
                            corr_id_int,
                            error_msg,
                        )
                else:
                    try:
                        json_str = (
                            msg.getElement("cmpJsonResponse")
                            .getElement("responseData")
                            .getValueAsString()
                        )
                        data = json.loads(json_str)
                        if "cmpExcelResponse" in data:
                            output = {}
                            for result in data["cmpExcelResponse"]["results"]:
                                output.update(
                                    {
                                        field["name"]: field["value"]
                                        for field in result["fields"]
                                    }
                                )
                            # Add a field to indicate the original request order.
                            output["_request_order"] = corr_id_int
                            responses[corr_id_int] = output
                            logging.debug(
                                "Received successful response for request %d.",
                                corr_id_int,
                            )
                        else:
                            responses[corr_id_int] = Exception(
                                "Unexpected response format: " + str(data)
                            )
                            logging.error(
                                "Unexpected format for request %d: %s",
                                corr_id_int,
                                data,
                            )
                    except Exception as e:
                        responses[corr_id_int] = e
                        logging.exception(
                            "Exception while parsing response for request %d.",
                            corr_id_int,
                        )
                    pending.remove(corr_id_int)

    # Return responses in the same order as the original requests.
    ordered_responses = [responses[i] for i in range(len(requests_json))]
    logging.info("All responses received.")
    return ordered_responses


# Get Response from CMP
def get_response(request, session):
    """
    Sends a request and waits for the response from the CMP service.

    Parameters:
        request (blpapi.Request): The request object to send.
        session (blpapi.Session): Active Bloomberg session.

    Returns:
        blpapi.Message: The response message from CMP.
    """
    session.sendRequest(request)

    while True:
        event = session.nextEvent(1000)
        for msg in event:
            if event.eventType() == blpapi.Event.RESPONSE:
                return msg
            elif msg.messageType() == "RequestFailure":
                print("Request failed.")
                return None


def parse_value(value):
    """
    Parses a value from the MBS analytics JSON response to the appropriate Python data type.

    Expected data types:
    - Integer, Float, String, ISO 8601 Date, Custom Date Formats
    - Lists of simple types, Lists of lists, Lists of dates

    Handles edge cases:
    - Empty strings, "null", or "None" values converted to None.
    - Boolean strings "true"/"false" converted to True/False.

    Parameters:
        value (str): The value from the JSON response to parse.

    Returns:
        The parsed value in the appropriate Python data type.
    """
    if value in ("", "null", "None"):
        return None

    if isinstance(value, list):
        return [parse_value(item) for item in value]

    if isinstance(value, str):
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False

    try:
        if isinstance(value, str) and value.isdigit():
            return int(value)
    except AttributeError:
        pass

    try:
        return float(value)
    except (ValueError, TypeError):
        pass

    try:
        if isinstance(value, str) and "T" in value:
            return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        pass

    try:
        if isinstance(value, str) and ("AM" in value or "PM" in value):
            return datetime.strptime(value, "%m/%d/%Y %I:%M %p")
    except (ValueError, TypeError):
        pass

    try:
        if isinstance(value, str):
            parsed_list = json.loads(value)
            if isinstance(parsed_list, list):
                return [parse_value(item) for item in parsed_list]
    except (ValueError, json.JSONDecodeError):
        pass

    return value


def table_to_dataframe(table_data):
    """
    Converts a list of lists (where the first row is the header) into a pandas DataFrame.

    Parameters:
        table_data (list of lists): The data to be converted. The first row should contain column headers.

    Returns:
        pd.DataFrame: A DataFrame created from the table data.
    """
    if (
        isinstance(table_data, list)
        and len(table_data) > 1
        and all(isinstance(row, list) for row in table_data)
    ):
        headers = table_data[0]
        data_rows = table_data[1:]
        return pd.DataFrame(data_rows, columns=headers)
    else:
        print("Invalid table format. Ensure the first row contains headers.")
        return pd.DataFrame()


print("Successfully imported required modules and helper functions")


def stack_dataframes(dataframes, columns=None, strict=False):
    """
    Stack a list of dataframes on top of each other.

    Parameters:
      dataframes (list of pd.DataFrame): List of dataframes to stack.
      columns (list of str, optional): List of columns for the aggregate dataframe.
      strict (bool): If True, dataframes must have the same columns. If False, missing columns will be filled with NA.

    Returns:
      pd.DataFrame: The aggregated dataframe.
    """

    if columns is None:
        # Determine the superset of all columns if columns parameter is not provided
        columns = OrderedDict()
        for df in dataframes:
            for col in df.columns:
                columns[col] = None
        columns = list(columns.keys())

    processed_dfs = []
    for df in dataframes:
        if strict:
            # Ensure the dataframe has exactly the same columns as specified
            if set(df.columns) != set(columns):
                raise ValueError(
                    "Dataframe columns do not match the specified columns."
                )
        else:
            # Add missing columns with NA values
            for col in columns:
                if col not in df.columns:
                    df[col] = pd.NA
            # Reorder columns to match the specified order
            df = df[columns]

        processed_dfs.append(df)

    # Concatenate all processed dataframes vertically
    result_df = pd.concat(processed_dfs, ignore_index=True)

    return result_df


def run_AssetRequest(
    session,
    service,
    security_list,
    factor_date=None,
    include_paiddown=False,
    field_list=[],
    collateral_report="",
):
    """
    Function to perform an AssetRequest on multiple securities using CMP service.

    Parameters:
      session: Bloomberg session object.
      service: Bloomberg service object.
      security_list: List of securities to query.
      factor_date: Optional factor date in YYYYMM format or a date object.
      include_paiddown: Boolean flag to include paid down assets.
      field_list: List of field names to retrieve or a comma-separated string of field names.
      collateral_report: Collateral report to run (CMBS only, only one report at a time)

    Returns:
      List of responses from the CMP service.
    """

    # Convert field_list to a comma-separated string if it is a list.
    if isinstance(field_list, list):
        formatted_field_list = ",".join(
            [field for field in field_list if field.strip()]
        )
    else:
        formatted_field_list = field_list

    # Ensure factor_date is in YYYYMM format.
    if factor_date is not None:
        if isinstance(factor_date, datetime):
            factor_date = int(factor_date.strftime("%Y%m"))
        elif isinstance(factor_date, str):
            try:
                factor_date = int(
                    datetime.strptime(factor_date, "%Y%m").strftime("%Y%m")
                )
            except ValueError:
                raise ValueError("factor_date should be in YYYYMM format.")
        elif isinstance(factor_date, int):
            pass
        else:
            raise ValueError("factor_date should be in YYYYMM format or a date object.")

    # Split collateral_report and ensure there no more than one report
    if collateral_report:
        reports = [
            report.strip()
            for report in re.split(r"[;,]", collateral_report)
            if report.strip()
        ]
        if len(reports) > 1:
            raise ValueError("Only one collateral report should be listed.")
        formatted_collateral_report = reports[0]
    else:
        formatted_collateral_report = ""

    # Build the list of request dictionaries.
    requests_json = []
    for sec in security_list:
        req = {
            "security": sec,
            "show_headers": "True",
            "operation": "Assets",
            "include_zero_balance": include_paiddown,
        }
        if factor_date is not None:
            req["factor_date"] = factor_date
        if formatted_field_list:
            req["fields"] = formatted_field_list
        if formatted_collateral_report:
            req["collateral_reports"] = formatted_collateral_report

        requests_json.append(req)

    responses = []
    dataframes = []
    df_agg = None

    for req in requests_json:
        try:
            response = make_request(req, service, session, parse_response=True)
            if len(formatted_collateral_report) > 0:
                data = response[formatted_collateral_report]
            else:
                data = response["assets"]
            df = table_to_dataframe(data)

            if df is None:
                print(
                    f"Failed to process request for security {req['security']}: No data returned."
                )
                continue
            else:
                dataframes.append(df)
        except Exception as e:
            print(f"Failed to process request for security {req['security']}: {e}")

    df_agg = stack_dataframes(dataframes)  # Stack the dataframes together

    return df_agg


# This will force pandas to display all columns of a dataframe(0ptional)
pd.set_option("display.max_columns", None)

# Apply CSS to limit the height of both code and output cells and
# make them scrollable. Applies to this Jupyter Notebook (optional).
display(
    HTML(
        """
    <style>
        /* Existing CSS rules */
        div.jp-Cell-inputWrapper {
            max-height: 600px;
            overflow: auto !important;
        }

        .jp-CodeMirror-editor {
            max-height: 600px;
            overflow-y: auto !important;
        }

        .CodeMirror-scroll {
            max-height: 600px !important;
            overflow-y: auto !important;
        }

        div.jp-OutputArea-output {
            max-height: 300px;
            overflow-y: auto !important;
            border: 1px solid #ccc;
            padding: 5px;
        }

        /* ✅ Prevent auto-scroll to the bottom */
        html, body {
            scroll-behavior: auto !important;
            overflow-anchor: none !important;
        }
    </style>
"""
    )
)
