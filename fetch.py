##########################################
# Multi-Security CMBS Collateral Reports Example
##########################################

import pandas as pd
from helpers import connect_to_cmp, run_AssetRequest

# Connect to Bloomberg CMP service
service, session = connect_to_cmp()

# Read security list from Excel file
security_df = pd.read_excel("cmbs_deals.xlsx")
security_list = security_df["Deal"].tolist()  # Read 'Deal' column and convert to list

# Define request parameters
factor_date = None  # This will default to the latest factor date for each individual security. YYYYMM otherwise
include_paiddown = False
field_list = []  # This will default to all fields if left empty

# List of all collateral report types
collateral_reports = [
    'cmbsloanbulk',
    'cmbspropertybulk',
    'cmbspropertydetailfinancials',
    'cmbsleasebulk',
    'cmbsabdetails',
    'cmbsreservedetails'
]

# Loop through each collateral report type
for collateral_report in collateral_reports:
    print(f"\nFetching data for: {collateral_report}")
    
    # Fetch data using the helper function
    df = run_AssetRequest(
        session,
        service,
        security_list,
        factor_date=factor_date,
        include_paiddown=include_paiddown,
        field_list=field_list,
        collateral_report=collateral_report,
    )
    
    # Display the first 2 rows
    print(df.head(2))
    
    # Write asset data to a CSV file named after the collateral report
    output_filename = f"{collateral_report}.csv"
    df.to_csv(output_filename, index=False)
    
    print(f"Data written to {output_filename}")