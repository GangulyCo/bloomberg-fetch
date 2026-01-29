##########################################
# Multi-Security CMBS Collateral Reports Example
##########################################

import pandas as pd
from helpers import connect_to_cmp, run_AssetRequest

# Connect to Bloomberg CMP service
service, session = connect_to_cmp()

# Define request parameters
security_list = [
    "FNA 1997-M1",
    "CMAC 1996-C2",
    "BANK5 2024-5YR10",
    "FDICR 1996-C1",
    "ASFS 1996-FHA1",
    "GMACN 2004-POKA",
]
factor_date = None  # This will default to the latest factor date for each individual security. YYYYMM otherwise
include_paiddown = False
field_list = []  # This will default to all fields if left empty
collateral_report = "cmbspropertybulk"

# Fetch data using the helper function
df = run_AssetRequest(
    session,
    service,
    security_list,
    factor_date=factor_date,
    include_paiddown=include_paiddown,
    field_list=field_list,
    collateral_report="cmbsloanbulk",
)

# Display the first 5 rows
print(df.head(5))

# Write asset data to an excel file
with pd.ExcelWriter("asset_request.xlsx", engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Sheet1", index=False)

print("Data written to asset_request.xlsx")
