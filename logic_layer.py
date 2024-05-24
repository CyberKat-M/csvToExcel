import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
from data_layer import store_dataframe

def process_files(client_id, asset_list_path, scan_report_path, ticket_results_path, database_uri):
    # Create a database engine
    engine = create_engine(database_uri)

    # Read the CSV files into Pandas DataFrames
    asset_list_df = pd.read_csv(asset_list_path)
    scan_report_df = pd.read_csv(scan_report_path)
    ticket_results_df = pd.read_csv(ticket_results_path)

    print("Asset List DataFrame:\n", asset_list_df.head())
    print("Scan Report DataFrame:\n", scan_report_df.head())
    print("Ticket Results DataFrame:\n", ticket_results_df.head())

    # Ensure DataFrames have necessary columns
    asset_list_df.columns = ['AssetID', 'HostID', 'AssetName']
    scan_report_df.columns = ['IP', 'Network', 'DNS']
    ticket_results_df.columns = ['TicketID', 'AutoTaskTicketNumber']

    # Store DataFrames in the SQL database
    store_dataframe('assets', asset_list_df, client_id, database_uri)
    store_dataframe('scans', scan_report_df, client_id, database_uri)
    store_dataframe('tickets', ticket_results_df, client_id, database_uri)

    # Adjust the join conditions to include all data
    combined_query = f'''
    SELECT asset_list.*, scan_report.IP, scan_report.Network, scan_report.DNS, ticket_results.TicketID, ticket_results.AutoTaskTicketNumber
    FROM assets as asset_list
    LEFT JOIN scans as scan_report ON asset_list.HostID = scan_report.Network
    LEFT JOIN tickets as ticket_results ON asset_list.AssetID = ticket_results.TicketID
    WHERE asset_list.client_id = {client_id}
    '''
    combined_df = pd.read_sql_query(combined_query, engine)

    print("Combined DataFrame:\n", combined_df.head())

    # Generate the Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        combined_df.to_excel(writer, index=False, sheet_name='CombinedData')

    output.seek(0)
    return output
