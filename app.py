from flask import Flask, request, render_template, redirect, url_for
import os
from logic_layer import process_files
from data_layer import get_clients, add_client
import config

app = Flask(__name__)

# Directory to store uploaded files temporarily
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def index():
    clients = get_clients(config.DATABASE_URI)
    return render_template('index.html', clients=clients)

@app.route('/upload', methods=['POST'])
def upload_files():
    client_id = request.form.get('client')
    new_client = request.form.get('new_client')

    if new_client:
        client_id = add_client(new_client, config.DATABASE_URI)
    elif not client_id:
        return "Client must be selected or created", 400

    asset_list = request.files['asset_list']
    scan_report = request.files['scan_report']
    ticket_results = request.files['ticket_results']

    # Save files to upload folder
    asset_list_path = os.path.join(UPLOAD_FOLDER, 'asset_list.csv')
    scan_report_path = os.path.join(UPLOAD_FOLDER, 'scan_report.csv')
    ticket_results_path = os.path.join(UPLOAD_FOLDER, 'ticket_results.csv')
    
    asset_list.save(asset_list_path)
    scan_report.save(scan_report_path)
    ticket_results.save(ticket_results_path)

    # Process the files and generate the Excel file
    excel_file = process_files(client_id, asset_list_path, scan_report_path, ticket_results_path, config.DATABASE_URI)

    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
