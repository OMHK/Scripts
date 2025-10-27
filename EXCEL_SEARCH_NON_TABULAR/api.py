from flask import Flask, request, jsonify
from flask_cors import CORS
from functools import wraps
import base64
import os
import json
import logging
import logging.config
from datetime import datetime
import tempfile
import pandas as pd
from dotenv import load_dotenv
from EXCEL_SEARCH_NON_TABULAR import (
    extract_fields, 
    get_cell_value, 
    find_next_unmerged
)

# Load environment variables
load_dotenv()

# Configure logging
with open('logging_config.json', 'r') as f:
    config = json.load(f)
    # Update log filenames for API
    config['handlers']['file']['filename'] = 'excel_api.log'
    config['handlers']['error_file']['filename'] = 'excel_api_error.log'
    logging.config.dictConfig(config)

logger = logging.getLogger('ExcelMonitor')

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        expected_key = os.getenv('API_KEY')
        
        if not api_key:
            logger.warning(f"Missing API key in request from {request.remote_addr}")
            return jsonify({
                'error': 'Missing API key',
                'detail': 'The x-api-key header is required'is
            }), 401
            
        if api_key != expected_key:
            logger.warning(
                f"Invalid API key attempt from {request.remote_addr}\n"
                f"Expected: {expected_key}\n"
                f"Received: {api_key}"
            )
            return jsonify({
                'error': 'Invalid API key',
                'detail': 'The provided API key does not match'
            }), 401
            
        logger.debug(f"Valid API key used from {request.remote_addr}")
        return f(*args, **kwargs)
    return decorated_function

def validate_request():
    """Validate request headers and body"""
    # Check Content-Type header
    if not request.headers.get('Content-Type', '').startswith('application/json'):
        logger.error("Invalid Content-Type header")
        return {'error': 'Content-Type must be application/json'}, 400

    # Validate request body
    data = request.get_json()
    if not data:
        logger.error("No JSON data in request")
        return {'error': 'Invalid JSON data'}, 400

    required_fields = ['filename', 'file_content']
    missing_fields = [field for field in required_fields if field not in data]
    
    if missing_fields:
        logger.error(f"Missing required fields: {missing_fields}")
        return {'error': f'Missing required fields: {missing_fields}'}, 400

    if not isinstance(data['filename'], str) or not isinstance(data['file_content'], str):
        logger.error("Invalid data types in request")
        return {'error': 'Invalid data types. Both filename and file_content must be strings'}, 400

    return None

def validate_request():
    """Validate request headers and body"""
    # Check Content-Type header
    if not request.headers.get('Content-Type', '').startswith('application/json'):
        logger.error("Invalid Content-Type header")
        return {'error': 'Content-Type must be application/json'}, 400

    # Validate request body
    data = request.get_json()
    if not data:
        logger.error("No JSON data in request")
        return {'error': 'Invalid JSON data'}, 400

    required_fields = ['filename', 'file_content']
    missing_fields = [field for field in required_fields if field not in data]
    
    if missing_fields:
        logger.error(f"Missing required fields: {missing_fields}")
        return {'error': f'Missing required fields: {missing_fields}'}, 400

    if not isinstance(data['filename'], str) or not isinstance(data['file_content'], str):
        logger.error("Invalid data types in request")
        return {'error': 'Invalid data types. Both filename and file_content must be strings'}, 400

    return None

@app.route('/process-excel', methods=['POST'])
@require_api_key
def process_excel():
    """Handle POST requests with Excel file content in base64 format"""
    logger.info("Received Excel processing request")
    
    try:
        # Validate request
        validation_error = validate_request()
        if validation_error:
            return jsonify(validation_error[0]), validation_error[1]
            
        # Get request data
        data = request.get_json()
        filename = data['filename']
        logger.info(f"Processing file: {filename}")

        # Decode base64 content
        try:
            file_content = base64.b64decode(data['file_content'])
            logger.debug("Successfully decoded base64 content")
        except Exception as e:
            logger.error(f"Failed to decode base64 content: {str(e)}")
            return jsonify({'error': f'Invalid base64 content: {str(e)}'}), 400

        # Create a temporary file to store the Excel content
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            temp_file.write(file_content)
            temp_path = temp_file.name
            logger.debug(f"Created temporary file: {temp_path}")

        try:
            # Load environment variables
            load_dotenv()
            logger.debug("Loaded environment variables")

            # Get fields from environment variables
            fields_to_extract = os.getenv('EXCEL_FIELDS', '').split(',')
            if not fields_to_extract or fields_to_extract[0] == '':
                fields_to_extract = [
                    "Avancement Financier :", 
                    "Montant total des attachements  :", 
                    "Montant du marché après avenants :", 
                    "Délai Consommé en jours :"
                ]
                logger.warning("Required fields not configured in environment, using defaults")
            else:
                logger.info(f"Using configured fields: {fields_to_extract}")

            # Process the Excel file using EXCEL_SEARCH_NON_TABULAR
            results = extract_fields(temp_path, fields_to_extract)
            
            # Check if any required fields were not found (have None values)
            missing_values = [field for field, value in results.items() if value is None]
            if missing_values:
                error_msg = f"Required fields not found in Excel file: {', '.join(missing_values)}"
                logger.error(error_msg)
                return jsonify({
                    'error': 'Missing required fields in Excel',
                    'detail': error_msg,
                    'missing_fields': missing_values,
                    'filename': filename
                }), 422  # Unprocessable Entity
            
            # Create DataFrame with timestamp
            results['Timestamp'] = datetime.now().isoformat()
            results['Filename'] = filename
            tbl = pd.DataFrame([results])
            
            # Define output file
            csv_filename = 'Output.csv'
            
            # Check if file exists to handle headers correctly
            file_exists = os.path.isfile(csv_filename)
            
            # Append to CSV, write headers only if file doesn't exist
            tbl.to_csv(csv_filename, 
                      mode='a' if file_exists else 'w',
                      header=not file_exists,
                      index=False, 
                      encoding='utf-8')
            
            logger.info(f"Appended results to {csv_filename}")

            # Upload to database
            try:
                from db_uploader import DatabaseUploader
                uploader = DatabaseUploader()
                try:
                    uploader.create_table()  # Ensure table exists
                    uploader.upload_csv_data(csv_filename)
                    logger.info("Data successfully uploaded to database")
                finally:
                    uploader.close()
            except Exception as db_error:
                logger.error(f"Database upload failed: {str(db_error)}", exc_info=True)
                # Continue processing even if database upload fails

            # Add timestamp to response
            response_data = {
                'timestamp': datetime.now().isoformat(),
                'results': results,
                'csv_file': csv_filename,
                'database_upload': 'success'
            }

            logger.info("Request processed successfully")
            return jsonify(response_data)

        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_path)
                logger.debug(f"Removed temporary file: {temp_path}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {temp_path}: {str(e)}")

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', 5000))
    logger.info(f"Starting API server on {host}:{port}")
    app.run(host=host, port=port, debug=False)