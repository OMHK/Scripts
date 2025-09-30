import os
import glob
import logging
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# Load environment variables
load_dotenv()

# Configure logging from environment variables
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))
error_log_file = os.getenv('ERROR_LOG_FILE', 'excel_monitor_error.log')
log_max_size = int(os.getenv('LOG_MAX_SIZE', 10485760))
log_backup_count = int(os.getenv('LOG_BACKUP_COUNT', 5))

# Configure logging with rotating file handler
handler = RotatingFileHandler(
    error_log_file,
    maxBytes=log_max_size,
    backupCount=log_backup_count
)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(log_level)

# Load environment variables
load_dotenv()

class DatabaseUploader:
    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        """Establish database connection with certificate authentication"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                sslmode=os.getenv('DB_SSLMODE'),
                sslcert=os.getenv('DB_SSLCERT'),
                sslkey=os.getenv('DB_SSLKEY'),
                sslrootcert=os.getenv('DB_SSLROOTCERT')
            )
            print("Successfully connected to PostgreSQL database")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def create_table(self):
        """Create the table if it doesn't exist"""
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS project_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            avancement_financier NUMERIC,
            montant_attachements NUMERIC,
            montant_marche NUMERIC,
            delai_consomme INTEGER,
            file_source VARCHAR(255),
            data_hash VARCHAR(64),
            checksum VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_latest BOOLEAN DEFAULT TRUE
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_project_data_hash ON project_data(data_hash);
        CREATE INDEX IF NOT EXISTS idx_project_data_checksum ON project_data(checksum);
        CREATE INDEX IF NOT EXISTS idx_project_data_latest ON project_data(is_latest);
        '''
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                self.connection.commit()
                print("Table created successfully")
        except Exception as e:
            print(f"Error creating table: {e}")
            self.connection.rollback()

    def process_value(self, value):
        """Process and clean numeric values"""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            # Remove currency symbols, spaces, and convert commas to dots
            value = value.replace(' ', '').replace('€', '').replace(',', '.')
            # Extract first number if multiple exist
            value = value.split()[0]
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def generate_hashes(self, data):
        """Generate both content hash and checksum for the data"""
        import hashlib
        
        # Generate content hash (for exact duplicates)
        content_values = [
            str(data['avancement_financier']),
            str(data['montant_attachements']),
            str(data['montant_marche']),
            str(data['delai_consomme'])
        ]
        content_string = '|'.join(content_values)
        data_hash = hashlib.sha256(content_string.encode()).hexdigest()
        
        # Generate checksum (for near-duplicates)
        # Round numeric values to handle minor floating-point differences
        checksum_values = [
            str(round(float(data['avancement_financier'] or 0), 2)),
            str(round(float(data['montant_attachements'] or 0), 2)),
            str(round(float(data['montant_marche'] or 0), 2)),
            str(int(float(data['delai_consomme'] or 0)))
        ]
        checksum_string = '|'.join(checksum_values)
        checksum = hashlib.sha256(checksum_string.encode()).hexdigest()
        
        return data_hash, checksum

    def check_duplicates(self, data_hash, checksum):
        """Check for both exact and near duplicates"""
        query = """
        SELECT id, data_hash, checksum, timestamp 
        FROM project_data 
        WHERE (data_hash = %s OR checksum = %s)
        AND is_latest = TRUE
        ORDER BY created_at DESC 
        LIMIT 1
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, (data_hash, checksum))
            result = cursor.fetchone()
            return result

    def upload_csv_data(self, csv_path):
        """Upload data from CSV file to PostgreSQL"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Process the data
            data = {
                'timestamp': datetime.now(),
                'avancement_financier': self.process_value(df.get('Avancement Financier :', [None])[0]),
                'montant_attachements': self.process_value(df.get('Montant total des attachements  :', [None])[0]),
                'montant_marche': self.process_value(df.get('Montant du marché après avenants :', [None])[0]),
                'delai_consomme': self.process_value(df.get('Délai Consommé en jours :', [None])[0]),
                'file_source': os.path.basename(csv_path)
            }
            
            # Generate hashes for duplicate checking
            data['data_hash'], data['checksum'] = self.generate_hashes(data)
            
            # Check for duplicates
            duplicate = self.check_duplicates(data['data_hash'], data['checksum'])
            
            if duplicate:
                dup_id, dup_hash, dup_checksum, dup_timestamp = duplicate
                if dup_hash == data['data_hash']:
                    logging.info(f"Exact duplicate found (ID: {dup_id}). Skipping insert.")
                    return False
                elif dup_checksum == data['checksum']:
                    logging.warning(f"Similar record found (ID: {dup_id}, Timestamp: {dup_timestamp}). Check for potential duplicate entry.")

            # Update existing records to not be latest
            update_query = '''
            UPDATE project_data 
            SET is_latest = FALSE 
            WHERE checksum = %(checksum)s AND is_latest = TRUE;
            '''
            
            # Insert new record
            insert_query = '''
            INSERT INTO project_data 
            (timestamp, avancement_financier, montant_attachements, montant_marche, 
             delai_consomme, file_source, data_hash, checksum, is_latest)
            VALUES (
                %(timestamp)s, %(avancement_financier)s, %(montant_attachements)s, 
                %(montant_marche)s, %(delai_consomme)s, %(file_source)s, 
                %(data_hash)s, %(checksum)s, TRUE
            )
            RETURNING id;
            '''
            
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, data)
                self.connection.commit()
                print(f"Successfully uploaded data from {csv_path}")

        except Exception as e:
            print(f"Error uploading data from {csv_path}: {e}")
            self.connection.rollback()

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")

def main():
    # Initialize database uploader
    uploader = DatabaseUploader()
    
    try:
        # Create table if it doesn't exist
        uploader.create_table()
        
        # Find the most recent CSV file
        csv_files = glob.glob('extracted_data_*.csv')
        if not csv_files:
            print("No CSV files found")
            return
        
        latest_csv = max(csv_files, key=os.path.getctime)
        print(f"Processing latest CSV file: {latest_csv}")
        
        # Upload the data
        uploader.upload_csv_data(latest_csv)
        
    finally:
        uploader.close()

if __name__ == "__main__":
    main()