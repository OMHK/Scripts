import sys
import time
import logging
import os
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

# Configure file logging
file_handler = logging.FileHandler('file_changes.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.excel_extensions = os.getenv('FILE_PATTERNS', '.xlsx,.xls').split(',')
        self.last_file = None
        self.process_lock = False
        self.process_lock_timeout = int(os.getenv('PROCESS_LOCK_TIMEOUT', 2))
        self.retry_interval = int(os.getenv('RETRY_INTERVAL', 5))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        # Create or clear the last file log
        with open('last_file.log', 'w') as f:
            f.write('')

    def _is_excel_file(self, path):
        return any(path.lower().endswith(ext.lower()) for ext in self.excel_extensions)

    def get_last_file(self):
        return self.last_file

    def _process_files(self, abs_path, event_type):
        if not self.process_lock:
            self.process_lock = True
            try:
                # Save to last_file.log with timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                with open('last_file.log', 'w') as f:
                    f.write(f"{timestamp} - {event_type}: {abs_path}")
                
                # Wait for file to be fully written
                time.sleep(2)
                
                # Process Excel file
                logging.info(f"Processing Excel file: {abs_path}")
                subprocess.run([sys.executable, 'EXCEL_SEARCH_NON_TABULAR.py', abs_path], check=True)
                logging.info("Excel processing completed")
                
                # Upload to database
                logging.info("Uploading to database...")
                subprocess.run([sys.executable, 'db_uploader.py'], check=True)
                logging.info("Database upload completed")
                
            except subprocess.CalledProcessError as e:
                logging.error(f"Error processing file: {e}")
            finally:
                self.process_lock = False

    def on_created(self, event):
        if not event.is_directory and self._is_excel_file(event.src_path):
            abs_path = os.path.abspath(event.src_path)
            logging.info(f"New Excel file created: {abs_path}")
            self.last_file = abs_path
            self._process_files(abs_path, "Created")

    def on_modified(self, event):
        if not event.is_directory and self._is_excel_file(event.src_path):
            abs_path = os.path.abspath(event.src_path)
            logging.info(f"Excel file modified: {abs_path}")
            self.last_file = abs_path
            self._process_files(abs_path, "Modified")

    def on_deleted(self, event):
        if not event.is_directory and self._is_valid_extension(event.src_path):
            logging.info(f"File deleted: {event.src_path}")

def monitor_folder(path, extensions=None):
    # Create an observer and event handler with specified extensions
    event_handler = FileChangeHandler(extensions)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)

    # Start the observer
    observer.start()
    logging.info(f"Started monitoring folder: {path}")

    try:
        while True:
            # Keep the script running
            time.sleep(1)
            # You can get the last file at any time using:
            last_file = event_handler.get_last_file()
            if last_file:
                return last_file
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Monitoring stopped")
    finally:
        observer.stop()
        observer.join()
        return event_handler.get_last_file()

if __name__ == "__main__":
    # Get folder path from environment variables
    folder_path = os.getenv('WATCH_FOLDER')
    if not folder_path:
        logging.error("WATCH_FOLDER not set in .env file")
        sys.exit(1)
    
    # Specify the file extensions to monitor (you can modify this list)
    extensions_to_monitor = [
        '.xlsx',  # Excel files
        '.xls',   # Old Excel files
        '.csv',   # CSV files
        '.txt'    # Text files
    ]
    
    # Get the last modified/created file path
    last_file_path = monitor_folder(folder_path, extensions_to_monitor)
    if last_file_path:
        print(f"\nLast modified/created file: {last_file_path}")