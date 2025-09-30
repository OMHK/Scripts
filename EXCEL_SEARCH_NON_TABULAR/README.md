# Excel File Monitor and Database Uploader

This system automatically monitors a folder for Excel files, extracts specific data fields, and uploads them to a PostgreSQL database. It's designed to run as a background service on both Windows and Linux systems.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [Python Environment Setup](#python-environment-setup)
  - [Database Setup](#database-setup)
  - [Service Installation](#service-installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Logging](#logging)
- [Troubleshooting](#troubleshooting)
- [Architecture](#architecture)

## Features

- Real-time folder monitoring for Excel files
- Automated data extraction from Excel files
- PostgreSQL database integration with SSL/TLS support
- Duplicate detection and handling
- Robust error handling and recovery
- Comprehensive logging system
- Runs as a system service (Windows/Linux)
- Automatic restart on failure
- Security features and resource management

## Prerequisites

- Python 3.12 or higher
- PostgreSQL 12 or higher
- Administrative privileges for service installation
- SSL certificates for PostgreSQL connection (if using certificate authentication)

Required Python packages (installed automatically):
```
openpyxl==3.1.5
pandas==2.3.2
Office365-REST-Python-Client==2.6.2
psycopg2-binary==2.9.10
python-dotenv==1.1.1
watchdog==6.0.0
numpy==2.3.3
pywin32==306 (Windows only)
```

## Installation

### Python Environment Setup

1. Clone or download this repository:
```bash
git clone <repository-url>
cd EXCEL_SEARCH_NON_TABULAR
```

2. Create and activate a virtual environment:

On Linux:
```bash
python -m venv venv
source venv/bin/activate
```

On Windows:
```cmd
python -m venv venv
venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

### Database Setup

1. Create a PostgreSQL database and user:
```sql
CREATE DATABASE your_database;
CREATE USER your_username WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE your_database TO your_username;
```

2. Configure database connection:
   - Copy the `.env.example` file to `.env`
   - Update the database credentials in `.env`:
```env
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_SSLMODE=verify-full
DB_SSLCERT=/path/to/client-cert.pem
DB_SSLKEY=/path/to/client-key.pem
DB_SSLROOTCERT=/path/to/server-ca.pem
```

### Service Installation

#### On Linux:

1. Update the service file with your username:
```bash
sed -i 's/your_username/actual_username/' excel-monitor.service
```

2. Install the service:
```bash
sudo cp excel-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo mkdir -p /var/log/excel_monitor
sudo chown $USER:$USER /var/log/excel_monitor
sudo systemctl enable excel-monitor
sudo systemctl start excel-monitor
```

#### On Windows:

1. Install as a Windows service:
```cmd
python windows_service.py install
python windows_service.py start
```

## Configuration

### Folder Monitoring
- Default monitored folder: `/home/omar/Downloads`
- To change the monitored folder:
  - Open `folder_monitor.py`
  - Update the `folder_path` variable

### Excel Field Extraction
- Configurable fields in `EXCEL_SEARCH_NON_TABULAR.py`:
```python
fields_to_extract = [
    "Avancement Financier :",
    "Montant total des attachements  :",
    "Montant du marché après avenants :",
    "Délai Consommé en jours :"
]
```

### Logging Configuration
- Log configuration in `logging_config.json`
- Default log rotation: 10MB file size, 5 backup files
- Separate logs for general info and errors

## Usage

### Monitoring Service Status

On Linux:
```bash
# Check service status
sudo systemctl status excel-monitor

# View logs
sudo journalctl -u excel-monitor -f
tail -f /var/log/excel_monitor/service.log

# Restart service
sudo systemctl restart excel-monitor
```

On Windows:
```cmd
# Check service status
sc query ExcelMonitorService

# View logs
type logs\excel_monitor.log
type logs\excel_monitor_error.log

# Restart service
python windows_service.py restart
```

### Manual Operation

For testing or debugging:
```bash
python folder_monitor.py
```

## Logging

Log files are stored in:
- Linux: `/var/log/excel_monitor/`
- Windows: `logs/` directory in the application folder

Log files:
- `service.log`: General operation logs
- `error.log`: Error and warning messages
- `file_changes.log`: File monitoring events

## Troubleshooting

Common issues and solutions:

1. Service won't start:
   - Check log files for specific errors
   - Verify database connection settings
   - Ensure proper permissions on directories

2. Database connection issues:
   - Verify SSL certificate paths and permissions
   - Check database credentials in .env
   - Ensure PostgreSQL is running

3. File monitoring issues:
   - Check folder permissions
   - Verify correct folder path configuration
   - Ensure Excel files are not locked

## Architecture

The system consists of three main components:

1. `folder_monitor.py`:
   - Watches for Excel file changes
   - Triggers processing pipeline
   - Handles file system events

2. `EXCEL_SEARCH_NON_TABULAR.py`:
   - Processes Excel files
   - Extracts specified fields
   - Generates CSV output

3. `db_uploader.py`:
   - Manages database connections
   - Handles data deduplication
   - Performs database updates

The service wrapper (`windows_service.py` or `excel-monitor.service`) manages:
- Process lifecycle
- Error recovery
- Resource management
- Logging
- Security