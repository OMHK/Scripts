import win32serviceutil
import win32service
import win32event
import win32con
import servicemanager
import socket
import sys
import os
import time
import json
import logging
import logging.config
import subprocess
import signal
from pathlib import Path
from datetime import datetime
from typing import Optional

class ExcelMonitorService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ExcelMonitorService"
    _svc_display_name_ = "Excel Monitor and Database Uploader"
    _svc_description_ = "Monitors Excel files and uploads data to PostgreSQL database"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True
        self.process = None
        self.recovery_count = 0
        self.last_error_time = None
        self.setup_logging()
        
    def setup_logging(self):
        """Set up logging configuration"""
        try:
            script_dir = Path(__file__).parent
            config_path = script_dir / 'logging_config.json'
            
            with open(config_path) as f:
                config = json.load(f)
                
            # Ensure log directory exists
            log_dir = script_dir / 'logs'
            log_dir.mkdir(exist_ok=True)
            
            # Update log file paths to be absolute
            for handler in config['handlers'].values():
                if 'filename' in handler:
                    handler['filename'] = str(log_dir / handler['filename'])
            
            logging.config.dictConfig(config)
            self.logger = logging.getLogger('ExcelMonitor')
            self.logger.info('Logging initialized for Excel Monitor Service')

    def handle_error(self, error: Exception, context: str = ""):
        """Handle errors with progressive backoff and recovery"""
        current_time = datetime.now()
        
        # Reset recovery count if last error was more than 1 hour ago
        if self.last_error_time and (current_time - self.last_error_time).seconds > 3600:
            self.recovery_count = 0
            
        self.last_error_time = current_time
        self.recovery_count += 1
        
        # Calculate backoff time (exponential with max of 30 minutes)
        backoff = min(300 * (2 ** (self.recovery_count - 1)), 1800)
        
        self.logger.error(f"{context} Error: {str(error)}", exc_info=True)
        self.logger.info(f"Waiting {backoff} seconds before retry. Recovery attempt {self.recovery_count}")
        
        return backoff
        
    def start_monitor_process(self) -> Optional[subprocess.Popen]:
        """Start the monitor process with proper environment"""
        try:
            script_dir = Path(__file__).parent
            python_exe = str(script_dir / 'venv' / 'Scripts' / 'python.exe')
            monitor_script = str(script_dir / 'folder_monitor.py')
            
            # Create a state file to track service status
            state_file = script_dir / 'service_state.json'
            state = {
                'last_start': datetime.now().isoformat(),
                'pid': None,
                'status': 'starting'
            }
            
            # Start process
            process = subprocess.Popen(
                [python_exe, monitor_script],
                cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=win32con.CREATE_NO_WINDOW
            )
            
            # Update state file
            state['pid'] = process.pid
            state['status'] = 'running'
            with open(state_file, 'w') as f:
                json.dump(state, f)
            
            self.logger.info(f"Started monitor process with PID {process.pid}")
            return process
            
        except Exception as e:
            self.logger.error("Failed to start monitor process", exc_info=True)
            return None
    
    def SvcStop(self):
        """Stop the service gracefully"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.running = False
        win32event.SetEvent(self.stop_event)
        
        if self.process:
            self.logger.info("Stopping monitor process...")
            try:
                self.process.terminate()
                self.process.wait(timeout=10)
            except Exception as e:
                self.logger.error("Error stopping process", exc_info=True)
                try:
                    self.process.kill()
                except:
                    pass
                    
        self.logger.info("Service stopped")

    def SvcDoRun(self):
        """Main service run method with error handling and recovery"""
        try:
            self.logger.info("Service starting...")
            script_dir = Path(__file__).parent
            os.chdir(script_dir)
            
            while self.running:
                try:
                    if not self.process or self.process.poll() is not None:
                        self.process = self.start_monitor_process()
                        if not self.process:
                            time.sleep(self.handle_error(Exception("Failed to start process")))
                            continue
                            
                    # Check process status
                    exit_code = self.process.poll()
                    if exit_code is not None:
                        self.logger.warning(f"Process exited with code {exit_code}")
                        stderr = self.process.stderr.read().decode() if self.process.stderr else "No error output"
                        if stderr:
                            self.logger.error(f"Process error output: {stderr}")
                        time.sleep(self.handle_error(Exception(f"Process exited unexpectedly: {exit_code}")))
                    else:
                        # Process is running, reset recovery count
                        self.recovery_count = 0
                        time.sleep(5)  # Regular check interval
                        
                except Exception as e:
                    time.sleep(self.handle_error(e, "Process monitoring error"))
                    
        except Exception as e:
            self.logger.critical("Critical service error", exc_info=True)
            raise
            
if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ExcelMonitorService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(ExcelMonitorService)