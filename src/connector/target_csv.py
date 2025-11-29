"""
target_csv.py
=============

Universal CSV target connector implementation.
Converts JSON data from transformers to CSV format and stores in LOCAL FILES.
Supports continuous writing with batching and frequent intervals.
OPTIMIZED FOR LOCAL FILE SYSTEM - Much more reliable for concurrent operations.
"""

import os
import json
import logging
import asyncio
import atexit
import threading
import csv
import io
import time
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timedelta
import concurrent.futures
from collections import defaultdict, deque
import threading
from dotenv import load_dotenv
import uuid
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Import base classes
try:
    from src.connector.base_target_connector import (
        BaseTargetConnector,
        BaseRateLimitHandler,
        ConnectorConfig,
        ConnectorResponse,
        convert_headers_to_dict,
        standardize_response_format
    )
except ImportError:
    # Fallback - define minimal base classes if base_target_connector isn't available
    class BaseRateLimitHandler:
        def is_rate_limited(self, response=None): 
            return False
        def get_retry_delay(self, response=None): 
            return 0
        def make_request_with_retry(self, operation_func, *args, **kwargs):
            return operation_func(*args, **kwargs)
    
    class BaseTargetConnector:
        def __init__(self, config):
            self.config = config
            self.rate_limiter = BaseRateLimitHandler()
    
    class ConnectorConfig:
        def __init__(self, domainUrl=None, api_key=None, **kwargs):
            # Handle required parameters for compatibility
            self.domainUrl = domainUrl
            self.api_key = api_key
            # Store all other kwargs as attributes
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    class ConnectorResponse:
        def __init__(self, status_code, success, data=None, error_message=None, **kwargs):
            self.status_code = status_code
            self.success = success
            self.data = data
            self.error_message = error_message
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    def convert_headers_to_dict(headers):
        if isinstance(headers, list):
            return {item["key"]: item["value"] for item in headers}
        return headers
    
    def standardize_response_format(response, headers):
        return {
            "status_code": response.status_code,
            "body": response.data if response.success else response.error_message,
            "headers": headers
        }


# ===================================================================
# LOCAL CSV TARGET CONFIGURATION
# ===================================================================

class LocalCSVTargetConfig(ConnectorConfig):
    """Configuration for Local CSV Target Connector"""
    
    def __init__(self, **kwargs):
        # Extract CSV-specific parameters before calling super()
        csv_params = {
            'output_directory': kwargs.pop('output_directory', './csv_exports'),
            'tickets_file_path': kwargs.pop('tickets_file_path', 'tickets.csv'),
            'conversations_file_path': kwargs.pop('conversations_file_path', 'conversations.csv'),
            'batch_size': kwargs.pop('batch_size', 100),
            'flush_interval_seconds': kwargs.pop('flush_interval_seconds', 30),
            'max_file_size_mb': kwargs.pop('max_file_size_mb', 50),
            'enable_file_rotation': kwargs.pop('enable_file_rotation', True),
            'date_format': kwargs.pop('date_format', '%Y-%m-%d %H:%M:%S'),
            'include_metadata': kwargs.pop('include_metadata', True),
            'create_directories': kwargs.pop('create_directories', True),
            'file_encoding': kwargs.pop('file_encoding', 'utf-8'),
            'backup_on_rotation': kwargs.pop('backup_on_rotation', True)
        }
        
        # Provide dummy values for required base class parameters
        kwargs.setdefault('domainUrl', 'local-csv-target')
        kwargs.setdefault('api_key', 'not-applicable')
        
        # Call parent with remaining kwargs
        super().__init__(**kwargs)
        
        # Set CSV-specific attributes
        for key, value in csv_params.items():
            setattr(self, key, value)
        
        # Ensure output directory exists
        if self.create_directories:
            Path(self.output_directory).mkdir(parents=True, exist_ok=True)


# ===================================================================
# LOCAL FILE RATE LIMIT HANDLER
# ===================================================================

class LocalFileRateLimitHandler:
    """Local file-specific rate limit handler (minimal since local I/O is fast)"""
    
    def __init__(self):
        self.request_times = deque()
        self.max_requests_per_second = 1000  # Local files can handle much higher throughput
        self.lock = threading.Lock()
    
    def is_rate_limited(self, response=None) -> bool:
        """Check if we should throttle requests"""
        with self.lock:
            self._cleanup_old_requests()
            return len(self.request_times) >= self.max_requests_per_second
    
    def get_retry_delay(self, response=None) -> int:
        """Get retry delay in seconds"""
        return 0.1  # Very short delay for local operations
    
    def _cleanup_old_requests(self):
        """Remove request timestamps older than 1 second"""
        current_time = time.time()
        while self.request_times and current_time - self.request_times[0] > 1.0:
            self.request_times.popleft()
    
    def _should_throttle(self) -> bool:
        """Check if we should throttle requests"""
        with self.lock:
            self._cleanup_old_requests()
            return len(self.request_times) >= self.max_requests_per_second
    
    def make_request_with_retry(self, operation_func, *args, **kwargs):
        """Execute operation with retry logic and throttling"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Minimal throttling for local files
                if self._should_throttle():
                    time.sleep(0.01)  # Very short sleep
                
                # Record request time
                with self.lock:
                    self.request_times.append(time.time())
                
                # Execute operation
                return operation_func(*args, **kwargs)
                
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                
                logging.warning(f"Local file operation failed, attempt {attempt + 1}/{max_retries}: {e}")
                time.sleep(0.1 * (attempt + 1))  # Short exponential backoff


# ===================================================================
# IMPROVED LOCAL CSV BUFFER AND MANAGEMENT
# ===================================================================

class LocalCSVBuffer:
    """Thread-safe CSV buffer for batching writes to local files"""
    
    def __init__(self, file_path: str, headers: List[str], max_size: int = 100):
        self.file_path = file_path
        self.headers = headers
        self.max_size = max_size
        self.records = []
        self.lock = threading.RLock()
        self.created_at = datetime.now()
        self.last_modified = datetime.now()
        self.total_added = 0
        self.total_flushed = 0
        self.buffer_id = uuid.uuid4().hex[:8]
    
    def add_record(self, record: Dict) -> bool:
        """Add record to buffer. Returns True if buffer should be flushed."""
        with self.lock:
            # Ensure record has all required headers
            normalized_record = {}
            for header in self.headers:
                normalized_record[header] = record.get(header, '')
            
            self.records.append(normalized_record)
            self.total_added += 1
            self.last_modified = datetime.now()
            
            logging.debug(f"Buffer {self.buffer_id}: Added record. Size: {len(self.records)}/{self.max_size}")
            
            return len(self.records) >= self.max_size
    
    def get_records_and_clear(self) -> List[Dict]:
        """Get all records and clear buffer"""
        with self.lock:
            records = self.records.copy()
            self.records.clear()
            self.total_flushed += len(records)
            
            if records:
                logging.info(f"Buffer {self.buffer_id}: Flushing {len(records)} records. Total flushed: {self.total_flushed}")
            
            return records
    
    def size(self) -> int:
        """Get current buffer size"""
        with self.lock:
            return len(self.records)
    
    def is_empty(self) -> bool:
        """Check if buffer is empty"""
        with self.lock:
            return len(self.records) == 0
    
    def age_seconds(self) -> float:
        """Get age of oldest record in seconds"""
        with self.lock:
            if not self.records:
                return 0
            return (datetime.now() - self.created_at).total_seconds()
    
    def get_stats(self) -> Dict:
        """Get buffer statistics"""
        with self.lock:
            return {
                'buffer_id': self.buffer_id,
                'current_size': len(self.records),
                'max_size': self.max_size,
                'total_added': self.total_added,
                'total_flushed': self.total_flushed,
                'age_seconds': self.age_seconds(),
                'file_path': self.file_path
            }


class LocalCSVFileManager:
    """Manages CSV file operations for local file system"""
    
    def __init__(self, config: LocalCSVTargetConfig, rate_limiter: LocalFileRateLimitHandler):
        self.config = config
        self.rate_limiter = rate_limiter
        self.file_headers = {}  # Track headers for each file
        self.file_sizes = {}  # Track file sizes for rotation
        self.file_locks = {}  # Per-file locks for write operations
        self.lock = threading.RLock()
        self._init_output_directory()
    
    def _init_output_directory(self):
        """Initialize output directory"""
        try:
            output_path = Path(self.config.output_directory)
            output_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"Local CSV output directory: {output_path.absolute()}")
        except Exception as e:
            logging.error(f"Failed to create output directory: {e}")
            raise
    
    def _get_file_lock(self, file_path: str) -> threading.RLock:
        """Get or create a lock for a specific file"""
        with self.lock:
            if file_path not in self.file_locks:
                self.file_locks[file_path] = threading.RLock()
            return self.file_locks[file_path]
    
    def _get_full_path(self, file_path: str) -> Path:
        """Get full path for file"""
        return Path(self.config.output_directory) / file_path
    
    def _generate_csv_content(self, records: List[Dict], headers: List[str], include_header: bool = True) -> str:
        """Generate CSV content from records"""
        if not records:
            return ""
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers, extrasaction='ignore')
        
        # Write header only if requested
        if include_header:
            writer.writeheader()
        
        for record in records:
            # Ensure all fields are strings and handle None values
            clean_record = {}
            for field in headers:
                value = record.get(field, '')
                if value is None:
                    clean_record[field] = ''
                elif isinstance(value, (dict, list)):
                    clean_record[field] = json.dumps(value)
                else:
                    clean_record[field] = str(value)
            
            writer.writerow(clean_record)
        
        return output.getvalue()
    
    def _get_rotated_filename(self, base_path: str) -> str:
        """Generate rotated filename if file is too large"""
        if not self.config.enable_file_rotation:
            return base_path
        
        full_path = self._get_full_path(base_path)
        
        try:
            current_size = full_path.stat().st_size if full_path.exists() else 0
        except:
            current_size = 0
        
        max_size_bytes = self.config.max_file_size_mb * 1024 * 1024
        
        if current_size < max_size_bytes:
            return base_path
        
        # Generate new filename with timestamp
        base_name = full_path.stem
        ext = full_path.suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Backup old file if configured
        if self.config.backup_on_rotation and full_path.exists():
            backup_name = f"{base_name}_{timestamp}_backup{ext}"
            backup_path = full_path.parent / backup_name
            try:
                full_path.rename(backup_path)
                logging.info(f"Backed up {full_path} to {backup_path}")
            except Exception as e:
                logging.warning(f"Failed to backup file: {e}")
        
        return base_path  # Return original path since we backed up the old file
    
    def _file_exists(self, file_path: str) -> bool:
        """Check if file exists"""
        return self._get_full_path(file_path).exists()
    
    def _get_file_size(self, file_path: str) -> int:
        """Get file size in bytes"""
        try:
            return self._get_full_path(file_path).stat().st_size
        except:
            return 0
    
    def _read_existing_headers(self, file_path: str) -> List[str]:
        """Read headers from existing CSV file"""
        try:
            full_path = self._get_full_path(file_path)
            
            if not full_path.exists():
                return []
            
            with open(full_path, 'r', encoding=self.config.file_encoding) as f:
                first_line = f.readline().strip()
                
            if not first_line:
                return []
            
            reader = csv.reader(io.StringIO(first_line))
            return next(reader, [])
            
        except Exception as e:
            logging.warning(f"Could not read headers from {file_path}: {e}")
            return []
    
    def write_records_to_file(self, file_path: str, records: List[Dict], headers: List[str]) -> bool:
        """Write records to CSV file in local file system"""
        if not records:
            logging.warning("No records to write")
            return True
        
        # Get file-specific lock to prevent concurrent writes to same file
        file_lock = self._get_file_lock(file_path)
        
        with file_lock:
            try:
                logging.info(f"Writing {len(records)} records to {file_path}")
                
                # Check for file rotation
                actual_file_path = self._get_rotated_filename(file_path)
                full_path = self._get_full_path(actual_file_path)
                
                # Check if file exists and get existing headers
                existing_headers = self._read_existing_headers(actual_file_path)
                file_exists = len(existing_headers) > 0
                
                # Merge headers if file exists
                if file_exists:
                    all_headers = existing_headers.copy()
                    for header in headers:
                        if header not in all_headers:
                            all_headers.append(header)
                    headers = all_headers
                    logging.debug(f"File exists, using merged headers: {len(headers)} columns")
                else:
                    logging.debug(f"New file, using provided headers: {len(headers)} columns")
                
                def write_operation():
                    # Ensure directory exists
                    full_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    if file_exists:
                        # Append to existing file without header
                        csv_content = self._generate_csv_content(records, headers, include_header=False)
                        
                        with open(full_path, 'a', encoding=self.config.file_encoding, newline='') as f:
                            f.write(csv_content)
                        
                        logging.info(f"Appended {len(records)} records to {full_path}")
                    else:
                        # Create new file with header
                        csv_content = self._generate_csv_content(records, headers, include_header=True)
                        
                        with open(full_path, 'w', encoding=self.config.file_encoding, newline='') as f:
                            f.write(csv_content)
                        
                        logging.info(f"Created new file {full_path} with {len(records)} records")
                    
                    return full_path.stat()
                
                # Execute write with retry
                file_stat = self.rate_limiter.make_request_with_retry(write_operation)
                
                # Update file size tracking
                self.file_sizes[actual_file_path] = file_stat.st_size
                self.file_headers[actual_file_path] = headers
                
                logging.info(f"Successfully wrote {len(records)} records to {full_path}. File size: {file_stat.st_size} bytes")
                return True
                
            except Exception as e:
                logging.error(f"Error writing records to {file_path}: {e}")
                import traceback
                logging.error(f"Full traceback: {traceback.format_exc()}")
                return False


# ===================================================================
# LOCAL BATCH WRITER SERVICE (SIMPLIFIED - NO BACKGROUND THREADS)
# ===================================================================

class LocalCSVBatchWriter:
    """Service for batched CSV writing with immediate flushing to local files"""
    
    def __init__(self, config: LocalCSVTargetConfig):
        self.config = config
        self.rate_limiter = LocalFileRateLimitHandler()
        self.file_manager = LocalCSVFileManager(config, self.rate_limiter)
        
        # Buffers for different data types
        self.ticket_buffer = None
        self.conversation_buffer = None
        
        # No background threads - use immediate flushing instead
        self.buffer_lock = threading.RLock()
        
        # Statistics
        self.stats = {
            'tickets_written': 0,
            'conversations_written': 0,
            'total_flushes': 0,
            'failed_writes': 0,
            'last_flush_time': None,
            'tickets_added': 0,
            'conversations_added': 0,
            'startup_time': datetime.now().isoformat()
        }
        self.stats_lock = threading.Lock()
        
        logging.info("Local CSV batch writer initialized (immediate flush mode)")
    
    def _should_flush_buffer(self, buffer) -> bool:
        """Check if buffer should be flushed based on size or age"""
        if not buffer or buffer.is_empty():
            return False
        
        # Flush if buffer is full
        if buffer.size() >= self.config.batch_size:
            return True
        
        # Flush if buffer is old enough
        if buffer.age_seconds() >= self.config.flush_interval_seconds:
            return True
        
        return False
    
    def _get_ticket_headers(self, record: Dict) -> List[str]:
        """Get standard headers for ticket CSV"""
        base_headers = [
            'id', 'number', 'subject', 'description', 'status', 'priority', 
            'severity', 'category', 'type', 'assignee', 'reporter', 
            'created_at', 'updated_at', 'resolved_at', 'closed_at', 'due_date'
        ]
        
        # Add custom fields from record
        custom_headers = []
        for key in record.keys():
            if key not in base_headers and not key.startswith('_'):
                custom_headers.append(key)
        
        # Add metadata headers if enabled
        metadata_headers = []
        if self.config.include_metadata:
            metadata_headers = ['_source', '_exported_at', '_batch_id']
        
        return base_headers + sorted(custom_headers) + metadata_headers
    
    def _get_conversation_headers(self, record: Dict) -> List[str]:
        """Get standard headers for conversation CSV"""
        base_headers = [
            'id', 'ticket_id', 'author', 'content', 'created_at', 'updated_at',
            'is_public', 'conversation_type', 'attachments'
        ]
        
        # Add custom fields from record
        custom_headers = []
        for key in record.keys():
            if key not in base_headers and not key.startswith('_'):
                custom_headers.append(key)
        
        # Add metadata headers if enabled
        metadata_headers = []
        if self.config.include_metadata:
            metadata_headers = ['_source', '_exported_at', '_batch_id']
        
        return base_headers + sorted(custom_headers) + metadata_headers
    
    def _add_metadata(self, record: Dict, record_type: str) -> Dict:
        """Add metadata to record if enabled"""
        if not self.config.include_metadata:
            return record
        
        enhanced_record = record.copy()
        enhanced_record['_source'] = record.get('_source', 'transformer')
        enhanced_record['_exported_at'] = datetime.now().strftime(self.config.date_format)
        enhanced_record['_batch_id'] = f"{record_type}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        return enhanced_record
    
    def add_ticket(self, ticket_data: Dict) -> bool:
        """Add ticket to buffer with immediate flushing when needed"""
        try:
            # Add metadata
            enhanced_ticket = self._add_metadata(ticket_data, 'ticket')
            
            with self.buffer_lock:
                # Initialize buffer if needed
                if self.ticket_buffer is None:
                    headers = self._get_ticket_headers(enhanced_ticket)
                    self.ticket_buffer = LocalCSVBuffer(
                        self.config.tickets_file_path, 
                        headers, 
                        self.config.batch_size
                    )
                    logging.info(f"Initialized ticket buffer with {len(headers)} headers")
                
                # Add to buffer
                self.ticket_buffer.add_record(enhanced_ticket)
                
                # Update stats
                with self.stats_lock:
                    self.stats['tickets_added'] += 1
                
                # Check if we should flush immediately
                should_flush = self._should_flush_buffer(self.ticket_buffer)
            
            # Flush immediately if needed (outside of buffer lock)
            if should_flush:
                logging.info("Ticket buffer needs flush, flushing immediately")
                self._flush_ticket_buffer()
            
            return True
            
        except Exception as e:
            logging.error(f"Error adding ticket to buffer: {e}")
            import traceback
            logging.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def add_conversation(self, conversation_data: Dict) -> bool:
        """Add conversation to buffer with immediate flushing when needed"""
        try:
            # Add metadata
            enhanced_conversation = self._add_metadata(conversation_data, 'conversation')
            
            with self.buffer_lock:
                # Initialize buffer if needed
                if self.conversation_buffer is None:
                    headers = self._get_conversation_headers(enhanced_conversation)
                    self.conversation_buffer = LocalCSVBuffer(
                        self.config.conversations_file_path, 
                        headers, 
                        self.config.batch_size
                    )
                    logging.info(f"Initialized conversation buffer with {len(headers)} headers")
                
                # Add to buffer
                self.conversation_buffer.add_record(enhanced_conversation)
                
                # Update stats
                with self.stats_lock:
                    self.stats['conversations_added'] += 1
                
                # Check if we should flush immediately
                should_flush = self._should_flush_buffer(self.conversation_buffer)
            
            # Flush immediately if needed (outside of buffer lock)
            if should_flush:
                logging.info("Conversation buffer needs flush, flushing immediately")
                self._flush_conversation_buffer()
            
            return True
            
        except Exception as e:
            logging.error(f"Error adding conversation to buffer: {e}")
            import traceback
            logging.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _flush_ticket_buffer(self):
        """Flush ticket buffer to file"""
        records_to_flush = []
        headers = []
        
        with self.buffer_lock:
            if self.ticket_buffer and not self.ticket_buffer.is_empty():
                records_to_flush = self.ticket_buffer.get_records_and_clear()
                headers = self.ticket_buffer.headers.copy()
        
        if records_to_flush:
            success = self.file_manager.write_records_to_file(
                self.config.tickets_file_path,
                records_to_flush,
                headers
            )
            
            with self.stats_lock:
                if success:
                    self.stats['tickets_written'] += len(records_to_flush)
                    self.stats['total_flushes'] += 1
                    self.stats['last_flush_time'] = datetime.now().isoformat()
                    logging.info(f"Successfully flushed {len(records_to_flush)} tickets to local CSV")
                else:
                    self.stats['failed_writes'] += 1
                    logging.error(f"Failed to flush {len(records_to_flush)} tickets")
    
    def _flush_conversation_buffer(self):
        """Flush conversation buffer to file"""
        records_to_flush = []
        headers = []
        
        with self.buffer_lock:
            if self.conversation_buffer and not self.conversation_buffer.is_empty():
                records_to_flush = self.conversation_buffer.get_records_and_clear()
                headers = self.conversation_buffer.headers.copy()
        
        if records_to_flush:
            success = self.file_manager.write_records_to_file(
                self.config.conversations_file_path,
                records_to_flush,
                headers
            )
            
            with self.stats_lock:
                if success:
                    self.stats['conversations_written'] += len(records_to_flush)
                    self.stats['total_flushes'] += 1
                    self.stats['last_flush_time'] = datetime.now().isoformat()
                    logging.info(f"Successfully flushed {len(records_to_flush)} conversations to local CSV")
                else:
                    self.stats['failed_writes'] += 1
                    logging.error(f"Failed to flush {len(records_to_flush)} conversations")
    
    def force_flush(self):
        """Force immediate flush of all buffers"""
        logging.info("Force flushing all local CSV buffers")
        self._flush_ticket_buffer()
        self._flush_conversation_buffer()
    
    def stop(self):
        """Stop the batch writer and flush remaining data"""
        logging.info("Stopping local CSV batch writer...")
        
        # Final flush
        self.force_flush()
        
        logging.info("Local CSV batch writer stopped")
    
    def get_statistics(self) -> Dict:
        """Get current statistics"""
        with self.stats_lock:
            stats = self.stats.copy()
        
        with self.buffer_lock:
            stats['tickets_buffered'] = self.ticket_buffer.size() if self.ticket_buffer else 0
            stats['conversations_buffered'] = self.conversation_buffer.size() if self.conversation_buffer else 0
            
            # Add buffer statistics
            if self.ticket_buffer:
                stats['ticket_buffer_stats'] = self.ticket_buffer.get_stats()
            if self.conversation_buffer:
                stats['conversation_buffer_stats'] = self.conversation_buffer.get_stats()
        
        # Add file system info
        try:
            output_path = Path(self.config.output_directory)
            if output_path.exists():
                stats['output_directory'] = str(output_path.absolute())
                stats['tickets_file_exists'] = (output_path / self.config.tickets_file_path).exists()
                stats['conversations_file_exists'] = (output_path / self.config.conversations_file_path).exists()
                
                # File sizes
                tickets_path = output_path / self.config.tickets_file_path
                conversations_path = output_path / self.config.conversations_file_path
                
                stats['tickets_file_size'] = tickets_path.stat().st_size if tickets_path.exists() else 0
                stats['conversations_file_size'] = conversations_path.stat().st_size if conversations_path.exists() else 0
        except Exception as e:
            stats['file_system_error'] = str(e)
        
        return stats


# ===================================================================
# MAIN LOCAL CSV TARGET CONNECTOR
# ===================================================================

class LocalCSVTargetConnector:
    """
    Local CSV target connector.
    Converts JSON data to CSV and stores in local files with batching.
    Uses a shared global batch writer to prevent thread creation issues.
    """
    
    def __init__(self, config: LocalCSVTargetConfig):
        self.config = config
        # Use shared global batch writer instead of creating new one
        self.batch_writer = get_or_create_local_batch_writer(config)
        self.rate_limiter = LocalFileRateLimitHandler()
    
    def _validate_instance(self) -> bool:
        """Validate local CSV target configuration"""
        try:
            # Test local directory access
            output_path = Path(self.config.output_directory)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Test write permissions
            test_file = output_path / ".test_write_permission"
            test_file.write_text("test")
            test_file.unlink()
            
            return True
        except Exception as e:
            logging.error(f"Local CSV target validation failed: {e}")
            return False
    
    async def create_ticket_csv(self, ticket_data: Dict, headers: Dict) -> ConnectorResponse:
        """
        Create/append ticket to local CSV file.
        Supports batching and automatic flushing.
        """
        try:
            # Normalize ticket data
            normalized_ticket = self._normalize_ticket_data(ticket_data)
            
            # Add to batch writer
            success = self.batch_writer.add_ticket(normalized_ticket)
            
            if success:
                return ConnectorResponse(
                    status_code=200,
                    success=True,
                    data={
                        'message': 'Ticket added to local CSV batch',
                        'ticket_id': normalized_ticket.get('id', 'unknown'),
                        'file_path': self.config.tickets_file_path,
                        'output_directory': self.config.output_directory
                    }
                )
            else:
                return ConnectorResponse(
                    status_code=500,
                    success=False,
                    error_message='Failed to add ticket to local CSV batch'
                )
            
        except Exception as e:
            logging.error(f"Error in create_ticket_csv: {e}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    async def create_conversation_csv(self, conversation_data: Dict, headers: Dict, 
                                    ticket_id: Optional[str] = None) -> ConnectorResponse:
        """
        Create/append conversation to local CSV file.
        Supports batching and automatic flushing.
        """
        try:
            # Normalize conversation data
            normalized_conversation = self._normalize_conversation_data(conversation_data, ticket_id)
            
            # Add to batch writer
            success = self.batch_writer.add_conversation(normalized_conversation)
            
            if success:
                return ConnectorResponse(
                    status_code=200,
                    success=True,
                    data={
                        'message': 'Conversation added to local CSV batch',
                        'conversation_id': normalized_conversation.get('id', 'unknown'),
                        'ticket_id': normalized_conversation.get('ticket_id', 'unknown'),
                        'file_path': self.config.conversations_file_path,
                        'output_directory': self.config.output_directory
                    }
                )
            else:
                return ConnectorResponse(
                    status_code=500,
                    success=False,
                    error_message='Failed to add conversation to local CSV batch'
                )
            
        except Exception as e:
            logging.error(f"Error in create_conversation_csv: {e}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    def _normalize_ticket_data(self, ticket_data: Dict) -> Dict:
        """Normalize ticket data for CSV export"""
        normalized = {}
        
        # Map common fields
        field_mapping = {
            'id': ['id', 'ticket_id', 'ticketid'],
            'number': ['number', 'ticket_number'],
            'subject': ['subject', 'title'],
            'description': ['description', 'body'],
            'status': ['status', 'state'],
            'priority': ['priority'],
            'severity': ['severity'],
            'category': ['category'],
            'type': ['type'],
            'assignee': ['assignee', 'assigned_to'],
            'reporter': ['reporter', 'created_by', 'requester'],
            'created_at': ['created_at', 'created_date'],
            'updated_at': ['updated_at', 'updated_date'],
            'resolved_at': ['resolved_at', 'resolved_date'],
            'closed_at': ['closed_at', 'closed_date'],
            'due_date': ['due_date', 'due_at']
        }
        
        # Apply field mapping
        for std_field, possible_fields in field_mapping.items():
            for field in possible_fields:
                if field in ticket_data:
                    normalized[std_field] = ticket_data[field]
                    break
        
        # Add unmapped fields as custom fields
        mapped_fields = set()
        for fields_list in field_mapping.values():
            mapped_fields.update(fields_list)
        
        for field, value in ticket_data.items():
            if field not in mapped_fields and not field.startswith('_'):
                normalized[field] = value
        
        # Handle attachments
        if 'attachments' in ticket_data:
            attachments = ticket_data['attachments']
            if isinstance(attachments, list):
                normalized['attachments'] = json.dumps(attachments)
            else:
                normalized['attachments'] = str(attachments)
        
        # Ensure dates are properly formatted
        date_fields = ['created_at', 'updated_at', 'resolved_at', 'closed_at', 'due_date']
        for field in date_fields:
            if field in normalized and normalized[field]:
                normalized[field] = self._normalize_date(normalized[field])
        
        return normalized
    
    def _normalize_conversation_data(self, conversation_data: Dict, ticket_id: Optional[str] = None) -> Dict:
        """Normalize conversation data for CSV export"""
        normalized = {}
        
        # Map common fields
        field_mapping = {
            'id': ['id', 'conversation_id', 'note_id'],
            'ticket_id': ['ticket_id', 'ticketid', 'parent_id'],
            'author': ['author', 'created_by', 'user'],
            'content': ['content', 'body', 'message'],
            'created_at': ['created_at', 'timestamp', 'date'],
            'updated_at': ['updated_at', 'modified_at'],
            'is_public': ['is_public', 'public', 'private'],
            'conversation_type': ['conversation_type', 'type', 'note_type']
        }
        
        # Apply field mapping
        for std_field, possible_fields in field_mapping.items():
            for field in possible_fields:
                if field in conversation_data:
                    normalized[std_field] = conversation_data[field]
                    break
        
        # Set ticket_id if provided
        if ticket_id:
            normalized['ticket_id'] = ticket_id
        
        # Add unmapped fields
        mapped_fields = set()
        for fields_list in field_mapping.values():
            mapped_fields.update(fields_list)
        
        for field, value in conversation_data.items():
            if field not in mapped_fields and not field.startswith('_'):
                normalized[field] = value
        
        # Handle attachments
        if 'attachments' in conversation_data:
            attachments = conversation_data['attachments']
            if isinstance(attachments, list):
                normalized['attachments'] = json.dumps(attachments)
            else:
                normalized['attachments'] = str(attachments)
        
        # Handle is_public field
        if 'is_public' in normalized:
            value = normalized['is_public']
            if isinstance(value, str):
                normalized['is_public'] = value.lower() in ['true', '1', 'yes', 'public']
            elif value == 'private':
                normalized['is_public'] = False
        
        # Ensure dates are properly formatted
        date_fields = ['created_at', 'updated_at']
        for field in date_fields:
            if field in normalized and normalized[field]:
                normalized[field] = self._normalize_date(normalized[field])
        
        return normalized
    
    def _normalize_date(self, date_value: Any) -> str:
        """Normalize date to consistent format"""
        if not date_value:
            return ''
        
        if isinstance(date_value, datetime):
            return date_value.strftime(self.config.date_format)
        
        if isinstance(date_value, str):
            try:
                if 'T' in date_value:
                    parsed_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                    return parsed_date.strftime(self.config.date_format)
                else:
                    return date_value
            except:
                return str(date_value)
        
        return str(date_value)
    
    def force_flush_all(self):
        """Force flush all pending data"""
        self.batch_writer.force_flush()
    
    def get_statistics(self) -> Dict:
        """Get connector statistics"""
        return self.batch_writer.get_statistics()
    
    def stop(self):
        """Stop the connector and flush all data"""
        self.batch_writer.stop()


# ===================================================================
# GLOBAL BATCH WRITER MANAGEMENT FOR LOCAL FILES (SINGLETON PATTERN)
# ===================================================================

# Single global batch writer instance that gets reused
_global_local_batch_writer: Optional[LocalCSVBatchWriter] = None
_local_batch_writer_lock = threading.RLock()

def get_or_create_local_batch_writer(config: LocalCSVTargetConfig) -> LocalCSVBatchWriter:
    """Get or create THE SINGLE global batch writer for local files"""
    global _global_local_batch_writer
    
    with _local_batch_writer_lock:
        if _global_local_batch_writer is None:
            _global_local_batch_writer = LocalCSVBatchWriter(config)
            logging.info("Created THE global local CSV batch writer (singleton)")
        else:
            # Update config if needed
            _global_local_batch_writer.config = config
        return _global_local_batch_writer

def cleanup_local_batch_writers():
    """Cleanup function for global local batch writer"""
    global _global_local_batch_writer
    
    with _local_batch_writer_lock:
        if _global_local_batch_writer:
            try:
                _global_local_batch_writer.stop()
                logging.info("Cleaned up global local CSV batch writer")
            except Exception as e:
                logging.error(f"Error cleaning up local batch writer: {e}")
            finally:
                _global_local_batch_writer = None

# Register cleanup function
atexit.register(cleanup_local_batch_writers)


# ===================================================================
# ASYNC FUNCTIONS FOR FASTAPI (LOCAL VERSION)
# ===================================================================

async def create_csv_ticket_async(body: Dict, headers) -> Dict:
    """Async version of local CSV ticket creation with enhanced debugging."""
    try:
        headers_dict = convert_headers_to_dict(headers)
        
        # Get configuration from headers or use defaults
        output_directory = headers_dict.get('output_directory', './csv_exports')
        
        # Debug: Print current working directory and intended output
        import os
        current_dir = os.getcwd()
        intended_path = os.path.abspath(output_directory)
        
        logging.info(f"=== CSV TICKET DEBUG ===")
        logging.info(f"Current working directory: {current_dir}")
        logging.info(f"Intended output directory: {intended_path}")
        logging.info(f"Headers received: {headers_dict}")
        logging.info(f"Body received: {body}")
        
        # Create connector configuration for local files
        config = LocalCSVTargetConfig(
            output_directory=output_directory,
            tickets_file_path=headers_dict.get('tickets_file_path', 'tickets.csv'),
            conversations_file_path=headers_dict.get('conversations_file_path', 'conversations.csv'),
            batch_size=int(headers_dict.get('batch_size', 10)),  # Small batch for immediate writing
            flush_interval_seconds=int(headers_dict.get('flush_interval_seconds', 5)),  # Quick flush
            max_file_size_mb=int(headers_dict.get('max_file_size_mb', 50)),
            enable_file_rotation=headers_dict.get('enable_file_rotation', 'true').lower() == 'true',
            include_metadata=headers_dict.get('include_metadata', 'true').lower() == 'true'
        )
        
        logging.info(f"Config created - Output dir: {config.output_directory}")
        logging.info(f"Config - Tickets file: {config.tickets_file_path}")
        logging.info(f"Config - Batch size: {config.batch_size}")
        
        # Ensure directory exists and log the result
        from pathlib import Path
        output_path = Path(config.output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Directory created/verified: {output_path.absolute()}")
        logging.info(f"Directory exists: {output_path.exists()}")
        logging.info(f"Directory is writable: {os.access(output_path, os.W_OK)}")
        
        # Create connector
        connector = LocalCSVTargetConnector(config)
        logging.info("Local CSV connector created successfully")
        
        # Create ticket
        response = await connector.create_ticket_csv(body, headers)
        logging.info(f"Response: {response.success}, Data: {response.data}")
        
        # Force flush to ensure immediate write
        connector.force_flush_all()
        logging.info("Force flush completed")
        
        # Check if file was actually created
        tickets_file = output_path / config.tickets_file_path
        logging.info(f"Checking for file: {tickets_file.absolute()}")
        logging.info(f"File exists: {tickets_file.exists()}")
        if tickets_file.exists():
            file_size = tickets_file.stat().st_size
            logging.info(f"File size: {file_size} bytes")
            
            # Read and log file contents for debugging
            try:
                with open(tickets_file, 'r') as f:
                    content = f.read()
                logging.info(f"File content preview (first 500 chars): {content[:500]}")
            except Exception as e:
                logging.error(f"Could not read file: {e}")
        
        logging.info("=== END CSV TICKET DEBUG ===")
        
        return standardize_response_format(response, headers)
        
    except Exception as e:
        logging.error(f"Error in create_csv_ticket_async: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }

async def create_csv_conversation_async(body: Dict, headers) -> Dict:
    """Async version of local CSV conversation creation with enhanced debugging."""
    try:
        headers_dict = convert_headers_to_dict(headers)
        
        # Get configuration from headers or use defaults
        output_directory = headers_dict.get('output_directory', './csv_exports')
        
        # Debug logging
        import os
        current_dir = os.getcwd()
        intended_path = os.path.abspath(output_directory)
        
        logging.info(f"=== CSV CONVERSATION DEBUG ===")
        logging.info(f"Current working directory: {current_dir}")
        logging.info(f"Intended output directory: {intended_path}")
        logging.info(f"Headers received: {headers_dict}")
        logging.info(f"Body received: {body}")
        
        # Extract ticket_id from body if present
        ticket_id = body.get('ticket_id') or body.get('ticketid') or body.get('parent_id')
        logging.info(f"Extracted ticket_id: {ticket_id}")
        
        # Create connector configuration for local files
        config = LocalCSVTargetConfig(
            output_directory=output_directory,
            tickets_file_path=headers_dict.get('tickets_file_path', 'tickets.csv'),
            conversations_file_path=headers_dict.get('conversations_file_path', 'conversations.csv'),
            batch_size=int(headers_dict.get('batch_size', 10)),  # Small batch for immediate writing
            flush_interval_seconds=int(headers_dict.get('flush_interval_seconds', 5)),  # Quick flush
            max_file_size_mb=int(headers_dict.get('max_file_size_mb', 50)),
            enable_file_rotation=headers_dict.get('enable_file_rotation', 'true').lower() == 'true',
            include_metadata=headers_dict.get('include_metadata', 'true').lower() == 'true'
        )
        
        logging.info(f"Config created - Output dir: {config.output_directory}")
        logging.info(f"Config - Conversations file: {config.conversations_file_path}")
        
        # Ensure directory exists
        from pathlib import Path
        output_path = Path(config.output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Directory created/verified: {output_path.absolute()}")
        
        # Create connector
        connector = LocalCSVTargetConnector(config)
        
        # Create conversation
        response = await connector.create_conversation_csv(body, headers, ticket_id)
        logging.info(f"Response: {response.success}, Data: {response.data}")
        
        # Force flush to ensure immediate write
        connector.force_flush_all()
        logging.info("Force flush completed")
        
        # Check if file was actually created
        conversations_file = output_path / config.conversations_file_path
        logging.info(f"Checking for file: {conversations_file.absolute()}")
        logging.info(f"File exists: {conversations_file.exists()}")
        if conversations_file.exists():
            file_size = conversations_file.stat().st_size
            logging.info(f"File size: {file_size} bytes")
            
            # Read and log file contents for debugging
            try:
                with open(conversations_file, 'r') as f:
                    content = f.read()
                logging.info(f"File content preview (first 500 chars): {content[:500]}")
            except Exception as e:
                logging.error(f"Could not read file: {e}")
        
        logging.info("=== END CSV CONVERSATION DEBUG ===")
        
        return standardize_response_format(response, headers)
        
    except Exception as e:
        logging.error(f"Error in create_csv_conversation_async: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


# ===================================================================
# SYNCHRONOUS WRAPPER FUNCTIONS
# ===================================================================

def create_csv_ticket_universal(body: Dict, headers) -> Dict:
    """Universal local CSV ticket creation."""
    try:
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_csv_ticket_async(body, headers))
                )
                return future.result()
        except RuntimeError:
            return asyncio.run(create_csv_ticket_async(body, headers))
            
    except Exception as e:
        logging.error(f"Error in create_csv_ticket_universal: {e}")
        return {
            "status_code": 500,
            "body": str(e),
        }

def create_csv_conversation_universal(body: Dict, headers) -> Dict:
    """Universal local CSV conversation creation."""
    try:
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_csv_conversation_async(body, headers))
                )
                return future.result()
        except RuntimeError:
            return asyncio.run(create_csv_conversation_async(body, headers))
            
    except Exception as e:
        logging.error(f"Error in create_csv_conversation_universal: {e}")
        return {
            "status_code": 500,
            "body": str(e),
        }


# ===================================================================
# BACKWARD COMPATIBILITY FUNCTIONS
# ===================================================================

def create_csv_ticket(**kwargs) -> Dict:
    """Backward compatible ticket creation function."""
    try:
        body = kwargs.get('body', {})
        headers = kwargs.get('targetHeaders', [])
        
        if not body:
            logging.error("No body provided to create_csv_ticket")
            return {
                "status_code": 400,
                "body": "Request body is required"
            }
        
        logging.info(f"create_csv_ticket called with ticket ID: {body.get('id', 'unknown')}")
        return create_csv_ticket_universal(body, headers)
        
    except Exception as e:
        logging.error(f"Error in create_csv_ticket backward compatibility: {e}")
        return {
            "status_code": 500,
            "body": str(e)
        }

def create_csv_conversation(**kwargs) -> Dict:
    """Backward compatible conversation creation function."""
    try:
        body = kwargs.get('body', {})
        headers = kwargs.get('targetHeaders', [])
        query_params = kwargs.get('queryParams', [])
        
        if not body:
            logging.error("No body provided to create_csv_conversation")
            return {
                "status_code": 400,
                "body": "Request body is required"
            }
        
        # Extract ticket_id from query params if provided
        ticket_id = None
        if query_params:
            for param in query_params:
                if param.get('key') == 'ticket_id':
                    ticket_id = param.get('value')
                    break
        
        # If ticket_id found in query params, add it to body
        if ticket_id and 'ticket_id' not in body:
            body['ticket_id'] = ticket_id
        
        logging.info(f"create_csv_conversation called for ticket: {body.get('ticket_id', 'unknown')}")
        return create_csv_conversation_universal(body, headers)
        
    except Exception as e:
        logging.error(f"Error in create_csv_conversation backward compatibility: {e}")
        return {
            "status_code": 500,
            "body": str(e)
        }

def create_csv_conversations(**kwargs) -> Dict:
    """Alias for create_csv_conversation (handles both singular and plural)"""
    return create_csv_conversation(**kwargs)


# ===================================================================
# UTILITY AND MANAGEMENT FUNCTIONS
# ===================================================================

def force_flush_csv_data(headers) -> Dict:
    """Force flush all pending local CSV data"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        output_directory = headers_dict.get('output_directory', './csv_exports')
        
        config = LocalCSVTargetConfig(output_directory=output_directory)
        batch_writer = get_or_create_local_batch_writer(config)
        batch_writer.force_flush()
        
        logging.info("Local CSV data force flush completed successfully")
        
        return {
            "status_code": 200,
            "body": {"message": "Local CSV data flushed successfully"},
            "headers": headers
        }
        
    except Exception as e:
        logging.error(f"Error flushing local CSV data: {e}")
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }

def get_csv_statistics(headers) -> Dict:
    """Get local CSV connector statistics"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        output_directory = headers_dict.get('output_directory', './csv_exports')
        
        config = LocalCSVTargetConfig(output_directory=output_directory)
        batch_writer = get_or_create_local_batch_writer(config)
        stats = batch_writer.get_statistics()
        
        # Add summary information
        stats['summary'] = {
            'tickets_pending': stats.get('tickets_buffered', 0),
            'conversations_pending': stats.get('conversations_buffered', 0),
            'total_pending': stats.get('tickets_buffered', 0) + stats.get('conversations_buffered', 0),
            'write_success_rate': (
                (stats.get('tickets_written', 0) + stats.get('conversations_written', 0)) / 
                max(1, stats.get('tickets_added', 0) + stats.get('conversations_added', 0))
            ) * 100 if (stats.get('tickets_added', 0) + stats.get('conversations_added', 0)) > 0 else 100
        }
        
        return {
            "status_code": 200,
            "body": stats,
            "headers": headers
        }
        
    except Exception as e:
        logging.error(f"Error getting local CSV statistics: {e}")
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }

def validate_csv_target_instance_v1(headers) -> Dict:
    """Validate local CSV target instance"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        output_directory = headers_dict.get('output_directory', './csv_exports')
        
        config = LocalCSVTargetConfig(output_directory=output_directory)
        connector = LocalCSVTargetConnector(config)
        is_valid = connector._validate_instance()
        
        return {
            "status_code": 200 if is_valid else 401,
            "body": {
                "valid": is_valid,
                "output_directory": output_directory,
                "message": "Local file system access successful" if is_valid else "Local file system access failed"
            },
            "headers": headers
        }
    except Exception as e:
        logging.error(f"Error validating local CSV target instance: {e}")
        return {
            "status_code": 500,
            "body": {"valid": False, "error": str(e)}
        }


# ===================================================================
# SIMPLE TEST FUNCTION TO VERIFY CSV WRITING
# ===================================================================

def test_csv_writing_simple(output_dir=None):
    """Simple test function to verify CSV writing is working"""
    import tempfile
    import json
    import os
    
    # Configure logging to see what's happening
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("=== SIMPLE CSV WRITING TEST ===")
    
    # Use specified directory or create a temporary one
    if output_dir:
        # Use specified directory
        test_dir = output_dir
        print(f"Using specified directory: {test_dir}")
        
        # Create directory if it doesn't exist
        from pathlib import Path
        Path(test_dir).mkdir(parents=True, exist_ok=True)
        
        # Test the function
        _run_csv_test(test_dir)
        
    else:
        # Use a temporary directory that we know exists and is writable
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"Using temporary directory: {temp_dir}")
            _run_csv_test(temp_dir)

def _run_csv_test(test_dir):
    """Run the actual CSV test in the specified directory"""
    import json
    
    # Simple headers
    headers = [
        {"key": "output_directory", "value": test_dir},
        {"key": "tickets_file_path", "value": "test_tickets.csv"},
        {"key": "conversations_file_path", "value": "test_conversations.csv"},
        {"key": "batch_size", "value": "1"},  # Write immediately
        {"key": "flush_interval_seconds", "value": "1"}
    ]
    
    # Simple test data
    ticket_data = {
        "id": "test_ticket_123",
        "subject": "Test Ticket Subject",
        "description": "This is a test ticket description",
        "status": "Open",
        "priority": "High",
        "created_at": "2024-01-15T10:30:00Z"
    }
    
    conversation_data = {
        "id": "test_conv_456",
        "ticket_id": "test_ticket_123",
        "author": "test.user@example.com",
        "content": "This is a test conversation",
        "created_at": "2024-01-15T11:00:00Z",
        "is_public": True
    }
    
    try:
        # Test ticket creation
        print("\n1. Testing ticket creation...")
        ticket_result = create_csv_ticket(body=ticket_data, targetHeaders=headers)
        print(f"Ticket result: {json.dumps(ticket_result, indent=2)}")
        
        # Test conversation creation
        print("\n2. Testing conversation creation...")
        conv_result = create_csv_conversation(body=conversation_data, targetHeaders=headers)
        print(f"Conversation result: {json.dumps(conv_result, indent=2)}")
        
        # Force flush
        print("\n3. Force flushing...")
        flush_result = force_flush_csv_data(headers)
        print(f"Flush result: {json.dumps(flush_result, indent=2)}")
        
        # Check files
        print("\n4. Checking created files...")
        from pathlib import Path
        
        tickets_file = Path(test_dir) / "test_tickets.csv"
        conversations_file = Path(test_dir) / "test_conversations.csv"
        
        print(f"Test directory: {test_dir}")
        print(f"Tickets file path: {tickets_file}")
        print(f"Tickets file exists: {tickets_file.exists()}")
        if tickets_file.exists():
            print(f"Tickets file size: {tickets_file.stat().st_size} bytes")
            with open(tickets_file, 'r') as f:
                content = f.read()
                print(f"Tickets file content:\n{content}")
        else:
            print("❌ Tickets file was not created!")
        
        print(f"Conversations file path: {conversations_file}")
        print(f"Conversations file exists: {conversations_file.exists()}")
        if conversations_file.exists():
            print(f"Conversations file size: {conversations_file.stat().st_size} bytes")
            with open(conversations_file, 'r') as f:
                content = f.read()
                print(f"Conversations file content:\n{content}")
        else:
            print("❌ Conversations file was not created!")
        
        # List all files in the directory
        print(f"\n5. All files in {test_dir}:")
        try:
            all_files = list(Path(test_dir).iterdir())
            if all_files:
                for file in all_files:
                    print(f"  - {file.name} ({file.stat().st_size} bytes)")
            else:
                print("  No files found in directory!")
        except Exception as e:
            print(f"  Error listing files: {e}")
        
        # Get statistics
        print("\n6. Getting statistics...")
        stats_result = get_csv_statistics(headers)
        print(f"Statistics: {json.dumps(stats_result, indent=2)}")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()


def test_csv_writing_in_current_dir():
    """Test CSV writing in the current working directory"""
    import os
    current_dir = os.getcwd()
    csv_dir = os.path.join(current_dir, "csv_test_output")
    
    print(f"Testing CSV writing in: {csv_dir}")
    test_csv_writing_simple(csv_dir)


def test_csv_writing_absolute_path():
    """Test CSV writing with an absolute path"""
    import os
    import tempfile
    
    # Get temp directory path but don't use context manager
    temp_base = tempfile.gettempdir()
    csv_dir = os.path.join(temp_base, "csv_test_manual")
    
    print(f"Testing CSV writing in absolute path: {csv_dir}")
    test_csv_writing_simple(csv_dir)


# ===================================================================
# EXAMPLE USAGE AND TESTING
# ===================================================================

if __name__ == "__main__":
    print("Choose a test option:")
    print("1. Test with temporary directory (auto-cleanup)")
    print("2. Test in current directory (./csv_test_output)")
    print("3. Test with absolute path")
    print("4. Run original comprehensive test")
    
    try:
        choice = input("Enter choice (1-4): ").strip()
        
        if choice == "1":
            print("\n=== Testing with temporary directory ===")
            test_csv_writing_simple()  # Uses temp directory
            
        elif choice == "2":
            print("\n=== Testing in current directory ===")
            test_csv_writing_in_current_dir()
            
        elif choice == "3":
            print("\n=== Testing with absolute path ===")
            test_csv_writing_absolute_path()
            
        elif choice == "4":
            print("\n=== Running original comprehensive test ===")
            # Original comprehensive test code here
            import json
            
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
            
            headers = [
                {"key": "output_directory", "value": "./comprehensive_test_output"},
                {"key": "tickets_file_path", "value": "tickets.csv"},
                {"key": "conversations_file_path", "value": "conversations.csv"},
                {"key": "batch_size", "value": "5"},
                {"key": "flush_interval_seconds", "value": "10"}
            ]
            
            ticket_data = {
                "id": "12345",
                "subject": "Test ticket for local CSV",
                "description": "This is a test ticket",
                "status": "Open", 
                "priority": "High",
                "created_at": "2024-01-15T10:30:00Z"
            }
            
            print("Creating test tickets and conversations...")
            for i in range(5):
                ticket_data_copy = ticket_data.copy()
                ticket_data_copy['id'] = f"ticket_{i}"
                ticket_data_copy['subject'] = f"Test ticket {i}"
                
                result = create_csv_ticket(body=ticket_data_copy, targetHeaders=headers)
                print(f"Ticket {i}: {result.get('status_code')}")
            
            force_flush_csv_data(headers)
            stats = get_csv_statistics(headers)
            print(f"Final stats: {json.dumps(stats, indent=2)}")
            
        else:
            print("Invalid choice")
            
    except KeyboardInterrupt:
        print("\nTest cancelled")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()