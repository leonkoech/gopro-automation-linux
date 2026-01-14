"""
Logging Service for GoPro Automation
Provides file-based logging with rotation and live streaming capabilities.
"""

import os
import sys
import logging
import threading
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from collections import deque
from typing import Optional, Generator
import time
import json

# Configuration
LOG_DIR = os.path.expanduser('~/gopro_logs')
LOG_FILE = os.path.join(LOG_DIR, 'gopro_automation.log')
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5
MAX_MEMORY_LINES = 1000  # Keep last 1000 lines in memory for streaming

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)


class LogBuffer:
    """Thread-safe circular buffer for storing recent log lines."""

    def __init__(self, max_size: int = MAX_MEMORY_LINES):
        self._buffer = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._subscribers = []
        self._subscriber_lock = threading.Lock()

    def append(self, log_entry: dict):
        """Add a log entry to the buffer and notify subscribers."""
        with self._lock:
            self._buffer.append(log_entry)

        # Notify all subscribers
        with self._subscriber_lock:
            dead_subscribers = []
            for callback in self._subscribers:
                try:
                    callback(log_entry)
                except Exception:
                    dead_subscribers.append(callback)

            # Remove dead subscribers
            for callback in dead_subscribers:
                self._subscribers.remove(callback)

    def get_recent(self, count: int = 100) -> list:
        """Get the most recent log entries."""
        with self._lock:
            return list(self._buffer)[-count:]

    def get_all(self) -> list:
        """Get all log entries in buffer."""
        with self._lock:
            return list(self._buffer)

    def subscribe(self, callback):
        """Subscribe to new log entries."""
        with self._subscriber_lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback):
        """Unsubscribe from log entries."""
        with self._subscriber_lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)


# Global log buffer
log_buffer = LogBuffer()


class BufferedHandler(logging.Handler):
    """Custom handler that writes to the in-memory buffer."""

    def __init__(self, buffer: LogBuffer):
        super().__init__()
        self.buffer = buffer

    def emit(self, record):
        try:
            log_entry = {
                'timestamp': datetime.fromtimestamp(record.created).isoformat(),
                'level': record.levelname,
                'logger': record.name,
                'message': self.format(record),
                'module': record.module,
                'line': record.lineno
            }
            self.buffer.append(log_entry)
        except Exception:
            self.handleError(record)


class LoggingService:
    """Central logging service for the application."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging with file and console handlers."""
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        # Clear existing handlers
        root_logger.handlers.clear()

        # File handler with rotation
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=MAX_LOG_SIZE,
            backupCount=BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # Buffer handler for live streaming
        buffer_handler = BufferedHandler(log_buffer)
        buffer_handler.setLevel(logging.DEBUG)
        buffer_handler.setFormatter(formatter)
        root_logger.addHandler(buffer_handler)

        # Store reference
        self.logger = root_logger
        self.log_file = LOG_FILE
        self.log_dir = LOG_DIR

    def get_logger(self, name: str) -> logging.Logger:
        """Get a named logger."""
        return logging.getLogger(name)

    def get_recent_logs(self, count: int = 100) -> list:
        """Get recent log entries from memory."""
        return log_buffer.get_recent(count)

    def get_all_buffered_logs(self) -> list:
        """Get all buffered log entries."""
        return log_buffer.get_all()

    def stream_logs(self) -> Generator[str, None, None]:
        """Generator that yields new log entries as they arrive."""
        queue = deque()
        queue_lock = threading.Lock()
        stop_event = threading.Event()

        def on_log(entry):
            with queue_lock:
                queue.append(entry)

        log_buffer.subscribe(on_log)

        try:
            while not stop_event.is_set():
                entries = []
                with queue_lock:
                    while queue:
                        entries.append(queue.popleft())

                for entry in entries:
                    yield f"data: {json.dumps(entry)}\n\n"

                time.sleep(0.1)  # Small delay to prevent busy-waiting
        finally:
            log_buffer.unsubscribe(on_log)

    def get_log_files(self) -> list:
        """Get list of available log files."""
        files = []
        for f in Path(self.log_dir).glob('gopro_automation.log*'):
            stat = f.stat()
            files.append({
                'name': f.name,
                'path': str(f),
                'size': stat.st_size,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
        return sorted(files, key=lambda x: x['name'])

    def read_log_file(self, filename: str, lines: int = 500, offset: int = 0) -> dict:
        """Read contents of a log file."""
        filepath = os.path.join(self.log_dir, filename)

        if not os.path.exists(filepath):
            return {'success': False, 'error': 'File not found'}

        # Security check - ensure file is in log directory
        if not os.path.abspath(filepath).startswith(os.path.abspath(self.log_dir)):
            return {'success': False, 'error': 'Invalid file path'}

        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                all_lines = f.readlines()

            total_lines = len(all_lines)

            # Get requested range
            start = max(0, total_lines - lines - offset)
            end = total_lines - offset
            selected_lines = all_lines[start:end]

            # Parse lines into structured format
            parsed = []
            for line in selected_lines:
                line = line.strip()
                if not line:
                    continue

                # Try to parse structured log line
                # Format: 2025-01-13 10:30:45 | INFO     | module | message
                parts = line.split(' | ', 3)
                if len(parts) >= 4:
                    parsed.append({
                        'timestamp': parts[0].strip(),
                        'level': parts[1].strip(),
                        'logger': parts[2].strip(),
                        'message': parts[3].strip()
                    })
                else:
                    parsed.append({
                        'timestamp': '',
                        'level': 'INFO',
                        'logger': '',
                        'message': line
                    })

            return {
                'success': True,
                'filename': filename,
                'total_lines': total_lines,
                'returned_lines': len(parsed),
                'offset': offset,
                'logs': parsed
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def search_logs(self, query: str, filename: Optional[str] = None) -> list:
        """Search logs for a query string."""
        results = []

        files = [filename] if filename else [f['name'] for f in self.get_log_files()]

        for fname in files:
            filepath = os.path.join(self.log_dir, fname)
            if not os.path.exists(filepath):
                continue

            try:
                with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                    for line_num, line in enumerate(f, 1):
                        if query.lower() in line.lower():
                            results.append({
                                'file': fname,
                                'line_number': line_num,
                                'content': line.strip()
                            })
            except Exception:
                pass

        return results[:500]  # Limit results


# Singleton instance
_logging_service = None


def get_logging_service() -> LoggingService:
    """Get the logging service singleton."""
    global _logging_service
    if _logging_service is None:
        _logging_service = LoggingService()
    return _logging_service


def get_logger(name: str) -> logging.Logger:
    """Convenience function to get a named logger."""
    get_logging_service()  # Ensure logging is initialized
    return logging.getLogger(name)
