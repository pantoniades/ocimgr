#!/usr/bin/env python3
"""
OCIMgr Utility Modules
Progress tracking, output formatting, and other utilities
"""

import time
import json
import csv
import random
import asyncio
from typing import List, Dict, Any, Optional, TextIO, Callable, TypeVar
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from io import StringIO
import sys

import oci
import socket

try:
    from urllib3.exceptions import ReadTimeoutError, ProtocolError, MaxRetryError, SSLError
except Exception:  # pragma: no cover - fallback when urllib3 isn't available
    ReadTimeoutError = type(None)
    ProtocolError = type(None)
    MaxRetryError = type(None)
    SSLError = type(None)

# Third-party imports for progress bar
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


@dataclass
class ProgressItem:
    """Represents an item in the progress tracker"""
    name: str
    estimated_time: int  # seconds
    completed: bool = False
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error: Optional[str] = None


class ProgressTracker:
    """
    Tracks progress of resource deletion operations.
    Provides real-time progress updates with time estimates.
    """
    
    def __init__(self, items: List[ProgressItem], show_progress: bool = True):
        self.items = items
        self.show_progress = show_progress
        self.start_time = None
        self.total_estimated_time = sum(item.estimated_time for item in items)
        self.current_index = 0
        self._progress_bar = None
        
        if self.show_progress and TQDM_AVAILABLE:
            self._progress_bar = tqdm(
                total=len(items),
                desc="Deleting resources",
                unit="resource",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
            )
    
    def start(self) -> None:
        """Start the progress tracking"""
        self.start_time = time.time()
        if self._progress_bar:
            self._progress_bar.refresh()
    
    def start_item(self, index: int) -> None:
        """Mark an item as started"""
        if 0 <= index < len(self.items):
            self.items[index].start_time = time.time()
            self.current_index = index
            
            if self._progress_bar:
                self._progress_bar.set_description(f"Deleting: {self.items[index].name}")
    
    def complete_item(self, index: int, error: Optional[str] = None) -> None:
        """Mark an item as completed"""
        if 0 <= index < len(self.items):
            item = self.items[index]
            item.end_time = time.time()
            item.completed = True
            item.error = error
            
            if self._progress_bar:
                self._progress_bar.update(1)
                if error:
                    self._progress_bar.set_description(f"Error: {item.name}")
                else:
                    self._progress_bar.set_description(f"Completed: {item.name}")
    
    def finish(self) -> None:
        """Finish progress tracking"""
        if self._progress_bar:
            self._progress_bar.close()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of progress"""
        completed = sum(1 for item in self.items if item.completed)
        failed = sum(1 for item in self.items if item.error)
        
        total_time = 0
        if self.start_time:
            total_time = time.time() - self.start_time
        
        return {
            'total_items': len(self.items),
            'completed': completed,
            'failed': failed,
            'success_rate': (completed - failed) / len(self.items) if self.items else 0,
            'total_time_seconds': total_time,
            'average_time_per_item': total_time / completed if completed > 0 else 0
        }
    
    def get_failed_items(self) -> List[ProgressItem]:
        """Get list of items that failed"""
        return [item for item in self.items if item.error]
    
    def print_summary(self) -> None:
        """Print a summary of the operation"""
        summary = self.get_summary()
        failed_items = self.get_failed_items()
        
        print(f"\nOperation Summary:")
        print(f"  Total items: {summary['total_items']}")
        print(f"  Completed: {summary['completed']}")
        print(f"  Failed: {summary['failed']}")
        print(f"  Success rate: {summary['success_rate']:.1%}")
        print(f"  Total time: {summary['total_time_seconds']:.1f}s")
        
        if failed_items:
            print(f"\nFailed items:")
            for item in failed_items:
                print(f"  - {item.name}: {item.error}")


class OutputFormatter:
    """
    Handles formatting output in different formats (table, JSON, CSV).
    Provides consistent formatting across the application.
    """
    
    @staticmethod
    def format_table(data: List[Dict[str, Any]], headers: Optional[List[str]] = None) -> str:
        """
        Format data as a simple text table.
        
        Args:
            data: List of dictionaries to format
            headers: Optional list of headers (uses dict keys if None)
        
        Returns:
            Formatted table string
        """
        if not data:
            return "No data to display"
        
        if headers is None:
            headers = list(data[0].keys())
        
        # Calculate column widths
        col_widths = {}
        for header in headers:
            col_widths[header] = len(str(header))
            for row in data:
                value = str(row.get(header, ''))
                col_widths[header] = max(col_widths[header], len(value))
        
        # Build table
        output = StringIO()
        
        # Header row
        header_row = " | ".join(header.ljust(col_widths[header]) for header in headers)
        output.write(header_row + "\n")
        
        # Separator row
        separator = "-+-".join("-" * col_widths[header] for header in headers)
        output.write(separator + "\n")
        
        # Data rows
        for row in data:
            data_row = " | ".join(str(row.get(header, '')).ljust(col_widths[header]) for header in headers)
            output.write(data_row + "\n")
        
        return output.getvalue()
    
    @staticmethod
    def format_numbered_list(items: List[str], start_num: int = 1) -> str:
        """
        Format items as a numbered list for interactive selection.
        
        Args:
            items: List of items to format
            start_num: Starting number (default 1)
        
        Returns:
            Formatted numbered list
        """
        if not items:
            return "No items to display"
        
        output = StringIO()
        for i, item in enumerate(items, start=start_num):
            output.write(f"{i:3d}. {item}\n")
        
        return output.getvalue()
    
    @staticmethod
    def format_json(data: Any, indent: int = 2) -> str:
        """
        Format data as JSON.
        
        Args:
            data: Data to format
            indent: JSON indentation level
        
        Returns:
            JSON formatted string
        """
        def json_serializer(obj):
            """Custom JSON serializer for special types"""
            if hasattr(obj, '__dict__'):
                return obj.__dict__
            elif isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        return json.dumps(data, indent=indent, default=json_serializer, ensure_ascii=False)
    
    @staticmethod
    def format_csv(data: List[Dict[str, Any]], headers: Optional[List[str]] = None) -> str:
        """
        Format data as CSV.
        
        Args:
            data: List of dictionaries to format
            headers: Optional list of headers (uses dict keys if None)
        
        Returns:
            CSV formatted string
        """
        if not data:
            return ""
        
        if headers is None:
            headers = list(data[0].keys())
        
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        
        for row in data:
            # Convert non-string values to strings for CSV
            csv_row = {k: str(v) if v is not None else '' for k, v in row.items()}
            writer.writerow(csv_row)
        
        return output.getvalue()
    
    @staticmethod
    def save_to_file(content: str, filename: str, mode: str = 'w') -> bool:
        """
        Save content to a file.
        
        Args:
            content: Content to save
            filename: Target filename
            mode: File mode (default 'w')
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filename, mode, encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            print(f"Error saving to file {filename}: {e}")
            return False


class InteractiveSelector:
    """
    Handles interactive selection of items from lists.
    Provides user-friendly selection interfaces.
    """
    
    @staticmethod
    def select_single(items: List[str], prompt: str = "Select an item") -> Optional[int]:
        """
        Select a single item from a numbered list.
        
        Args:
            items: List of items to choose from
            prompt: Prompt message
        
        Returns:
            Selected index (0-based) or None if cancelled
        """
        if not items:
            print("No items available for selection")
            return None
        
        print(f"\n{prompt}:")
        print(OutputFormatter.format_numbered_list(items))
        
        while True:
            try:
                choice = input(f"\nEnter number (1-{len(items)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    return None
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(items):
                    return choice_num - 1  # Convert to 0-based index
                else:
                    print(f"Please enter a number between 1 and {len(items)}")
                    
            except ValueError:
                print("Please enter a valid number or 'q' to quit")
            except KeyboardInterrupt:
                print("\nOperation cancelled")
                return None
    
    @staticmethod
    def select_multiple(items: List[str], prompt: str = "Select items") -> Optional[List[int]]:
        """
        Select multiple items from a numbered list.
        
        Args:
            items: List of items to choose from
            prompt: Prompt message
        
        Returns:
            List of selected indices (0-based) or None if cancelled
        """
        if not items:
            print("No items available for selection")
            return None
        
        print(f"\n{prompt}:")
        print(OutputFormatter.format_numbered_list(items))
        
        while True:
            try:
                choice = input(f"\nEnter numbers (comma-separated, 1-{len(items)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    return None
                
                if not choice:
                    print("Please enter at least one number")
                    continue
                
                # Parse comma-separated numbers
                selected_nums = []
                for part in choice.split(','):
                    part = part.strip()
                    if '-' in part:
                        # Range selection (e.g., "1-5")
                        start, end = map(int, part.split('-', 1))
                        selected_nums.extend(range(start, end + 1))
                    else:
                        selected_nums.append(int(part))
                
                # Validate all numbers are in range
                invalid_nums = [num for num in selected_nums if not (1 <= num <= len(items))]
                if invalid_nums:
                    print(f"Invalid numbers: {invalid_nums}. Please use numbers between 1 and {len(items)}")
                    continue
                
                # Remove duplicates and convert to 0-based indices
                selected_indices = sorted(list(set(num - 1 for num in selected_nums)))
                return selected_indices
                
            except ValueError:
                print("Please enter valid numbers separated by commas, or 'q' to quit")
            except KeyboardInterrupt:
                print("\nOperation cancelled")
                return None
    
    @staticmethod
    def confirm_action(message: str, default: bool = False) -> bool:
        """
        Get yes/no confirmation from user.
        
        Args:
            message: Confirmation message
            default: Default value if user just presses enter
        
        Returns:
            True if confirmed, False otherwise
        """
        suffix = " [Y/n]" if default else " [y/N]"
        
        while True:
            try:
                response = input(f"{message}{suffix}: ").strip().lower()
                
                if not response:
                    return default
                
                if response in ['y', 'yes']:
                    return True
                elif response in ['n', 'no']:
                    return False
                else:
                    print("Please enter 'y' for yes or 'n' for no")
                    
            except KeyboardInterrupt:
                print("\nOperation cancelled")
                return False


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} EB"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def truncate_string(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """Truncate string to maximum length with suffix"""
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


T = TypeVar("T")


def is_throttle_error(error: Exception) -> bool:
    """Return True if error represents an OCI throttling response."""
    if isinstance(error, oci.exceptions.ServiceError):
        return error.status == 429
    return False


def is_auth_error(error: Exception) -> bool:
    """Return True if error represents an authentication/authorization failure."""
    if isinstance(error, oci.exceptions.ServiceError):
        return error.status in {401, 403}
    return False


def is_transient_network_error(error: Exception) -> bool:
    """Return True for retryable network errors (timeouts, connection resets)."""
    # Handle tuple format from OCI SDK
    if isinstance(error, tuple) and len(error) >= 1:
        error = error[0]
    
    if ReadTimeoutError and isinstance(error, ReadTimeoutError):
        return True
    if ProtocolError and isinstance(error, ProtocolError):
        return True
    if MaxRetryError and isinstance(error, MaxRetryError):
        return True
    if SSLError and isinstance(error, SSLError):
        return True
    # Fallback for when urllib3 not available: check class name
    if hasattr(error, '__class__') and hasattr(error.__class__, '__name__'):
        class_name = error.__class__.__name__
        if class_name in ('ProtocolError', 'ReadTimeoutError', 'MaxRetryError', 'SSLError'):
            return True
    if isinstance(error, socket.timeout):
        return True
    if isinstance(error, ConnectionResetError):
        return True
    if isinstance(error, ConnectionAbortedError):
        return True
    # unwrap OCI request exceptions that may wrap network errors
    if isinstance(error, oci.exceptions.RequestException):
        cause = getattr(error, '__cause__', None)
        if cause and is_transient_network_error(cause):
            return True
        # sometimes args contain tuple with ProtocolError
        for arg in error.args:
            if isinstance(arg, tuple) and arg and isinstance(arg[0], ProtocolError):
                return True
    return False


async def run_with_backoff(
    operation: Callable[[], T],
    *,
    max_retries: int = 6,
    base_delay: float = 0.75,
    max_delay: float = 12.0,
    jitter: float = 0.2
) -> T:
    """
    Run an async operation with exponential backoff for OCI throttling.
    
    Args:
        operation: Async callable with no args.
        max_retries: Max retry attempts for throttling.
        base_delay: Base delay in seconds.
        max_delay: Max delay in seconds.
        jitter: Random jitter ratio to avoid thundering herds.
    """
    attempt = 0
    while True:
        try:
            return await operation()
        except Exception as exc:
            if is_auth_error(exc):
                raise
            if (not is_throttle_error(exc) and not is_transient_network_error(exc)) or attempt >= max_retries:
                raise

            delay = min(max_delay, base_delay * (2 ** attempt))
            delay = delay * (1 + random.uniform(-jitter, jitter))
            await asyncio.sleep(delay)
            attempt += 1


if __name__ == "__main__":
    # Example usage / testing
    
    # Test progress tracker
    items = [
        ProgressItem("Test Item 1", 2),
        ProgressItem("Test Item 2", 3),
        ProgressItem("Test Item 3", 1),
    ]
    
    tracker = ProgressTracker(items, show_progress=False)  # Disable for testing
    tracker.start()
    
    for i, item in enumerate(items):
        tracker.start_item(i)
        time.sleep(0.1)  # Simulate work
        if i == 1:  # Simulate an error
            tracker.complete_item(i, "Test error")
        else:
            tracker.complete_item(i)
    
    tracker.finish()
    tracker.print_summary()
    
    # Test output formatting
    test_data = [
        {"name": "Resource 1", "type": "compute", "region": "us-ashburn-1"},
        {"name": "Resource 2", "type": "database", "region": "us-phoenix-1"},
    ]
    
    print("\nTable format:")
    print(OutputFormatter.format_table(test_data))
    
    print("\nJSON format:")
    print(OutputFormatter.format_json(test_data))
    
    print("\nCSV format:")
    print(OutputFormatter.format_csv(test_data))