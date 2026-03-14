#!/usr/bin/env python3
"""
OCIMgr - Oracle Cloud Infrastructure Management Tool
Core module with async support and modern Python 3.13 features
"""

import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Type, Set, AsyncIterator
from dataclasses import dataclass
from enum import Enum
import configparser
import logging
import functools
import os
from pathlib import Path
import oci
from oci.config import from_file
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime


# Registry for resource types
_RESOURCE_REGISTRY: Dict[str, Type['AbstractOCIResource']] = {}

class AsyncResourceMixin:
    """Mixin providing common async utilities for resource implementations"""
    
    @staticmethod
    async def _run_oci_operation(operation, *args, **kwargs):
        """
        Run OCI SDK operation in thread pool to avoid blocking.
        Handles both positional and keyword arguments properly.
        """
        try:
            loop = asyncio.get_event_loop()
            
            # Use functools.partial to create a no-argument callable
            bound_operation = functools.partial(operation, *args, **kwargs)
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                result = await loop.run_in_executor(executor, bound_operation)
                return result
                
        except Exception as e:
            logging.debug(f"OCI operation failed: {e}")
            raise
    
    @staticmethod
    async def _wait_for_state(
        get_operation, 
        target_states: List[str], 
        max_wait: int = 300,
        poll_interval: int = 5
    ) -> bool:
        """Wait for a resource to reach one of the target states"""
        from ocimgr.utils import run_with_backoff

        start_time = time.time()
        log_interval = max(poll_interval, 15)
        last_log = start_time
        
        while (time.time() - start_time) < max_wait:
            try:
                response = await run_with_backoff(
                    lambda: AsyncResourceMixin._run_oci_operation(get_operation)
                )
                
                if hasattr(response, 'data') and hasattr(response.data, 'lifecycle_state'):
                    current_state = response.data.lifecycle_state
                    
                    if current_state in target_states:
                        return True
                else:
                    logging.warning("Response does not have expected lifecycle_state attribute")

                now = time.time()
                if now - last_log >= log_interval:
                    logging.info(
                        "Waiting for state %s (current=%s, elapsed=%ds/%ds)",
                        target_states,
                        current_state,
                        int(now - start_time),
                        max_wait
                    )
                    last_log = now
                
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                logging.warning(f"Error checking resource state: {e}")
                await asyncio.sleep(poll_interval)
        
        logging.warning(f"Timeout waiting for states {target_states} after {max_wait} seconds")
        return False
    
def register_resource_type(cls: Type['AbstractOCIResource']) -> Type['AbstractOCIResource']:
    """Decorator to register resource types for automatic discovery"""
    if hasattr(cls, 'resource_type'):
        _RESOURCE_REGISTRY[cls.resource_type] = cls
        logging.debug(f"Registered resource type: {cls.resource_type}")
    else:
        raise ValueError(f"Resource class {cls.__name__} must define 'resource_type' attribute")
    return cls


def get_registered_resource_types() -> Dict[str, Type['AbstractOCIResource']]:
    """Get all registered resource types"""
    return _RESOURCE_REGISTRY.copy()


class DeletionOrder(Enum):
    """Standard deletion order priorities - lower numbers deleted first"""
    APPLICATIONS = 10      # K8s apps, functions
    COMPUTE = 20          # Instances, instance pools
    DATABASES = 30        # All database types
    LOAD_BALANCERS = 40   # Load balancers
    STORAGE = 50          # Block volumes, file systems
    NETWORK = 60          # Subnets, security lists
    CORE_NETWORK = 70     # VCNs, internet gateways


@dataclass
class ResourceInfo:
    """Information about a discovered resource"""
    ocid: str
    name: str
    compartment_id: str
    region: str
    resource_type: str
    lifecycle_state: str
    estimated_deletion_time: int  # seconds
    deletion_order: int
    has_delete_protection: bool = False
    dependencies: List[str] = None  # List of resource types that depend on this
    metadata: Dict[str, Any] = None  # Additional resource-specific metadata
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.metadata is None:
            self.metadata = {}


class ResourceStatus(Enum):
    """Status of resource operations"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class OperationResult:
    """Result of a resource operation"""
    resource_ocid: str
    operation: str  # 'discover', 'disable_protection', 'delete'
    status: ResourceStatus
    message: str = ""
    duration: float = 0.0
    error_details: Optional[Dict[str, Any]] = None


class AbstractOCIResource(ABC):
    """
    Abstract base class for all OCI resource types.
    Each resource type implements its own async discovery and deletion logic.
    """
    
    # Must be overridden by subclasses
    resource_type: str = None
    deletion_order: DeletionOrder = DeletionOrder.COMPUTE
    
    def __init__(self, resource_info: ResourceInfo):
        self.info = resource_info
        self._session: Optional['AsyncOCISession'] = None
    
    def set_session(self, session: 'AsyncOCISession') -> None:
        """Set the OCI session for API calls"""
        self._session = session
    
    @classmethod
    @abstractmethod
    async def discover(cls, session: 'AsyncOCISession', compartment_id: str) -> List['AbstractOCIResource']:
        """
        Discover all resources of this type in the given compartment.
        Returns list of resource instances.
        """
        pass
    
    @abstractmethod
    async def disable_delete_protection(self) -> OperationResult:
        """
        Disable delete protection if it exists for this resource.
        Returns operation result with status and details.
        """
        pass
    
    @abstractmethod
    async def delete(self) -> OperationResult:
        """
        Delete this resource.
        Returns operation result with status and details.
        """
        pass
    
    def get_dependencies(self) -> List[str]:
        """
        Get list of resource types that depend on this resource.
        Override if resource has specific dependencies.
        """
        return self.info.dependencies.copy()
    
    def get_deletion_order_priority(self) -> int:
        """Get the deletion order priority (lower = delete first)"""
        return self.deletion_order.value
    
    def get_estimated_deletion_time(self) -> int:
        """Get estimated deletion time in seconds"""
        return self.info.estimated_deletion_time
    
    def __str__(self) -> str:
        return f"{self.resource_type}: {self.info.name} ({self.info.ocid})"
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.info.name}>"


class OCIConfig:
    """Handles OCI configuration from INI files"""
    
    DEFAULT_CONFIG_PATHS = [
        "~/.oci/config",
        "~/.oci_mgr/config", 
        "./oci_mgr.ini"
    ]
    
    def __init__(self, config_path: Optional[str] = None, profile: str = "DEFAULT"):
        self.config_path = self._resolve_config_path(config_path)
        self.profile = profile
        self._config = None
        self._regions: List[str] = []
        self._load_config()
    
    def _resolve_config_path(self, config_path: Optional[str]) -> str:
        """Find and resolve the configuration file path"""
        if config_path:
            path = Path(config_path).expanduser()
            if not path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
            return str(path)
        
        # Try default locations
        for default_path in self.DEFAULT_CONFIG_PATHS:
            path = Path(default_path).expanduser()
            if path.exists():
                return str(path)
        
        raise FileNotFoundError(f"No config file found in default locations: {self.DEFAULT_CONFIG_PATHS}")
    
    def _load_config(self) -> None:
        """Load OCI configuration"""
        try:
            # Use OCI SDK's config loader for standard OCI config format
            self._config = from_file(self.config_path, self.profile)
            
            # Load regions configuration
            config_parser = configparser.ConfigParser()
            config_parser.read(self.config_path)
            
            # Get regions from config or use default
            if config_parser.has_option(self.profile, 'regions'):
                regions_str = config_parser.get(self.profile, 'regions')
                self._regions = [r.strip() for r in regions_str.split(',')]
            else:
                # Default to the configured region
                self._regions = [self._config.get('region', 'us-ashburn-1')]
            
            logging.info(f"Loaded OCI config from {self.config_path}, profile: {self.profile}")
            logging.info(f"Configured regions: {', '.join(self._regions)}")
            
        except Exception as e:
            logging.error(f"Failed to load OCI config: {e}")
            raise
    
    def get_config(self) -> Dict[str, Any]:
        """Get the OCI configuration dictionary"""
        return self._config.copy()
    
    def get_regions(self) -> List[str]:
        """Get list of regions to operate on"""
        return self._regions.copy()

    def set_regions(self, regions: List[str]) -> None:
        """Override configured regions (used when discovering all regions)."""
        self._regions = regions.copy()


class AsyncOCISession:
    """
    Async-enabled OCI session manager supporting concurrent multi-region operations.
    Provides unified interface for async OCI operations across regions.
    """
    
    def __init__(self, config: OCIConfig, max_concurrent_regions: int = 5):
        self.config = config
        self.oci_config = config.get_config()
        self.regions = config.get_regions()
        self.max_concurrent_regions = max_concurrent_regions
        self._clients: Dict[str, Dict[str, Any]] = {}
        self._current_region = self.regions[0] if self.regions else 'us-ashburn-1'
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent_regions * 2)
        self._unauthorized_regions: set[str] = set()
        self._init_task: Optional[asyncio.Task] = None
        
        # Initialize clients for all regions
        self._init_task = asyncio.create_task(self._initialize_clients())
    
    async def _initialize_clients(self) -> None:
        """Initialize OCI clients for all regions asynchronously"""
        async def init_region_clients(region: str) -> None:
            """Initialize clients for a single region"""
            region_config = self.oci_config.copy()
            region_config['region'] = region
            
            try:
                # Initialize clients in thread pool to avoid blocking
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._create_region_clients,
                    region,
                    region_config
                )
                logging.info(f"Initialized OCI clients for region: {region}")
            except Exception as e:
                logging.error(f"Failed to initialize clients for region {region}: {e}")
                raise
        
        # Initialize all regions concurrently
        tasks = [init_region_clients(region) for region in self.regions]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_until_ready(self) -> None:
        """Await client initialization to ensure regions are ready before use."""
        if self._init_task:
            await self._init_task
    
    def _create_region_clients(self, region: str, region_config: Dict[str, Any]) -> None:
        """Create clients for a region (runs in thread pool)"""
        client_kwargs = {
            "timeout": (30, 120)
        }
        self._clients[region] = {
            'compute': oci.core.ComputeClient(region_config, **client_kwargs),
            'compute_management': oci.core.ComputeManagementClient(region_config, **client_kwargs),
            'database': oci.database.DatabaseClient(region_config, **client_kwargs),
            'mysql': oci.mysql.DbSystemClient(region_config, **client_kwargs),
            'container_engine': oci.container_engine.ContainerEngineClient(region_config, **client_kwargs),
            'identity': oci.identity.IdentityClient(region_config, **client_kwargs),
            'block_storage': oci.core.BlockstorageClient(region_config, **client_kwargs),
            'object_storage': oci.object_storage.ObjectStorageClient(region_config, **client_kwargs),
            'load_balancer': oci.load_balancer.LoadBalancerClient(region_config, **client_kwargs),
            'network_load_balancer': oci.network_load_balancer.NetworkLoadBalancerClient(region_config, **client_kwargs),
            'virtual_network': oci.core.VirtualNetworkClient(region_config, **client_kwargs),
        }
    
    async def get_client(self, service: str, region: Optional[str] = None) -> Any:
        """
        Get OCI client for specified service and region.
        
        Args:
            service: Service name (compute, database, mysql, etc.)
            region: Region name (uses current region if None)
        
        Returns:
            OCI client instance
        """
        target_region = region or self._current_region
        
        if target_region not in self._clients:
            raise ValueError(f"Region not configured: {target_region}")
        
        if service not in self._clients[target_region]:
            raise ValueError(f"Service not available: {service}")
        
        return self._clients[target_region][service]
    
    def set_current_region(self, region: str) -> None:
        """Set the current region for operations"""
        if region not in self.regions:
            raise ValueError(f"Region not configured: {region}")
        self._current_region = region
        logging.debug(f"Switched to region: {region}")
    
    def get_current_region(self) -> str:
        """Get the current region"""
        return self._current_region
    
    def get_all_regions(self) -> List[str]:
        """Get list of all configured regions"""
        return self.regions.copy()

    def mark_region_unauthorized(self, region: str) -> None:
        """Record a region as unauthorized to avoid repeated calls."""
        if region in self.regions:
            self._unauthorized_regions.add(region)

    def is_region_authorized(self, region: str) -> bool:
        """Return True if region is not marked unauthorized."""
        return region not in self._unauthorized_regions

    def get_authorized_regions(self) -> List[str]:
        """Return regions excluding those marked unauthorized."""
        return [region for region in self.regions if region not in self._unauthorized_regions]
    
    async def execute_across_regions(
        self, 
        func, 
        *args, 
        max_concurrent: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute an async function across all regions concurrently.
        
        Args:
            func: Async function to execute (should accept session as first param)
            *args: Additional arguments to pass to function
            max_concurrent: Max concurrent regions (uses default if None)
            **kwargs: Additional keyword arguments to pass to function
        
        Returns:
            Dictionary mapping region names to function results
        """
        max_concurrent = max_concurrent or self.max_concurrent_regions
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def execute_in_region(region: str) -> tuple[str, Any]:
            async with semaphore:
                original_region = self._current_region
                try:
                    self.set_current_region(region)
                    result = await func(self, *args, **kwargs)
                    return region, result
                except Exception as e:
                    logging.error(f"Error executing function in region {region}: {e}")
                    return region, None
                finally:
                    self.set_current_region(original_region)
        
        # Execute across all regions
        tasks = [execute_in_region(region) for region in self.regions]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert to dictionary
        return {region: result for region, result in results if not isinstance(result, Exception)}
    
    async def close(self) -> None:
        """Clean up resources"""
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except KeyboardInterrupt:
            # Allow Ctrl-C to exit without noisy shutdown tracebacks
            logging.warning("Executor shutdown interrupted by user")
            self._executor.shutdown(wait=False)


class ResourceDiscoveryEngine:
    """
    Async engine for discovering resources across all registered types and regions.
    Uses modern Python patterns for efficient concurrent operations.
    """
    
    def __init__(self, session: AsyncOCISession):
        self.session = session
        self.resource_types = get_registered_resource_types()
    
    async def discover_all_resources(
        self, 
        compartment_ids: List[str],
        resource_type_filter: Optional[List[str]] = None,
        skip_unauthorized: bool = True
    ) -> Dict[str, List[AbstractOCIResource]]:
        """
        Discover all resources across compartments and regions.
        
        Args:
            compartment_ids: List of compartment IDs to scan
            resource_type_filter: Optional filter for specific resource types
        
        Returns:
            Dictionary mapping compartment_id to list of resources
        
        Raises:
            RuntimeError: If circuit breaker opens (rate limit exhausted)
        """
        # Filter resource types if specified
        types_to_discover = self.resource_types
        if resource_type_filter:
            types_to_discover = {
                name: cls for name, cls in self.resource_types.items() 
                if name in resource_type_filter
            }
        
        all_resources = {}
        circuit_open_error = None
        
        # Discover resources for each compartment
        for compartment_id in compartment_ids:
            logging.info(f"Discovering resources in compartment: {compartment_id}")
            compartment_resources = []
            
            # Create discovery tasks for all resource types
            discovery_tasks = []
            for resource_type_name, resource_class in types_to_discover.items():
                task = self._discover_resource_type(
                    resource_class, 
                    compartment_id, 
                    resource_type_name,
                    skip_unauthorized=skip_unauthorized
                )
                discovery_tasks.append(task)
            
            # Execute all discoveries concurrently
            discovery_results = await asyncio.gather(*discovery_tasks, return_exceptions=True)
            
            # Collect results
            for i, result in enumerate(discovery_results):
                resource_type_name = list(types_to_discover.keys())[i]
                
                match result:
                    case list() if result:  # Non-empty list of resources
                        compartment_resources.extend(result)
                        logging.info(f"  Found {len(result)} {resource_type_name}(s)")
                    case list():  # Empty list
                        logging.debug(f"  No {resource_type_name} found")
                    case Exception() as e:
                        # Check if circuit breaker has opened (rate limiting cascade)
                        error_str = str(e)
                        if "Circuit" in error_str and "OPEN" in error_str:
                            logging.error(f"Circuit breaker opened due to excessive failures: {e}")
                            circuit_open_error = e
                            # Continue to preserve further error context
                        elif skip_unauthorized and getattr(e, "status", None) == 401:
                            logging.warning(f"  Skipping unauthorized {resource_type_name} discovery: {e}")
                        else:
                            logging.error(f"  Error discovering {resource_type_name}: {e}")
            
            all_resources[compartment_id] = compartment_resources
            logging.info(f"  Total: {len(compartment_resources)} resources in compartment")
        
        # If circuit breaker opened, propagate it so caller can handle gracefully
        if circuit_open_error:
            raise RuntimeError(f"Service rate limited - circuit breaker open: {circuit_open_error}")
        
        return all_resources
    
    async def _discover_resource_type(
        self, 
        resource_class: Type[AbstractOCIResource], 
        compartment_id: str,
        resource_type_name: str,
        skip_unauthorized: bool = True
    ) -> List[AbstractOCIResource]:
        """
        Discover resources of a specific type with error handling.
        
        Args:
            resource_class: Resource class to discover
            compartment_id: Compartment to search in
            resource_type_name: Name for logging
        
        Returns:
            List of discovered resources
        """
        # Call the discovery implementation and centrally handle authorization skips.
        try:
            try:
                resources = await resource_class.discover(self.session, compartment_id, skip_unauthorized=skip_unauthorized)
            except TypeError:
                # older implementations may not support the parameter
                resources = await resource_class.discover(self.session, compartment_id)

            # Set session for all discovered resources
            for resource in resources:
                resource.set_session(self.session)
            return resources

        except Exception as e:
            # If caller requested to skip unauthorized, quietly return no resources when a 401 occurs.
            try:
                status = getattr(e, 'status', None)
            except Exception:
                status = None

            if skip_unauthorized and status == 401:
                logging.debug(f"Skipping unauthorized {resource_type_name} discovery in {compartment_id}: {e}")
                return []

            logging.error(f"Discovery failed for {resource_type_name} in {compartment_id}: {e}")
            raise


# Modern async logging setup
def generate_default_log_filename(prefix: str = "ocimgr") -> str:
    """Generate a unique log filename for the current run."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{timestamp}.log"


async def setup_async_logging(log_file: Optional[str] = None, level: str = "INFO") -> str:
    """Setup async-compatible logging configuration and return the log filename used."""
    if not log_file:
        log_file = generate_default_log_filename()
    log_level = getattr(logging, level.upper())
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    
    # Console handler (for errors and warnings)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        handlers=[file_handler, console_handler],
        force=True  # Python 3.8+ feature to reconfigure existing loggers
    )
    
    logging.info(f"Async logging initialized - Level: {level}, File: {log_file}")
    return log_file


def setup_logging(log_file: Optional[str] = None, level: str = "INFO") -> None:
    """Synchronous wrapper for logging setup"""
    asyncio.create_task(setup_async_logging(log_file, level))


if __name__ == "__main__":
    async def main():
        """Example usage / testing"""
        await setup_async_logging()
        
        try:
            config = OCIConfig()
            session = AsyncOCISession(config)
            await asyncio.sleep(1)  # Give time for client initialization
            
            print(f"Initialized OCIMgr for regions: {session.get_all_regions()}")
            print(f"Current region: {session.get_current_region()}")
            print(f"Registered resource types: {list(get_registered_resource_types().keys())}")
            
            await session.close()
        except Exception as e:
            print(f"Initialization failed: {e}")
    
    asyncio.run(main())