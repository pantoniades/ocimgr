#!/usr/bin/env python3
"""
OCIMgr Database Resource Models - Python 3.13
Autonomous Database and MySQL Database System management with proper match syntax
"""

import asyncio
import logging
from typing import List, Dict, Any
import time
import oci
from oci.exceptions import ServiceError

from ..core import (
    AbstractOCIResource, AsyncResourceMixin, ResourceInfo, DeletionOrder,
    register_resource_type, AsyncOCISession, OperationResult, ResourceStatus
)
from ..utils import run_with_backoff


@register_resource_type
class AutonomousDatabase(AbstractOCIResource, AsyncResourceMixin):
    """OCI Autonomous Database resource with async operations"""
    
    resource_type = "autonomous_database"
    deletion_order = DeletionOrder.DATABASES
    
    @classmethod
    async def discover(cls, session: AsyncOCISession, compartment_id: str, skip_unauthorized: bool = False) -> List['AutonomousDatabase']:
        """Discover all autonomous databases across all regions concurrently

        Args:
            session: asynchronous OCI session
            compartment_id: compartment OCID
            skip_unauthorized: if True, ignore 401 NotAuthenticated errors silently
        """
        
        async def discover_in_region(region: str) -> List['AutonomousDatabase']:
            """Discover ADBs in a specific region"""
            databases = []
            
            try:
                database_client = await session.get_client('database', region)
                
                # Run list operation in thread pool
                async def list_operation():
                    return await cls._run_oci_operation(
                        database_client.list_autonomous_databases,
                        compartment_id=compartment_id
                    )

                list_adb_response = await run_with_backoff(list_operation)
                
                # Iterate through the response data                
                for adb in list_adb_response.data:
                    # Skip terminated databases
                    if adb.lifecycle_state in ['TERMINATED', 'TERMINATING']:
                        continue
                    
                    # Get delete protection status
                    has_delete_protection = getattr(adb, 'is_delete_protected', False)
                    
                    # Estimate deletion time based on database configuration using proper match
                    cpu_count = getattr(adb, 'cpu_core_count', 1)
                    db_workload = getattr(adb, 'db_workload', 'OLTP')
                    
                    estimated_time = 180  # default 3 minutes
                    match (cpu_count, db_workload):
                        case (count, 'DW') if count >= 4:
                            estimated_time = 900  # Data warehouse with high CPU - 15 minutes
                        case (count, _) if count >= 8:
                            estimated_time = 600  # High CPU databases - 10 minutes
                        case (count, _) if count >= 2:
                            estimated_time = 300  # Medium databases - 5 minutes
                        case _:
                            estimated_time = 180  # Small databases - 3 minutes
                    
                    # Collect metadata
                    metadata = {
                        'db_workload': db_workload,
                        'cpu_core_count': cpu_count,
                        'data_storage_size_in_tbs': getattr(adb, 'data_storage_size_in_tbs', 0),
                        'is_auto_scaling_enabled': getattr(adb, 'is_auto_scaling_enabled', False)
                    }
                    
                    resource_info = ResourceInfo(
                        ocid=adb.id,
                        name=adb.display_name or adb.db_name,
                        compartment_id=compartment_id,
                        region=region,
                        resource_type=cls.resource_type,
                        lifecycle_state=adb.lifecycle_state,
                        estimated_deletion_time=estimated_time,
                        deletion_order=cls.deletion_order.value,
                        has_delete_protection=has_delete_protection,
                        dependencies=[],  # ADB typically doesn't have dependents
                        metadata=metadata
                    )
                    
                    databases.append(cls(resource_info))
                    
            except ServiceError as e:
                if skip_unauthorized and getattr(e, 'status', None) == 401:
                    # quietly skip unauthorized regions
                    return []
                logging.error(f"Error discovering autonomous databases in {region}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error discovering autonomous databases in {region}: {e}")
        
            return databases
        
        # Discover across all regions concurrently
        region_tasks = [
            discover_in_region(region) 
            for region in session.get_all_regions()
        ]
        
        region_results = await asyncio.gather(*region_tasks, return_exceptions=True)
        
        # Flatten results
        all_databases = []
        for result in region_results:
            if isinstance(result, list):
                all_databases.extend(result)
            elif isinstance(result, Exception):
                logging.error(f"Region discovery failed: {result}")
        
        logging.info(f"Discovered {len(all_databases)} autonomous databases in compartment {compartment_id}")
        return all_databases
    
    async def disable_delete_protection(self) -> OperationResult:
        """Disable delete protection for the autonomous database"""
        start_time = time.time()
        
        try:
            if not self.info.has_delete_protection:
                return OperationResult(
                    resource_ocid=self.info.ocid,
                    operation="disable_protection",
                    status=ResourceStatus.COMPLETED,
                    message="No delete protection enabled",
                    duration=time.time() - start_time
                )
            
            database_client = await self._session.get_client('database', self.info.region)
            
            logging.info(f"Disabling delete protection for database {self.info.name}")
            
            # OCI SDK update expects deletion_policy, not is_delete_protected
            # the SDK expects a UpdateDeletionPolicyDetails object
            # Build policy object with best effort; some SDK versions expect
            # `policy` string, others use `is_delete_protected` boolean.
            try:
                policy_obj = oci.database.models.UpdateDeletionPolicyDetails(policy="DELETE")
            except TypeError:
                policy_obj = oci.database.models.UpdateDeletionPolicyDetails(is_delete_protected=False)

            update_details = oci.database.models.UpdateAutonomousDatabaseDetails(
                deletion_policy=policy_obj
            )
            
            await self._run_oci_operation(
                database_client.update_autonomous_database,
                autonomous_database_id=self.info.ocid,
                update_autonomous_database_details=update_details
            )
            
            # Wait for update to complete
            update_success = await self._wait_for_state(
                lambda: database_client.get_autonomous_database(self.info.ocid),
                ['AVAILABLE'],
                max_wait=300  # 5 minutes
            )
            
            if not update_success:
                error_msg = "Timeout waiting for delete protection update"
                return OperationResult(
                    resource_ocid=self.info.ocid,
                    operation="disable_protection",
                    status=ResourceStatus.FAILED,
                    message=error_msg,
                    duration=time.time() - start_time
                )
            
            logging.info(f"Successfully disabled delete protection for database {self.info.name}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="disable_protection",
                status=ResourceStatus.COMPLETED,
                message="Delete protection disabled successfully",
                duration=time.time() - start_time
            )
            
        except ServiceError as e:
            error_msg = f"OCI API error disabling protection: {e}"
            logging.error(f"Error disabling delete protection for {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="disable_protection",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'oci_error': str(e)}
            )
            
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logging.error(f"Unexpected error disabling delete protection for {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="disable_protection",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )
    
    async def delete(self) -> OperationResult:
        """Delete the autonomous database"""
        start_time = time.time()
        
        try:
            database_client = await self._session.get_client('database', self.info.region)
            
            # Check database state before deletion
            match self.info.lifecycle_state:
                case 'AVAILABLE':
                    # Ready to delete
                    pass
                case 'STOPPED':
                    # Start the database first, then delete
                    logging.info(f"Starting database {self.info.name} before deletion")
                    await self._run_oci_operation(
                        database_client.start_autonomous_database,
                        autonomous_database_id=self.info.ocid
                    )
                    
                    # Wait for database to start
                    start_success = await self._wait_for_state(
                        lambda: database_client.get_autonomous_database(self.info.ocid),
                        ['AVAILABLE'],
                        max_wait=300  # 5 minutes
                    )
                    
                    if not start_success:
                        error_msg = "Timeout waiting for database to start before deletion"
                        return OperationResult(
                            resource_ocid=self.info.ocid,
                            operation="delete",
                            status=ResourceStatus.FAILED,
                            message=error_msg,
                            duration=time.time() - start_time
                        )
                
                case 'PROVISIONING' | 'SCALING' | 'STARTING' | 'STOPPING':
                    # Wait for stable state
                    stable_success = await self._wait_for_state(
                        lambda: database_client.get_autonomous_database(self.info.ocid),
                        ['AVAILABLE', 'STOPPED'],
                        max_wait=600  # 10 minutes
                    )
                    
                    if not stable_success:
                        error_msg = f"Timeout waiting for stable state from {self.info.lifecycle_state}"
                        return OperationResult(
                            resource_ocid=self.info.ocid,
                            operation="delete",
                            status=ResourceStatus.FAILED,
                            message=error_msg,
                            duration=time.time() - start_time
                        )
                
                case _:
                    error_msg = f"Cannot delete database in state: {self.info.lifecycle_state}"
                    return OperationResult(
                        resource_ocid=self.info.ocid,
                        operation="delete",
                        status=ResourceStatus.FAILED,
                        message=error_msg,
                        duration=time.time() - start_time
                    )
            
            logging.info(f"Deleting autonomous database {self.info.name}")
            
            await self._run_oci_operation(
                database_client.delete_autonomous_database,
                autonomous_database_id=self.info.ocid
            )
            
            logging.info(f"Successfully initiated deletion of database {self.info.name}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.COMPLETED,
                message="Database deletion initiated successfully",
                duration=time.time() - start_time
            )
            
        except ServiceError as e:
            error_msg = f"OCI API error during deletion: {e}"
            logging.error(f"Error deleting autonomous database {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'oci_error': str(e)}
            )
            
        except Exception as e:
            error_msg = f"Unexpected error during deletion: {e}"
            logging.error(f"Unexpected error deleting autonomous database {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )


@register_resource_type
class MySQLDBSystem(AbstractOCIResource, AsyncResourceMixin):
    """OCI MySQL Database System (HeatWave) resource with async operations"""
    
    resource_type = "mysql_db_system"
    deletion_order = DeletionOrder.DATABASES
    
    @classmethod
    async def discover(cls, session: AsyncOCISession, compartment_id: str, skip_unauthorized: bool = False) -> List['MySQLDBSystem']:
        """Discover all MySQL DB systems across all regions concurrently

        Args:
            session: asynchronous OCI session
            compartment_id: compartment OCID
            skip_unauthorized: if True, ignore 401 NotAuthenticated errors silently
        """
        
        async def discover_in_region(region: str) -> List['MySQLDBSystem']:
            """Discover MySQL systems in a specific region"""
            db_systems = []
            
            try:
                # Use the correct client and method name
                mysql_client = await session.get_client('mysql', region)
                
                # The correct method name in OCI SDK
                async def list_operation():
                    return await cls._run_oci_operation(
                        mysql_client.list_db_systems,  # This should be correct
                        compartment_id=compartment_id
                    )

                list_response = await run_with_backoff(list_operation)
                
                
                for db_system in list_response.data:
                    # Skip deleted/deleting systems
                    if db_system.lifecycle_state in ['DELETED', 'DELETING']:
                        continue
                    
                    # Get delete protection status (MySQL uses deletion_policy)
                    deletion_policy = getattr(db_system, 'deletion_policy', None)
                    has_delete_protection = getattr(db_system, 'is_delete_protected', False)
                    if deletion_policy is not None:
                        policy_value = getattr(deletion_policy, 'value', deletion_policy)
                        policy_text = str(policy_value).upper()
                        has_delete_protection = policy_text != 'DELETE'
                    
                    # Estimate deletion time based on system configuration using proper match
                    shape_name = getattr(db_system, 'shape_name', '')
                    
                    estimated_time = 300  # default 5 minutes
                    match shape_name:
                        case s if 'E4' in s:
                            estimated_time = 1200  # High-end shapes - 20 minutes
                        case s if 'E3' in s:
                            estimated_time = 900   # Mid-range shapes - 15 minutes
                        case s if 'E2' in s:
                            estimated_time = 600   # Standard shapes - 10 minutes
                        case _:
                            estimated_time = 300   # Small shapes - 5 minutes
                    
                    # Check for HeatWave cluster (adds deletion time)
                    has_heat_wave = False
                    try:
                        heat_wave_response = await cls._run_oci_operation(
                            mysql_client.get_heat_wave_cluster,
                            db_system_id=db_system.id
                        )
                        if heat_wave_response.data:
                            estimated_time += 600  # Additional 10 minutes for HeatWave
                            has_heat_wave = True
                    except:
                        pass  # No HeatWave cluster or error getting it
                    
                    # Collect metadata
                    metadata = {
                        'shape_name': shape_name,
                        'mysql_version': getattr(db_system, 'mysql_version', 'unknown'),
                        'data_storage_size_in_gb': getattr(db_system, 'data_storage_size_in_gb', 0),
                        'has_heat_wave': has_heat_wave
                    }
                    
                    resource_info = ResourceInfo(
                        ocid=db_system.id,
                        name=db_system.display_name,
                        compartment_id=compartment_id,
                        region=region,
                        resource_type=cls.resource_type,
                        lifecycle_state=db_system.lifecycle_state,
                        estimated_deletion_time=estimated_time,
                        deletion_order=cls.deletion_order.value,
                        has_delete_protection=has_delete_protection,
                        dependencies=[],  # MySQL systems typically don't have dependents
                        metadata=metadata
                    )
                    
                    db_systems.append(cls(resource_info))
                    
            except ServiceError as e:
                if skip_unauthorized and getattr(e, 'status', None) == 401:
                    return []
                logging.error(f"Error discovering MySQL DB systems in {region}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error discovering MySQL DB systems in {region}: {e}")
        
            return db_systems
        
        # Discover across all regions concurrently
        region_tasks = [
            discover_in_region(region) 
            for region in session.get_all_regions()
        ]
        
        region_results = await asyncio.gather(*region_tasks, return_exceptions=True)
        
        # Flatten results
        all_db_systems = []
        for result in region_results:
            if isinstance(result, list):
                all_db_systems.extend(result)
            elif isinstance(result, Exception):
                logging.error(f"Region discovery failed: {result}")
        
        logging.info(f"Discovered {len(all_db_systems)} MySQL DB systems in compartment {compartment_id}")
        return all_db_systems
    
    async def disable_delete_protection(self) -> OperationResult:
        """Disable delete protection for the MySQL DB system"""
        start_time = time.time()
        
        try:
            if not self.info.has_delete_protection:
                return OperationResult(
                    resource_ocid=self.info.ocid,
                    operation="disable_protection",
                    status=ResourceStatus.COMPLETED,
                    message="No delete protection enabled",
                    duration=time.time() - start_time
                )
            
            mysql_client = await self._session.get_client('mysql', self.info.region)
            
            logging.info(f"Disabling delete protection for MySQL system {self.info.name}")
            
            # For MySQL DB systems the UpdateDeletionPolicyDetails uses
            # `is_delete_protected` (bool) rather than a `policy` string.
            # Set `is_delete_protected=False` to allow deletion.
            # Some mysql SDK versions use `policy` instead of
            # `is_delete_protected` on UpdateDeletionPolicyDetails.
            try:
                policy_obj = oci.mysql.models.UpdateDeletionPolicyDetails(is_delete_protected=False)
            except TypeError:
                policy_obj = oci.mysql.models.UpdateDeletionPolicyDetails(policy="DELETE")

            update_details = oci.mysql.models.UpdateDbSystemDetails(
                deletion_policy=policy_obj
            )
            
            await self._run_oci_operation(
                mysql_client.update_db_system,
                db_system_id=self.info.ocid,
                update_db_system_details=update_details
            )
            
            # Wait for update to complete
            update_success = await self._wait_for_state(
                lambda: mysql_client.get_db_system(self.info.ocid),
                ['ACTIVE'],
                max_wait=300  # 5 minutes
            )
            
            if not update_success:
                error_msg = "Timeout waiting for delete protection update"
                return OperationResult(
                    resource_ocid=self.info.ocid,
                    operation="disable_protection",
                    status=ResourceStatus.FAILED,
                    message=error_msg,
                    duration=time.time() - start_time
                )
            
            logging.info(f"Successfully disabled delete protection for MySQL system {self.info.name}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="disable_protection",
                status=ResourceStatus.COMPLETED,
                message="Delete protection disabled successfully",
                duration=time.time() - start_time
            )
            
        except ServiceError as e:
            error_msg = f"OCI API error disabling protection: {e}"
            logging.error(f"Error disabling delete protection for {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="disable_protection",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'oci_error': str(e)}
            )
            
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logging.error(f"Unexpected error disabling delete protection for {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="disable_protection",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )
    
    async def delete(self) -> OperationResult:
        """Delete the MySQL DB system"""
        start_time = time.time()
        
        try:
            mysql_client = await self._session.get_client('mysql', self.info.region)
            
            # Check for and delete HeatWave cluster first if present
            if self.info.metadata.get('has_heat_wave', False):
                try:
                    logging.info(f"Deleting HeatWave cluster for MySQL system {self.info.name}")
                    
                    await self._run_oci_operation(
                        mysql_client.delete_heat_wave_cluster,
                        db_system_id=self.info.ocid
                    )
                    
                    # Wait for HeatWave cluster deletion
                    heat_wave_deleted = await self._wait_for_state(
                        lambda: mysql_client.get_heat_wave_cluster(self.info.ocid),
                        ['DELETED'],
                        max_wait=600  # 10 minutes for HeatWave deletion
                    )
                    
                    if not heat_wave_deleted:
                        error_msg = "Timeout waiting for HeatWave cluster deletion"
                        return OperationResult(
                            resource_ocid=self.info.ocid,
                            operation="delete",
                            status=ResourceStatus.FAILED,
                            message=error_msg,
                            duration=time.time() - start_time
                        )
                        
                except ServiceError as e:
                    if e.status != 404:  # Ignore "not found" errors
                        logging.warning(f"Error handling HeatWave cluster: {e}")
            
            # Check system state before deletion
            match self.info.lifecycle_state:
                case 'ACTIVE':
                    # Stop the DB system first before deletion
                    logging.info(f"Stopping MySQL DB system {self.info.name} before deletion")
                    
                    stop_details = {'shutdown_type': 'FAST'}
                    await self._run_oci_operation(
                        mysql_client.stop_db_system,
                        self.info.ocid,
                        stop_details
                    )
                    
                    # Wait for system to stop
                    stop_success = await self._wait_for_state(
                        lambda: mysql_client.get_db_system(self.info.ocid),
                        ['INACTIVE'],
                        max_wait=600  # 10 minutes
                    )
                    
                    if not stop_success:
                        error_msg = "Timeout waiting for DB system to stop before deletion"
                        return OperationResult(
                            resource_ocid=self.info.ocid,
                            operation="delete",
                            status=ResourceStatus.FAILED,
                            message=error_msg,
                            duration=time.time() - start_time
                        )
                    
                case 'INACTIVE':
                    # System is already stopped, can delete
                    pass
                case 'CREATING' | 'UPDATING' | 'DELETING' | 'DELETED':
                    error_msg = f"Cannot delete MySQL system in state: {self.info.lifecycle_state}"
                    return OperationResult(
                        resource_ocid=self.info.ocid,
                        operation="delete",
                        status=ResourceStatus.FAILED,
                        message=error_msg,
                        duration=time.time() - start_time
                    )
                case _:
                    # Wait for stable state
                    stable_success = await self._wait_for_state(
                        lambda: mysql_client.get_db_system(self.info.ocid),
                        ['ACTIVE', 'INACTIVE'],
                        max_wait=600  # 10 minutes
                    )
                    
                    if not stable_success:
                        error_msg = f"Timeout waiting for stable state from {self.info.lifecycle_state}"
                        return OperationResult(
                            resource_ocid=self.info.ocid,
                            operation="delete",
                            status=ResourceStatus.FAILED,
                            message=error_msg,
                            duration=time.time() - start_time
                        )
            
            # Delete the MySQL DB system
            logging.info(f"Deleting MySQL DB system {self.info.name}")
            
            await self._run_oci_operation(
                mysql_client.delete_db_system,
                db_system_id=self.info.ocid
            )
            
            logging.info(f"Successfully initiated deletion of MySQL system {self.info.name}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.COMPLETED,
                message="MySQL system deletion initiated successfully",
                duration=time.time() - start_time
            )
            
        except ServiceError as e:
            error_msg = f"OCI API error during deletion: {e}"
            logging.error(f"Error deleting MySQL DB system {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'oci_error': str(e)}
            )
            
        except Exception as e:
            error_msg = f"Unexpected error during deletion: {e}"
            logging.error(f"Unexpected error deleting MySQL DB system {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )