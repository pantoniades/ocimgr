#!/usr/bin/env python3
"""
OCIMgr Compute Resource Model - Python 3.13
Compute instance management with proper match syntax
"""

import asyncio
import logging
from typing import List, Dict, Any
import time
import oci
from oci.exceptions import ServiceError

from ocimgr.core import (
    AbstractOCIResource, AsyncResourceMixin, ResourceInfo, DeletionOrder,
    register_resource_type, AsyncOCISession, OperationResult, ResourceStatus,
    discover_across_regions
)
from ocimgr.utils import run_with_backoff, is_transient_network_error


@register_resource_type
class ComputeInstance(AbstractOCIResource, AsyncResourceMixin):
    """OCI Compute Instance resource with async operations"""
    
    resource_type = "compute_instance"
    deletion_order = DeletionOrder.COMPUTE
    
    @classmethod
    async def discover(
        cls,
        session: AsyncOCISession,
        compartment_id: str,
        skip_unauthorized: bool = False
    ) -> List['ComputeInstance']:
        """Discover all compute instances across all regions concurrently.

        Args:
            session: async OCI session
            compartment_id: compartment OCID
            skip_unauthorized: if True, ignore 401/403 regions silently
        """
        
        async def discover_in_region(region: str) -> List['ComputeInstance']:
            """Discover instances in a specific region"""
            instances = []
            
            try:
                compute_client = await session.get_client('compute', region)
                
                async def list_operation():
                    return await cls._run_oci_operation(
                        oci.pagination.list_call_get_all_results,
                        compute_client.list_instances,
                        compartment_id=compartment_id
                    )

                list_instances_response = await run_with_backoff(list_operation)
                
                for instance in list_instances_response.data:
                    # Skip terminated instances
                    if instance.lifecycle_state in ['TERMINATED', 'TERMINATING']:
                        continue
                    
                    # Check for delete protection and get additional details
                    has_delete_protection, metadata = await cls._get_instance_details(
                        compute_client, instance.id
                    )
                    
                    # Estimate deletion time based on instance state using proper match
                    estimated_time = 90  # default
                    match instance.lifecycle_state:
                        case 'RUNNING':
                            estimated_time = 120  # Running instances take longer (stop + terminate)
                        case 'STOPPED':
                            estimated_time = 60   # Stopped instances terminate faster
                        case _:
                            estimated_time = 90   # Default estimate
                    
                    resource_info = ResourceInfo(
                        ocid=instance.id,
                        name=instance.display_name,
                        compartment_id=compartment_id,
                        region=region,
                        resource_type=cls.resource_type,
                        lifecycle_state=instance.lifecycle_state,
                        estimated_deletion_time=estimated_time,
                        deletion_order=cls.deletion_order.value,
                        has_delete_protection=has_delete_protection,
                        dependencies=["load_balancer", "file_system"],  # Resources that might depend on compute
                        metadata=metadata
                    )
                    
                    instances.append(cls(resource_info))
                    
            except ServiceError as e:
                if e.status in {401, 403}:
                    session.mark_region_unauthorized(region)
                    if skip_unauthorized:
                        logging.debug(f"Skipping unauthorized compute discovery in {region}: {e}")
                        return []
                logging.error(f"Error discovering compute instances in {region}: {e}")
            except Exception as e:
                if skip_unauthorized and is_transient_network_error(e):
                    session.mark_region_unauthorized(region)
                    logging.warning(
                        "Skipping region %s due to transient network error: %s",
                        region,
                        e
                    )
                    return []
                logging.error(f"Unexpected error discovering compute instances in {region}: {e}")
        
            return instances
        
        return await discover_across_regions(
            session, discover_in_region, cls.resource_type,
            skip_unauthorized=skip_unauthorized,
        )
    
    @classmethod
    async def _get_instance_details(cls, compute_client, instance_id: str) -> tuple[bool, Dict[str, Any]]:
        """Get detailed instance information including delete protection status"""
        try:
            get_response = await cls._run_oci_operation(
                compute_client.get_instance, 
                instance_id
            )
            instance_details = get_response.data
            
            # Check various protection mechanisms
            has_protection = False
            metadata = {
                'shape': getattr(instance_details, 'shape', 'unknown'),
                'availability_domain': getattr(instance_details, 'availability_domain', 'unknown'),
                'fault_domain': getattr(instance_details, 'fault_domain', None)
            }
            
            # Check for delete protection via instance metadata or options
            if hasattr(instance_details, 'metadata') and instance_details.metadata:
                protection_flag = instance_details.metadata.get('delete_protection', 'false')
                has_protection = protection_flag.lower() == 'true'
            
            return has_protection, metadata
            
        except Exception as e:
            logging.warning(f"Could not get instance details for {instance_id}: {e}")
            return False, {}
    
    async def disable_delete_protection(self) -> OperationResult:
        """Disable delete protection for the compute instance"""
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
            
            compute_client = await self._session.get_client('compute', self.info.region)
            
            # Get current instance details
            get_response = await self._run_oci_operation(
                compute_client.get_instance, 
                self.info.ocid
            )
            instance = get_response.data
            
            # Update instance metadata to disable protection
            current_metadata = getattr(instance, 'metadata', {}) or {}
            updated_metadata = current_metadata.copy()
            updated_metadata['delete_protection'] = 'false'
            
            update_details = oci.core.models.UpdateInstanceDetails(
                metadata=updated_metadata
            )
            
            await self._run_oci_operation(
                compute_client.update_instance,
                instance_id=self.info.ocid,
                update_instance_details=update_details
            )
            
            logging.info(f"Disabled delete protection for instance {self.info.name}")
            
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
        """Delete the compute instance with proper state handling"""
        start_time = time.time()
        
        try:
            compute_client = await self._session.get_client('compute', self.info.region)
            
            # Handle instance state with proper match syntax
            match self.info.lifecycle_state:
                case 'RUNNING':
                    logging.info(f"Stopping instance {self.info.name} before deletion")
                    
                    await self._run_oci_operation(
                        compute_client.instance_action,
                        instance_id=self.info.ocid,
                        action="STOP"
                    )
                    
                    # Wait for instance to stop with timeout
                    stop_success = await self._wait_for_state(
                        lambda: compute_client.get_instance(self.info.ocid),
                        ['STOPPED'],
                        max_wait=180  # 3 minutes
                    )
                    
                    if not stop_success:
                        error_msg = "Timeout waiting for instance to stop"
                        return OperationResult(
                            resource_ocid=self.info.ocid,
                            operation="delete",
                            status=ResourceStatus.FAILED,
                            message=error_msg,
                            duration=time.time() - start_time
                        )
                
                case 'STOPPED':
                    # Ready to terminate
                    pass
                
                case 'PROVISIONING' | 'STARTING' | 'STOPPING':
                    # Wait for stable state
                    stable_success = await self._wait_for_state(
                        lambda: compute_client.get_instance(self.info.ocid),
                        ['RUNNING', 'STOPPED'],
                        max_wait=300  # 5 minutes
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
                    error_msg = f"Cannot delete instance in state: {self.info.lifecycle_state}"
                    return OperationResult(
                        resource_ocid=self.info.ocid,
                        operation="delete",
                        status=ResourceStatus.FAILED,
                        message=error_msg,
                        duration=time.time() - start_time
                    )
            
            # Terminate the instance
            logging.info(f"Terminating compute instance {self.info.name}")
            
            await self._run_oci_operation(
                compute_client.terminate_instance,
                instance_id=self.info.ocid,
                preserve_boot_volume=False,  # Delete boot volume as well
                preserve_data_volumes_created_at_launch=False
            )
            
            logging.info(f"Successfully initiated termination of instance {self.info.name}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.COMPLETED,
                message="Instance termination initiated successfully",
                duration=time.time() - start_time
            )
            
        except ServiceError as e:
            error_msg = f"OCI API error during deletion: {e}"
            logging.error(f"Error deleting compute instance {self.info.name}: {error_msg}")
            
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
            logging.error(f"Unexpected error deleting compute instance {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )


@register_resource_type
class InstancePool(AbstractOCIResource, AsyncResourceMixin):
    """OCI Compute Instance Pool resource with async operations"""

    resource_type = "instance_pool"
    deletion_order = DeletionOrder.COMPUTE

    @classmethod
    async def discover(
        cls,
        session: AsyncOCISession,
        compartment_id: str,
        skip_unauthorized: bool = False
    ) -> List['InstancePool']:
        """Discover all instance pools across all regions concurrently."""

        async def discover_in_region(region: str) -> List['InstancePool']:
            pools: List[InstancePool] = []
            try:
                compute_mgmt_client = await session.get_client('compute_management', region)

                async def list_operation():
                    return await cls._run_oci_operation(
                        oci.pagination.list_call_get_all_results,
                        compute_mgmt_client.list_instance_pools,
                        compartment_id=compartment_id
                    )

                list_response = await run_with_backoff(list_operation)

                for pool in list_response.data:
                    if pool.lifecycle_state in ['DELETED', 'DELETING']:
                        continue

                    estimated_time = 180
                    match pool.lifecycle_state:
                        case 'RUNNING' | 'SCALING' | 'UPDATING':
                            estimated_time = 300
                        case _:
                            estimated_time = 180

                    metadata = {
                        'size': getattr(pool, 'size', None),
                        'instance_configuration_id': getattr(pool, 'instance_configuration_id', None)
                    }

                    resource_info = ResourceInfo(
                        ocid=pool.id,
                        name=pool.display_name,
                        compartment_id=compartment_id,
                        region=region,
                        resource_type=cls.resource_type,
                        lifecycle_state=pool.lifecycle_state,
                        estimated_deletion_time=estimated_time,
                        deletion_order=cls.deletion_order.value,
                        has_delete_protection=False,
                        dependencies=[],
                        metadata=metadata
                    )
                    pools.append(cls(resource_info))
            except ServiceError as e:
                if e.status in {401, 403}:
                    session.mark_region_unauthorized(region)
                    if skip_unauthorized:
                        logging.debug(f"Skipping unauthorized instance pool discovery in {region}: {e}")
                        return []
                logging.error(f"Error discovering instance pools in {region}: {e}")
            except Exception as e:
                if skip_unauthorized and is_transient_network_error(e):
                    session.mark_region_unauthorized(region)
                    logging.warning(
                        "Skipping region %s due to transient network error: %s",
                        region,
                        e
                    )
                    return []
                logging.error(f"Unexpected error discovering instance pools in {region}: {e}")

            return pools

        return await discover_across_regions(
            session, discover_in_region, cls.resource_type,
            skip_unauthorized=skip_unauthorized,
        )

    async def disable_delete_protection(self) -> OperationResult:
        """Instance pools do not have delete protection."""
        return OperationResult(
            resource_ocid=self.info.ocid,
            operation="disable_protection",
            status=ResourceStatus.COMPLETED,
            message="No delete protection enabled",
            duration=0.0
        )

    async def delete(self) -> OperationResult:
        """Delete the instance pool."""
        start_time = time.time()
        try:
            compute_mgmt_client = await self._session.get_client(
                'compute_management',
                self.info.region
            )

            logging.info(f"Deleting instance pool {self.info.name}")
            await self._run_oci_operation(
                compute_mgmt_client.terminate_instance_pool,
                instance_pool_id=self.info.ocid
            )

            logging.info(f"Successfully initiated deletion of instance pool {self.info.name}")
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.COMPLETED,
                message="Instance pool deletion initiated successfully",
                duration=time.time() - start_time
            )
        except ServiceError as e:
            error_msg = f"OCI API error during deletion: {e}"
            logging.error(f"Error deleting instance pool {self.info.name}: {error_msg}")
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
            logging.error(f"Unexpected error deleting instance pool {self.info.name}: {error_msg}")
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )