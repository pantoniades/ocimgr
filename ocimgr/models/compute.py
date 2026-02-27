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
    register_resource_type, AsyncOCISession, OperationResult, ResourceStatus
)


@register_resource_type
class ComputeInstance(AbstractOCIResource, AsyncResourceMixin):
    """OCI Compute Instance resource with async operations"""
    
    resource_type = "compute_instance"
    deletion_order = DeletionOrder.COMPUTE
    
    @classmethod
    async def discover(cls, session: AsyncOCISession, compartment_id: str) -> List['ComputeInstance']:
        """Discover all compute instances across all regions concurrently"""
        
        async def discover_in_region(region: str) -> List['ComputeInstance']:
            """Discover instances in a specific region"""
            instances = []
            
            try:
                compute_client = await session.get_client('compute', region)
                
                # Run list operation in thread pool
                list_instances_response = await cls._run_oci_operation(
                    compute_client.list_instances,
                    compartment_id=compartment_id
                )
                
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
                logging.error(f"Error discovering compute instances in {region}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error discovering compute instances in {region}: {e}")
        
            return instances
        
        # Discover across all regions concurrently
        region_tasks = [
            discover_in_region(region) 
            for region in session.get_all_regions()
        ]
        
        region_results = await asyncio.gather(*region_tasks, return_exceptions=True)
        
        # Flatten results
        all_instances = []
        for result in region_results:
            if isinstance(result, list):
                all_instances.extend(result)
            elif isinstance(result, Exception):
                logging.error(f"Region discovery failed: {result}")
        
        logging.info(f"Discovered {len(all_instances)} compute instances in compartment {compartment_id}")
        return all_instances
    
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
                preserve_boot_volume=False  # Delete boot volume as well
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