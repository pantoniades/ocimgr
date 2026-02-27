#!/usr/bin/env python3
"""
OCIMgr Kubernetes Resource Model - Python 3.13
OKE cluster management with proper match syntax
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


@register_resource_type
class OKECluster(AbstractOCIResource, AsyncResourceMixin):
    """OCI Container Engine for Kubernetes (OKE) cluster with async operations"""
    
    resource_type = "oke_cluster"
    deletion_order = DeletionOrder.APPLICATIONS
    
    @classmethod
    async def discover(cls, session: AsyncOCISession, compartment_id: str) -> List['OKECluster']:
        """Discover all OKE clusters across all regions concurrently"""
        
        async def discover_in_region(region: str) -> List['OKECluster']:
            """Discover OKE clusters in a specific region"""
            clusters = []
            
            try:
                container_client = await session.get_client('container_engine', region)
                
                # Run list operation in thread pool
                list_response = await cls._run_oci_operation(
                    container_client.list_clusters,
                    compartment_id=compartment_id
                )
                
                for cluster in list_response.data:
                    # Skip deleted/deleting clusters
                    if cluster.lifecycle_state in ['DELETED', 'DELETING']:
                        continue
                    
                    # OKE clusters don't typically have delete protection
                    has_delete_protection = False
                    
                    # Get node pools to estimate deletion time
                    node_pool_count = 0
                    total_node_count = 0
                    
                    try:
                        node_pools_response = await cls._run_oci_operation(
                            container_client.list_node_pools,
                            compartment_id=compartment_id,
                            cluster_id=cluster.id
                        )
                        
                        node_pool_count = len(node_pools_response.data)
                        
                        # Count total nodes across all pools
                        for pool in node_pools_response.data:
                            if hasattr(pool, 'node_config_details') and pool.node_config_details:
                                size = getattr(pool.node_config_details, 'size', 0)
                                total_node_count += size
                                
                    except Exception as e:
                        logging.warning(f"Could not get node pools for cluster {cluster.name}: {e}")
                    
                    # Estimate deletion time based on cluster complexity using proper match
                    estimated_time = 600  # default 10 minutes
                    match (node_pool_count, total_node_count):
                        case (pools, nodes) if pools >= 3 or nodes >= 10:
                            estimated_time = 1800  # Complex clusters - 30 minutes
                        case (pools, nodes) if pools >= 2 or nodes >= 5:
                            estimated_time = 1200  # Medium clusters - 20 minutes  
                        case (pools, nodes) if pools >= 1 or nodes >= 1:
                            estimated_time = 900   # Simple clusters - 15 minutes
                        case _:
                            estimated_time = 600   # Empty clusters - 10 minutes
                    
                    # Collect metadata
                    metadata = {
                        'kubernetes_version': getattr(cluster, 'kubernetes_version', 'unknown'),
                        'node_pool_count': node_pool_count,
                        'total_node_count': total_node_count,
                        'cluster_type': getattr(cluster, 'type', 'unknown')
                    }
                    
                    resource_info = ResourceInfo(
                        ocid=cluster.id,
                        name=cluster.name,
                        compartment_id=compartment_id,
                        region=region,
                        resource_type=cls.resource_type,
                        lifecycle_state=cluster.lifecycle_state,
                        estimated_deletion_time=estimated_time,
                        deletion_order=cls.deletion_order.value,
                        has_delete_protection=has_delete_protection,
                        dependencies=["load_balancer", "compute_instance", "block_volume"],  # Dependencies
                        metadata=metadata
                    )
                    
                    clusters.append(cls(resource_info))
                    
            except ServiceError as e:
                logging.error(f"Error discovering OKE clusters in {region}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error discovering OKE clusters in {region}: {e}")
        
            return clusters
        
        # Discover across all regions concurrently
        region_tasks = [
            discover_in_region(region) 
            for region in session.get_all_regions()
        ]
        
        region_results = await asyncio.gather(*region_tasks, return_exceptions=True)
        
        # Flatten results
        all_clusters = []
        for result in region_results:
            if isinstance(result, list):
                all_clusters.extend(result)
            elif isinstance(result, Exception):
                logging.error(f"Region discovery failed: {result}")
        
        logging.info(f"Discovered {len(all_clusters)} OKE clusters in compartment {compartment_id}")
        return all_clusters
    
    async def disable_delete_protection(self) -> OperationResult:
        """OKE clusters don't have delete protection"""
        return OperationResult(
            resource_ocid=self.info.ocid,
            operation="disable_protection",
            status=ResourceStatus.COMPLETED,
            message="OKE clusters do not have delete protection",
            duration=0.0
        )
    
    async def delete(self) -> OperationResult:
        """Delete the OKE cluster and all node pools"""
        start_time = time.time()
        
        try:
            container_client = await self._session.get_client('container_engine', self.info.region)
            
            # First, delete all node pools concurrently
            logging.info(f"Deleting node pools for OKE cluster {self.info.name}")
            
            try:
                node_pools_response = await self._run_oci_operation(
                    container_client.list_node_pools,
                    compartment_id=self.info.compartment_id,
                    cluster_id=self.info.ocid
                )
                
                # Delete node pools concurrently
                node_pool_deletion_tasks = []
                for node_pool in node_pools_response.data:
                    if node_pool.lifecycle_state not in ['DELETED', 'DELETING']:
                        logging.info(f"Deleting node pool {node_pool.name}")
                        
                        task = self._run_oci_operation(
                            container_client.delete_node_pool,
                            node_pool_id=node_pool.id
                        )
                        node_pool_deletion_tasks.append(task)
                
                # Wait for all node pool deletions to start
                if node_pool_deletion_tasks:
                    await asyncio.gather(*node_pool_deletion_tasks, return_exceptions=True)
                    
                    # Wait for node pools to be deleted
                    for node_pool in node_pools_response.data:
                        if node_pool.lifecycle_state not in ['DELETED', 'DELETING']:
                            await self._wait_for_state(
                                lambda: container_client.get_node_pool(node_pool.id),
                                ['DELETED'],
                                max_wait=900  # 15 minutes for node pool deletion
                            )
                        
            except Exception as e:
                logging.warning(f"Error deleting node pools for cluster {self.info.name}: {e}")
                # Continue with cluster deletion even if node pool deletion fails
            
            # Check cluster state before deletion
            match self.info.lifecycle_state:
                case 'ACTIVE':
                    # Ready to delete
                    pass
                case 'CREATING' | 'UPDATING' | 'DELETING' | 'DELETED' | 'FAILED':
                    error_msg = f"Cannot delete OKE cluster in state: {self.info.lifecycle_state}"
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
                        lambda: container_client.get_cluster(self.info.ocid),
                        ['ACTIVE'],
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
            
            # Delete the cluster
            logging.info(f"Deleting OKE cluster {self.info.name}")
            
            await self._run_oci_operation(
                container_client.delete_cluster,
                cluster_id=self.info.ocid
            )
            
            logging.info(f"Successfully initiated deletion of OKE cluster {self.info.name}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.COMPLETED,
                message="OKE cluster deletion initiated successfully",
                duration=time.time() - start_time
            )
            
        except ServiceError as e:
            error_msg = f"OCI API error during deletion: {e}"
            logging.error(f"Error deleting OKE cluster {self.info.name}: {error_msg}")
            
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
            logging.error(f"Unexpected error deleting OKE cluster {self.info.name}: {error_msg}")
            
            return OperationResult(
                resource_ocid=self.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
                duration=time.time() - start_time,
                error_details={'exception': str(e)}
            )