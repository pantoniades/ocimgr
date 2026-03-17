#!/usr/bin/env python3
"""
OCIMgr Compartment Management - Python 3.13
Async compartment operations and validation
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
import oci
from oci.exceptions import ServiceError

from ..core import AsyncResourceMixin, AsyncOCISession, get_registered_resource_types
from ..utils import run_with_backoff, get_oci_request_id


class CompartmentManager:
    """Async compartment management with enhanced validation"""
    
    def __init__(self, session: AsyncOCISession):
        self.session = session
    
    async def list_compartments(self, parent_compartment_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all compartments accessible to the user asynchronously.
        
        Args:
            parent_compartment_id: Parent compartment ID (uses tenancy root if None)
        
        Returns:
            List of compartment information dictionaries
        """
        compartments = []
        
        try:
            # Always use the configured home region for identity to avoid timeouts
            identity_region = self.session.oci_config.get('region')
            identity_client = await self.session.get_client('identity', identity_region)
            
            if parent_compartment_id is None:
                # Get tenancy ID as root compartment
                async def get_user_operation():
                    return await AsyncResourceMixin._run_oci_operation(
                        identity_client.get_user,
                        self.session.oci_config['user']
                    )
                user_response = await run_with_backoff(get_user_operation, max_retries=6, base_delay=1.0)
                parent_compartment_id = user_response.data.compartment_id
            
            # List compartments (paginated)
            async def list_operation():
                return await AsyncResourceMixin._run_oci_operation(
                    oci.pagination.list_call_get_all_results,
                    identity_client.list_compartments,
                    compartment_id=parent_compartment_id,
                    access_level="ANY",
                    compartment_id_in_subtree=True
                )
            list_response = await run_with_backoff(list_operation, max_retries=6, base_delay=1.0)

            for compartment in list_response.data:
                if compartment.lifecycle_state == 'ACTIVE':
                    compartments.append({
                        'id': compartment.id,
                        'parent_id': getattr(compartment, 'compartment_id', None),
                        'name': compartment.name,
                        'description': getattr(compartment, 'description', ''),
                        'time_created': str(compartment.time_created),
                        'lifecycle_state': compartment.lifecycle_state
                    })
            
            # Also include the root compartment
            async def get_compartment_operation():
                return await AsyncResourceMixin._run_oci_operation(
                    identity_client.get_compartment,
                    parent_compartment_id
                )
            root_response = await run_with_backoff(get_compartment_operation, max_retries=6, base_delay=1.0)
            root_compartment = root_response.data
            compartments.insert(0, {
                'id': root_compartment.id,
                'parent_id': getattr(root_compartment, 'compartment_id', None),
                'name': f"{root_compartment.name} (Root)",
                'description': getattr(root_compartment, 'description', 'Root compartment'),
                'time_created': str(root_compartment.time_created),
                'lifecycle_state': root_compartment.lifecycle_state
            })
            
        except Exception as e:
            logging.error(f"Error listing compartments: {e}")
            raise
        
        logging.info(f"Found {len(compartments)} accessible compartments")
        return compartments
    
    async def validate_compartment_empty(self, compartment_id: str) -> tuple[bool, List[str]]:
        """
        Check if a compartment is empty (ready for deletion) asynchronously.
        
        Args:
            compartment_id: Compartment ID to check
        
        Returns:
            Tuple of (is_empty, list_of_remaining_resources)
        """
        remaining_resources = []
        resource_types = get_registered_resource_types()
        
        # Check all resource types concurrently
        async def check_resource_type(resource_type_name: str, resource_class):
            try:
                resources = await resource_class.discover(self.session, compartment_id)
                if resources:
                    return [f"{resource_type_name}: {resource.info.name}" for resource in resources]
                return []
            except Exception as e:
                logging.warning(f"Error checking {resource_type_name} in compartment: {e}")
                return []
        
        check_tasks = [
            check_resource_type(type_name, type_class)
            for type_name, type_class in resource_types.items()
        ]
        
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
        
        # Flatten results
        for result in check_results:
            if isinstance(result, list):
                remaining_resources.extend(result)
            elif isinstance(result, Exception):
                logging.warning(f"Resource type check failed: {result}")
        
        is_empty = len(remaining_resources) == 0
        return is_empty, remaining_resources
    
    async def get_compartment_details(self, compartment_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific compartment.
        
        Args:
            compartment_id: Compartment ID to get details for
        
        Returns:
            Compartment details dictionary or None if not found
        """
        try:
            identity_region = self.session.oci_config.get('region')
            identity_client = await self.session.get_client('identity', identity_region)
            
            async def get_compartment_operation():
                return await AsyncResourceMixin._run_oci_operation(
                    identity_client.get_compartment,
                    compartment_id
                )
            compartment_response = await run_with_backoff(get_compartment_operation, max_retries=6, base_delay=1.0)
            
            compartment = compartment_response.data
            
            # Get additional details like tags and defined tags
            details = {
                'id': compartment.id,
                'name': compartment.name,
                'description': getattr(compartment, 'description', ''),
                'time_created': str(compartment.time_created),
                'lifecycle_state': compartment.lifecycle_state,
                'freeform_tags': getattr(compartment, 'freeform_tags', {}),
                'defined_tags': getattr(compartment, 'defined_tags', {}),
            }
            
            return details
            
        except ServiceError as e:
            if e.status == 404:
                logging.warning(f"Compartment {compartment_id} not found")
                return None
            else:
                logging.error(f"Error getting compartment details: {e}")
                raise
        except Exception as e:
            logging.error(f"Unexpected error getting compartment details: {e}")
            raise
    
    async def get_compartment_hierarchy(self, compartment_id: str) -> List[Dict[str, Any]]:
        """
        Get the full hierarchy path for a compartment (from root to target).
        
        Args:
            compartment_id: Target compartment ID
        
        Returns:
            List of compartment details from root to target
        """
        hierarchy = []
        current_id = compartment_id
        
        try:
            identity_region = self.session.oci_config.get('region')
            identity_client = await self.session.get_client('identity', identity_region)
            
            # Walk up the hierarchy
            while current_id:
                async def get_compartment_operation():
                    return await AsyncResourceMixin._run_oci_operation(
                        identity_client.get_compartment,
                        current_id
                    )
                compartment_response = await run_with_backoff(get_compartment_operation, max_retries=6, base_delay=1.0)
                
                compartment = compartment_response.data
                hierarchy.insert(0, {  # Insert at beginning to build path from root
                    'id': compartment.id,
                    'name': compartment.name,
                    'description': getattr(compartment, 'description', ''),
                    'lifecycle_state': compartment.lifecycle_state
                })
                
                # Move to parent (if exists)
                parent_id = getattr(compartment, 'compartment_id', None)
                if parent_id and parent_id != current_id:  # Avoid infinite loop
                    current_id = parent_id
                else:
                    break  # Reached root
            
            return hierarchy
            
        except Exception as e:
            logging.error(f"Error getting compartment hierarchy: {e}")
            return []

    async def delete_compartment(
        self,
        compartment_id: str,
        region: Optional[str] = None,
        timeout_seconds: int = 120
    ) -> bool:
        """
        Delete a compartment (must be empty first) with timeout.

        Args:
            compartment_id: Compartment ID to delete
            region: Region for deletion (defaults to home region)
            timeout_seconds: Max time to wait for deletion (default 120s for retry-based deletes)

        Returns:
            True if deletion initiated, False otherwise
        """
        try:
            identity_client = await self.session.get_client('identity', region)

            logging.info(
                "event=compartment_delete_attempt compartment_id=%s region=%s timeout=%s",
                compartment_id,
                region or self.session.oci_config.get('region'),
                timeout_seconds
            )

            details_response = await AsyncResourceMixin._run_oci_operation(
                identity_client.get_compartment,
                compartment_id
            )
            lifecycle_state = details_response.data.lifecycle_state
            if lifecycle_state != 'ACTIVE':
                if lifecycle_state in {'DELETING', 'DELETED'}:
                    logging.info(
                        "Compartment %s already in state %s; treating as deleted",
                        compartment_id,
                        lifecycle_state
                    )
                    return True
                logging.warning(
                    f"Skipping delete for compartment {compartment_id} in state {lifecycle_state}"
                )
                return False

            async def delete_operation():
                return await AsyncResourceMixin._run_oci_operation(
                    identity_client.delete_compartment,
                    compartment_id=compartment_id
                )

            # Wrap with timeout to fail fast instead of hanging 7+ minutes
            try:
                await asyncio.wait_for(
                    run_with_backoff(
                        delete_operation,
                        max_retries=12,
                        base_delay=4.0,
                        max_delay=120.0,
                        jitter=0.3,
                        retry_log_label="compartment_delete"
                    ),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                logging.warning(
                    f"⏱️  Timeout deleting compartment {compartment_id} after {timeout_seconds}s (OCI API unresponsive)"
                )
                return False

            logging.info(f"Deletion initiated for compartment {compartment_id}")
            return True

        except ServiceError as e:
            request_id = get_oci_request_id(e)
            if e.status == 404:
                logging.warning(f"Compartment {compartment_id} not found")
                return False
            if e.status == 429 or e.status >= 500:
                # Treat throttling and server errors as transient
                raise
            logging.error(
                "Error deleting compartment %s (status=%s, request_id=%s): %s",
                compartment_id,
                e.status,
                request_id or "n/a",
                e
            )
            return False
        except Exception as e:
            # For transient network issues or throttling the inner backoff
            # may have retried already, but if we still got an exception let
            # the caller retry as well by re-raising.  This helps when OCI is
            # flaking or the connection is closed mid-request.
            try:
                from ..utils import is_transient_network_error, is_throttle_error
            except ImportError:
                is_transient_network_error = lambda _: False
                is_throttle_error = lambda _: False

            # Fallback check for protocol errors
            if 'ProtocolError' in str(e):
                logging.info(f"Raising protocol error for retry: {e}")
                raise

            if is_transient_network_error(e) or is_throttle_error(e):
                logging.info(f"Raising transient error for retry: {e}")
                raise

            logging.error(f"Unexpected error deleting compartment {compartment_id}: {e}")
            return False
    
    async def search_compartments(self, search_term: str) -> List[Dict[str, Any]]:
        """
        Search for compartments by name or description.
        
        Args:
            search_term: Term to search for (case-insensitive)
        
        Returns:
            List of matching compartments
        """
        all_compartments = await self.list_compartments()
        search_term_lower = search_term.lower()
        
        matching_compartments = []
        for compartment in all_compartments:
            name_match = search_term_lower in compartment['name'].lower()
            desc_match = search_term_lower in compartment.get('description', '').lower()
            
            if name_match or desc_match:
                # Add match score for sorting
                score = 0
                if compartment['name'].lower().startswith(search_term_lower):
                    score += 10  # Exact prefix match gets highest score
                elif name_match:
                    score += 5   # Name contains term
                elif desc_match:
                    score += 1   # Description contains term
                
                compartment['match_score'] = score
                matching_compartments.append(compartment)
        
        # Sort by match score (descending) then by name
        matching_compartments.sort(key=lambda x: (-x['match_score'], x['name']))
        
        # Remove match_score from final results
        for comp in matching_compartments:
            comp.pop('match_score', None)
        
        return matching_compartments
    
    async def get_compartment_resource_summary(self, compartment_id: str) -> Dict[str, Any]:
        """
        Get a summary of all resources in a compartment by type.
        
        Args:
            compartment_id: Compartment ID to analyze
        
        Returns:
            Dictionary with resource counts and total estimated deletion time
        """
        resource_types = get_registered_resource_types()
        summary = {
            'compartment_id': compartment_id,
            'resource_counts': {},
            'total_resources': 0,
            'total_estimated_deletion_time': 0,
            'has_protected_resources': False,
            'resource_details': {}
        }
        
        # Discover all resource types concurrently
        async def discover_resource_type(type_name: str, resource_class):
            try:
                resources = await resource_class.discover(self.session, compartment_id)
                
                if resources:
                    count = len(resources)
                    total_time = sum(r.get_estimated_deletion_time() for r in resources)
                    protected_count = sum(1 for r in resources if r.info.has_delete_protection)
                    
                    return type_name, {
                        'count': count,
                        'estimated_deletion_time': total_time,
                        'protected_resources': protected_count,
                        'resources': resources
                    }
                else:
                    return type_name, {
                        'count': 0,
                        'estimated_deletion_time': 0,
                        'protected_resources': 0,
                        'resources': []
                    }
                    
            except Exception as e:
                logging.warning(f"Error discovering {type_name} in compartment: {e}")
                return type_name, {
                    'count': 0,
                    'estimated_deletion_time': 0,
                    'protected_resources': 0,
                    'resources': [],
                    'error': str(e)
                }
        
        # Execute all discoveries concurrently
        discovery_tasks = [
            discover_resource_type(type_name, resource_class)
            for type_name, resource_class in resource_types.items()
        ]
        
        discovery_results = await asyncio.gather(*discovery_tasks, return_exceptions=True)
        
        # Process results
        for result in discovery_results:
            if isinstance(result, tuple):
                type_name, type_summary = result
                
                summary['resource_counts'][type_name] = type_summary['count']
                summary['total_resources'] += type_summary['count']
                summary['total_estimated_deletion_time'] += type_summary['estimated_deletion_time']
                summary['resource_details'][type_name] = type_summary
                
                if type_summary['protected_resources'] > 0:
                    summary['has_protected_resources'] = True
            
            elif isinstance(result, Exception):
                logging.error(f"Resource discovery failed: {result}")
        
        return summary
    
    async def estimate_compartment_cleanup_time(self, compartment_id: str) -> Dict[str, Any]:
        """
        Estimate the total time required to clean up a compartment.
        
        Args:
            compartment_id: Compartment ID to analyze
        
        Returns:
            Dictionary with time estimates and cleanup strategy
        """
        summary = await self.get_compartment_resource_summary(compartment_id)
        
        # Calculate cleanup phases based on deletion order
        phases = {}
        total_time = 0
        
        for type_name, details in summary['resource_details'].items():
            if details['count'] > 0:
                # Get deletion order from resource class
                resource_class = get_registered_resource_types()[type_name]
                deletion_order = resource_class.deletion_order.value
                
                if deletion_order not in phases:
                    phases[deletion_order] = {
                        'resource_types': [],
                        'total_resources': 0,
                        'estimated_time': 0,
                        'protected_resources': 0
                    }
                
                phases[deletion_order]['resource_types'].append(type_name)
                phases[deletion_order]['total_resources'] += details['count']
                phases[deletion_order]['estimated_time'] += details['estimated_deletion_time']
                phases[deletion_order]['protected_resources'] += details['protected_resources']
        
        # Calculate total time (phases run sequentially)
        total_time = sum(phase['estimated_time'] for phase in phases.values())
        
        # Add time for protection disabling
        if summary['has_protected_resources']:
            total_time += 120  # Additional 2 minutes for protection disabling
        
        return {
            'compartment_id': compartment_id,
            'total_resources': summary['total_resources'],
            'estimated_total_time': total_time,
            'has_protected_resources': summary['has_protected_resources'],
            'cleanup_phases': phases,
            'recommendations': self._generate_cleanup_recommendations(summary, phases)
        }
    
    def _generate_cleanup_recommendations(self, summary: Dict[str, Any], phases: Dict[int, Any]) -> List[str]:
        """Generate cleanup recommendations based on resource analysis"""
        recommendations = []
        
        # Check for high-cost resources
        for type_name, details in summary['resource_details'].items():
            if details['count'] > 0:
                match type_name:
                    case "autonomous_database" | "mysql_db_system":
                        if details['count'] > 1:
                            recommendations.append(f"⚠️ Multiple databases ({details['count']}) detected - verify they're not production")
                    
                    case "oke_cluster":
                        recommendations.append(f"☸️ Kubernetes cluster(s) found - check for persistent volumes and load balancers")
                    
                    case "compute_instance":
                        if details['count'] > 5:
                            recommendations.append(f"🖥️ High compute usage ({details['count']} instances) - consider bulk operations")
        
        # Check for protection
        if summary['has_protected_resources']:
            recommendations.append("🔒 Some resources have delete protection - will be disabled automatically")
        
        # Time-based recommendations
        if summary['total_estimated_deletion_time'] > 1800:  # 30 minutes
            recommendations.append("⏰ Long cleanup time estimated - consider running during maintenance window")
        
        # Phase-based recommendations
        if len(phases) > 3:
            recommendations.append("📋 Multiple deletion phases required - progress will be shown per phase")
        
        if not recommendations:
            recommendations.append("✅ Compartment appears ready for safe cleanup")
        
        return recommendations


if __name__ == "__main__":
    async def main():
        """Example usage / testing"""
        print("OCIMgr Compartment Manager (Python 3.13)")
        print("Async compartment management with enhanced validation")
    
    asyncio.run(main())