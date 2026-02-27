"""
OCIMgr - Oracle Cloud Infrastructure Management Tool
"""

__version__ = "1.0.0"

def get_version() -> str:
    """Get the current version of OCIMgr"""
    return __version__

# Minimal exports for initial testing
__all__ = ["get_version"]
