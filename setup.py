#!/usr/bin/env python3
from setuptools import setup, find_packages
import os

# Read requirements
def read_requirements():
    req_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(req_file):
        with open(req_file) as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return [
        "oci>=2.100.0",
        "click>=8.1.0", 
        "tqdm>=4.64.0",
    ]

setup(
    name="ocimgr",
    version="1.0.0",
    author="OCIMgr Development Team",
    description="Oracle Cloud Infrastructure Management Tool",
    packages=find_packages(),  # This automatically finds 'ocimgr' package
    install_requires=read_requirements(),
    entry_points={
        "console_scripts": [
            "ocimgr=ocimgr.cli:cli",
        ],
    },
    python_requires=">=3.8",  # Changed from 3.13 for compatibility
    include_package_data=True,
    zip_safe=False,
)