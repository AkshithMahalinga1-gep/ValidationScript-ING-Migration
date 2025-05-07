import sys
import os

# Get the prefix used to find platform-specific libraries
prefix = sys.prefix

# Get the standard installation prefix for the current platform
standard_prefix = sys.base_prefix

if prefix != standard_prefix:
    print(f"You are currently in a virtual environment: {prefix}")
else:
    print("You are NOT currently in a virtual environment.")