#!/usr/bin/env python3
"""
Example script demonstrating how to use the Acceldata API Client.

This script shows how to:
1. Create an API client
2. Get asset segments
3. Create new segments
4. Update existing segments
"""

import sys
import json
from pathlib import Path

# Add the src directory to the path so we can import our package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from adoc_migration_toolkit import create_api_client, setup_logging


def main():
    """Example usage of the Acceldata API Client."""
    
    # Setup logging
    logger = setup_logging(verbose=True, log_level="INFO")
    
    try:
        # Create API client using environment file
        # You can also pass host and bearer_token directly
        client = create_api_client(
            env_file="config.env",  # Path to your config file
            logger=logger
        )
        
        # Test the connection
        if not client.test_connection():
            logger.error("Failed to connect to API")
            return
        
        # Example: Get segments for asset ID 15697168
        asset_id = 15697168
        logger.info(f"Getting segments for asset {asset_id}")
        
        try:
            segments = client.get_asset_segments(asset_id)
            print(f"Asset {asset_id} segments:")
            print(json.dumps(segments, indent=2))
        except Exception as e:
            logger.error(f"Failed to get segments: {e}")
        
        # Example: Create a new segment
        logger.info("Creating a new segment...")
        
        new_segment_conditions = [
            {
                "id": None,
                "columnId": 15697355,
                "condition": "CUSTOM",
                "value": "Chicago"
            },
            {
                "id": None,
                "columnId": 15697334,
                "condition": "CUSTOM",
                "value": "0|1|2|3|4|6|11|17|20|22|40|51|67|75|92|95|97|98|99|100"
            }
        ]
        
        try:
            result = client.create_segment(
                asset_id=asset_id,
                segment_name="WEATHER_LOCATION-CLOUD_COVER",
                conditions=new_segment_conditions
            )
            print("Segment created successfully:")
            print(json.dumps(result, indent=2))
        except Exception as e:
            logger.error(f"Failed to create segment: {e}")
        
        # Close the client
        client.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 