"""
API Client for Acceldata Environment.

This module provides functionality to make HTTP calls to the Acceldata environment
with proper authentication and headers.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path
import requests
from requests.exceptions import RequestException


class AcceldataAPIClient:
    """Client for making API calls to Acceldata environment."""
    
    def __init__(self, host: Optional[str] = None, access_key: Optional[str] = None, 
                 secret_key: Optional[str] = None, tenant: Optional[str] = None, 
                 env_file: Optional[str] = None, logger: Optional[logging.Logger] = None):
        """Initialize the API client.
        
        Args:
            host: The host URL (e.g., 'https://se-demo.acceldata.app')
            access_key: The access key for authentication
            secret_key: The secret key for authentication
            tenant: The tenant name
            env_file: Path to environment file containing host, keys, and tenant
            logger: Logger instance for logging
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # Load configuration from environment file if provided
        if env_file:
            self._load_env_config(env_file)
        else:
            self.host = host or os.getenv('AD_HOST')
            self.access_key = access_key or os.getenv('AD_SOURCE_ACCESS_KEY')
            self.secret_key = secret_key or os.getenv('AD_SOURCE_SECRET_KEY')
            self.tenant = tenant or os.getenv('AD_SOURCE_TENANT')
        
        # Validate configuration
        if not self.host:
            raise ValueError("Host URL is required. Set AD_HOST environment variable or provide host parameter.")
        
        if not self.access_key:
            raise ValueError("Access key is required. Set AD_SOURCE_ACCESS_KEY environment variable or provide access_key parameter.")
        
        if not self.secret_key:
            raise ValueError("Secret key is required. Set AD_SOURCE_SECRET_KEY environment variable or provide secret_key parameter.")
        
        if not self.tenant:
            raise ValueError("Tenant is required. Set AD_SOURCE_TENANT environment variable or provide tenant parameter.")
        
        # Remove trailing slash from host
        self.host = self.host.rstrip('/')
        
        # Setup session with default headers
        self.session = requests.Session()
        self._setup_default_headers()
        
        self.logger.info(f"API Client initialized for host: {self.host}, tenant: {self.tenant}")
    
    def _load_env_config(self, env_file: str) -> None:
        """Load configuration from environment file.
        
        Args:
            env_file: Path to the environment file
        """
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")
        
        self.logger.info(f"Loading configuration from: {env_file}")
        
        # Simple environment file parser
        config = {}
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip().strip('"').strip("'")
        
        self.host = config.get('AD_HOST')
        self.access_key = config.get('AD_SOURCE_ACCESS_KEY')
        self.secret_key = config.get('AD_SOURCE_SECRET_KEY')
        self.tenant = config.get('AD_SOURCE_TENANT')
        
        # Load target configuration if available
        self.target_access_key = config.get('AD_TARGET_ACCESS_KEY')
        self.target_secret_key = config.get('AD_TARGET_SECRET_KEY')
        self.target_tenant = config.get('AD_TARGET_TENANT')
        
        if not self.host or not self.access_key or not self.secret_key or not self.tenant:
            raise ValueError(f"Missing required configuration in {env_file}. Need AD_HOST, AD_SOURCE_ACCESS_KEY, AD_SOURCE_SECRET_KEY, and AD_SOURCE_TENANT")
    
    def _setup_default_headers(self) -> None:
        """Setup default headers for all requests."""
        self.session.headers.update({
            'accept': 'application/json',
            'accessKey': self.access_key,
            'secretKey': self.secret_key,
            'ad-tenant': self.tenant,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'x-domain-ids': ''
        })
    
    def get_asset_by_uid(self, uid: str) -> Dict[str, Any]:
        """Get asset details by UID.
        
        Args:
            uid: The asset UID to search for
            
        Returns:
            Dictionary containing the asset response
            
        Raises:
            RequestException: If the API call fails
        """
        url = f"{self.host}/catalog-server/api/assets?uid={uid}"
        
        self.logger.info(f"Getting asset details for UID: {uid}")
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Successfully retrieved asset details for UID: {uid}")
            return data
            
        except RequestException as e:
            self.logger.error(f"Failed to get asset details for UID {uid}: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the API connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Try a simple GET request to test connection
            response = self.session.get(f"{self.host}/catalog-server/api/health", timeout=10)
            response.raise_for_status()
            self.logger.info("API connection test successful")
            return True
        except RequestException as e:
            self.logger.error(f"API connection test failed: {e}")
            return False
    
    def close(self) -> None:
        """Close the session."""
        self.session.close()
        self.logger.info("API client session closed")

    def make_api_call(self, endpoint: str, method: str = 'GET', json_payload: Optional[Dict[str, Any]] = None, 
                     use_target_auth: bool = False, use_target_tenant: bool = False, return_binary: bool = False) -> Any:
        """Make a generic API call with configurable endpoint and method.
        
        Args:
            endpoint: The API endpoint (e.g., '/catalog-server/api/assets?uid=123')
            method: HTTP method ('GET' or 'PUT')
            json_payload: JSON payload for PUT/POST requests
            use_target_auth: Whether to use target access/secret keys instead of source
            use_target_tenant: Whether to use target tenant instead of source
            return_binary: If True, return raw response content (for binary data like ZIP files)
        
        Returns:
            Dictionary containing the API response, or bytes if return_binary is True
        
        Raises:
            RequestException: If the API call fails
        """
        # Determine which authentication and tenant to use
        if use_target_auth:
            access_key = getattr(self, 'target_access_key', None)
            secret_key = getattr(self, 'target_secret_key', None)
            if not access_key or not secret_key:
                raise ValueError("Target access key and secret key not configured")
        else:
            access_key = self.access_key
            secret_key = self.secret_key
        
        if use_target_tenant:
            tenant = getattr(self, 'target_tenant', None)
            if not tenant:
                raise ValueError("Target tenant not configured")
        else:
            tenant = self.tenant
        
        # Build the full URL
        url = f"{self.host}{endpoint}"
        
        # Setup headers for this request
        headers = {
            'accept': 'application/json',
            'accessKey': access_key,
            'secretKey': secret_key,
            'ad-tenant': tenant,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'x-domain-ids': ''
        }
        
        self.logger.info(f"Making {method} request to: {url}")
        if use_target_auth:
            self.logger.info("Using target authentication")
        if use_target_tenant:
            self.logger.info("Using target tenant")
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, headers=headers)
            elif method.upper() == 'PUT':
                if json_payload is None:
                    raise ValueError("JSON payload is required for PUT requests")
                response = self.session.put(url, headers=headers, json=json_payload)
            elif method.upper() == 'POST':
                if json_payload is None:
                    raise ValueError("JSON payload is required for POST requests")
                response = self.session.post(url, headers=headers, json=json_payload)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            if return_binary:
                self.logger.info(f"Returning binary content for {endpoint}")
                return response.content
            else:
                data = response.json()
                self.logger.info(f"Successfully completed {method} request to {endpoint}")
                return data
            
        except RequestException as e:
            self.logger.error(f"Failed to make {method} request to {endpoint}: {e}")
            raise


def create_api_client(env_file: Optional[str] = None, 
                     host: Optional[str] = None, 
                     access_key: Optional[str] = None,
                     secret_key: Optional[str] = None,
                     tenant: Optional[str] = None,
                     logger: Optional[logging.Logger] = None) -> AcceldataAPIClient:
    """Factory function to create an API client.
    
    Args:
        env_file: Path to environment file
        host: Host URL (overrides env_file)
        access_key: Access key (overrides env_file)
        secret_key: Secret key (overrides env_file)
        tenant: Tenant name (overrides env_file)
        logger: Logger instance
        
    Returns:
        Configured AcceldataAPIClient instance
    """
    return AcceldataAPIClient(
        host=host,
        access_key=access_key,
        secret_key=secret_key,
        tenant=tenant,
        env_file=env_file,
        logger=logger
    ) 