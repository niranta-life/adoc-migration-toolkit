"""
API Client for Acceldata Environment.

This module provides a robust HTTP client for making API calls to the Acceldata environment
with proper authentication, headers, and timeout management. The client supports both
source and target environment configurations for migration scenarios.

Key Features:
- Configurable timeout management (default: 10 seconds)
- Support for source and target environment authentication
- Comprehensive error handling and logging
- File upload support via multipart/form-data
- Environment file configuration support
- Session management for connection reuse

Example Usage:
    # Create client from environment file
    client = create_api_client(env_file='config.env')
    
    # Test connection
    if client.test_connection():
        # Get asset details
        asset = client.get_asset_by_uid('asset-123')
        
        # Make custom API call
        response = client.make_api_call('/api/endpoint', method='POST', json_payload={'key': 'value'})
    
    # Clean up
    client.close()
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError


class AcceldataAPIClient:
    """
    Robust HTTP client for Acceldata API interactions.
    
    This client provides a comprehensive interface for making authenticated API calls
    to Acceldata environments with proper timeout management, error handling, and
    support for both source and target configurations.
    
    Attributes:
        host (str): The base URL of the Acceldata environment
        access_key (str): Authentication access key
        secret_key (str): Authentication secret key  
        tenant (str): Tenant identifier
        session (requests.Session): HTTP session for connection reuse
        logger (logging.Logger): Logger instance for operation tracking
        target_access_key (Optional[str]): Target environment access key
        target_secret_key (Optional[str]): Target environment secret key
        target_tenant (Optional[str]): Target environment tenant
    """
    
    # Default timeout for all API calls (10 seconds)
    DEFAULT_TIMEOUT = 10
    
    def __init__(self, host: Optional[str] = None, access_key: Optional[str] = None, 
                 secret_key: Optional[str] = None, tenant: Optional[str] = None, 
                 env_file: Optional[str] = None, logger: Optional[logging.Logger] = None, 
                 tenant_type: str = "source"):
        """
        Initialize the Acceldata API client.
        
        Configuration can be provided directly via parameters or loaded from an
        environment file. Environment variables are used as fallback for missing
        parameters.
        
        Args:
            host: The host URL (e.g., 'https://se-demo.acceldata.app' or 'https://${tenant}.acceldata.app')
            access_key: The access key for authentication
            secret_key: The secret key for authentication
            tenant: The tenant name
            env_file: Path to environment file containing host, keys, and tenant
            logger: Logger instance for logging operations
            tenant_type: 'source' or 'target' - which tenant to use for ${tenant} substitution
        Raises:
            ValueError: If required configuration parameters are missing
            FileNotFoundError: If the specified environment file doesn't exist
        """
        self.logger = logger or logging.getLogger(__name__)
        self.tenant_type = tenant_type
        self.log_file_path = None  # Will be set from environment file if available
        self.host_template = None  # Will store the original host URL with ${tenant} placeholder
        
        # Load configuration from environment file if provided
        if env_file:
            self._load_env_config(env_file, tenant_type=tenant_type)
        else:
            self.host = host or os.getenv('AD_HOST')
            self.access_key = access_key or os.getenv('AD_SOURCE_ACCESS_KEY')
            self.secret_key = secret_key or os.getenv('AD_SOURCE_SECRET_KEY')
            self.tenant = tenant or os.getenv('AD_SOURCE_TENANT')
            # Check for log file path in environment variables
            self.log_file_path = os.getenv('AD_LOG_FILE_PATH')
            # Store the original host template if it contains ${tenant}
            if self.host and "${tenant}" in self.host:
                self.host_template = self.host
                # If host contains ${tenant}, substitute with correct tenant
                sub_tenant = self.tenant
                self.host = self.host.replace("${tenant}", sub_tenant)
        # Validate required configuration
        self._validate_configuration()
        # Remove trailing slash from host for consistency
        self.host = self.host.rstrip('/')
        # Setup session with default headers
        self.session = requests.Session()
        self._setup_default_headers()
        self.logger.info(f"API Client initialized for host: {self.host}, tenant: {self.tenant}")
    
    def _validate_configuration(self) -> None:
        """
        Validate that all required configuration parameters are present.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        if not self.host:
            raise ValueError("Host URL is required. Set AD_HOST environment variable or provide host parameter.")
        
        if not self.access_key:
            raise ValueError("Access key is required. Set AD_SOURCE_ACCESS_KEY environment variable or provide access_key parameter.")
        
        if not self.secret_key:
            raise ValueError("Secret key is required. Set AD_SOURCE_SECRET_KEY environment variable or provide secret_key parameter.")
        
        if not self.tenant:
            raise ValueError("Tenant is required. Set AD_SOURCE_TENANT environment variable or provide tenant parameter.")
    
    def _load_env_config(self, env_file: str, tenant_type: str = "source") -> None:
        """
        Load configuration from environment file.
        
        Parses a simple key=value format environment file and extracts
        Acceldata-specific configuration parameters.
        
        Args:
            env_file: Path to the environment file
            tenant_type: 'source' or 'target' - which tenant to use for ${tenant} substitution
        Raises:
            FileNotFoundError: If the environment file doesn't exist
            ValueError: If required configuration is missing from the file
        """
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")
        self.logger.info(f"Loading configuration from: {env_file}")
        # Parse environment file
        config = self._parse_env_file(env_path)
        # Extract source configuration
        self.host = config.get('AD_HOST')
        self.access_key = config.get('AD_SOURCE_ACCESS_KEY')
        self.secret_key = config.get('AD_SOURCE_SECRET_KEY')
        self.tenant = config.get('AD_SOURCE_TENANT')
        # Extract target configuration if available
        self.target_access_key = config.get('AD_TARGET_ACCESS_KEY')
        self.target_secret_key = config.get('AD_TARGET_SECRET_KEY')
        self.target_tenant = config.get('AD_TARGET_TENANT')
        # Extract log file path if available
        self.log_file_path = config.get('AD_LOG_FILE_PATH')
        # Store the original host template if it contains ${tenant}
        if self.host and "${tenant}" in self.host:
            self.host_template = self.host
            # Substitute ${tenant} in host
            if tenant_type == "target" and self.target_tenant:
                sub_tenant = self.target_tenant
            else:
                sub_tenant = self.tenant
            self.host = self.host.replace("${tenant}", sub_tenant)
        # Validate required configuration
        if not self.host or not self.access_key or not self.secret_key or not self.tenant:
            raise ValueError(f"Missing required configuration in {env_file}. Need AD_HOST, AD_SOURCE_ACCESS_KEY, AD_SOURCE_SECRET_KEY, and AD_SOURCE_TENANT")
    
    def _parse_env_file(self, env_path: Path) -> Dict[str, str]:
        """
        Parse environment file and extract key-value pairs.
        
        Args:
            env_path: Path to the environment file
            
        Returns:
            Dictionary containing parsed configuration
        """
        config = {}
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip().strip('"').strip("'")
        except UnicodeDecodeError as e:
            self.logger.error(f"Failed to decode environment file {env_path}: {e}")
            raise ValueError(f"Environment file {env_path} contains invalid encoding")
        
        return config
    
    def _setup_default_headers(self) -> None:
        """
        Setup default headers for all requests.
        
        Configures the session with standard headers including authentication
        credentials and content type preferences.
        """
        self.session.headers.update({
            'accept': 'application/json',
            'accessKey': self.access_key,
            'secretKey': self.secret_key,
            'X-Tenant': self.tenant,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'x-domain-ids': ''
        })
    
    def get_asset_by_uid(self, uid: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        Retrieve asset details by UID.
        
        Makes a GET request to the catalog server API to fetch asset information
        using the provided UID.
        
        Args:
            uid: The asset UID to search for
            timeout: Request timeout in seconds (default: 10 seconds)
            
        Returns:
            Dictionary containing the asset response data
            
        Raises:
            RequestException: If the API call fails due to network or server errors
            Timeout: If the request times out
            ValueError: If the UID is empty or invalid
        """
        if not uid or not uid.strip():
            raise ValueError("Asset UID cannot be empty")
        
        timeout = timeout or self.DEFAULT_TIMEOUT
        host_url = self._build_host_url(use_target_tenant=False)  # Always use source for this method
        url = f"{host_url}/catalog-server/api/assets?uid={uid}"
        
        self.logger.info(f"Getting asset details for UID: {uid} (timeout: {timeout}s)")
        
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Successfully retrieved asset details for UID: {uid}")
            return data
            
        except Timeout:
            self.logger.error(f"Request timed out while getting asset details for UID {uid}")
            raise
        except RequestException as e:
            self.logger.error(f"Failed to get asset details for UID {uid}: {e}")
            raise
    
    def test_connection(self, timeout: Optional[int] = None) -> bool:
        """
        Test the API connection by making a health check request.
        
        Performs a simple GET request to the health endpoint to verify
        connectivity and authentication.
        
        Args:
            timeout: Request timeout in seconds (default: 10 seconds)
            
        Returns:
            True if connection is successful, False otherwise
        """
        timeout = timeout or self.DEFAULT_TIMEOUT
        
        try:
            host_url = self._build_host_url(use_target_tenant=False)  # Always use source for connection test
            response = self.session.get(f"{host_url}/catalog-server/api/health", timeout=timeout)
            response.raise_for_status()
            self.logger.info("API connection test successful")
            return True
        except (RequestException, Timeout, ConnectionError) as e:
            self.logger.error(f"API connection test failed: {e}")
            return False
    
    def get_log_file_path(self) -> Optional[str]:
        """
        Get the log file path from configuration.
        
        Returns:
            Log file path if configured, None otherwise
        """
        return self.log_file_path

    def close(self) -> None:
        """
        Close the HTTP session and clean up resources.
        
        Should be called when the client is no longer needed to properly
        close network connections and free resources.
        """
        if hasattr(self, 'session'):
            self.session.close()
            self.logger.info("API client session closed")

    def _build_host_url(self, use_target_tenant: bool = False) -> str:
        """
        Build the host URL with the correct tenant substitution.
        
        Args:
            use_target_tenant: Whether to use target tenant
            
        Returns:
            Host URL with tenant substituted
        """
        if self.host_template and "${tenant}" in self.host_template:
            # Use the template and substitute with the correct tenant
            if use_target_tenant and self.target_tenant:
                tenant = self.target_tenant
            else:
                tenant = self.tenant
            return self.host_template.replace("${tenant}", tenant).rstrip('/')
        else:
            # No template, use the host as is
            return self.host.rstrip('/')

    def make_api_call(self, endpoint: str, method: str = 'GET', json_payload: Optional[Dict[str, Any]] = None, 
                     use_target_auth: bool = False, use_target_tenant: bool = False, return_binary: bool = False,
                     files: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> Any:
        """
        Make a generic API call with configurable endpoint and method.
        
        This is the primary method for making HTTP requests to the Acceldata API.
        It supports all common HTTP methods, file uploads, and flexible authentication
        configuration for both source and target environments.
        
        Args:
            endpoint: The API endpoint (e.g., '/catalog-server/api/assets?uid=123')
            method: HTTP method ('GET', 'PUT', or 'POST')
            json_payload: JSON payload for PUT/POST requests
            use_target_auth: Whether to use target access/secret keys instead of source
            use_target_tenant: Whether to use target tenant instead of source
            return_binary: If True, return raw response content (for binary data like ZIP files)
            files: Files to upload for multipart/form-data requests
            timeout: Request timeout in seconds (default: 10 seconds)
        
        Returns:
            Dictionary containing the API response, or bytes if return_binary is True
        
        Raises:
            ValueError: If required parameters are missing or invalid
            RequestException: If the API call fails due to network or server errors
            Timeout: If the request times out
        """
        # Validate method parameter
        method = method.upper()
        if method not in ['GET', 'PUT', 'POST']:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        # Validate endpoint
        if not endpoint or not endpoint.strip():
            raise ValueError("Endpoint cannot be empty")
        
        # Set default timeout
        timeout = timeout or self.DEFAULT_TIMEOUT
        
        # Determine authentication and tenant configuration
        access_key, secret_key = self._get_auth_credentials(use_target_auth)
        tenant = self._get_tenant(use_target_tenant)
        
        # Build the full URL with dynamic host
        host_url = self._build_host_url(use_target_tenant)
        url = f"{host_url}{endpoint}"
        
        # Setup headers for this request
        headers = self._build_request_headers(access_key, secret_key, tenant)
        
        # Log request details
        self._log_request_details(method, url, timeout, use_target_auth, use_target_tenant, files)
        
        try:
            response = self._execute_request(method, url, headers, json_payload, files, timeout)
            response.raise_for_status()
            
            return self._process_response(response, endpoint, method, return_binary)
            
        except Timeout:
            self.logger.error(f"Request timed out for {method} {endpoint}")
            raise
        except RequestException as e:
            self._log_error_details(e, method, endpoint)
            raise
    
    def _get_auth_credentials(self, use_target_auth: bool) -> tuple[str, str]:
        """
        Get authentication credentials based on configuration.
        
        Args:
            use_target_auth: Whether to use target credentials
            
        Returns:
            Tuple of (access_key, secret_key)
            
        Raises:
            ValueError: If target credentials are not configured
        """
        if use_target_auth:
            access_key = getattr(self, 'target_access_key', None)
            secret_key = getattr(self, 'target_secret_key', None)
            if not access_key or not secret_key:
                raise ValueError("Target access key and secret key not configured")
        else:
            access_key = self.access_key
            secret_key = self.secret_key
        
        return access_key, secret_key
    
    def _get_tenant(self, use_target_tenant: bool) -> str:
        """
        Get tenant identifier based on configuration.
        
        Args:
            use_target_tenant: Whether to use target tenant
            
        Returns:
            Tenant identifier
            
        Raises:
            ValueError: If target tenant is not configured
        """
        if use_target_tenant:
            tenant = getattr(self, 'target_tenant', None)
            if not tenant:
                raise ValueError("Target tenant not configured")
        else:
            tenant = self.tenant
        
        return tenant
    
    def _build_request_headers(self, access_key: str, secret_key: str, tenant: str) -> Dict[str, str]:
        """
        Build request headers for API calls.
        
        Args:
            access_key: Authentication access key
            secret_key: Authentication secret key
            tenant: Tenant identifier
            
        Returns:
            Dictionary containing request headers
        """
        return {
            'accept': 'application/json',
            'accessKey': access_key,
            'secretKey': secret_key,
            'X-Tenant': tenant,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'x-domain-ids': ''
        }
    
    def _log_request_details(self, method: str, url: str, timeout: int, 
                           use_target_auth: bool, use_target_tenant: bool, 
                           files: Optional[Dict[str, Any]]) -> None:
        """
        Log request details for debugging and monitoring.
        
        Args:
            method: HTTP method
            url: Full request URL
            timeout: Request timeout
            use_target_auth: Whether using target authentication
            use_target_tenant: Whether using target tenant
            files: Files being uploaded
        """
        self.logger.info(f"Making {method} request to: {url} (timeout: {timeout}s)")
        if use_target_auth:
            self.logger.info("Using target authentication")
        if use_target_tenant:
            self.logger.info("Using target tenant")
        if files:
            self.logger.info("Uploading files in multipart/form-data format")
    
    def _execute_request(self, method: str, url: str, headers: Dict[str, str], 
                        json_payload: Optional[Dict[str, Any]], 
                        files: Optional[Dict[str, Any]], timeout: int) -> requests.Response:
        """
        Execute the HTTP request based on method and parameters.
        
        Args:
            method: HTTP method
            url: Request URL
            headers: Request headers
            json_payload: JSON payload for PUT/POST
            files: Files for multipart upload
            timeout: Request timeout
            
        Returns:
            HTTP response object
            
        Raises:
            ValueError: If required parameters are missing for the method
        """
        if method == 'GET':
            return self.session.get(url, headers=headers, timeout=timeout)
        elif method == 'PUT':
            if json_payload is None and files is None:
                raise ValueError("JSON payload or files are required for PUT requests")
            if files:
                return self.session.put(url, headers=headers, files=files, timeout=timeout)
            else:
                return self.session.put(url, headers=headers, json=json_payload, timeout=timeout)
        elif method == 'POST':
            if json_payload is None and files is None:
                raise ValueError("JSON payload or files are required for POST requests")
            if files:
                return self.session.post(url, headers=headers, files=files, timeout=timeout)
            else:
                return self.session.post(url, headers=headers, json=json_payload, timeout=timeout)
    
    def _process_response(self, response: requests.Response, endpoint: str, 
                         method: str, return_binary: bool) -> Any:
        """
        Process the HTTP response and return appropriate data.
        
        Args:
            response: HTTP response object
            endpoint: API endpoint that was called
            method: HTTP method used
            return_binary: Whether to return binary content
            
        Returns:
            Response data (dict for JSON, bytes for binary)
        """
        if return_binary:
            self.logger.info(f"Returning binary content for {endpoint}")
            return response.content
        else:
            data = response.json()
            self.logger.info(f"Successfully completed {method} request to {endpoint}")
            return data
    
    def _log_error_details(self, exception: RequestException, method: str, endpoint: str) -> None:
        """
        Log detailed error information for debugging.
        
        Args:
            exception: The request exception that occurred
            method: HTTP method that was attempted
            endpoint: API endpoint that was called
        """
        self.logger.error(f"Failed to make {method} request to {endpoint}: {exception}")
        
        # Log response content for debugging if available
        if hasattr(exception, 'response') and exception.response is not None:
            try:
                self.logger.error(f"Response status code: {exception.response.status_code}")
                self.logger.error(f"Response headers: {dict(exception.response.headers)}")
                self.logger.error(f"Response content: {exception.response.text}")
            except Exception as log_error:
                self.logger.error(f"Could not log response content: {log_error}")


def create_api_client(env_file: Optional[str] = None, 
                     host: Optional[str] = None, 
                     access_key: Optional[str] = None,
                     secret_key: Optional[str] = None,
                     tenant: Optional[str] = None,
                     logger: Optional[logging.Logger] = None,
                     tenant_type: str = "source") -> AcceldataAPIClient:
    """
    Factory function to create a configured Acceldata API client.
    
    This function provides a convenient way to create an AcceldataAPIClient instance
    with proper configuration. Parameters provided directly will override values
    from the environment file or environment variables.
    
    Args:
        env_file: Path to environment file containing configuration
        host: Host URL (overrides env_file and environment variables)
        access_key: Access key (overrides env_file and environment variables)
        secret_key: Secret key (overrides env_file and environment variables)
        tenant: Tenant name (overrides env_file and environment variables)
        logger: Logger instance for operation tracking
        tenant_type: 'source' or 'target' - which tenant to use for ${tenant} substitution
    Returns:
        Configured AcceldataAPIClient instance
    Raises:
        ValueError: If required configuration parameters are missing
        FileNotFoundError: If the specified environment file doesn't exist
    """
    return AcceldataAPIClient(
        host=host,
        access_key=access_key,
        secret_key=secret_key,
        tenant=tenant,
        env_file=env_file,
        logger=logger,
        tenant_type=tenant_type
    ) 