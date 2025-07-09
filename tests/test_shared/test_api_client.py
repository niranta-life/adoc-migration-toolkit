"""
Tests for the AcceldataAPIClient and related functionality.

This module contains comprehensive test cases for the API client functionality
in the shared module, including unit tests, integration tests, and mock tests.
"""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
from requests.exceptions import RequestException, Timeout, ConnectionError
import requests

from adoc_migration_toolkit.shared.api_client import AcceldataAPIClient, create_api_client


class TestAcceldataAPIClient:
    """Test cases for AcceldataAPIClient class."""

    def test_init_with_parameters(self):
        """Test client initialization with direct parameters."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        assert client.host == "https://test.acceldata.app"
        assert client.access_key == "test_access"
        assert client.secret_key == "test_secret"
        assert client.tenant == "test_tenant"

    def test_init_with_env_variables(self):
        """Test client initialization with environment variables."""
        with patch.dict(os.environ, {
            'AD_HOST': 'https://env.acceldata.app',
            'AD_SOURCE_ACCESS_KEY': 'env_access',
            'AD_SOURCE_SECRET_KEY': 'env_secret',
            'AD_SOURCE_TENANT': 'env_tenant'
        }):
            client = AcceldataAPIClient()
            
            assert client.host == "https://env.acceldata.app"
            assert client.access_key == "env_access"
            assert client.secret_key == "env_secret"
            assert client.tenant == "env_tenant"

    def test_init_with_env_file(self):
        """Test client initialization with environment file."""
        env_content = """
        AD_HOST=https://file.acceldata.app
        AD_SOURCE_ACCESS_KEY=file_access
        AD_SOURCE_SECRET_KEY=file_secret
        AD_SOURCE_TENANT=file_tenant
        AD_TARGET_ACCESS_KEY=target_access
        AD_TARGET_SECRET_KEY=target_secret
        AD_TARGET_TENANT=target_tenant
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file = f.name
        
        try:
            client = AcceldataAPIClient(env_file=env_file)
            
            assert client.host == "https://file.acceldata.app"
            assert client.access_key == "file_access"
            assert client.secret_key == "file_secret"
            assert client.tenant == "file_tenant"
            assert client.target_access_key == "target_access"
            assert client.target_secret_key == "target_secret"
            assert client.target_tenant == "target_tenant"
        finally:
            os.unlink(env_file)

    def test_init_missing_host(self):
        """Test initialization with missing host."""
        with pytest.raises(ValueError, match="Host URL is required"):
            AcceldataAPIClient(
                access_key="test_access",
                secret_key="test_secret",
                tenant="test_tenant"
            )

    def test_init_missing_access_key(self):
        """Test initialization with missing access key."""
        with pytest.raises(ValueError, match="Access key is required"):
            AcceldataAPIClient(
                host="https://test.acceldata.app",
                secret_key="test_secret",
                tenant="test_tenant"
            )

    def test_init_missing_secret_key(self):
        """Test initialization with missing secret key."""
        with pytest.raises(ValueError, match="Secret key is required"):
            AcceldataAPIClient(
                host="https://test.acceldata.app",
                access_key="test_access",
                tenant="test_tenant"
            )

    def test_init_missing_tenant(self):
        """Test initialization with missing tenant."""
        with pytest.raises(ValueError, match="Tenant is required"):
            AcceldataAPIClient(
                host="https://test.acceldata.app",
                access_key="test_access",
                secret_key="test_secret"
            )

    def test_init_env_file_not_found(self):
        """Test initialization with non-existent environment file."""
        with pytest.raises(FileNotFoundError):
            AcceldataAPIClient(env_file="/nonexistent/file.env")

    def test_init_env_file_missing_required_config(self):
        """Test initialization with environment file missing required config."""
        env_content = """
        AD_HOST=https://file.acceldata.app
        # Missing required keys
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file = f.name
        
        try:
            with pytest.raises(ValueError, match="Missing required configuration"):
                AcceldataAPIClient(env_file=env_file)
        finally:
            os.unlink(env_file)

    def test_host_trailing_slash_removal(self):
        """Test that trailing slash is removed from host URL."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app/",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        assert client.host == "https://test.acceldata.app"

    def test_default_headers_setup(self):
        """Test that default headers are properly set up."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        headers = client.session.headers
        assert headers['Accept'] == 'application/json'
        assert headers['accessKey'] == 'test_access'
        assert headers['secretKey'] == 'test_secret'
        assert headers['X-Tenant'] == 'test_tenant'
        assert 'User-Agent' in headers
        assert headers['x-domain-ids'] == ''

    def test_get_asset_by_uid_success(self):
        """Test successful asset retrieval by UID."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"uid": "test-123", "name": "Test Asset"}
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            result = client.get_asset_by_uid("test-123")
            assert result == {"uid": "test-123", "name": "Test Asset"}
            mock_get.assert_called_once_with(
                "https://test.acceldata.app/catalog-server/api/assets?uid=test-123",
                timeout=10
            )

    def test_get_asset_by_uid_timeout(self):
        """Test asset retrieval timeout handling."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', side_effect=Timeout("Request timed out")) as mock_get:
            with pytest.raises(Timeout):
                client.get_asset_by_uid("test-123")

    def test_get_asset_by_uid_request_exception(self):
        """Test asset retrieval request exception handling."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', side_effect=RequestException("Network error")) as mock_get:
            with pytest.raises(RequestException):
                client.get_asset_by_uid("test-123")

    def test_get_asset_by_uid_empty_uid(self):
        """Test asset retrieval with empty UID."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="Asset UID cannot be empty"):
            client.get_asset_by_uid("")

    def test_get_asset_by_uid_custom_timeout(self):
        """Test asset retrieval with custom timeout."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with patch.object(client.session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"uid": "test-123"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            client.get_asset_by_uid("test-123", timeout=30)
            
            mock_get.assert_called_once_with(
                "https://test.acceldata.app/catalog-server/api/assets?uid=test-123",
                timeout=30
            )

    def test_test_connection_success(self):
        """Test successful connection test."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            result = client.test_connection()
            assert result is True
            mock_get.assert_called_once_with(
                "https://test.acceldata.app/catalog-server/api/health",
                timeout=10
            )

    def test_test_connection_failure(self):
        """Test connection test failure."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', side_effect=RequestException("Connection failed")) as mock_get:
            result = client.test_connection()
            assert result is False

    def test_test_connection_timeout(self):
        """Test connection test timeout."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', side_effect=Timeout("Request timed out")) as mock_get:
            result = client.test_connection()
            assert result is False

    def test_close_session(self):
        """Test session closure."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with patch.object(client.session, 'close') as mock_close:
            client.close()
            mock_close.assert_called_once()

    def test_make_api_call_get_success(self):
        """Test successful GET API call."""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            result = client.make_api_call("/api/test")
            assert result == {"status": "success"}
            mock_get.assert_called_once_with(
                "https://test.acceldata.app/api/test",
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'accessKey': 'test_access',
                    'secretKey': 'test_secret',
                    'X-Tenant': 'test_tenant',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
                    'x-domain-ids': ''
                },
                timeout=10
            )

    def test_make_api_call_post_success(self):
        """Test successful POST API call."""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "created"}
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'post', return_value=mock_response) as mock_post:
            result = client.make_api_call("/api/test", method="POST", json_payload={"foo": "bar"})
            assert result == {"status": "created"}
            mock_post.assert_called_once_with(
                "https://test.acceldata.app/api/test",
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'accessKey': 'test_access',
                    'secretKey': 'test_secret',
                    'X-Tenant': 'test_tenant',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
                    'x-domain-ids': ''
                },
                json={"foo": "bar"},
                timeout=10
            )

    def test_make_api_call_put_success(self):
        """Test successful PUT API call."""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "updated"}
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'put', return_value=mock_response) as mock_put:
            result = client.make_api_call("/api/test", method="PUT", json_payload={"foo": "bar"})
            assert result == {"status": "updated"}
            mock_put.assert_called_once_with(
                "https://test.acceldata.app/api/test",
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'accessKey': 'test_access',
                    'secretKey': 'test_secret',
                    'X-Tenant': 'test_tenant',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
                    'x-domain-ids': ''
                },
                json={"foo": "bar"},
                timeout=10
            )

    def test_make_api_call_invalid_method(self):
        """Test API call with invalid HTTP method."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="Unsupported HTTP method"):
            client.make_api_call("/api/test", method="DELETE")

    def test_make_api_call_empty_endpoint(self):
        """Test API call with empty endpoint."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="Endpoint cannot be empty"):
            client.make_api_call("")

    def test_make_api_call_post_missing_payload(self):
        """Test POST API call without required payload."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="JSON payload or files are required"):
            client.make_api_call("/api/test", method="POST")

    def test_make_api_call_put_missing_payload(self):
        """Test PUT API call without required payload."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="JSON payload or files are required"):
            client.make_api_call("/api/test", method="PUT")

    def test_make_api_call_target_auth_not_configured(self):
        """Test API call with target auth when not configured."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="Target access key and secret key not configured"):
            client.make_api_call("/api/test", use_target_auth=True)

    def test_make_api_call_target_tenant_not_configured(self):
        """Test API call with target tenant when not configured."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        with pytest.raises(ValueError, match="Target tenant not configured"):
            client.make_api_call("/api/test", use_target_tenant=True)

    def test_make_api_call_with_files(self):
        """Test API call with file upload."""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "uploaded"}
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'post', return_value=mock_response) as mock_post:
            files = {"file": ("test.txt", "test content")}
            result = client.make_api_call("/api/upload", method="POST", files=files)
            assert result == {"status": "uploaded"}
            mock_post.assert_called_once()

    def test_make_api_call_return_binary(self):
        """Test API call returning binary content."""
        mock_response = Mock()
        mock_response.content = b"binary data"
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            result = client.make_api_call("/api/download", method="GET", return_binary=True)
            assert result == b"binary data"
            mock_get.assert_called_once()

    def test_make_api_call_custom_timeout(self):
        """Test API call with custom timeout."""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status.return_value = None
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            result = client.make_api_call("/api/test", method="GET", timeout=30)
            assert result == {"status": "success"}
            call_args = mock_get.call_args
            assert call_args[1]['timeout'] == 30

    def test_make_api_call_timeout_exception(self):
        """Test API call timeout exception."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', side_effect=Timeout("Request timed out")):
            with pytest.raises(Timeout):
                client.make_api_call("/api/test", method="GET")

    def test_make_api_call_request_exception(self):
        """Test API call request exception."""
        client = AcceldataAPIClient(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        with patch.object(client.session, 'get', side_effect=RequestException("Network error")):
            with pytest.raises(RequestException):
                client.make_api_call("/api/test", method="GET")

    def test_tenant_substitution_in_host(self):
        """Test that ${tenant} in AD_HOST is substituted correctly for source and target."""
        env_content = """
        AD_HOST=https://${tenant}.acceldata.app
        AD_SOURCE_ACCESS_KEY=source_key
        AD_SOURCE_SECRET_KEY=source_secret
        AD_SOURCE_TENANT=source-tenant
        AD_TARGET_ACCESS_KEY=target_key
        AD_TARGET_SECRET_KEY=target_secret
        AD_TARGET_TENANT=target-tenant
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file = f.name
        try:
            # Source client
            client_source = create_api_client(env_file=env_file, tenant_type="source")
            assert client_source.host == "https://source-tenant.acceldata.app"
            assert client_source.tenant == "source-tenant"
            # Target client
            client_target = create_api_client(env_file=env_file, tenant_type="target")
            assert client_target.host == "https://target-tenant.acceldata.app"
            assert client_target.tenant == "source-tenant"  # tenant always set to source for now
        finally:
            os.unlink(env_file)


class TestCreateAPIClient:
    """Test cases for create_api_client factory function."""

    def test_create_api_client_with_parameters(self):
        """Test factory function with direct parameters."""
        client = create_api_client(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        assert isinstance(client, AcceldataAPIClient)
        assert client.host == "https://test.acceldata.app"
        assert client.access_key == "test_access"
        assert client.secret_key == "test_secret"
        assert client.tenant == "test_tenant"

    def test_create_api_client_with_env_file(self):
        """Test factory function with environment file."""
        env_content = """
        AD_HOST=https://file.acceldata.app
        AD_SOURCE_ACCESS_KEY=file_access
        AD_SOURCE_SECRET_KEY=file_secret
        AD_SOURCE_TENANT=file_tenant
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file = f.name
        
        try:
            client = create_api_client(env_file=env_file)
            
            assert isinstance(client, AcceldataAPIClient)
            assert client.host == "https://file.acceldata.app"
            assert client.access_key == "file_access"
            assert client.secret_key == "file_secret"
            assert client.tenant == "file_tenant"
        finally:
            os.unlink(env_file)

    def test_create_api_client_with_logger(self):
        """Test factory function with custom logger."""
        logger = Mock()
        
        client = create_api_client(
            host="https://test.acceldata.app",
            access_key="test_access",
            secret_key="test_secret",
            tenant="test_tenant",
            logger=logger
        )
        
        assert isinstance(client, AcceldataAPIClient)
        assert client.logger == logger

    def test_create_api_client_missing_config(self):
        """Test factory function with missing configuration."""
        with pytest.raises(ValueError):
            create_api_client()


if __name__ == "__main__":
    pytest.main([__file__]) 