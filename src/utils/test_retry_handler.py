import pytest
import unittest.mock as mock
from unittest.mock import Mock, patch, MagicMock
import time
import asyncio
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError

from src.utils.retry_handler import (
    RetryHandler, RetryStrategy, APIError, CircuitBreaker,
    handle_api_request, safe_api_call, get_retry_statistics,
    sleeper_retry_handler, mfl_retry_handler, general_retry_handler,
    sleeper_circuit_breaker, mfl_circuit_breaker
)


class TestAPIError:
    """Test cases for APIError exception class"""
    
    def test_api_error_creation(self):
        """Test APIError creation with all parameters"""
        error = APIError(
            message="Test error", 
            status_code=500, 
            retry_after=60,
            platform="test"
        )
        
        assert str(error) == "Test error"
        assert error.status_code == 500
        assert error.retry_after == 60
        assert error.platform == "test"
    
    def test_api_error_defaults(self):
        """Test APIError with default parameters"""
        error = APIError("Simple error")
        
        assert str(error) == "Simple error"
        assert error.status_code is None
        assert error.retry_after is None
        assert error.platform == "unknown"


class TestRetryStrategy:
    """Test cases for RetryStrategy enum"""
    
    def test_retry_strategy_values(self):
        """Test RetryStrategy enum values"""
        assert RetryStrategy.EXPONENTIAL_BACKOFF.value == "exponential_backoff"
        assert RetryStrategy.FIXED_DELAY.value == "fixed_delay"
        assert RetryStrategy.LINEAR_BACKOFF.value == "linear_backoff"


class TestRetryHandler:
    """Test cases for RetryHandler class"""
    
    @pytest.fixture
    def retry_handler(self):
        """Create a RetryHandler instance for testing"""
        return RetryHandler(
            max_retries=3,
            base_delay=1.0,
            max_delay=10.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF
        )
    
    def test_retry_handler_creation(self, retry_handler):
        """Test RetryHandler initialization"""
        assert retry_handler.max_retries == 3
        assert retry_handler.base_delay == 1.0
        assert retry_handler.max_delay == 10.0
        assert retry_handler.strategy == RetryStrategy.EXPONENTIAL_BACKOFF
        assert retry_handler.retry_stats["total_attempts"] == 0
    
    def test_calculate_delay_exponential(self, retry_handler):
        """Test exponential backoff delay calculation"""
        # Test exponential backoff
        retry_handler.strategy = RetryStrategy.EXPONENTIAL_BACKOFF
        retry_handler.base_delay = 1.0
        retry_handler.max_delay = 10.0
        
        assert retry_handler._calculate_delay(0) == 1.0  # 1 * 2^0
        assert retry_handler._calculate_delay(1) == 2.0  # 1 * 2^1
        assert retry_handler._calculate_delay(2) == 4.0  # 1 * 2^2
        assert retry_handler._calculate_delay(3) == 8.0  # 1 * 2^3
        assert retry_handler._calculate_delay(4) == 10.0  # Capped at max_delay
    
    def test_calculate_delay_linear(self, retry_handler):
        """Test linear backoff delay calculation"""
        retry_handler.strategy = RetryStrategy.LINEAR_BACKOFF
        retry_handler.base_delay = 2.0
        retry_handler.max_delay = 10.0
        
        assert retry_handler._calculate_delay(0) == 2.0  # 2 * (0 + 1)
        assert retry_handler._calculate_delay(1) == 4.0  # 2 * (1 + 1)
        assert retry_handler._calculate_delay(2) == 6.0  # 2 * (2 + 1)
        assert retry_handler._calculate_delay(3) == 8.0  # 2 * (3 + 1)
        assert retry_handler._calculate_delay(4) == 10.0  # Capped at max_delay
    
    def test_calculate_delay_fixed(self, retry_handler):
        """Test fixed delay calculation"""
        retry_handler.strategy = RetryStrategy.FIXED_DELAY
        retry_handler.base_delay = 3.0
        
        assert retry_handler._calculate_delay(0) == 3.0
        assert retry_handler._calculate_delay(1) == 3.0
        assert retry_handler._calculate_delay(2) == 3.0
        assert retry_handler._calculate_delay(10) == 3.0
    
    def test_should_retry_network_errors(self, retry_handler):
        """Test retry logic for network errors"""
        # Should retry on network errors
        assert retry_handler._should_retry(ConnectionError("Connection failed"), 0) is True
        assert retry_handler._should_retry(Timeout("Request timeout"), 1) is True
        
        # Should not retry if max retries exceeded
        assert retry_handler._should_retry(ConnectionError("Connection failed"), 3) is False
    
    def test_should_retry_http_errors(self, retry_handler):
        """Test retry logic for HTTP errors"""
        # Mock HTTP error responses
        mock_response_500 = Mock()
        mock_response_500.status_code = 500
        error_500 = HTTPError(response=mock_response_500)
        
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        error_429 = HTTPError(response=mock_response_429)
        
        mock_response_404 = Mock()
        mock_response_404.status_code = 404
        error_404 = HTTPError(response=mock_response_404)
        
        # Should retry on 5xx errors
        assert retry_handler._should_retry(error_500, 0) is True
        
        # Should retry on 429 (rate limiting)
        assert retry_handler._should_retry(error_429, 1) is True
        
        # Should not retry on 404
        assert retry_handler._should_retry(error_404, 0) is False
    
    def test_should_retry_api_errors(self, retry_handler):
        """Test retry logic for API errors"""
        api_error = APIError("API failed", status_code=500)
        
        # Should retry on API errors
        assert retry_handler._should_retry(api_error, 0) is True
        assert retry_handler._should_retry(api_error, 2) is True
        
        # Should not retry if max retries exceeded
        assert retry_handler._should_retry(api_error, 3) is False
    
    def test_get_retry_after(self, retry_handler):
        """Test extracting retry-after header"""
        # Mock response with retry-after header
        mock_response = Mock()
        mock_response.headers = {"Retry-After": "30"}
        
        result = retry_handler._get_retry_after(mock_response)
        assert result == 30
        
        # Mock response without retry-after header
        mock_response.headers = {}
        result = retry_handler._get_retry_after(mock_response)
        assert result is None
        
        # Mock response with invalid retry-after header
        mock_response.headers = {"Retry-After": "invalid"}
        result = retry_handler._get_retry_after(mock_response)
        assert result is None
    
    def test_update_stats(self, retry_handler):
        """Test retry statistics updating"""
        # Test successful retry
        retry_handler._update_stats("test_platform", True, 2)
        
        stats = retry_handler.retry_stats
        assert stats["total_attempts"] == 2
        assert stats["successful_retries"] == 1
        assert stats["failed_retries"] == 0
        assert stats["platforms"]["test_platform"]["attempts"] == 2
        assert stats["platforms"]["test_platform"]["successes"] == 1
        
        # Test failed retry
        retry_handler._update_stats("test_platform", False, 3)
        
        stats = retry_handler.retry_stats
        assert stats["total_attempts"] == 5  # 2 + 3
        assert stats["successful_retries"] == 1
        assert stats["failed_retries"] == 1
        assert stats["platforms"]["test_platform"]["attempts"] == 5
        assert stats["platforms"]["test_platform"]["failures"] == 1
    
    @patch('src.utils.retry_handler.time.sleep')
    def test_retry_decorator_success(self, mock_sleep, retry_handler):
        """Test retry decorator with successful function"""
        call_count = 0
        
        @retry_handler.retry_on_failure("test_platform")
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = test_function()
        
        assert result == "success"
        assert call_count == 1
        mock_sleep.assert_not_called()
    
    @patch('src.utils.retry_handler.time.sleep')
    def test_retry_decorator_success_after_retries(self, mock_sleep, retry_handler):
        """Test retry decorator with function that succeeds after failures"""
        call_count = 0
        
        @retry_handler.retry_on_failure("test_platform")
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
            return "success"
        
        result = test_function()
        
        assert result == "success"
        assert call_count == 3
        assert mock_sleep.call_count == 2  # Two retries
    
    @patch('src.utils.retry_handler.time.sleep')
    def test_retry_decorator_failure(self, mock_sleep, retry_handler):
        """Test retry decorator with function that always fails"""
        call_count = 0
        
        @retry_handler.retry_on_failure("test_platform")
        def test_function():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Connection failed")
        
        with pytest.raises(ConnectionError):
            test_function()
        
        assert call_count == 4  # Initial + 3 retries
        assert mock_sleep.call_count == 3  # Three retries
    
    @pytest.mark.asyncio
    @patch('src.utils.retry_handler.asyncio.sleep')
    async def test_async_retry_decorator(self, mock_sleep, retry_handler):
        """Test async retry decorator"""
        call_count = 0
        
        @retry_handler.async_retry_on_failure("test_platform")
        async def test_async_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "async_success"
        
        result = await test_async_function()
        
        assert result == "async_success"
        assert call_count == 2
        mock_sleep.assert_called_once()
    
    def test_get_retry_stats(self, retry_handler):
        """Test getting retry statistics"""
        # Add some test data
        retry_handler.retry_stats["total_attempts"] = 10
        retry_handler.retry_stats["successful_retries"] = 3
        
        stats = retry_handler.get_retry_stats()
        
        assert stats["total_attempts"] == 10
        assert stats["successful_retries"] == 3
        
        # Should return a copy, not the original
        stats["total_attempts"] = 999
        assert retry_handler.retry_stats["total_attempts"] == 10
    
    def test_reset_stats(self, retry_handler):
        """Test resetting retry statistics"""
        # Add some test data
        retry_handler.retry_stats["total_attempts"] = 10
        retry_handler.retry_stats["platforms"]["test"] = {"attempts": 5}
        
        retry_handler.reset_stats()
        
        assert retry_handler.retry_stats["total_attempts"] == 0
        assert retry_handler.retry_stats["successful_retries"] == 0
        assert retry_handler.retry_stats["failed_retries"] == 0
        assert retry_handler.retry_stats["platforms"] == {}


class TestCircuitBreaker:
    """Test cases for CircuitBreaker class"""
    
    @pytest.fixture
    def circuit_breaker(self):
        """Create a CircuitBreaker instance for testing"""
        return CircuitBreaker(failure_threshold=3, timeout=60)
    
    def test_circuit_breaker_creation(self, circuit_breaker):
        """Test CircuitBreaker initialization"""
        assert circuit_breaker.failure_threshold == 3
        assert circuit_breaker.timeout == 60
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.state == "closed"
        assert circuit_breaker.last_failure_time is None
    
    def test_should_attempt_call_closed(self, circuit_breaker):
        """Test should_attempt_call when circuit is closed"""
        assert circuit_breaker._should_attempt_call() is True
    
    @patch('src.utils.retry_handler.time.time')
    def test_should_attempt_call_open_timeout_not_reached(self, mock_time, circuit_breaker):
        """Test should_attempt_call when circuit is open and timeout not reached"""
        circuit_breaker.state = "open"
        circuit_breaker.last_failure_time = 100
        mock_time.return_value = 150  # 50 seconds later, timeout is 60
        
        assert circuit_breaker._should_attempt_call() is False
    
    @patch('src.utils.retry_handler.time.time')
    def test_should_attempt_call_open_timeout_reached(self, mock_time, circuit_breaker):
        """Test should_attempt_call when circuit is open and timeout reached"""
        circuit_breaker.state = "open"
        circuit_breaker.last_failure_time = 100
        mock_time.return_value = 170  # 70 seconds later, timeout is 60
        
        result = circuit_breaker._should_attempt_call()
        
        assert result is True
        assert circuit_breaker.state == "half-open"
    
    def test_should_attempt_call_half_open(self, circuit_breaker):
        """Test should_attempt_call when circuit is half-open"""
        circuit_breaker.state = "half-open"
        
        assert circuit_breaker._should_attempt_call() is True
    
    def test_on_success(self, circuit_breaker):
        """Test _on_success method"""
        circuit_breaker.failure_count = 5
        circuit_breaker.state = "half-open"
        
        circuit_breaker._on_success()
        
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.state == "closed"
    
    @patch('src.utils.retry_handler.time.time')
    def test_on_failure_below_threshold(self, mock_time, circuit_breaker):
        """Test _on_failure when below threshold"""
        mock_time.return_value = 123.456
        
        circuit_breaker._on_failure()
        
        assert circuit_breaker.failure_count == 1
        assert circuit_breaker.last_failure_time == 123.456
        assert circuit_breaker.state == "closed"
    
    @patch('src.utils.retry_handler.time.time')
    def test_on_failure_threshold_reached(self, mock_time, circuit_breaker):
        """Test _on_failure when threshold is reached"""
        mock_time.return_value = 123.456
        circuit_breaker.failure_count = 2  # One below threshold
        
        circuit_breaker._on_failure()
        
        assert circuit_breaker.failure_count == 3
        assert circuit_breaker.last_failure_time == 123.456
        assert circuit_breaker.state == "open"
    
    def test_call_success(self, circuit_breaker):
        """Test successful function call through circuit breaker"""
        def test_function():
            return "success"
        
        result = circuit_breaker.call(test_function)
        
        assert result == "success"
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.state == "closed"
    
    def test_call_failure(self, circuit_breaker):
        """Test failed function call through circuit breaker"""
        def test_function():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            circuit_breaker.call(test_function)
        
        assert circuit_breaker.failure_count == 1
    
    def test_call_circuit_open(self, circuit_breaker):
        """Test function call when circuit is open"""
        circuit_breaker.state = "open"
        
        def test_function():
            return "should not be called"
        
        with pytest.raises(APIError) as exc_info:
            circuit_breaker.call(test_function)
        
        assert "Circuit breaker is open" in str(exc_info.value)


class TestModuleFunctions:
    """Test module-level functions"""
    
    @patch('src.utils.retry_handler.sleeper_retry_handler')
    @patch('src.utils.retry_handler.sleeper_circuit_breaker')
    def test_handle_api_request_sleeper(self, mock_circuit_breaker, mock_retry_handler):
        """Test handle_api_request with Sleeper platform"""
        mock_retry_handler.retry_on_failure.return_value = lambda f: f
        mock_circuit_breaker.call = Mock(side_effect=lambda f, *args, **kwargs: f(*args, **kwargs))
        
        def test_function():
            return "test_result"
        
        enhanced_function = handle_api_request(test_function, platform="sleeper")
        result = enhanced_function()
        
        assert result == "test_result"
        mock_retry_handler.retry_on_failure.assert_called_once_with("sleeper")
    
    @patch('src.utils.retry_handler.mfl_retry_handler')
    @patch('src.utils.retry_handler.mfl_circuit_breaker')
    def test_handle_api_request_mfl(self, mock_circuit_breaker, mock_retry_handler):
        """Test handle_api_request with MFL platform"""
        mock_retry_handler.retry_on_failure.return_value = lambda f: f
        mock_circuit_breaker.call = Mock(side_effect=lambda f, *args, **kwargs: f(*args, **kwargs))
        
        def test_function():
            return "test_result"
        
        enhanced_function = handle_api_request(test_function, platform="mfl")
        result = enhanced_function()
        
        assert result == "test_result"
        mock_retry_handler.retry_on_failure.assert_called_once_with("mfl")
    
    @patch('src.utils.retry_handler.general_retry_handler')
    def test_handle_api_request_unknown_platform(self, mock_retry_handler):
        """Test handle_api_request with unknown platform"""
        mock_retry_handler.retry_on_failure.return_value = lambda f: f
        
        def test_function():
            return "test_result"
        
        enhanced_function = handle_api_request(test_function, platform="unknown")
        result = enhanced_function()
        
        assert result == "test_result"
        mock_retry_handler.retry_on_failure.assert_called_once_with("unknown")
    
    @patch('src.utils.retry_handler.handle_api_request')
    def test_safe_api_call_success(self, mock_handle):
        """Test safe_api_call with successful function"""
        mock_enhanced_func = Mock(return_value="success")
        mock_handle.return_value = mock_enhanced_func
        
        def test_function():
            return "original_result"
        
        result = safe_api_call(test_function, platform="test")
        
        assert result == "success"
        mock_handle.assert_called_once_with(test_function, platform="test")
    
    @patch('src.utils.retry_handler.handle_api_request')
    def test_safe_api_call_failure(self, mock_handle):
        """Test safe_api_call with failing function"""
        mock_enhanced_func = Mock(side_effect=Exception("API failed"))
        mock_handle.return_value = mock_enhanced_func
        
        def test_function():
            return "original_result"
        
        result = safe_api_call(test_function, default_return="default", platform="test")
        
        assert result == "default"
    
    @patch('src.utils.retry_handler.sleeper_retry_handler')
    @patch('src.utils.retry_handler.mfl_retry_handler')
    @patch('src.utils.retry_handler.general_retry_handler')
    def test_get_retry_statistics(self, mock_general, mock_mfl, mock_sleeper):
        """Test get_retry_statistics function"""
        mock_sleeper.get_retry_stats.return_value = {"sleeper": "stats"}
        mock_mfl.get_retry_stats.return_value = {"mfl": "stats"}
        mock_general.get_retry_stats.return_value = {"general": "stats"}
        
        result = get_retry_statistics()
        
        assert result["sleeper"] == {"sleeper": "stats"}
        assert result["mfl"] == {"mfl": "stats"}
        assert result["general"] == {"general": "stats"}


class TestGlobalInstances:
    """Test global retry handlers and circuit breakers"""
    
    def test_global_retry_handlers_exist(self):
        """Test that global retry handlers are properly initialized"""
        assert sleeper_retry_handler is not None
        assert mfl_retry_handler is not None
        assert general_retry_handler is not None
        
        # Test configurations
        assert sleeper_retry_handler.max_retries == 3
        assert mfl_retry_handler.max_retries == 3
        assert general_retry_handler.max_retries == 2
        
        assert sleeper_retry_handler.base_delay == 1.0
        assert mfl_retry_handler.base_delay == 2.0
        assert general_retry_handler.base_delay == 1.0
    
    def test_global_circuit_breakers_exist(self):
        """Test that global circuit breakers are properly initialized"""
        assert sleeper_circuit_breaker is not None
        assert mfl_circuit_breaker is not None
        
        # Test configurations
        assert sleeper_circuit_breaker.failure_threshold == 5
        assert mfl_circuit_breaker.failure_threshold == 5
        
        assert sleeper_circuit_breaker.timeout == 300
        assert mfl_circuit_breaker.timeout == 300


class TestIntegration:
    """Integration tests for retry handler components"""
    
    @patch('src.utils.retry_handler.time.sleep')
    def test_full_retry_flow(self, mock_sleep):
        """Test complete retry flow with real RetryHandler"""
        handler = RetryHandler(max_retries=2, base_delay=0.1, strategy=RetryStrategy.EXPONENTIAL_BACKOFF)
        
        call_count = 0
        
        @handler.retry_on_failure("integration_test")
        def intermittent_function():
            nonlocal call_count
            call_count += 1
            
            if call_count == 1:
                raise ConnectionError("First attempt fails")
            elif call_count == 2:
                raise Timeout("Second attempt fails")
            else:
                return f"Success on attempt {call_count}"
        
        result = intermittent_function()
        
        assert result == "Success on attempt 3"
        assert call_count == 3
        assert mock_sleep.call_count == 2
        
        # Check statistics
        stats = handler.get_retry_stats()
        assert stats["total_attempts"] == 3
        assert stats["successful_retries"] == 1
        assert stats["platforms"]["integration_test"]["successes"] == 1
    
    def test_circuit_breaker_integration(self):
        """Test circuit breaker integration with retry handler"""
        circuit_breaker = CircuitBreaker(failure_threshold=2, timeout=1)
        
        call_count = 0
        
        def failing_function():
            nonlocal call_count
            call_count += 1
            raise ConnectionError(f"Failure {call_count}")
        
        # First failure
        with pytest.raises(ConnectionError):
            circuit_breaker.call(failing_function)
        
        # Second failure - should open circuit
        with pytest.raises(ConnectionError):
            circuit_breaker.call(failing_function)
        
        # Circuit should now be open
        assert circuit_breaker.state == "open"
        
        # Third call should be blocked by circuit breaker
        with pytest.raises(APIError) as exc_info:
            circuit_breaker.call(failing_function)
        
        assert "Circuit breaker is open" in str(exc_info.value)
        assert call_count == 2  # Third call was blocked


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__])