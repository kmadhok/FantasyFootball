import logging
import time
import asyncio
from typing import Callable, Any, Optional, List, Dict
from functools import wraps
from enum import Enum
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

logger = logging.getLogger(__name__)

class RetryStrategy(Enum):
    """Retry strategies for different scenarios"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIXED_DELAY = "fixed_delay"
    LINEAR_BACKOFF = "linear_backoff"

class APIError(Exception):
    """Custom exception for API-related errors"""
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 retry_after: Optional[int] = None, platform: str = "unknown"):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after
        self.platform = platform

class RetryHandler:
    """Handles retry logic for API calls with various strategies"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.strategy = strategy
        self.retry_stats = {
            'total_attempts': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'platforms': {}
        }
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay based on retry strategy"""
        if self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay * (2 ** attempt)
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay * (attempt + 1)
        else:  # FIXED_DELAY
            delay = self.base_delay
        
        # Cap at max_delay
        return min(delay, self.max_delay)
    
    def _should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if we should retry based on the exception type"""
        if attempt >= self.max_retries:
            return False
        
        # Always retry on network-related errors
        if isinstance(exception, (ConnectionError, Timeout)):
            return True
        
        # Retry on specific HTTP errors
        if isinstance(exception, HTTPError):
            status_code = exception.response.status_code if exception.response else 0
            
            # Retry on server errors (5xx)
            if 500 <= status_code < 600:
                return True
            
            # Retry on rate limiting (429)
            if status_code == 429:
                return True
            
            # Retry on specific 4xx errors that might be transient
            if status_code in [408, 409, 423, 424, 425, 426, 428, 431]:
                return True
        
        # Retry on API-specific errors
        if isinstance(exception, APIError):
            return True
        
        return False
    
    def _get_retry_after(self, response) -> Optional[int]:
        """Extract retry-after header if present"""
        if hasattr(response, 'headers'):
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    return int(retry_after)
                except ValueError:
                    pass
        return None
    
    def _log_retry_attempt(self, attempt: int, exception: Exception, platform: str = "unknown"):
        """Log retry attempt with details"""
        logger.warning(f"API call failed (attempt {attempt + 1}/{self.max_retries + 1}) "
                      f"for {platform}: {type(exception).__name__}: {exception}")
    
    def _update_stats(self, platform: str, success: bool, attempts: int):
        """Update retry statistics"""
        self.retry_stats['total_attempts'] += attempts
        
        if success and attempts > 1:
            self.retry_stats['successful_retries'] += 1
        elif not success:
            self.retry_stats['failed_retries'] += 1
        
        if platform not in self.retry_stats['platforms']:
            self.retry_stats['platforms'][platform] = {
                'attempts': 0,
                'successes': 0,
                'failures': 0
            }
        
        self.retry_stats['platforms'][platform]['attempts'] += attempts
        if success:
            self.retry_stats['platforms'][platform]['successes'] += 1
        else:
            self.retry_stats['platforms'][platform]['failures'] += 1
    
    def retry_on_failure(self, platform: str = "unknown"):
        """Decorator to add retry logic to functions"""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None
                
                for attempt in range(self.max_retries + 1):
                    try:
                        result = func(*args, **kwargs)
                        self._update_stats(platform, True, attempt + 1)
                        return result
                    
                    except Exception as e:
                        last_exception = e
                        
                        if not self._should_retry(e, attempt):
                            break
                        
                        self._log_retry_attempt(attempt, e, platform)
                        
                        # Calculate delay
                        if isinstance(e, (HTTPError, APIError)) and hasattr(e, 'response'):
                            retry_after = self._get_retry_after(e.response)
                            delay = retry_after if retry_after else self._calculate_delay(attempt)
                        else:
                            delay = self._calculate_delay(attempt)
                        
                        logger.info(f"Retrying in {delay:.2f} seconds...")
                        time.sleep(delay)
                
                # All retries exhausted
                self._update_stats(platform, False, self.max_retries + 1)
                logger.error(f"All retry attempts exhausted for {platform}")
                raise last_exception
            
            return wrapper
        return decorator
    
    async def async_retry_on_failure(self, platform: str = "unknown"):
        """Async decorator to add retry logic to async functions"""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def wrapper(*args, **kwargs):
                last_exception = None
                
                for attempt in range(self.max_retries + 1):
                    try:
                        result = await func(*args, **kwargs)
                        self._update_stats(platform, True, attempt + 1)
                        return result
                    
                    except Exception as e:
                        last_exception = e
                        
                        if not self._should_retry(e, attempt):
                            break
                        
                        self._log_retry_attempt(attempt, e, platform)
                        
                        # Calculate delay
                        if isinstance(e, (HTTPError, APIError)) and hasattr(e, 'response'):
                            retry_after = self._get_retry_after(e.response)
                            delay = retry_after if retry_after else self._calculate_delay(attempt)
                        else:
                            delay = self._calculate_delay(attempt)
                        
                        logger.info(f"Retrying in {delay:.2f} seconds...")
                        await asyncio.sleep(delay)
                
                # All retries exhausted
                self._update_stats(platform, False, self.max_retries + 1)
                logger.error(f"All retry attempts exhausted for {platform}")
                raise last_exception
            
            return wrapper
        return decorator
    
    def get_retry_stats(self) -> Dict[str, Any]:
        """Get current retry statistics"""
        return self.retry_stats.copy()
    
    def reset_stats(self):
        """Reset retry statistics"""
        self.retry_stats = {
            'total_attempts': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'platforms': {}
        }

class CircuitBreaker:
    """Circuit breaker pattern for API calls"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
    
    def _should_attempt_call(self) -> bool:
        """Check if we should attempt the API call"""
        if self.state == "closed":
            return True
        
        if self.state == "open":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "half-open"
                return True
            return False
        
        if self.state == "half-open":
            return True
        
        return False
    
    def _on_success(self):
        """Handle successful API call"""
        self.failure_count = 0
        self.state = "closed"
    
    def _on_failure(self):
        """Handle failed API call"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if not self._should_attempt_call():
            raise APIError("Circuit breaker is open - API calls are temporarily disabled")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

# Global retry handler instances
sleeper_retry_handler = RetryHandler(max_retries=3, base_delay=1.0, strategy=RetryStrategy.EXPONENTIAL_BACKOFF)
mfl_retry_handler = RetryHandler(max_retries=3, base_delay=2.0, strategy=RetryStrategy.EXPONENTIAL_BACKOFF)
general_retry_handler = RetryHandler(max_retries=2, base_delay=1.0, strategy=RetryStrategy.FIXED_DELAY)

# Global circuit breakers
sleeper_circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=300)  # 5 minutes
mfl_circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=300)  # 5 minutes

def handle_api_request(func: Callable, platform: str = "unknown", 
                      use_circuit_breaker: bool = True) -> Callable:
    """Comprehensive API request handler with retry and circuit breaker"""
    
    # Select appropriate retry handler
    if platform.lower() == 'sleeper':
        retry_handler = sleeper_retry_handler
        circuit_breaker = sleeper_circuit_breaker
    elif platform.lower() == 'mfl':
        retry_handler = mfl_retry_handler
        circuit_breaker = mfl_circuit_breaker
    else:
        retry_handler = general_retry_handler
        circuit_breaker = None
    
    # Apply retry logic
    func = retry_handler.retry_on_failure(platform)(func)
    
    # Apply circuit breaker if enabled
    if use_circuit_breaker and circuit_breaker:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return circuit_breaker.call(func, *args, **kwargs)
        return wrapper
    
    return func

def safe_api_call(func: Callable, *args, default_return=None, platform: str = "unknown", **kwargs):
    """Safely execute an API call with comprehensive error handling"""
    try:
        enhanced_func = handle_api_request(func, platform=platform)
        return enhanced_func(*args, **kwargs)
    except Exception as e:
        logger.error(f"API call failed for {platform}: {type(e).__name__}: {e}")
        return default_return

def get_retry_statistics() -> Dict[str, Any]:
    """Get comprehensive retry statistics from all handlers"""
    return {
        'sleeper': sleeper_retry_handler.get_retry_stats(),
        'mfl': mfl_retry_handler.get_retry_stats(),
        'general': general_retry_handler.get_retry_stats()
    }

# Test function
def test_retry_handler():
    """Test the retry handler functionality"""
    print("Testing Retry Handler...")
    print("=" * 50)
    
    # Test with a function that always fails
    @sleeper_retry_handler.retry_on_failure(platform="test")
    def always_fails():
        raise requests.exceptions.ConnectionError("Test connection error")
    
    # Test with a function that succeeds after failures
    attempt_count = 0
    @mfl_retry_handler.retry_on_failure(platform="test")
    def succeeds_after_failures():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise requests.exceptions.Timeout("Test timeout")
        return "Success!"
    
    try:
        print("\n1. Testing function that always fails...")
        always_fails()
    except Exception as e:
        print(f"   Expected failure: {type(e).__name__}")
    
    try:
        print("\n2. Testing function that succeeds after failures...")
        result = succeeds_after_failures()
        print(f"   Result: {result}")
    except Exception as e:
        print(f"   Unexpected failure: {type(e).__name__}: {e}")
    
    print("\n3. Retry statistics:")
    stats = get_retry_statistics()
    for platform, platform_stats in stats.items():
        print(f"   {platform.upper()}:")
        print(f"     Total attempts: {platform_stats['total_attempts']}")
        print(f"     Successful retries: {platform_stats['successful_retries']}")
        print(f"     Failed retries: {platform_stats['failed_retries']}")

if __name__ == "__main__":
    test_retry_handler()