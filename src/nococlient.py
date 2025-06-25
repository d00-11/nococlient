import os
import logging
import time
import requests
import random
import datetime
import colorlog

from email.utils import parsedate_to_datetime
from typing import Optional, Union, Dict, List, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path


# Exception hierarchy
class NoCoDBError(Exception):
    """Base exception class for NoCoDB errors."""
    pass

class NoCoDBAPIError(NoCoDBError):
    """General API error with context."""
    def __init__(self, message, original_exception=None, context=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.context = context or {}

class NoCoDBConnectionError(NoCoDBAPIError):
    """Raised when connection to NoCoDB server fails."""
    pass

class NoCoDBTimeoutError(NoCoDBAPIError):
    """Raised when request times out."""
    pass

class NoCoDBAuthError(NoCoDBError):
    """Raised when authentication fails."""
    pass

class NoCoDBPermissionError(NoCoDBError):
    """Raised when permission is denied."""
    pass

class NoCoDBNotFoundError(NoCoDBError):
    """Raised when requested resource is not found."""
    pass

class NoCoDBValidationError(NoCoDBError):
    """Raised when request validation fails."""
    def __init__(self, message, status_code=None, details=None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}

class NoCoDBRateLimitError(NoCoDBError):
    """Raised when rate limit is exceeded."""
    def __init__(self, message, retry_after=None, details=None):
        super().__init__(message)
        self.retry_after = retry_after
        self.details = details or {}

class NoCoDBServerError(NoCoDBError):
    """Raised when server-side errors occur."""
    def __init__(self, message, status_code=None, details=None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}

class NoCoDBResponseError(NoCoDBError):
    """Raised when response parsing fails."""
    pass

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay: float = 0.1  # seconds
    max_delay: float = 10.0  # seconds
    max_total_delay: float = 30.0  # seconds
    retry_status_codes: set = frozenset({408, 429, 500, 502, 503, 504})

@dataclass
class NocoDBConfig:
    """Configuration class for NocoDB API settings."""
    base_url: str
    api_key: str

    @property
    def headers(self) -> dict[str, str]:
        """Return headers required for API requests."""
        return {"xc-token": self.api_key}

class NocoDBClient:

    #region -- class definitions and static fucntions ---
    ALLOWED_FIELDS = {
        "ai", "altered", "ck", "clen", "column_name", "ct", "dt", "dtx",
        "dtxp", "dtxs", "np", "nrqd", "ns", "pk", "rqd", "title", "uicn",
        "uidt", "uip", "un", "columns", "table_name", "colOptions", "meta"
    }

    FORBIDDEN_COLUMN_NAMES = {
        "updated_at", "created_at", "created_by", "updated_by", "nc_order"
    }

    FORBIDDEN_UIDTS = {"LinkToAnotherRecord", "Links", "ForeignKey"}

    VALID_UIDTS = {
        'Links', 'ID', 'SingleLineText', 'LongText', 'Attachment', 'Checkbox',
        'MultiSelect', 'SingleSelect', 'Collaborator', 'Date', 'Year',
        'Time', 'PhoneNumber', 'Email', 'URL', 'Number', 'Decimal',
        'Currency', 'Percent', 'Duration', 'Rating', 'Formula', 'Rollup',
        'Count', 'DateTime', 'CreateTime', 'LastModifiedTime', 'AutoNumber',
        'Geometry', 'JSON', 'SpecificDBType'
    }


    def __init__(self):
        """
        Initialize NocoDBClient with optional custom configuration.
        Sets up proper error handling and logging.
        """
        # Configure logging with colors
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            # Only add handlers if none exist, to avoid duplicate logs
            log_colors = {
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            }
            formatter = colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                log_colors=log_colors
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Initialize configuration and caches
        self.config = self._get_global_config()
        self._sessions = {}
        self._table_caches = {}
        self._column_caches = {}
        self._base_caches = {}
        
        self.logger.info(f"NocoDBClient initialized with base URL: {self.config.base_url}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clear all caches
        self.clear_cache()
        # Close all sessions
        self.close()

    @staticmethod
    def _calculate_retry_delay(attempt: int, config: RetryConfig) -> float:
        """Calculate delay for the next retry attempt using exponential backoff."""
        delay = min(
            config.base_delay * (2 ** attempt),  # exponential backoff
            config.max_delay
        )
        # Add jitter (±10%) to prevent thundering herd
        jitter = delay * 0.1 * (2 * random.random() - 1)
        return delay + jitter
    @staticmethod
    def _should_retry(exc: requests.RequestException, config: RetryConfig) -> bool:
        """Determine if the request should be retried based on the exception."""
        if isinstance(exc, requests.Timeout):
            return True
        if isinstance(exc, requests.HTTPError):
            return exc.response.status_code in config.retry_status_codes
        return isinstance(exc, (requests.ConnectionError, requests.URLRequired))
    @staticmethod
    def _get_global_config() -> NocoDBConfig:
        """
        Return the global configuration for NocoDB API.

        Returns:
            NocoDBConfig: Configuration object with NocoDB settings
        """
        load_dotenv()
        return NocoDBConfig(
            base_url=os.getenv('NOCODB_BASE_URL', 'http://localhost:8080/api/v2'),
            api_key=os.getenv('NOCODB_API_KEY', 'default-api-key')
        )

    def _create_session(self) -> requests.Session:
        """
        Create and configure a requests session with retry logic.

        Returns:
            requests.Session: Configured session object
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "PATCH"],
            raise_on_status=True
        )

        # Mount retry strategy for both HTTP and HTTPS
        session.mount('http://', HTTPAdapter(max_retries=retry_strategy))
        session.mount('https://', HTTPAdapter(max_retries=retry_strategy))

        # Set default headers
        session.headers.update(self.config.headers)

        return session

    def close(self, session_key: Optional[str] = None) -> None:
        """
        Close sessions and clean up resources.

        Args:
            session_key: Optional session identifier to close. If None, closes all sessions.
        """
        if session_key is None:
            # Close all sessions
            for session in self._sessions.values():
                session.close()
            self._sessions.clear()
        elif session_key in self._sessions:
            self._sessions[session_key].close()
            del self._sessions[session_key]

    def get_session(self, session_key: str = 'default') -> requests.Session:
        """
        Get or create a session for a specific key.

        Args:
            session_key: Unique identifier for the session. Defaults to 'default'.

        Returns:
            requests.Session: Configured session object
        """
        if session_key not in self._sessions:
            self._sessions[session_key] = self._create_session()
        return self._sessions[session_key]

    def validate_connection(self, session_key: str = 'default') -> bool:
        """
        Validate the connection to NocoDB by attempting to list bases.
        Handles connection errors gracefully with minimal retries for initial connection.

        Args:
            session_key: Session identifier to use for validation

        Returns:
            bool: True if connection is valid, False otherwise
        """
        max_attempts = 2  # Limited retries for initial connection
        attempt = 0
        
        while attempt < max_attempts:
            try:
                endpoint = f"{self.config.base_url}/meta/bases"
                session = self.get_session(session_key)
                response = session.get(endpoint, timeout=5)  # Shorter timeout for connectivity check
                response.raise_for_status()
                
                bases = response.json().get("list", [])
                self.logger.info(f"Successfully connected to NocoDB. Found {len(bases)} bases.")
                return True
            
            except requests.ConnectionError as e:
                attempt += 1
                if attempt < max_attempts:
                    self.logger.warning(f"Connection failed, retrying ({attempt}/{max_attempts}): {e}")
                    time.sleep(1)  # Short delay before retry
                else:
                    self.logger.error(f"Connection failed after {max_attempts} attempts: {e}")
                    self.logger.error(f"NocoDB server may not be running at {self.config.base_url}")
                    return False
            
            except Exception as e:
                self.logger.error(f"Validation failed: {e}")
                return False
        
        return False

    def clear_cache(self, session_key: Optional[str] = None) -> None:
        """
        Clear cached data for specific or all sessions.

        Args:
            session_key: Optional session identifier to clear. If None, clears all sessions.
        """
        if session_key is None:
            # Clear all caches
            self._table_caches.clear()
            self._column_caches.clear()
            self._base_caches.clear()
            self._fetch_table_id.cache_clear()
            self._fetch_column_id.cache_clear()
        else:
            # Clear specific session cache
            if session_key in self._table_caches:
                del self._table_caches[session_key]
            if session_key in self._column_caches:
                del self._column_caches[session_key]
            if session_key in self._base_caches:
                del self._base_caches[session_key]
    @staticmethod
    def _parse_retry_after(raw_header: str, default: int = 60) -> int:
        """
        Parse a Retry-After header value, supporting both delta-seconds and HTTP-date formats.
        Returns an integer number of seconds to wait.
        """
        if not raw_header:
            return default
        # Try delta-seconds
        try:
            return int(raw_header)
        except ValueError:
            pass

        # Try HTTP-date per RFC 7231
        try:
            dt = parsedate_to_datetime(raw_header)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            delta = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            return max(0, int(delta))
        except (TypeError, ValueError) as e:
            logging.warning("Could not parse Retry-After header %r: %s", raw_header, e)
            return default

    def _raise_for_status_with_mapping(self, response: requests.Response) -> None:
        """
        Inspect response.status_code and raise appropriate NoCoDB* exception, including API-provided details.
        """
        code = response.status_code
        text = response.text

        if code == 401:
            raise NoCoDBAuthError(f"Authentication failed ({code}): {text}")
        if code == 403:
            raise NoCoDBPermissionError(f"Permission denied ({code}): {text}")
        if code == 404:
            raise NoCoDBNotFoundError(f"Resource not found ({code}): {text}")
        if code == 429:
            raw = response.headers.get('Retry-After')
            retry_secs = self._parse_retry_after(raw)
            # attempt to capture JSON error body too
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            raise NoCoDBRateLimitError(
                f"Rate limit exceeded ({code}), retry after {retry_secs}s. Details: {payload or text}",
                retry_after=retry_secs,
                details=payload
            )
        if 400 <= code < 500:
            # Client-side validation errors, include full JSON payload
            try:
                payload = response.json()
                msg = payload.get('message') or payload.get('error') or repr(payload)
            except ValueError:
                payload = {}
                msg = text
            raise NoCoDBValidationError(
                f"Validation failed ({code}): {msg}. Payload: {payload}",
                status_code=code,
                details=payload
            )
        if code >= 500:
            # Server-side errors, include JSON or text
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            raise NoCoDBServerError(
                f"Server error ({code}): {payload or text}",
                status_code=code,
                details=payload
            )

        # For any other non-2xx, let requests handle it, but capture JSON if possible
        try:
            response.raise_for_status()
        except requests.HTTPError as http_exc:
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            raise NoCoDBAPIError(
                f"Unexpected status ({code}): {payload or text}",
                original_exception=http_exc,
                context={'status_code': code, 'details': payload}
            )

    #endregion
    #region -- Raw functions ---
    def _request_raw(
            self,
            method: str,
            endpoint: str,
            *,
            session_key: str = 'default',
            json: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
            data: Optional[Dict[str, Any]] = None,
            files: Optional[Dict[str, Any]] = None,
            params: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            retry_config: Optional[RetryConfig] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Low-level HTTP request with retries, detailed error mapping, and schema validation.
        """
        retry_config = retry_config or RetryConfig()
        session = self.get_session(session_key)
        start = time.monotonic()
        last_exc = None
        last_resp = None

        for attempt in range(retry_config.max_retries):
            try:
                resp = session.request(
                    method, endpoint,
                    json=json, data=data, files=files, params=params,
                    timeout=timeout
                )
                last_resp = resp

                # Map error status codes to custom exceptions
                self._raise_for_status_with_mapping(resp)

                # Successful 2xx: parse JSON with validation
                try:
                    body = resp.json()
                except ValueError as e:
                    ct = resp.headers.get('Content-Type', 'unknown')
                    preview = resp.text[:500] + ('...' if len(resp.text) > 500 else '')
                    raise NoCoDBResponseError(
                        f"JSON parse error: {e}; Content-Type={ct}; Preview='{preview}'"
                    )
                return body

            except (requests.ConnectionError) as exc:
                last_exc = NoCoDBConnectionError(
                    f"Connection failed to {endpoint}: {exc}", original_exception=exc
                )
            except requests.Timeout as exc:
                last_exc = NoCoDBTimeoutError(
                    f"Timeout after {timeout}s calling {endpoint}", original_exception=exc
                )
            except NoCoDBRateLimitError as exc:
                last_exc = exc
                wait = exc.retry_after
                if wait and attempt + 1 < retry_config.max_retries:
                    logging.warning("Rate-limited, sleeping %ds before retry…", wait)
                    time.sleep(min(wait, retry_config.max_delay))
                    continue
            except (NoCoDBAuthError, NoCoDBPermissionError,
                    NoCoDBNotFoundError, NoCoDBValidationError,
                    NoCoDBResponseError, NoCoDBServerError) as exc:
                last_exc = exc
                break
            except requests.RequestException as exc:
                last_exc = exc

            # Retryable exceptions? backoff
            elapsed = time.monotonic() - start
            if (attempt + 1 < retry_config.max_retries and
                    elapsed < retry_config.max_total_delay and
                    self._should_retry(last_exc, retry_config)):
                delay = self._calculate_retry_delay(attempt, retry_config)
                logging.warning(
                    "%s %s failed (attempt %d/%d): %s. retry in %.2fs",
                    method, endpoint, attempt+1, retry_config.max_retries,
                    last_exc, delay
                )
                time.sleep(delay)
                continue
            break

        # Exhausted retries or fatal error
        context = {
            'method': method,
            'endpoint': endpoint,
            'attempts': retry_config.max_retries,
            'elapsed_seconds': time.monotonic() - start,
            'status_code': getattr(last_resp, 'status_code', None),
            'headers': dict(getattr(last_resp, 'headers', {}))
        }
        # Sanitize logs
        if isinstance(last_exc, NoCoDBAPIError):
            last_exc.context.update(context)
        logging.error(
            "%s %s ultimately failed: %s; context=%s",
            method, endpoint, last_exc, context
        )
        if isinstance(last_exc, NoCoDBError):
            raise last_exc
        raise NoCoDBAPIError(
            f"{method} {endpoint} failed: {last_exc}", original_exception=last_exc, context=context
        )

    def _post_raw(
            self,
            endpoint: str,
            *,
            session_key: str = 'default',
            json: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
            data: Optional[Dict[str, Any]] = None,
            files: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            retry_config: Optional[RetryConfig] = None
    ) -> Optional[Dict[str, Any]]:
        """Delegate POST requests to _request_raw with enhanced error handling."""
        return self._request_raw(
            'POST', endpoint,
            session_key=session_key,
            json=json, data=data, files=files,
            timeout=timeout, retry_config=retry_config
        )

    def _get_raw(
            self,
            endpoint: str,
            *,
            session_key: str = 'default',
            params: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            retry_config: Optional[RetryConfig] = None
    ) -> Optional[Dict[str, Any]]:
        """Delegate GET requests to _request_raw with enhanced error handling."""
        return self._request_raw(
            'GET', endpoint,
            session_key=session_key,
            params=params,
            timeout=timeout, retry_config=retry_config
        )

    def _delete_raw(
            self,
            endpoint: str,
            *,
            session_key: str = 'default',
            json: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
            data: Optional[Dict[str, Any]] = None,
            files: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            retry_config: Optional[RetryConfig] = None
    ) -> Optional[Dict[str, Any]]:
        """Delegate DELETE requests to _request_raw with enhanced error handling."""
        return self._request_raw(
            'DELETE', endpoint,
            session_key=session_key,
            json=json, data=data, files=files,
            timeout=timeout, retry_config=retry_config
        )

    def _patch_raw(
            self,
            endpoint: str,
            *,
            session_key: str = 'default',
            json: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
            data: Optional[Dict[str, Any]] = None,
            files: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            retry_config: Optional[RetryConfig] = None
    ) -> Optional[Dict[str, Any]]:
        """Delegate PATCH requests to _request_raw with enhanced error handling."""
        return self._request_raw(
            'PATCH', endpoint,
            session_key=session_key,
            json=json, data=data, files=files,
            timeout=timeout, retry_config=retry_config
        )

    # endregion
    # region -- Base functions ---

    def get_base_id(self, base_name: str, session_key: str = 'default', disambiguation_strategy: str = 'first') -> \
    Optional[str]:
        """
        Get base ID using hybrid caching strategy (session cache + LRU cache).
        Handles duplicate base names according to the specified disambiguation strategy.

        Args:
            base_name: Name of the base
            session_key: Session identifier
            disambiguation_strategy: Strategy to use when duplicate bases exist:
                - 'first': Return the first match (default)
                - 'newest': Return the most recently created base
                - 'oldest': Return the oldest created base
                - 'error': Raise an error if duplicates exist

        Returns:
            Optional[str]: Base ID if found, None otherwise

        Raises:
            ValueError: If disambiguation_strategy is 'error' and duplicate bases are found
        """
        # Initialize cache for this session if not exists
        if session_key not in self._base_caches:
            self._base_caches[session_key] = {}

        # Check session-specific cache first
        if base_name in self._base_caches[session_key]:
            return self._base_caches[session_key][base_name]

        # Try LRU cache next
        base_id = self._fetch_base_id(base_name, session_key)
        if base_id:
            # Update session cache with the found value
            self._base_caches[session_key][base_name] = base_id
            return base_id

        # If not found in either cache, fetch from API
        bases = self.list_bases(session_key)
        if not bases:
            self.logger.error(f"Failed to retrieve bases from NocoDB")
            return None

        # Find bases with matching name
        matching_bases = [base for base in bases if base.get("title") == base_name]

        if not matching_bases:
            self.logger.warning(f"Base '{base_name}' not found")
            return None

        if len(matching_bases) > 1:
            # Multiple bases with the same name found
            self.logger.warning(
                f"Multiple bases found with name '{base_name}'. Using {disambiguation_strategy} strategy.")

            if disambiguation_strategy == 'error':
                ids = [b.get("id") for b in matching_bases]
                raise ValueError(f"Multiple bases found with name '{base_name}': {ids}")

            elif disambiguation_strategy == 'newest':
                # Sort by creation time (newest first)
                matching_bases.sort(key=lambda b: b.get("created_at", ""), reverse=True)
                self.logger.info(f"Selected newest base '{base_name}' with ID {matching_bases[0].get('id')}")

            elif disambiguation_strategy == 'oldest':
                # Sort by creation time (oldest first)
                matching_bases.sort(key=lambda b: b.get("created_at", ""))
                self.logger.info(f"Selected oldest base '{base_name}' with ID {matching_bases[0].get('id')}")

        # Use the selected base (first by default, or after sorting)
        selected_base = matching_bases[0]
        base_id = selected_base.get("id")

        if base_id:
            # Update both caches
            self._base_caches[session_key][base_name] = base_id
            self._fetch_base_id.cache_clear()  # Clear LRU cache to force update

        return base_id

    @lru_cache(maxsize=100)
    def _fetch_base_id(self, base_name: str, session_key: str) -> Optional[str]:
        """
        Fetch the base ID for a given base name with LRU caching.
        Used as a fallback cache in the hybrid caching strategy.

        Args:
            base_name: Name of the base to look up
            session_key: Session identifier

        Returns:
            Optional[str]: Base ID if found, None otherwise
        """
        bases = self.list_bases(session_key)
        if not bases:
            return None

        for base in bases:
            if base.get("title") == base_name:
                return base.get("id")
        return None

    def create_base(
            self,
            base_name: str,
            *,
            description: Optional[str] = None,
            icon_color: Optional[str] = None,
            session_key: str = 'default',
            prevent_duplicates: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new base in NocoDB.

        Args:
            base_name: Name of the base to create
            description: Optional description of the base
            icon_color: Optional color for the base icon (e.g., "#FF0000")
            session_key: Session identifier to use for the request
            prevent_duplicates: If True, automatically generate a unique name
                                by adding a suffix (_2, _3, etc.) if needed

        Returns:
            Optional[Dict[str, Any]]: JSON response containing the created base information if successful,
                                    None otherwise

        Raises:
            ValueError: If base_name is empty or invalid
        """
        if not base_name or not base_name.strip():
            raise ValueError("base_name cannot be empty")

        # Check for duplicates
        bases = self.list_bases(session_key)
        if bases:
            # Create a list of existing bases with this name
            existing_bases = [base for base in bases if base.get("title") == base_name]

            if existing_bases:
                if not prevent_duplicates:
                    # Just warn about duplicates
                    ids = [b.get("id") for b in existing_bases]
                    self.logger.warning(
                        f"Creating a duplicate base named '{base_name}'. {len(existing_bases)} bases with this name already exist: {ids}")
                else:
                    # Generate a unique name with suffix
                    original_name = base_name
                    base_name = self._generate_unique_base_name(base_name, session_key)
                    self.logger.info(
                        f"Generated unique name '{base_name}' instead of '{original_name}' to prevent duplicates")

        endpoint = f"{self.config.base_url}/meta/bases"
        payload = {
            "title": base_name
        }

        if description:
            payload["description"] = description

        if icon_color:
            payload["meta"] = {
                "iconColor": icon_color
            }

        response = self._post_raw(
            endpoint=endpoint,
            session_key=session_key,
            json=payload
        )

        # Clear base cache after creating a new base
        if session_key in self._base_caches:
            del self._base_caches[session_key]
        self._fetch_base_id.cache_clear()

        return response

    def list_bases(self, session_key: str = 'default') -> Optional[List[Dict[str, Any]]]:
        """
        List all bases in the workspace.

        Args:
            session_key: Session identifier to use for the request

        Returns:
            Optional[List[Dict[str, Any]]]: List of base dictionaries if successful, None otherwise
        """
        endpoint = f"{self.config.base_url}/meta/bases"
        response = self._get_raw(endpoint, session_key=session_key)
        if not response:
            self.logger.error("Failed to retrieve bases from NocoDB")
            return None

        bases = response.get("list", [])
        if not bases:
            self.logger.warning("No bases found in workspace")

        # Check for duplicate base names
        self._check_duplicate_bases(bases)

        return bases

    def _check_duplicate_bases(self, bases: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Check for duplicate base names and log warnings if found.

        Args:
            bases: List of base dictionaries from the API

        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary of base names to list of matching bases
        """
        name_to_bases = {}

        # Group bases by title
        for base in bases:
            title = base.get("title")
            if not title:
                continue

            if title not in name_to_bases:
                name_to_bases[title] = []
            name_to_bases[title].append(base)

        # Log warnings for duplicates
        duplicates = {}
        for title, base_list in name_to_bases.items():
            if len(base_list) > 1:
                duplicates[title] = base_list
                ids = [b.get("id") for b in base_list]
                created_at = [b.get("created_at", "unknown") for b in base_list]
                self.logger.warning(
                    f"Duplicate base name detected: '{title}' appears {len(base_list)} times with IDs: {ids}")
                self.logger.warning(f"  Creation times for '{title}': {created_at}")

        return duplicates

    def _generate_unique_base_name(self, base_name: str, session_key: str = 'default') -> str:
        """
        Generate a unique base name by adding an incremental suffix (_2, _3, etc.) if needed.

        Args:
            base_name: The original base name
            session_key: Session identifier for the request

        Returns:
            str: A unique base name guaranteed not to exist in the workspace
        """
        bases = self.list_bases(session_key)
        if not bases:
            return base_name  # No bases exist, so original name is unique

        # Get all existing base names
        existing_names = [base.get("title", "") for base in bases]
        if base_name not in existing_names:
            return base_name  # Original name is already unique

        # Find all bases with the same base name or with numeric suffixes
        name_pattern = f"{base_name}_"
        matching_names = [
            name for name in existing_names
            if name == base_name or (name.startswith(name_pattern) and name[len(name_pattern):].isdigit())
        ]

        if not matching_names:
            return base_name  # Shouldn't happen, but just in case

        # Find the highest suffix number
        highest_suffix = 1
        for name in matching_names:
            if name == base_name:
                continue  # Skip the exact match

            if name.startswith(name_pattern):
                suffix_str = name[len(name_pattern):]
                if suffix_str.isdigit():
                    suffix = int(suffix_str)
                    highest_suffix = max(highest_suffix, suffix)

        # Generate new name with suffix one higher than the highest found
        return f"{base_name}_{highest_suffix + 1}"

    #endregion
    #region -- Table functions ---
    def get_table_id(self, base_id: str, table_name: str, session_key: str = 'default') -> Optional[str]:
        """
        Get table ID using hybrid caching strategy (session cache + LRU cache).

        Args:
            base_id: ID of the base containing the table
            table_name: Name of the table
            session_key: Session identifier

        Returns:
            Optional[str]: Table ID if found, None otherwise
        """
        # Initialize cache for this session if not exists
        if session_key not in self._table_caches:
            self._table_caches[session_key] = {}

        # Check session-specific cache first
        cache_key = f"{base_id}:{table_name}"
        if cache_key in self._table_caches[session_key]:
            return self._table_caches[session_key][cache_key]

        # Try LRU cache next
        table_id = self._fetch_table_id(base_id, table_name, session_key)
        if table_id:
            # Update session cache with the found value
            self._table_caches[session_key][cache_key] = table_id
            return table_id

        # If not found in either cache, fetch from API
        tables = self.list_tables(base_id, session_key)
        if not tables:
            self.logger.error(f"Failed to retrieve tables from base {base_id}")
            return None

        # Find the table with matching name
        for table in tables:
            if table.get("title") == table_name:
                table_id = table.get("id")
                if table_id:
                    # Update both caches
                    self._table_caches[session_key][cache_key] = table_id
                    self._fetch_table_id.cache_clear()  # Clear LRU cache to force update
                    return table_id
                break

        self.logger.warning(f"Table '{table_name}' not found in base {base_id}")
        return None

    @lru_cache(maxsize=100)
    def _fetch_table_id(self, base_id: str, table_name: str, session_key: str) -> Optional[str]:
        """
        Fetch the table ID for a given table name with LRU caching.
        Used as a fallback cache in the hybrid caching strategy.

        Args:
            base_id: ID of the base containing the table
            table_name: Name of the table to look up
            session_key: Session identifier

        Returns:
            Optional[str]: Table ID if found, None otherwise
        """
        tables = self.list_tables(base_id, session_key)
        if not tables:
            return None

        for table in tables:
            if table.get("title") == table_name:
                return table.get("id")
        return None

    def create_table(self, base_id: str, payload: dict, session_key: str = 'default') -> Optional[dict]:
        """
        Create a new table using the provided schema, following the API’s conventions.
        If a table with the same name already exists, return that instead.

        Args:
            base_id: ID of the base to create the table in
            payload: Schema definition for the new table (must include 'name')
            session_key: Session identifier to use for the request

        Returns:
            Optional[dict]: Existing or newly-created table dict if successful, None otherwise
        """
        table_name = payload.get("title")
        if not table_name:
            self.logger.error("Payload must include a 'title' field to create a table")
            return None

        tables = self.list_tables(base_id, session_key=session_key)
        if tables is None:
            self.logger.error(f"Cannot check for existing tables in base {base_id}")
            return None

        for tbl in tables:
            if tbl.get("title") == table_name:
                self.logger.info(f"Table '{table_name}' already exists in base {base_id} (id={tbl.get('id')})")
                return tbl

        endpoint = f"{self.config.base_url}/meta/bases/{base_id}/tables"
        return self._post_raw(endpoint, session_key=session_key, json=payload)

    def get_tables_meta(self, base_id: str, session_key: str = 'default') -> Optional[List[Dict[str, Any]]]:
        """
        Fetch metadata for all tables in the base.
        
        Args:
            base_id: ID of the base
            session_key: Session identifier to use for the request
            
        Returns:
            Optional[List[Dict[str, Any]]]: List of table metadata if successful, None otherwise
        """
        endpoint = f"{self.config.base_url}/meta/bases/{base_id}/tables"
        result = self._get_raw(endpoint, session_key=session_key, timeout=30)
        return result.get("list", []) if result else None

    def get_table_meta(self, table_id: str, session_key: str = 'default') -> Optional[dict]:
        """
        Retrieve the metadata for a table, including its columns.
        """
        endpoint = f"{self.config.base_url}/meta/tables/{table_id}"
        return self._get_raw(endpoint, session_key=session_key)

    def list_tables(self, base_id: str, session_key: str = 'default') -> Optional[List[Dict[str, Any]]]:
        """
        List all tables in a base.

        Args:
            base_id: ID of the base
            session_key: Session identifier to use for the request

        Returns:
            Optional[List[Dict[str, Any]]]: List of table dictionaries if successful, None otherwise
        """
        endpoint = f"{self.config.base_url}/meta/bases/{base_id}/tables"
        response = self._get_raw(endpoint, session_key=session_key)
        if not response:
            self.logger.error(f"Failed to retrieve tables from base {base_id}")
            return None

        tables = response.get("list", [])
        if not tables:
            self.logger.warning(f"No tables found in base {base_id}")
        return tables

    def delete_table(self, table_id: str, session_key: str = 'default', require_confirmation: bool = True) -> Optional[dict]:
        """
        Delete an existing table from the database.
        """
        if require_confirmation:
            user_input = input(f"Are you sure you want to delete table '{table_id}'? (y/n): ")
            if user_input.lower() not in ('y', 'yes'):
                print("Deletion aborted by user.")
                return None

        # Build the endpoint for table deletion. Adjust the endpoint if needed.
        endpoint = f"{self.config.base_url}/meta/tables/{table_id}"
        return self._delete_raw(endpoint, session_key=session_key)

    def _filter_table(self, table: Dict) -> Dict:
        """Filter a single table dictionary."""
        filtered_table = {k: v for k, v in table.items() if k in self.ALLOWED_FIELDS}

        if "columns" in table:
            filtered_table["columns"] = [
                col for col in (self._filter_column(column) for column in table["columns"])
                if col  # Remove empty dicts
            ]

        return filtered_table
    #endregion
    #region -- Column functions ---

    def get_column_id(self, table_id: str, column_name: str, session_key: str = 'default') -> Optional[str]:
        """
        Get column ID using hybrid caching strategy (session cache + LRU cache).

        Args:
            table_id: ID of the table containing the column
            column_name: Name of the column
            session_key: Session identifier

        Returns:
            Optional[str]: Column ID if found, None otherwise
        """
        # Initialize cache for this session if not exists
        if session_key not in self._column_caches:
            self._column_caches[session_key] = {}

        # Check session-specific cache first
        cache_key = f"{table_id}:{column_name}"
        if cache_key in self._column_caches[session_key]:
            return self._column_caches[session_key][cache_key]

        # Try LRU cache next
        column_id = self._fetch_column_id(table_id, column_name, session_key)
        if column_id:
            # Update session cache with the found value
            self._column_caches[session_key][cache_key] = column_id
            return column_id

        # If not found in either cache, fetch from API
        columns = self.list_columns(table_id, session_key)
        if not columns:
            self.logger.error(f"Failed to retrieve columns from table {table_id}")
            return None

        # Find the column with matching name
        for column in columns:
            if column.get("title") == column_name:
                column_id = column.get("id")
                if column_id:
                    # Update both caches
                    self._column_caches[session_key][cache_key] = column_id
                    self._fetch_column_id.cache_clear()  # Clear LRU cache to force update
                    return column_id
                break

        self.logger.warning(f"Column '{column_name}' not found in table {table_id}")
        return None

    @lru_cache(maxsize=100)
    def _fetch_column_id(self, table_id: str, column_name: str, session_key: str) -> Optional[str]:
        """
        Fetch the column ID for a given table ID and column name with LRU caching.
        Used as a fallback cache in the hybrid caching strategy.

        Args:
            table_id: ID of the table containing the column
            column_name: Name of the column to look up
            session_key: Session identifier

        Returns:
            Optional[str]: Column ID if found, None otherwise
        """
        columns = self.list_columns(table_id, session_key)
        if not columns:
            return None

        for column in columns:
            if column.get("title") == column_name:
                return column.get("id")
        return None

    def list_columns(self, table_id: str, session_key: str = 'default') -> Optional[List[Dict[str, Any]]]:
        """
        List all columns in a table.

        Args:
            table_id: ID of the table
            session_key: Session identifier to use for the request

        Returns:
            Optional[List[Dict[str, Any]]]: List of column dictionaries if successful, None otherwise
        """
        endpoint = f"{self.config.base_url}/meta/tables/{table_id}"
        response = self._get_raw(endpoint, session_key=session_key)
        if not response:
            self.logger.error(f"Failed to retrieve columns from table {table_id}")
            return None

        columns = response.get("columns", [])
        if not columns:
            self.logger.warning(f"No columns found in table {table_id}")
        return columns

    def get_column_meta(self, column_id: str, session_key: str = 'default') -> Optional[dict]:
        """
        Retrieve the metadata for a column.
        """
        endpoint = f"{self.config.base_url}/meta/columns/{column_id}"
        return self._get_raw(endpoint, session_key=session_key)

    def create_column(self, table_id: str, payload: dict, session_key: str = 'default') -> Optional[dict]:
        """
        Add a new column to an existing table. If a column with the same title already exists,
        returns that column’s dict instead of creating a duplicate.

        Args:
            table_id: ID of the table
            payload: Payload for the new column (must include 'title')
            session_key: Session identifier to use for the request

        Returns:
            Optional[dict]: Existing or newly-created column dict if successful, None otherwise
        """
        # 1) Extract desired title
        column_title = payload.get("title")
        if not column_title:
            self.logger.error("Payload must include a 'title' field to create a column")
            return None

        # 2) Check if column already exists
        existing_id = self.get_column_id(table_id, column_title, session_key=session_key)
        if existing_id:
            # grab full column dict if possible
            columns = self.list_columns(table_id, session_key=session_key)
            if columns:
                for col in columns:
                    if col.get("id") == existing_id:
                        self.logger.info(
                            f"Column '{column_title}' already exists in table {table_id} (id={existing_id})"
                        )
                        return col
            # fallback: just return the id if we couldn't fetch the full list
            self.logger.info(
                f"Column '{column_title}' already exists in table {table_id} (id={existing_id}); "
                "returning minimal info."
            )
            return {"id": existing_id, "title": column_title}

        # 3) Not found → create new
        endpoint = f"{self.config.base_url}/meta/tables/{table_id}/columns"
        response = self._post_raw(endpoint, json=payload, session_key=session_key)
        return response

    def update_column(self, column_id: str, column_definition: dict, session_key: str = 'default') -> Optional[dict]:
        """
        Update an existing column in a table.
        """
        endpoint = f"{self.config.base_url}/meta/columns/{column_id}"
        return self._patch_raw(endpoint, session_key=session_key, json=column_definition)

    def delete_column(self, column_id: str, session_key: str = 'default', require_confirmation: bool = True) -> Optional[dict]:
        """
        Delete an existing column from a table.
        """
        if require_confirmation:
            user_input = input(f"Are you sure you want to delete column '{column_id}'? (y/n): ")
            if user_input.lower() not in ('y', 'yes'):
                print("Deletion aborted by user.")
                return None

        endpoint = f"{self.config.base_url}/meta/columns/{column_id}"
        return self._delete_raw(endpoint, session_key=session_key)

    def _filter_column(self, column: Dict) -> Dict:
        """Filter a single column dictionary."""
        if (column.get("column_name") in self.FORBIDDEN_COLUMN_NAMES or
                column.get("uidt") in self.FORBIDDEN_UIDTS):
            return {}

        return {k: v for k, v in column.items() if k in self.ALLOWED_FIELDS}
    #endregion
    #region -- Record functions ---
    ### --------------------------------

    def create_records(
            self,
            table_id: str,
            records: List[Dict[str, Any]],
            session_key: str = 'default'
    ) -> Optional[Dict[str, Any]]:
        """
        Create multiple records in a given NoCoDB table.

        Args:
            table_id (str): Unique identifier for the table.
            records (List[Dict[str, Any]]): List of record dictionaries to create.
            session_key (str): Which session to use for the request.

        Returns:
            Optional[Dict[str, Any]]: JSON response from the server.

        Raises:
            ValueError: If table_id is empty or records is empty.
        """
        try:
            if not table_id or not table_id.strip():
                raise ValueError("table_id cannot be empty")
        except:
            raise ValueError("table_id is not a String")
        if not records:
            raise ValueError("records list cannot be empty")

        endpoint = f"{self.config.base_url}/tables/{table_id}/records"
        return self._post_raw(endpoint, session_key=session_key, json=records)

    def update_record(
            self,
            table_id: str,
            payload: Dict[str, Any],
            session_key: str = 'default'
    ) -> Optional[Dict[str, Any]]:
        """
        Update a single record in a NocoDB table.

        Args:
            table_id (str): Unique identifier for the table.
            payload (Dict[str, Any]): Dictionary containing the field values to update.
            session_key (str, optional): Which session to use for the request. Defaults to 'default'.

        Returns:
            Optional[Dict[str, Any]]: JSON response from the server.

        Raises:
            ValueError: If table_id or record_id is empty, or if data is empty.
        """
        # Validate input parameters
        try:
            if not table_id or not table_id.strip():
                raise ValueError("table_id cannot be empty")
        except:
            raise ValueError("table_id is not a String")

        if not payload:
            raise ValueError("payload cannot be empty")

        # Construct the endpoint URL for updating a specific record
        endpoint = f"{self.config.base_url}/tables/{table_id}/records"

        # Use the _patch_raw method to send the PATCH request
        return self._patch_raw(endpoint, session_key=session_key, json=payload)

    def link_records(
            self,
            table_id: str,
            link_field_id: str,
            record_id: str,
            links: List[Dict[str, Any]],
            session_key: str = 'default'
    ) -> Optional[Dict[str, Any]]:
        """
        Link records in NoCoDB.

        Args:
            table_id (str): Unique identifier for the table containing the link field.
            link_field_id (str): Identifier for the field that links two tables.
            record_id (str): The record ID to link with others.
            links (List[Dict[str, Any]]): List of items with 'Id' referencing records to link.
            session_key (str): Which session to use for the request.

        Returns:
            Optional[Dict[str, Any]]: JSON response from the server.

        Raises:
            ValueError: If any required parameter is empty.
        """
        if not all([table_id, link_field_id, record_id]):
            raise ValueError("table_id, link_field_id, and record_id cannot be empty")
        if not links:
            raise ValueError("links list cannot be empty")

        endpoint = f"{self.config.base_url}/tables/{table_id}/links/{link_field_id}/records/{record_id}"
        return self._post_raw(endpoint, session_key=session_key, json=links)

    def list_records(
            self,
            table_id: str,
            *,
            fields: Optional[str] = None,
            sort: Optional[str] = None,
            where: Optional[str] = None,
            offset: Optional[int] = None,
            limit: Optional[int] = None,
            view_id: Optional[str] = None,
            session_key: str = 'default',
            timeout: int = 30,
            retry_config: Optional[RetryConfig] = None
    ) -> Optional[Dict[str, Any]]:
        """
        List records from a given NoCoDB table, with optional filtering, sorting, pagination,
        and view-based parameters.

        Args:
            table_id (str): The unique identifier (ID) of the table in NoCoDB.
            fields (str, optional): Comma-separated list of fields to include in the response.
            sort (str, optional): Comma-separated list of fields to sort by, prefix with '-' for descending.
            where (str, optional): Where/filter conditions in NoCoDB query format.
            offset (int, optional): Number of records to skip for pagination.
            limit (int, optional): Maximum number of records to return.
            view_id (str, optional): Retrieve records based on a particular view's config.
            session_key (str, optional): Which session to use. Defaults to 'default'.
            timeout (int, optional): Request timeout in seconds. Defaults to 30.
            retry_config (RetryConfig, optional): Custom retry configuration.

        Returns:
            Optional[Dict[str, Any]]: JSON response containing records, pageInfo, etc.
        """
        try:
            if not table_id.strip():
                raise ValueError("table_id cannot be empty or whitespace.")
        except:
            raise ValueError("table_id is not a String")


        endpoint = f"{self.config.base_url}/tables/{table_id}/records"

        # Build query parameters
        params = {}
        if fields:
            params["fields"] = fields
        if sort:
            params["sort"] = sort
        if where:
            params["where"] = where
        if offset is not None:
            params["offset"] = offset
        if limit is not None:
            params["limit"] = limit
        if view_id:
            params["viewId"] = view_id

        return self._get_raw(
            endpoint=endpoint,
            session_key=session_key,
            params=params,
            timeout=timeout,
            retry_config=retry_config
        )

    def upload_file(
            self,
            file_path: Union[str, Path],
            title: str,
            mimetype: Optional[str] = None,  # Changed default to None
            session_key: str = 'default',
            max_size: int = 100 * 1024 * 1024  # 100MB default limit
    ) -> Optional[Dict[str, Any]]:
        """
        Upload a file to NoCoDB with proper resource management.

        Args:
            file_path (Union[str, Path]): Path to the file to upload
            title (str): Title/name of the file
            mimetype (Optional[str]): MIME type of the file. If not provided, it will be determined automatically.
            session_key (str): Which session to use for the request
            max_size (int): Maximum allowed file size in bytes

        Returns:
            Optional[Dict[str, Any]]: JSON response from the server

        Raises:
            ValueError: If file info is invalid or file is too large
            FileNotFoundError: If the file doesn't exist
        """
        from mimetypes import guess_type  # Importing inside the function as per user's note

        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = file_path.stat().st_size
        if file_size > max_size:
            raise ValueError(
                f"File size ({file_size} bytes) exceeds maximum allowed size ({max_size} bytes)"
            )

        # Determine MIME type if not provided
        if not mimetype:
            detected_mime, _ = guess_type(file_path)
            mimetype = detected_mime if detected_mime else 'application/octet-stream'

        endpoint = f"{self.config.base_url}/storage/upload"
        metadata_json = {
            'title': title,
            'size': file_size,
            'mimetype': mimetype
        }

        with open(file_path, 'rb') as f:
            files = {
                'file': (
                    title,
                    f,
                    mimetype
                )
            }
            return self._post_raw(
                endpoint,
                session_key=session_key,
                files=files,
                json=metadata_json
            )
    #endregion
    #region-- Composite functions ---
    def fetch_schema(self, base_id: str, session_key: str = 'default') -> Optional[List[Dict[str, Any]]]:
        """
        Fetch the complete schema, preserving all table metadata as well as all column metadata.
        
        Args:
            base_id: ID of the base to fetch schema for
            session_key: Session identifier to use for the request
            
        Returns:
            Optional[List[Dict[str, Any]]]: Complete schema if successful, None otherwise
        """
        full_schema = []
        try:
            # Fetch full table metadata
            tables_list = self.get_tables_meta(base_id, session_key)
            if not tables_list:
                self.logger.error(f"No tables found in base {base_id}")
                return None
        except Exception as e:
            self.logger.error(f"get_tables_meta() failed to retrieve data: {e}")
            return None

        for table in tables_list:
            table_id = table.get("id")
            table_title = table.get("title")
            self.logger.debug(f"Processing table: {table_title}")

            try:
                # Fetch full column metadata for the table
                columns_data = self.get_table_meta(table_id, session_key)
            except Exception as e:
                self.logger.error(f"Failed to retrieve columns for table {table_title}: {e}")
                return None

            if columns_data is None:
                self.logger.error(f"No column metadata returned for table {table_title}")
                return None

            # Attach the complete column metadata to the table metadata.
            # This preserves all the table metadata as well as the full column data.
            table["columns"] = columns_data.get("columns", columns_data)
            full_schema.append(table)

        return full_schema

    def create_schema(self, base_id: str, tables_schema: list[dict], session_key: str = 'default') -> Optional[dict]:
        """
        Create tables according to the provided schema.
        
        Args:
            base_id: ID of the base to create tables in
            tables_schema: List of table schemas to create
            session_key: Session identifier to use for the request
            
        Returns:
            Optional[dict]: Dictionary mapping table names to creation responses
        """
        response = {}
        filtered_schema = self._filter_schema(tables_schema)
        for table in filtered_schema:
            response[table["table_name"]] = self.create_table(
                base_id=base_id,
                payload=table,
                session_key=session_key
            )

        return response

    def _filter_schema(self, schema: list[dict]) -> list[dict]:
        """Process the schena in json format containing tables and columns."""

        filtered_data = [
            table for table in (self._filter_table(table) for table in schema)
            if table  # Remove empty dicts
        ]
        return filtered_data
    # endregion