
Many websites require authentication to access content. qCrawl supports various authentication methods including session-based (cookies), API tokens, and custom authentication flows.

## Session-based authentication

Use cookies to maintain login sessions:

```python
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request

class AuthSpider(Spider):
    name = "auth_spider"
    start_urls = ["https://example.com/login"]

    custom_settings = {
        "COOKIES_ENABLED": True,  # Enable cookie middleware
    }

    async def parse(self, response):
        # The login page: extract the CSRF token and submit the form.
        rv = self.response_view(response)
        csrf_token = rv.doc.cssselect("input[name=csrf_token]")[0].get("value")

        # Form login uses x-www-form-urlencoded, so encode the body (bytes).
        from urllib.parse import urlencode

        yield Request(
            url="https://example.com/login",
            method="POST",
            body=urlencode(
                {
                    "username": "user@example.com",
                    "password": "password123",
                    "csrf_token": csrf_token,
                }
            ).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            callback=self.after_login,
        )

    async def after_login(self, response):
        # Cookies from the login are stored automatically; crawl protected pages.
        yield Request(url="https://example.com/dashboard")
```

**Verify login success:**

```python
async def parse(self, response):
    rv = self.response_view(response)

    # Check if login form is still present
    login_form = rv.doc.cssselect("form#login")
    if login_form:
        self.logger.error("Login failed - still seeing login form")
        return

    # Check for logged-in indicator
    user_menu = rv.doc.cssselect(".user-menu")
    if not user_menu:
        self.logger.error("Login may have failed - no user menu found")
        return

    # Login succeeded, proceed with crawling
    for link in rv.doc.cssselect("a.protected-content"):
        yield rv.follow(link.get("href"))
```


## API token authentication

Use bearer tokens or API keys in headers:

```python
class ApiSpider(Spider):
    name = "api_spider"
    start_urls = []

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "Authorization": "Bearer YOUR_API_TOKEN",
            "Accept": "application/json"
        }
    }

    async def start_requests(self):
        yield Request(url="https://api.example.com/data")

    async def parse(self, response):
        data = response.json()

        for item in data.get("items", []):
            yield {
                "id": item["id"],
                "name": item["name"]
            }

        # Paginate API
        next_page = data.get("next_page_url")
        if next_page:
            yield Request(url=next_page)
```

**Token from environment variables:**

```python
import os

class ApiSpider(Spider):
    name = "api_spider"
    start_urls = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load token from environment
        api_token = os.getenv("API_TOKEN")
        if not api_token:
            raise ValueError("API_TOKEN environment variable not set")

        self.custom_settings = {
            "DEFAULT_REQUEST_HEADERS": {
                "Authorization": f"Bearer {api_token}",
                "Accept": "application/json"
            }
        }
```


## Basic HTTP authentication

Use username and password in URL or headers:

```python
from qcrawl.core.request import Request

class BasicAuthSpider(Spider):
    name = "basic_auth"
    start_urls = []

    async def start_requests(self):
        # Method 1: Include in URL
        yield Request(url="https://username:password@example.com/protected")

        # Method 2: Use Authorization header
        import base64
        credentials = base64.b64encode(b"username:password").decode("utf-8")
        yield Request(
            url="https://example.com/protected",
            headers={"Authorization": f"Basic {credentials}"}
        )
```


## OAuth 2.0 authentication

Handle OAuth token refresh:

```python
import time
from qcrawl.core.request import Request

class OAuthSpider(Spider):
    name = "oauth_spider"
    start_urls = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.access_token = None
        self.token_expires_at = 0

    async def get_access_token(self):
        """Fetch or refresh OAuth access token."""
        # Request new token from OAuth endpoint
        token_url = "https://api.example.com/oauth/token"
        response = await self.make_token_request(token_url)

        data = response.json()
        self.access_token = data["access_token"]
        self.token_expires_at = time.time() + data["expires_in"]

        return self.access_token

    async def start_requests(self):
        # Get initial token
        token = await self.get_access_token()

        yield Request(
            url="https://api.example.com/data",
            headers={"Authorization": f"Bearer {token}"}
        )

    async def parse(self, response):
        # Check if token expired
        if time.time() >= self.token_expires_at:
            token = await self.get_access_token()
            # Retry request with new token
            yield Request(
                url=response.url,
                headers={"Authorization": f"Bearer {token}"}
            )
            return

        # Process response
        data = response.json()
        for item in data.get("items", []):
            yield item
```


## Custom authentication flows

Handle multi-step authentication:

```python
class CustomAuthSpider(Spider):
    name = "custom_auth"
    start_urls = ["https://example.com/step1"]

    custom_settings = {
        "COOKIES_ENABLED": True,
    }

    async def parse(self, response):
        """Step 1: get the initial token, then submit it (routed to step 2)."""
        rv = self.response_view(response)
        initial_token = rv.doc.cssselect("input[name=token]")[0].get("value")

        yield Request(
            url="https://example.com/step2",
            method="POST",
            json={"token": initial_token},
            callback=self.submit_credentials,
        )

    async def submit_credentials(self, response):
        """Step 2: submit credentials (routed to step 3)."""
        rv = self.response_view(response)
        csrf = rv.doc.cssselect("input[name=csrf]")[0].get("value")

        yield Request(
            url="https://example.com/login",
            method="POST",
            json={"username": "user", "password": "pass", "csrf": csrf},
            callback=self.start_crawl,
        )

    async def start_crawl(self, response):
        """Step 3: authentication complete, start crawling."""
        yield Request(url="https://example.com/protected")
```


## Handling authentication errors

Detect and handle auth failures:

```python
async def parse(self, response):
    # Check for authentication errors
    if response.status_code == 401:
        self.logger.error("Unauthorized - authentication failed")
        return

    if response.status_code == 403:
        self.logger.error("Forbidden - insufficient permissions")
        return

    # Check for redirect to login page
    if "login" in response.url.lower():
        self.logger.warning("Redirected to login - session may have expired")
        return

    # Process authenticated response
    rv = self.response_view(response)
    for item in rv.doc.cssselect(".item"):
        yield self.extract_item(item)
```


## Best practices

- **Handle authentication properly**: Use cookie middleware for sessions, headers for API tokens
- **Validate authentication state**: Check login success before proceeding with protected pages
- **Clean up sensitive data**: Don't log passwords or tokens
- **Use environment variables**: Store credentials outside code
- **Handle token expiration**: Implement refresh logic for OAuth/JWT tokens
- **Verify auth success**: Check for login indicators, not just HTTP 200
- **Respect auth rate limits**: API tokens often have usage quotas
- **Test authentication flow**: Verify login works before full crawl
- **Handle session expiration**: Detect and re-authenticate if session expires

See also: [Error Recovery](error_recovery.md), [Rate Limiting](rate_limiting.md)
