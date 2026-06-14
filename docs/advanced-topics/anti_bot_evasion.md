
!!! danger "Disclaimer"

    qCrawl is intended for ethical and research purposes only. Users should use this software at their own risk.


Modern websites employ sophisticated anti-bot detection systems to prevent automated scraping.
qCrawl provides a comprehensive set of framework-level features to help you avoid detection while respecting website policies and legal boundaries.

qCrawl's anti-bot capabilities work at the framework level and apply to **all downloaders** (HTTP, Camoufox, etc.).

These features help you:

- **Mimic human behavior** - Natural request patterns and timing
- **Avoid fingerprinting** - Rotate user agents, headers, and proxies
- **Handle rate limiting** - Respect server capacity with intelligent throttling
- **Manage sessions** - Automatic cookie and header handling
- **Recover gracefully** - Smart retry strategies with exponential backoff


## qCrawl Anti-Bot Capabilities

### 1. Rate Limiting and Throttling

Control request rate to mimic human browsing patterns:

```python
custom_settings = {
    # Global concurrency limit
    "CONCURRENCY": 3,

    # Per-domain concurrency (prevents overwhelming single domain)
    "CONCURRENCY_PER_DOMAIN": 1,

    # Delay between requests to same domain (seconds)
    "DELAY_PER_DOMAIN": 2.0,

    # Jitter the per-domain delay to 50-150% per request
    "RANDOMIZE_DELAY": True,
}
```

**Key Settings:**

- `CONCURRENCY`: Maximum concurrent requests across all domains
- `CONCURRENCY_PER_DOMAIN`: Maximum concurrent requests per domain (most important for anti-bot)
- `DELAY_PER_DOMAIN`: Minimum delay between requests to same domain
- `RANDOMIZE_DELAY`: Jitters `DELAY_PER_DOMAIN` to 50-150% per request

**Example Spider:**

```python
from qcrawl.core.spider import Spider

class PoliteSpider(Spider):
    name = "polite_spider"

    custom_settings = {
        "CONCURRENCY_PER_DOMAIN": 1,  # One request at a time per domain
        "DELAY_PER_DOMAIN": 3.0,       # 3 seconds between requests
        "RANDOMIZE_DELAY": True,  # Vary timing naturally
    }
```

### 2. User Agent Rotation

Rotate user agents to avoid fingerprinting:

```python
import random
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request

class RotatingUserAgentSpider(Spider):
    name = "rotating_ua_spider"

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    # Pool of realistic user agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    async def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url=url,
                headers={"User-Agent": random.choice(self.user_agents)}
            )
```

**Best Practices:**

- Use recent, realistic user agent strings
- Match user agent to other headers (Accept, Accept-Language, etc.)
- Consider matching viewport size for browser automation
- Don't rotate too frequently (appears suspicious)

### 3. Cookie Management

Cookies are automatically managed by qCrawl's `CookiesMiddleware`:

```python
custom_settings = {
    # Enable cookie handling (enabled by default)
    "DOWNLOADER_MIDDLEWARES": {
        "qcrawl.middleware.downloader.cookies.CookiesMiddleware": 700,
    },

    # Cookies persist across requests to same domain automatically
    # No manual cookie handling needed
}
```

**Features:**

- Automatic cookie storage per domain
- Sends cookies with subsequent requests
- Handles Set-Cookie headers
- Respects cookie expiration
- Thread-safe cookie jar

**Manual Cookie Control (if needed):**

```python
from qcrawl.core.request import Request

# Set cookies for a specific request
yield Request(
    url="https://example.com",
    headers={"Cookie": "session_id=abc123; user_pref=dark_mode"}
)

# Access cookies in parse method
def parse(self, response):
    cookies = response.headers.get("Set-Cookie")
    # Process cookies if needed
```

### 4. Proxy Support

Rotate proxies to distribute requests across different IP addresses:

```python
custom_settings = {
    "DOWNLOADER_MIDDLEWARES": {
        "qcrawl.middleware.downloader.httpproxy.HttpProxyMiddleware": 750,
    },
}

# Per-request proxy
yield Request(
    url="https://example.com",
    meta={"proxy": "http://proxy.example.com:8080"}
)
```

**Proxy Rotation Example:**

```python
import random
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request

class ProxyRotatingSpider(Spider):
    name = "proxy_spider"

    # Pool of proxy servers
    proxies = [
        "http://proxy1.example.com:8080",
        "http://proxy2.example.com:8080",
        "http://proxy3.example.com:8080",
    ]

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "qcrawl.middleware.downloader.httpproxy.HttpProxyMiddleware": 750,
        },
    }

    async def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url=url,
                meta={"proxy": random.choice(self.proxies)}
            )
```

**Proxy Authentication:**

```python
# HTTP Basic Auth
yield Request(
    url="https://example.com",
    meta={"proxy": "http://username:password@proxy.example.com:8080"}
)
```

### 5. Retry Strategy with Backoff

Intelligent retry behavior mimics human patterns and handles transient errors:

```python
custom_settings = {
    "DOWNLOADER_MIDDLEWARES": {
        "qcrawl.middleware.downloader.retry.RetryMiddleware": 550,
    },

    # Retry configuration
    "MAX_RETRIES": 3,  # Maximum retry attempts

    # HTTP status codes that trigger retry
    "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],

    # Priority adjustment for retried requests (negative = lower priority)
    "RETRY_PRIORITY_ADJUST": -1,
}
```

**How It Works:**

1. Request fails with retryable status code (e.g., 503 Service Unavailable)
2. Request is re-queued with lower priority
3. Natural delay occurs before retry (due to queue processing)
4. Process repeats up to `MAX_RETRIES` attempts

**Custom Retry Logic:**

```python
from qcrawl.core.request import Request

# Disable retry for specific request
yield Request(
    url="https://example.com",
    meta={"dont_retry": True}
)

# Custom retry count for specific request
yield Request(
    url="https://example.com",
    meta={"max_retry_times": 5}
)
```

### 6. Custom Request Headers

Customize headers per request or globally to appear more legitimate:

```python
custom_settings = {
    # Default headers for all requests
    "DEFAULT_REQUEST_HEADERS": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",  # Do Not Track
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    },
}
```

**Per-Request Headers:**

```python
from qcrawl.core.request import Request

# Add referer to appear as if coming from another page
yield Request(
    url="https://example.com/page2",
    headers={
        "Referer": "https://example.com/page1",
        "X-Requested-With": "XMLHttpRequest",  # AJAX request
    }
)
```

**Common Header Patterns:**

```python
# Standard browser headers
BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Mobile browser headers
MOBILE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
}

# API client headers
API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}
```

### 7. Realistic Navigation Patterns

Use PageMethod with Camoufox to simulate human-like interactions:

```python
import random
from qcrawl.core.request import Request
from qcrawl.core.page import PageMethod

yield Request(
    url="https://example.com",
    meta={
        "use_handler": "camoufox",
        "camoufox_page_methods": [
            # Wait for page to load
            PageMethod("wait_for_selector", "body"),

            # Simulate reading time (2-5 seconds)
            PageMethod("wait_for_timeout", random.randint(2000, 5000)),

            # Scroll like a human (partial scroll first)
            PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight / 2)"),
            PageMethod("wait_for_timeout", random.randint(500, 1500)),

            # Scroll to bottom
            PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
            PageMethod("wait_for_timeout", random.randint(500, 1000)),

            # Random mouse movement (if needed)
            PageMethod("evaluate", f"document.elementFromPoint({random.randint(100, 500)}, {random.randint(100, 500)})"),
        ],
    }
)
```

See [Browser Automation](browser_automation.md) for more details on PageMethod.

## Combining with Camoufox

For maximum stealth, combine qCrawl's framework-level features with Camoufox's browser-level anti-detection:

```python
import random
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request
from qcrawl.core.page import PageMethod

class StealthSpider(Spider):
    """Maximum stealth spider combining all techniques."""

    name = "stealth_spider"
    start_urls = ["https://example.com"]

    # User agent pool
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # Proxy pool (optional)
    proxies = [
        "http://proxy1.example.com:8080",
        "http://proxy2.example.com:8080",
    ]

    custom_settings = {
        # Camoufox configuration
        "DOWNLOAD_HANDLERS": {
            "http": "qcrawl.downloaders.HTTPDownloader",
            "camoufox": "qcrawl.downloaders.CamoufoxDownloader",
        },
        "CAMOUFOX_CONTEXTS": {
            "default": {
                "viewport": {"width": 1920, "height": 1080},
            }
        },
        "CAMOUFOX_LAUNCH_OPTIONS": {
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled"],
        },

        # Rate limiting (qCrawl framework-level)
        "CONCURRENCY": 2,
        "CONCURRENCY_PER_DOMAIN": 1,
        "DELAY_PER_DOMAIN": 3.0,
        "RANDOMIZE_DELAY": True,

        # Headers (qCrawl framework-level)
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
        },

        # Retry with backoff (qCrawl framework-level)
        "MAX_RETRIES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],

        # Middlewares (qCrawl framework-level)
        "DOWNLOADER_MIDDLEWARES": {
            "qcrawl.middleware.downloader.cookies.CookiesMiddleware": 700,
            "qcrawl.middleware.downloader.retry.RetryMiddleware": 550,
            "qcrawl.middleware.downloader.httpproxy.HttpProxyMiddleware": 750,
        },
    }

    async def start_requests(self):
        for url in self.start_urls:
            # Rotate user agent and proxy
            user_agent = random.choice(self.user_agents)
            proxy = random.choice(self.proxies) if self.proxies else None

            meta = {
                "use_handler": "camoufox",
                "camoufox_page_methods": [
                    # Wait for content
                    PageMethod("wait_for_selector", "body"),

                    # Simulate human reading time
                    PageMethod("wait_for_timeout", random.randint(2000, 4000)),

                    # Random scroll pattern
                    PageMethod("evaluate", f"window.scrollTo(0, {random.randint(200, 800)})"),
                    PageMethod("wait_for_timeout", random.randint(500, 1500)),
                ],
            }

            if proxy:
                meta["proxy"] = proxy

            yield Request(
                url=url,
                headers={"User-Agent": user_agent},
                meta=meta
            )

    async def parse(self, response):
        # Extract data
        rv = self.response_view(response)
        # ... your parsing logic

        # Follow links with same stealth techniques
        for link in rv.doc.cssselect("a"):
            href = link.get("href")
            if href:
                yield rv.follow(
                    href,
                    headers={"User-Agent": random.choice(self.user_agents)},
                    meta={
                        "use_handler": "camoufox",
                        "camoufox_page_methods": [
                            PageMethod("wait_for_timeout", random.randint(1000, 3000)),
                        ],
                    }
                )
```

## Best Practices

### 1. Start Slow, Scale Gradually

```python
# Start with conservative settings
initial_settings = {
    "CONCURRENCY": 1,
    "DELAY_PER_DOMAIN": 5.0,
}

# Gradually increase if no issues
optimized_settings = {
    "CONCURRENCY": 3,
    "DELAY_PER_DOMAIN": 2.0,
}
```

### 2. Monitor Response Codes

Watch for signs of detection:

```python
class MonitoringSpider(Spider):
    async def parse(self, response):
        # Log suspicious responses
        if response.status_code == 429:  # Too Many Requests
            self.logger.warning(f"Rate limited at {response.url}")
        elif response.status_code == 403:  # Forbidden
            self.logger.warning(f"Access denied at {response.url}")

        # Adjust behavior based on response
        if response.status_code in [429, 503]:
            # Slow down
            self.custom_settings["DELAY_PER_DOMAIN"] *= 1.5
```

### 3. Respect robots.txt

```python
custom_settings = {
    "DOWNLOADER_MIDDLEWARES": {
        "qcrawl.middleware.downloader.robotstxt.RobotsTxtMiddleware": 100,
    },
}
```

### 4. Use Appropriate Delays

**Rule of thumb:**
- **Light scraping** (few pages): 1-2 seconds delay
- **Medium scraping** (hundreds of pages): 2-5 seconds delay
- **Heavy scraping** (thousands of pages): 5-10 seconds delay

### 5. Rotate Everything

Don't just rotate one parameter - rotate multiple aspects:

```python
# Good: Rotate multiple parameters
yield Request(
    url=url,
    headers={
        "User-Agent": random.choice(user_agents),
        "Accept-Language": random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.9"]),
    },
    meta={
        "proxy": random.choice(proxies),
        "use_handler": random.choice(["http", "camoufox"]),
    }
)
```

### 6. Time Your Requests

Scrape during off-peak hours when possible:

```python
import datetime

def should_scrape_now():
    """Scrape during off-peak hours (e.g., 2-6 AM target timezone)."""
    hour = datetime.datetime.now().hour
    return 2 <= hour <= 6

if should_scrape_now():
    # Run spider
    pass
```

### 7. Handle CAPTCHAs Gracefully

```python
async def parse(self, response):
    if "captcha" in response.text().lower()():
        self.logger.warning(f"CAPTCHA detected at {response.url}")
        # Option 1: Stop scraping
        return

        # Option 2: Use CAPTCHA solving service (if authorized)
        # captcha_solution = await solve_captcha(response)

        # Option 3: Switch to different proxy/user agent
        # yield retry_with_different_identity(response.request)
```

### 8. Implement Backoff on Detection

```python
class AdaptiveSpider(Spider):
    def __init__(self):
        super().__init__()
        self.detection_count = 0
        self.base_delay = 2.0

    async def parse(self, response):
        if self.is_detected(response):
            self.detection_count += 1
            new_delay = self.base_delay * (2 ** self.detection_count)
            self.logger.warning(f"Detection! Increasing delay to {new_delay}s")
            self.custom_settings["DELAY_PER_DOMAIN"] = new_delay
        else:
            # Gradually decrease delay if successful
            if self.detection_count > 0:
                self.detection_count -= 1

    def is_detected(self, response):
        return response.status_code in [429, 403] or "captcha" in response.text().lower()()
```

## Common Anti-Bot Patterns and Solutions

### Pattern 1: Rate Limiting (429 Response)

**Detection:** Server returns 429 Too Many Requests

**Solution:**
```python
custom_settings = {
    "DELAY_PER_DOMAIN": 5.0,
    "CONCURRENCY_PER_DOMAIN": 1,
    "RETRY_HTTP_CODES": [429],
    "MAX_RETRIES": 5,
}
```

### Pattern 2: IP-Based Blocking

**Detection:** 403 Forbidden or connection refused

**Solution:**
```python
# Use proxy rotation
custom_settings = {
    "DOWNLOADER_MIDDLEWARES": {
        "qcrawl.middleware.downloader.httpproxy.HttpProxyMiddleware": 750,
    },
}

# Rotate proxies per request
yield Request(url=url, meta={"proxy": random.choice(proxy_pool)})
```

### Pattern 3: JavaScript Challenge

**Detection:** Page requires JavaScript execution

**Solution:**
```python
# Use Camoufox for JavaScript rendering
yield Request(
    url=url,
    meta={
        "use_handler": "camoufox",
        "camoufox_page_methods": [
            PageMethod("wait_for_selector", ".content"),
        ],
    }
)
```

### Pattern 4: User Agent Filtering

**Detection:** Different content for different user agents

**Solution:**
```python
# Use realistic, current user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0",
]

yield Request(url=url, headers={"User-Agent": random.choice(user_agents)})
```

### Pattern 5: Session/Cookie Requirements

**Detection:** Access denied without valid session

**Solution:**
```python
# Let CookiesMiddleware handle sessions automatically
custom_settings = {
    "DOWNLOADER_MIDDLEWARES": {
        "qcrawl.middleware.downloader.cookies.CookiesMiddleware": 700,
    },
}

# Or manually handle login
async def start_requests(self):
    # Login first
    yield Request(
        url="https://example.com/login",
        method="POST",
        json={"username": "...", "password": "..."},
        callback=self.after_login,
    )

async def after_login(self, response):
    # Cookies are automatically stored, proceed with scraping
    yield Request(url="https://example.com/data")
```

## Monitoring and Debugging

### Enable Detailed Logging

```python
custom_settings = {
    "LOG_LEVEL": "DEBUG",
}

# In spider
self.logger.info(f"Successfully scraped {response.url}")
self.logger.warning(f"Retrying {response.url} - attempt {retry_count}")
```

### Track Success Rates

```python
class MonitoredSpider(Spider):
    def __init__(self):
        super().__init__()
        self.success_count = 0
        self.failure_count = 0

    async def parse(self, response):
        if response.status_code == 200:
            self.success_count += 1
        else:
            self.failure_count += 1

        # Log metrics
        total = self.success_count + self.failure_count
        success_rate = self.success_count / total if total > 0 else 0
        self.logger.info(f"Success rate: {success_rate:.2%}")
```


## Summary

qCrawl provides comprehensive anti-bot evasion capabilities:

| Feature             | Purpose                 | Key Settings                                 |
|---------------------|-------------------------|----------------------------------------------|
| Rate Limiting       | Mimic human speed       | `DELAY_PER_DOMAIN`, `CONCURRENCY_PER_DOMAIN` |
| User Agent Rotation | Avoid fingerprinting    | Per-request headers                          |
| Cookie Management   | Session handling        | `CookiesMiddleware` (automatic)              |
| Proxy Support       | IP rotation             | `HttpProxyMiddleware`, per-request meta      |
| Retry Strategy      | Handle transient errors | `MAX_RETRIES`, `RETRY_HTTP_CODES`            |
| Custom Headers      | Appear legitimate       | `DEFAULT_REQUEST_HEADERS`, per-request       |
| Navigation Patterns | Human-like behavior     | PageMethod with Camoufox                     |

