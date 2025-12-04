
Many modern websites rely heavily on JavaScript to render content, making traditional HTTP requests insufficient for scraping.
qCrawl supports browser automation through the [Camoufox](https://camoufox.com/) downloader, a stealth browser based on Firefox that can bypass anti-bot detection systems.

## Installation

Install qCrawl with Camoufox support:

```bash
pip install qcrawl[camoufox]
```

## Basic Usage

Enable the Camoufox downloader by configuring it in your spider's `custom_settings`:

```python
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request
from qcrawl.core.page import PageMethod

class BrowserSpider(Spider):
    name = "browser_spider"
    start_urls = ["https://quotes.toscrape.com/js/"]

    custom_settings = {
        # Register Camoufox downloader
        "DOWNLOAD_HANDLERS": {
            "http": "qcrawl.downloaders.HTTPDownloader",
            "https": "qcrawl.downloaders.HTTPDownloader",
            "camoufox": "qcrawl.downloaders.CamoufoxDownloader",
        },
        # Configure browser contexts
        "CAMOUFOX_CONTEXTS": {
            "default": {
                "viewport": {"width": 1280, "height": 720},
            }
        },
        # Browser pool settings
        "CAMOUFOX_MAX_CONTEXTS": 2,
        "CAMOUFOX_MAX_PAGES_PER_CONTEXT": 3,
        "CAMOUFOX_LAUNCH_OPTIONS": {"headless": True},
    }

    async def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url=url,
                meta={
                    "use_handler": "camoufox",
                    "camoufox_page_methods": [
                        PageMethod("wait_for_selector", ".quote")
                    ]
                }
            )

    async def parse(self, response):
        # Parse JavaScript-rendered HTML
        rv = self.response_view(response)
        quotes = rv.doc.cssselect("div.quote")

        for q in quotes:
            text = q.cssselect("span.text")[0].text_content().strip()
            author = q.cssselect("small.author")[0].text_content().strip()
            yield {"text": text, "author": author}
```

## Configuration Settings

### Browser Contexts

Contexts define different browser environments (viewport, user agent, etc.). You can create multiple contexts for different use cases:

```python
"CAMOUFOX_CONTEXTS": {
    "default": {
        "viewport": {"width": 1280, "height": 720},
        "ignore_https_errors": False,
    },
    "mobile": {
        "viewport": {"width": 375, "height": 667},
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
    },
    "tablet": {
        "viewport": {"width": 768, "height": 1024},
    }
}
```

Specify which context to use in the request meta:

```python
yield Request(
    url="https://example.com",
    meta={
        "use_handler": "camoufox",
        "camoufox_context": "mobile"  # Use mobile context
    }
)
```

### Browser Pool Settings

Control the number of browser instances and concurrent pages:

```python
"CAMOUFOX_MAX_CONTEXTS": 10,              # Maximum browser contexts
"CAMOUFOX_MAX_PAGES_PER_CONTEXT": 5,      # Max pages per context
"CAMOUFOX_DEFAULT_NAVIGATION_TIMEOUT": 30000.0,  # Timeout in milliseconds
```

### Launch Options

Configure browser launch parameters:

```python
"CAMOUFOX_LAUNCH_OPTIONS": {
    "headless": True,  # Run without GUI
    "args": [
        "--no-sandbox",
        "--disable-dev-shm-usage",
    ],
}
```

### Remote Browser Connection

Connect to a remote browser via Chrome DevTools Protocol (CDP):

```python
"CAMOUFOX_CDP_URL": "http://localhost:9222",  # Remote browser URL
```

## Page Interactions

Execute page methods before or after navigation using `PageMethod` objects:

```python
from qcrawl.core.page import PageMethod

yield Request(
    url="https://example.com",
    meta={
        "use_handler": "camoufox",
        "camoufox_page_methods": [
            # Wait for selector after page loads
            PageMethod("wait_for_selector", ".content"),

            # Click a button
            PageMethod("click", "#load-more"),

            # Wait for content to load
            PageMethod("wait_for_timeout", 2000),

            # Execute JavaScript
            PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),

            # Take a screenshot with keyword arguments
            PageMethod("screenshot", path="/tmp/page.png", full_page=True),
        ]
    }
)
```

**Timing control:**

By default, methods execute **after** navigation. Use the `timing` parameter for before-navigation execution:

```python
# Execute JavaScript before navigating to the page
PageMethod("evaluate", "navigator.webdriver = false", timing="before")

# Wait for element after navigation (default)
PageMethod("wait_for_selector", ".content")  # timing="after" is default
```

!!! note "Complex Custom Interactions"
    For complex custom page interactions, use `camoufox_include_page=True` to get direct access to the page object in your parse method. See the [Manual Page Control](#manual-page-control) section below.

**Accessing results:**

Page method results are stored and accessible in the parse method:

```python
async def parse(self, response):
    # Access page methods with their results
    methods = response.meta.get("camoufox_page_methods", [])

    # Get screenshot result (if screenshot method was used)
    for method in methods:
        if method.method == "screenshot":
            print(f"Screenshot saved, result: {method.result}")
```

**Common page methods:**

- `wait_for_selector(selector)`: Wait for element to appear
- `wait_for_timeout(ms)`: Wait for specified milliseconds
- `click(selector)`: Click an element
- `fill(selector, text)`: Fill an input field
- `evaluate(js_code)`: Execute JavaScript in the page context
- `screenshot(path, full_page=True)`: Take a screenshot

**Dict format (backward compatible):**

For config files, you can still use dict format:

```python
{
    "method": "wait_for_selector",
    "args": [".content"],
    "timing": "after"
}
```

## Manual Page Control

For complex interactions, request the page object and control it manually:

```python
class InteractiveSpider(Spider):
    def start_requests(self):
        yield Request(
            url="https://example.com",
            meta={
                "use_handler": "camoufox",
                "camoufox_include_page": True  # Keep page object alive
            }
        )

    async def parse(self, response):
        if "camoufox_page" in response.meta:
            page = response.meta["camoufox_page"]

            # Perform custom interactions
            await page.click("#button")
            await page.wait_for_selector(".results")
            content = await page.content()

            # IMPORTANT: Close the page when done
            await page.close()

        # Continue parsing
        rv = self.response_view(response)
        # ...
```

!!! warning "Resource Management"
    When using `camoufox_include_page`, you must manually close the page with `await page.close()` to avoid resource leaks.

## Request Header Processing

Control how qCrawl headers are sent to the browser:

```python
"CAMOUFOX_PROCESS_REQUEST_HEADERS": "use_qcrawl_headers"  # Default
```

**Options:**

- `"use_qcrawl_headers"`: Merge qCrawl headers with browser requests (default)
- `"ignore"`: Don't send qCrawl headers (use browser defaults only)
- Custom callable: `lambda request, default_headers: {...}` for custom processing

## Anti-Bot Evasion

!!! danger "Disclaimer"

    qCrawl is intended for ethical and research purposes only. Users should use this software at their own risk.


Camoufox provides browser-level anti-detection features. For framework-level anti-bot capabilities (rate limiting, proxies, user agent rotation, etc.), see the **[Anti-Bot Evasion Guide](anti_bot_evasion.md)**.

### Camoufox-Specific Configuration

Camoufox provides browser-level anti-detection features:

#### 1. Stealth Mode

Automatically patches WebDriver detection and fingerprinting:

```python
custom_settings = {
    "DOWNLOAD_HANDLERS": {
        "camoufox": "qcrawl.downloaders.CamoufoxDownloader",
    },
    # Stealth mode is enabled by default - no configuration needed
    # Automatically patches:
    # - navigator.webdriver detection
    # - Chrome DevTools Protocol traces
    # - Automation-specific properties
}
```

#### 2. Browser Fingerprint Randomization

Each context gets a unique, realistic browser fingerprint:

```python
custom_settings = {
    "CAMOUFOX_CONTEXTS": {
        "default": {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        "mobile": {
            "viewport": {"width": 390, "height": 844},
            "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)",
        },
    },
}
```

#### 3. Headless Detection Bypass

Configure headless mode with anti-detection:

```python
custom_settings = {
    "CAMOUFOX_LAUNCH_OPTIONS": {
        "headless": True,  # Runs without GUI but appears as normal browser
        "args": [
            "--disable-blink-features=AutomationControlled",  # Remove automation flags
            "--disable-dev-shm-usage",  # Improve stability
            "--no-sandbox",  # Linux compatibility
        ],
    },
}
```

#### 4. Canvas and WebGL Fingerprinting

Camoufox randomizes canvas and WebGL fingerprints automatically:

```python
# No configuration needed - handled automatically
# Each context generates unique but realistic:
# - Canvas fingerprints
# - WebGL vendor/renderer strings
# - Audio context fingerprints
```

#### 5. HTTPS Error Handling

Control certificate validation for testing environments:

```python
custom_settings = {
    "CAMOUFOX_CONTEXTS": {
        "default": {
            "ignore_https_errors": True,  # For testing/development only
        }
    },
}
```

#### 6. Browser Context Isolation

Isolate different scraping targets with separate contexts:

```python
custom_settings = {
    "CAMOUFOX_CONTEXTS": {
        "site_a": {"viewport": {"width": 1920, "height": 1080}},
        "site_b": {"viewport": {"width": 1366, "height": 768}},
    },
}

# Use different contexts per domain
yield Request(
    url="https://site-a.com",
    meta={"use_handler": "camoufox", "camoufox_context": "site_a"}
)

yield Request(
    url="https://site-b.com",
    meta={"use_handler": "camoufox", "camoufox_context": "site_b"}
)
```

## Navigation Options

Customize page navigation behavior via `camoufox_page_goto_kwargs`:

```python
yield Request(
    url="https://example.com",
    meta={
        "use_handler": "camoufox",
        "camoufox_page_goto_kwargs": {
            "wait_until": "networkidle",  # Wait for network to be idle
            "timeout": 60000,  # Custom timeout
            "referer": "https://google.com"  # Set referer
        }
    }
)
```

**wait_until options:**

- `"domcontentloaded"`: Wait for DOM to be ready (default)
- `"load"`: Wait for load event
- `"networkidle"`: Wait for network to be idle

## Event Handlers

Register event handlers for browser events:

```python
def handle_console(msg):
    print(f"Browser console: {msg.text}")

def handle_dialog(dialog):
    dialog.dismiss()

yield Request(
    url="https://example.com",
    meta={
        "use_handler": "camoufox",
        "camoufox_event_handlers": {
            "console": handle_console,
            "dialog": handle_dialog
        }
    }
)
```

## Request Abortion

Abort requests matching certain patterns to save bandwidth:

```python
def should_abort(route_request):
    # Block images and stylesheets
    resource_type = route_request.resource_type
    return resource_type in ["image", "stylesheet", "font"]

custom_settings = {
    "CAMOUFOX_ABORT_REQUEST": should_abort
}
```

## Performance Tips

1. **Use HTTP downloader when possible**: Only use Camoufox for JavaScript-rendered pages or when anti-bot evasion required
2. **Limit browser contexts**: Keep `CAMOUFOX_MAX_CONTEXTS` low
3. **Abort unnecessary requests**: Block images/fonts if not needed
4. **Reuse contexts**: Use `camoufox_context` to reuse browser contexts
5. **Increase delays**: Use higher `DELAY_PER_DOMAIN` for browser requests

## Debugging

### Enable Browser Visibility

Run browser in non-headless mode to see what's happening:

```python
"CAMOUFOX_LAUNCH_OPTIONS": {"headless": False}
```

### Take Screenshots

Capture screenshots for debugging:

```python
from qcrawl.core.page import PageMethod

"camoufox_page_methods": [
    PageMethod("screenshot", path="/tmp/debug.png", full_page=True)
]
```

### Enable Logging

Set log level to see browser activity:

```python
custom_settings = {
    "LOG_LEVEL": "DEBUG"
}
```
