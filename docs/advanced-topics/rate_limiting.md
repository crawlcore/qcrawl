
Rate limiting controls how fast you send requests to a server. This is important for:

- Being polite to target websites
- Avoiding rate limit bans or IP blocks
- Staying within API quotas
- Preventing server overload

## Delay between requests to same domain

Configure delays in spider settings:

```python
class RateLimitedSpider(Spider):
    name = "rate_limited"
    start_urls = ["https://example.com"]

    custom_settings = {
        "DELAY_PER_DOMAIN": 2.0,  # 2 seconds between requests to same domain
        "RANDOMIZE_DELAY": True,   # Add random jitter (50-150% of delay)
    }
```

**Settings explanation:**

- `DELAY_PER_DOMAIN`: Minimum delay between requests to the same domain
- `RANDOMIZE_DELAY`: Adds randomization to appear more human-like


## Limit concurrent requests per domain

Control concurrency to avoid overwhelming servers:

```python
class ConcurrencyLimitedSpider(Spider):
    name = "concurrency_limited"
    start_urls = ["https://example.com"]

    custom_settings = {
        "CONCURRENCY": 16,              # Global max concurrent requests
        "CONCURRENCY_PER_DOMAIN": 2,    # Max 2 concurrent requests per domain
    }
```

**Use cases:**

- `CONCURRENCY_PER_DOMAIN`: Limit load on individual sites
- `CONCURRENCY`: Control overall crawler resource usage


## Per-request delay with meta

Control delay for individual requests. `DownloadDelayMiddleware` reads the
per-request delay from `meta["retry_delay"]` (the same key the retry middleware
uses for backoff); the effective wait is `max(DELAY_PER_DOMAIN, retry_delay)`:

```python
async def parse(self, response):
    rv = self.response_view(response)

    for link in rv.doc.cssselect("a"):
        # High-priority requests: faster
        if "important" in link.get("class", ""):
            yield rv.follow(
                link.get("href"),
                meta={"retry_delay": 0.5}
            )
        # Regular requests: slower
        else:
            yield rv.follow(
                link.get("href"),
                meta={"retry_delay": 3.0}
            )
```


## Time-based throttling with meta

Track and enforce minimum time between requests:

```python
import time

async def parse(self, response):
    rv = self.response_view(response)

    # Check time since last request
    last_request_time = response.request.meta.get("last_request_time", 0)
    current_time = time.time()

    min_delay = 5.0  # 5 seconds minimum
    elapsed = current_time - last_request_time

    if elapsed < min_delay:
        # Add delay to next request
        delay = min_delay - elapsed
        for link in rv.doc.cssselect("a"):
            yield rv.follow(
                link.get("href"),
                meta={
                    "retry_delay": delay,
                    "last_request_time": current_time
                }
            )
    else:
        for link in rv.doc.cssselect("a"):
            yield rv.follow(
                link.get("href"),
                meta={"last_request_time": current_time}
            )
```


## Adaptive rate limiting

Automatically adjust rate based on errors:

```python
class AdaptiveSpider(Spider):
    name = "adaptive"
    start_urls = ["https://example.com"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_count = 0
        self.success_count = 0

    async def parse(self, response):
        rv = self.response_view(response)

        # Track success
        self.success_count += 1

        # Adjust delay based on error rate
        error_rate = self.error_count / max(self.success_count, 1)

        if error_rate > 0.1:  # More than 10% errors
            delay = 5.0
        elif error_rate > 0.05:  # 5-10% errors
            delay = 2.0
        else:
            delay = 1.0

        for link in rv.doc.cssselect("a"):
            yield rv.follow(
                link.get("href"),
                meta={"retry_delay": delay}
            )
```

**Error tracking:**

```python
def handle_error(self, failure):
    """Called when a request fails."""
    self.error_count += 1
    self.logger.warning(f"Request failed: {failure}")
```


## Respect API rate limits

Track API quota usage:

```python
import time

class ApiSpider(Spider):
    name = "api_rate_limited"
    start_urls = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.requests_this_minute = 0
        self.minute_start = time.time()
        self.max_requests_per_minute = 60

    async def start_requests(self):
        for page in range(1, 100):
            # Check rate limit
            if self.requests_this_minute >= self.max_requests_per_minute:
                # Wait until next minute
                elapsed = time.time() - self.minute_start
                if elapsed < 60:
                    delay = 60 - elapsed
                    yield Request(
                        url=f"https://api.example.com/data?page={page}",
                        meta={"retry_delay": delay}
                    )
                    # Reset counter
                    self.requests_this_minute = 0
                    self.minute_start = time.time()
                    continue

            yield Request(url=f"https://api.example.com/data?page={page}")
            self.requests_this_minute += 1
```


## Per-domain rate limiting

Different delays for different domains:

```python
from urllib.parse import urlparse

class MultiDomainSpider(Spider):
    name = "multi_domain"
    start_urls = ["https://fast-site.com", "https://slow-site.com"]

    domain_delays = {
        "fast-site.com": 0.5,   # Fast site: 0.5s delay
        "slow-site.com": 3.0,   # Slow site: 3s delay
    }

    async def parse(self, response):
        rv = self.response_view(response)

        for link in rv.doc.cssselect("a"):
            href = link.get("href")
            if href:
                full_url = rv.urljoin(href)
                domain = urlparse(full_url).netloc

                # Get delay for this domain
                delay = self.domain_delays.get(domain, 1.0)  # Default 1s

                yield rv.follow(
                    href,
                    meta={"retry_delay": delay}
                )
```


## Time-of-day rate limiting

Adjust rates based on time:

```python
from datetime import datetime

class TimeAwareSpider(Spider):
    name = "time_aware"
    start_urls = ["https://example.com"]

    def get_current_delay(self):
        """Adjust delay based on time of day."""
        current_hour = datetime.now().hour

        # Slow down during business hours (9am-5pm)
        if 9 <= current_hour < 17:
            return 5.0
        # Faster during off-hours
        else:
            return 1.0

    async def parse(self, response):
        rv = self.response_view(response)

        delay = self.get_current_delay()

        for link in rv.doc.cssselect("a"):
            yield rv.follow(
                link.get("href"),
                meta={"retry_delay": delay}
            )
```


## Best practices

- **Use meta for state management**: Pass data through request chains using the `meta` dict
- **Respect rate limits**: Configure `DELAY_PER_DOMAIN` and `CONCURRENCY_PER_DOMAIN`
- **Monitor error rates**: Track failures and adjust behavior accordingly
- **Add randomization**: Use `RANDOMIZE_DELAY` to appear more human-like
- **Track API quotas**: Monitor usage against rate limits
- **Be conservative**: Start with longer delays, reduce if stable
- **Respect robots.txt**: Check crawl-delay directives
- **Monitor server load**: Watch for 429 (Too Many Requests) responses
- **Use adaptive limiting**: Automatically slow down on errors
- **Test thoroughly**: Verify rate limiting works before full crawl

See also: [Authentication](authentication.md), [Error Recovery](error_recovery.md), [Crawl Ordering](crawl_ordering.md)