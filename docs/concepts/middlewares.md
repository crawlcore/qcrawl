

Middleware is a lightweight, low-level system for globally altering:

* request/response processing (**downloader middleware**)
* wrapping/filtering streams in/out of the spider (**spider middleware**)


## Middleware activation

To activate a middleware component, register middleware instances before the crawl using *Declarative* or
*Programmatic* methods.

Middleware has the following precedence order for applying settings:

```mermaid
flowchart LR
  A["Declarative (settings)"] --> B[Programmatic]
```

### Declarative (settings)
Provide `DOWNLOADER_MIDDLEWARES` and/or `SPIDER_MIDDLEWARES` mappings in code-based settings or spider `custom_settings`.

```py
from qcrawl.middleware.downloader import RetryMiddleware
from qcrawl.middleware.spider import DepthMiddleware

# Global settings (code)
DOWNLOADER_MIDDLEWARES = {
    RetryMiddleware: 400,  # higher number = earlier execution
}

SPIDER_MIDDLEWARES = {
    DepthMiddleware: 100,
}
```

### Programmatic (runtime)

Register middleware before the crawl begins. `Crawler.add_middleware()` accepts:

- an instantiated middleware instance,
- a middleware class (will be instantiated with no args),
- a factory callable that accepts `spider` / `runtime_settings` and returns an instance or `None`.


``` py
from qcrawl.core.crawler import Crawler
from qcrawl.middleware.downloader import RetryMiddleware

spider = QuotesSpider()
crawler = Crawler(spider, runtime_settings)

# instance
crawler.add_middleware(RetryMiddleware(max_retries=5))

# class (will be instantiated with no args)
crawler.add_middleware(RetryMiddleware)

# factory callable
def factory(settings):
    return RetryMiddleware(max_retries=settings.MAX_RETRIES)
crawler.add_middleware(factory)
```


## Available downloader middleware

| Name                      | Purpose                                                                                                        | Configuration parameters                                                                                                                                                                                                                                                                                                                                                                                                              |
|---------------------------|----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ConcurrencyMiddleware`   | Limit concurrent requests per-domain and globally, manage request delays per-domain.                           | `concurrency_per_domain: int` — max concurrent requests per domain (default `8`)<br>`concurrency: int` — max global concurrent requests (default `32`)<br>`delay_per_domain: float` — delay between requests to same domain in seconds (default `0.0`)                                                                                                                                                                                |
| `CookiesMiddleware`       | Manage cookies per-spider and per-domain: send `Cookie` headers and extract `Set-Cookie`.                      | --                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `DownloadDelayMiddleware` | Enforce a fixed delay between requests to the same domain.                                                     | `delay_per_domain: float` — delay between requests to same domain in seconds (default `0.0`)                                                                                                                                                                                                                                                                                                                                          |
| `HttpAuthMiddleware`      | Handle Basic and Digest HTTP authentication (proactive Basic, reactive Digest with 401).                       | `credentials: dict[str, tuple[str, str]]` — per-domain credentials (optional)<br>`auth_type: 'basic', 'digest'` — default `basic`<br>`digest_qop_auth_int: bool` — enable qop=`auth-int` support (default `False`).                                                                                                                                                                                                                   |
| `ProxyMiddleware`         | Route requests via HTTP/SOCKS proxies, with optional per-domain and per-request overrides.                     | `proxies: dict[str, str]` — per-domain proxy URLs (optional)<br>`default_proxy: str` — default proxy URL (optional)<br>Per-request: `request.meta['proxy']` to override proxy URL or disable with `None`.                                                                                                                                                                                                                             |
| `RedirectMiddleware`      | Follow HTTP 3xx redirects, build new `Request`s and enforce redirect hop limits.                               | `max_redirects: int` — maximum redirect hops (default `10`)                                                                                                                                                                                                                                                                                                                                                                           |
| `RetryMiddleware`         | Retry transient network failures and specified HTTP status codes with exponential backoff.                     | `max_retries: int` — maximum attempts (default `3`)<br>`retry_http_codes: [int]` — HTTP status codes to retry (default `{429,500,502,503,504}`)<br>`priority_adjust: int` — priority delta for retries (default `-1`)<br>`backoff_base: float` — base seconds for exponential backoff (default `1.0`)<br>`backoff_max: float` — cap for backoff (default `60.0`)<br>`backoff_jitter: float` — jitter factor `0.0-1.0` (default `0.3`) |
| `RobotsTxtMiddleware`     | Fetch and parse `robots.txt`, enforce `allow/deny` and apply crawl-delay as `request.meta['retry_delay']`.     | `user_agent: str` (default `*`)<br>`obey_robots_txt: bool` (default `True`)<br>`cache_ttl: float` (seconds, default `3600.0`)                                                                                                                                                                                                                                                                                                         |


## Available spider middlewares

| Name                  | Purpose                                                                                   | Configuration parameters                                                                                        |
|-----------------------|-------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `DepthMiddleware`     | Limit crawl depth, track depth distribution, and adjust request priority.                 | `default_max_depth: int` (default `0`)<br>`default_priority: int` (default `1`)                                 |
| `HttpErrorMiddleware` | Filter responses with disallowed HTTP status codes and emit stats for filtered responses. | `allowed_codes: list[int]` (default `200-399`)<br>Per-spider: `HTTPERROR_ALLOW_ALL`, `HTTPERROR_ALLOWED_CODES`  |
| `OffsiteMiddleware`   | Filter requests to URLs outside configured allowed domains.                               | `enabled: bool` — default `True`<br>Per-spider: `ALLOWED_DOMAINS` (str/list/tuple/set)                          |
