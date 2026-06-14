
The scheduler processes requests based on priority values. Requests with a lower priority value are processed first (priority `0` is the default). By adjusting priorities, you can control whether your crawler explores pages breadth-first (level by level), depth-first (following paths deeply), or with custom focus on specific content.

## Breadth-first crawl (default)

All same-depth URLs have equal priority:

```python
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request

class BreadthFirstSpider(Spider):
    name = "breadth_first"
    start_urls = ["https://example.com"]

    async def parse(self, response):
        rv = self.response_view(response)

        # All links get same priority (processed in order discovered)
        for link in rv.doc.cssselect("a"):
            href = link.get("href")
            if href:
                yield rv.follow(href, priority=0)
```

**Use cases:**

- Discovering all pages at each level before going deeper
- Site mapping and structure discovery
- When order doesn't matter


## Depth-first crawl

Prioritize deeper pages by increasing priority with depth:

```python
async def parse(self, response):
    rv = self.response_view(response)

    current_depth = response.request.meta.get("depth", 0)
    next_depth = current_depth + 1

    for link in rv.doc.cssselect("a"):
        href = link.get("href")
        if href:
            # Lower priority value = processed sooner, so each request gets
            # priority -depth: the deepest queued link is always followed first.
            yield rv.follow(
                href,
                priority=-next_depth,
                meta={"depth": next_depth},
            )
```

**Use cases:**

- Following specific content paths deeply
- Getting to target pages quickly
- Exploring hierarchical structures


## Focused crawling

Prioritize specific content types or URL patterns:

```python
async def parse(self, response):
    rv = self.response_view(response)

    # High priority for target content (lower value = processed first)
    if "product" in response.url:
        for link in rv.doc.cssselect("a.product"):
            yield rv.follow(link.get("href"), priority=-100)

    # Lower priority for other pages
    else:
        for link in rv.doc.cssselect("a"):
            yield rv.follow(link.get("href"), priority=10)
```

**Use cases:**

- Prioritizing valuable content
- Targeted data extraction
- Efficient resource usage


## Combining with depth limits

Control crawl depth using settings:

```python
class MySpider(Spider):
    name = "limited_depth"
    start_urls = ["https://example.com"]

    custom_settings = {
        "MAX_DEPTH": 3,  # Stop after 3 levels
    }

    async def parse(self, response):
        rv = self.response_view(response)

        for link in rv.doc.cssselect("a"):
            yield rv.follow(link.get("href"))
```


## Best practices

- **Use appropriate crawl order**: Choose breadth-first, depth-first, or focused based on your needs
- **Use priority sparingly**: Most requests should be priority 0; reserve high priority for critical paths
- **Track depth with meta**: Monitor crawl depth to prevent excessive nesting
- **Set MAX_DEPTH**: Limit crawl depth to prevent runaway crawls
- **Document your strategy**: Comment why certain priorities are set
- **Test incrementally**: Verify crawl order matches expectations with small test runs

See also: [Link Filtering](link_filtering.md), [Pagination](pagination.md), [Scheduler](../extending/scheduler.md)