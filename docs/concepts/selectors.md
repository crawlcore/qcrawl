When scraping web pages, the primary task is extracting data from the documents.
qCrawl uses lxml as the default HTML parser because of its speed and native XPath support.

## Using selectors

I will assume you have a response view object `rv` created from a response:

```python
async def parse(self, response):
    rv = self.response_view(response)
    tree = rv.doc  # lxml.html.HtmlElement
```

### Basic Examples

```python
# Element with id="main"
tree.cssselect('#main')

# Element with class="highlight" (note: class is multi-valued)
tree.cssselect('.highlight')

# <div> with class="post"
tree.cssselect('div.post')

# <a> tags with href attribute containing "login"
tree.cssselect('a[href*="login"]')

# Direct child: <ul> > <li>
tree.cssselect('ul > li')

# Any descendant: <ul> <li>
tree.cssselect('ul li')

```

### Common Patterns

```python
# All text inside .article-body
body_text = tree.cssselect('.article-body')[0].text_content()

# All image srcs
images = [img.get('src') for img in tree.cssselect('img') if img.get('src')]

# Product prices (common patterns)
prices = tree.cssselect('.price, .product-price, [itemprop="price"]')

# OpenGraph / meta tags
meta_title = tree.cssselect('meta[property="og:title"]')[0].get('content')
```

### Advanced Examples

#### Selecting by Multiple Classes (AND logic)

```html title="example.html"
<div class="post featured important">...</div>
```
```python
# Matches elements that have ALL three classes
tree.cssselect('.post.featured.important')
# No space between dots = AND
```

#### Case-Insensitive Attribute Matching (CSS4)

```python
# HTML5 allows this in selectors
tree.cssselect('[rel="NoFollow" i]')   # matches "nofollow", "NoFollow", etc.
```

#### Using :has() for Complex Relationships
```python
# All articles that contain an image
articles_with_images = tree.cssselect('article:has(img)')

# All comments that contain a reply form
tree.cssselect('.comment:has(> form)')
```

#### Using :is() to Reduce Repetition
```python
# Instead of repeating header selectors
headers = tree.cssselect('h1.title, h2.title, h3.title, h4.title')

# Cleaner with :is()
headers = tree.cssselect(':is(h1,h2,h3,h4).title')
```

#### Form Inputs by Type
```python
tree.cssselect('input[type="text"]')
tree.cssselect('input[type="checkbox"]:checked')  # Note: :checked works
```

### Performance Tips

1. Be as specific as possible from the left

    ```python
    # Fast
    tree.cssselect('div#content p.title')

    # Slower (scans entire document)
    tree.cssselect('p.title')
    ```

2. Use direct child > when possible

    ```html title="example.html"
    <article class="product">
      <div class="title">Awesome Gadget</div>
      <div class="price">$99.99</div>
      <div class="reviews">
        <span class="rating">4.8</span>
        <div class="title">Customer Reviews</div>   <!-- nested title! -->
      </div>
    </article>

    <article class="product"> ... another product ... </article>
    ```

    ```python
    # This will accidentally match the "Customer Reviews" title too!
    titles = tree.cssselect('.product .title')

    # Only matches <div class="title"> that is a DIRECT child of .product
    titles = tree.cssselect('.product > .title')
    ```

3. For repeated selections, compile the selector once:

    ```python
    from cssselect import GenericTranslator
    from qcrawl.core.spider import Spider
    from qcrawl.core.item import Item

    # compile once (module/class level) for reuse
    _XPATH_LINKS = GenericTranslator().css_to_xpath("article.post > a.title")

    class ExampleSpider(Spider):
        name = "example"
        start_urls = ["https://example.com/"]

        async def parse(self, response):
            rv = self.response_view(response)
            tree = rv.doc  # lxml.html.HtmlElement

            for a in tree.xpath(_XPATH_LINKS):
                href = a.get("href")
                text = a.text_content().strip() if a is not None else ""
                if not href:
                    continue
                yield Item(data={"url": rv.follow(href), "title": text})
    ```

### Common Pitfalls & Gotchas

| Issue                                  | Solution                                                                                                                                                                                                                                                |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Namespaces (e.g., XHTML, SVG)          | Choose HTML vs XML mode deliberately: use `html.fromstring()` for HTML (lenient parsing), use `etree.fromstring()` (or `etree.XMLParser`) for XML with namespace-aware XPath. Pass `namespaces` mapping to `.xpath()` when needed.                      |
| Dynamic content (JavaScript-loaded)    | lxml does not execute JavaScript. Render with a browser automation tool (Selenium, Playwright), then pass the rendered HTML to lxml for parsing.                                                                                                        |
| Anti-scraping (obfuscated class names) | Prefer structure- or attribute-based selectors (tags, ARIA roles, data- attributes, text), use heuristics (position, parent/child relationships), or render+interact via a headless browser. Rotate user-agents / IPs and respect robots.txt and terms. |

Notes

- For mixed XML/HTML sources (RSS, Atom, SVG embedded in HTML), parse with the appropriate parser and use `namespaces` in XPath queries.
- When using browser rendering, capture the final page HTML and call `html.fromstring(rendered_html)` to continue using lxml selectors.
- Keep selectors resilient: avoid relying solely on ephemeral class names; prefer semantic attributes when possible.

## Supported CSS Selectors

| Selector                         | Example                        | Description                                     |
|----------------------------------|--------------------------------|-------------------------------------------------|
| Type selector                    | `div`                          | All `<div>` elements                            |
| Universal                        | `*`                            | All elements                                    |
| Class                            | `.warning`                     | Any element with class containing `"warning"`   |
| ID                               | `#header`                      | Element with `id="header"`                      |
| Attribute (exact)                | `[href="https://example.com"]` | Exact match                                     |
| Attribute (whitespace-separated) | `[class~="special"]`           | Class contains word `"special"`                 |
| Attribute (starts with)          | `[href^="https://"]`           | `href` begins with `https://`                   |
| Attribute (ends with)            | `[href$=".pdf"]`               | `href` ends with `.pdf`                         |
| Attribute (contains)             | `[href*="example"]`            | `href` contains `"example"`                     |
| Child combinator                 | `div > p`                      | Direct children only                            |
| Descendant combinator            | `div p`                        | Any `<p>` inside `<div>`                        |
| Adjacent sibling                 | `h2 + p`                       | `<p>` immediately after `<h2>`                  |
| General sibling                  | `h2 ~ p`                       | Any `<p>` after `<h2>` (not necessarily direct) |
| `:first-child`                   | `li:first-child`               | First `<li>` in its parent                      |
| `:last-child`                    | `li:last-child`                | Last `<li>` in its parent                       |
| `:nth-child(n)`                  | `tr:nth-child(odd)`            | Odd rows (or even/number expressions)           |
| `:nth-child(an+b)`               | `li:nth-child(3n+1)`           | 1st, 4th, 7th...                                |
| `:nth-last-child()`              | `li:nth-last-child(2)`         | Second-to-last `<li>`                           |
| `:only-child`                    | `p:only-child`                 | `<p>` that is the only child                    |
| `:empty`                         | `td:empty`                     | Elements with no children/text                  |
| `:not()`                         | `a:not([href])`                | `<a>` without `href`                            |
| `:has()` (CSS4)                  | `div:has(> img)`               | `<div>` that directly contains an `<img>`       |
| `:is()` / `:where()`             | `section :is(h1, h2, h3)`      | Any `h1`/`h2`/`h3` inside `section`             |


