import argparse
import os
import re
from datetime import timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from feedgen.feed import FeedGenerator
from playwright.sync_api import sync_playwright


BASE = "https://www.colliers.com"
DEFAULT_LISTING = "https://www.colliers.com/en/news#sort=%40datez32xpublished%20descending"

ARTICLE_RE = re.compile(
    r"^https://www\.colliers\.com/en/news/[^/?#]+/[^/?#]+/?$",
    re.IGNORECASE,
)


def extract_article_links(listing_url: str, limit: int = 50) -> list[str]:
    """
    Loads the JS-rendered listing page and extracts unique article URLs.
    Uses scrolling to load more items.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        page = context.new_page()

        # Speed: block images/fonts/media
        def route_handler(route):
            rtype = route.request.resource_type
            if rtype in ("image", "font", "media"):
                route.abort()
            else:
                route.continue_()

        page.route("**/*", route_handler)

        page.goto(listing_url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)

        seen = set()
        results: list[str] = []

        last_height = 0
        for _ in range(30):  # max scroll passes
            hrefs = page.eval_on_selector_all(
                "a[href]",
                "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
            )

            for h in hrefs:
                full = urljoin(BASE, h)
                full = full.split("#")[0].rstrip("/") + "/"
                if ARTICLE_RE.match(full) and full not in seen:
                    seen.add(full)
                    results.append(full.rstrip("/"))
                    if len(results) >= limit:
                        context.close()
                        browser.close()
                        return results

            # scroll and see if page grows
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)
            height = page.evaluate("document.body.scrollHeight")
            if height == last_height:
                break
            last_height = height

        context.close()
        browser.close()
        return results


def parse_article(url: str) -> dict:
    """
    Fetch title/date/description from an article page.
    """
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0 (RSS generator)"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Title
    title = None
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
    if not title:
        ogt = soup.find("meta", attrs={"property": "og:title"})
        if ogt and ogt.get("content"):
            title = ogt["content"].strip()
    title = title or url

    # Description
    desc = ""
    md = soup.find("meta", attrs={"name": "description"})
    if md and md.get("content"):
        desc = md["content"].strip()

    # Published date (try common meta tags first)
    pub_dt = None
    for meta_key in (
        ("property", "article:published_time"),
        ("property", "og:updated_time"),
        ("name", "pubdate"),
        ("name", "publish-date"),
    ):
        m = soup.find("meta", attrs={meta_key[0]: meta_key[1]})
        if m and m.get("content"):
            try:
                pub_dt = dateparser.parse(m["content"])
                break
            except Exception:
                pass

    if pub_dt is None:
        time_tag = soup.find("time")
        if time_tag:
            raw = time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
            try:
                pub_dt = dateparser.parse(raw)
            except Exception:
                pub_dt = None

    # last resort: look for "Feb 4, 2026" style text
    if pub_dt is None:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b", text)
        if m:
            try:
                pub_dt = dateparser.parse(m.group(0))
            except Exception:
                pub_dt = None

    if pub_dt and pub_dt.tzinfo is None:
        pub_dt = pub_dt.replace(tzinfo=timezone.utc)

    return {"url": url, "title": title, "description": desc, "published": pub_dt}


def build_rss(items: list[dict], out_file: str) -> None:
    fg = FeedGenerator()
    fg.title("Colliers News (unofficial)")
    fg.link(href="https://www.colliers.com/en/news", rel="alternate")
    fg.description("Unofficial RSS feed generated from https://www.colliers.com/en/news")
    fg.language("en")

    for it in items:
        fe = fg.add_entry()
        fe.id(it["url"])
        fe.link(href=it["url"])
        fe.title(it["title"])
        if it.get("published"):
            fe.published(it["published"])
        if it.get("description"):
            fe.description(it["description"])

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    fg.rss_file(out_file, pretty=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listing", default=DEFAULT_LISTING)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", default="docs/colliers-news.xml")
    args = ap.parse_args()

    links = extract_article_links(args.listing, limit=args.limit)
    articles = []
    for u in links:
        try:
            articles.append(parse_article(u))
        except Exception as e:
            print(f"Skip {u}: {e}")

    # sort newest-first
    def sort_key(x):
        return x["published"] or dateparser.parse("1970-01-01T00:00:00Z")

    articles.sort(key=sort_key, reverse=True)

    build_rss(articles, out_file=args.out)
    print(f"Wrote {args.out} ({len(articles)} items)")


if __name__ == "__main__":
    main()
