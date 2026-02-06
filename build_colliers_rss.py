import argparse
import os
import re
from datetime import timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from feedgen.feed import FeedGenerator

DATE_RE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b"
)

SKIP_TEXT = {
    "read more",
    "view more",
    "view all news",
    "view all",
    "view podcasts",
    "view media mentions",
    "learn more",
}


def normalize_url(base: str, href: str) -> str:
    u = urljoin(base, href)
    p = urlparse(u)
    u = p._replace(query="", fragment="").geturl()
    return u.rstrip("/")


def guess_date_near(anchor) -> str | None:
    # Usually the date appears right before the title link on the listing page.
    prev = anchor.find_previous(string=DATE_RE)
    if not prev:
        return None
    m = DATE_RE.search(str(prev))
    return m.group(0) if m else None


def guess_description(anchor, title: str, date_str: str | None) -> str:
    # Try to find a short snippet in the same “card”.
    card = anchor
    for _ in range(6):
        if not getattr(card, "parent", None):
            break
        card = card.parent
        txt = card.get_text(" ", strip=True) if hasattr(card, "get_text") else ""
        if title in txt and (date_str is None or date_str in txt):
            # likely container
            break

    # Prefer <p> text inside the card
    if hasattr(card, "find_all"):
        for p in card.find_all("p"):
            t = p.get_text(" ", strip=True)
            if not t:
                continue
            low = t.lower()
            if low in SKIP_TEXT:
                continue
            if t == title:
                continue
            if date_str and date_str in t:
                continue
            if len(t) > 400:
                continue
            if len(t) < 10:
                continue
            return t

    return ""


def get_listing_items(listing_url: str, limit: int) -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Unofficial RSS generator)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(listing_url, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    items: list[dict] = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        url = normalize_url(listing_url, href)

        # Only Colliers news article URLs
        if "/en/news/" not in url:
            continue
        if url.endswith("/en/news"):
            continue

        title = a.get_text(" ", strip=True)
        if not title:
            continue
        if title.strip().lower() in SKIP_TEXT:
            continue

        if url in seen_urls:
            continue

        date_str = guess_date_near(a)
        published = None
        if date_str:
            try:
                published = dateparser.parse(date_str)
                if published.tzinfo is None:
                    published = published.replace(tzinfo=timezone.utc)
            except Exception:
                published = None

        desc = guess_description(a, title, date_str)

        items.append(
            {
                "url": url,
                "title": title,
                "description": desc,
                "published": published,
            }
        )
        seen_urls.add(url)

        if len(items) >= limit:
            break

    # Newest first (if dates exist)
    items.sort(
        key=lambda x: x["published"] or dateparser.parse("1970-01-01T00:00:00Z"),
        reverse=True,
    )
    return items


def build_rss(listing_url: str, items: list[dict], out_file: str) -> None:
    fg = FeedGenerator()
    fg.title("Colliers News (unofficial)")
    fg.link(href=listing_url, rel="alternate")
    fg.description("Unofficial RSS feed generated from Colliers listing pages.")
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
    ap.add_argument("--listing", required=True)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", default="docs/colliers-news.xml")
    args = ap.parse_args()

    items = get_listing_items(args.listing, args.limit)
    build_rss(args.listing, items, args.out)
    print(f"Wrote {args.out} ({len(items)} items)")


if __name__ == "__main__":
    main()
