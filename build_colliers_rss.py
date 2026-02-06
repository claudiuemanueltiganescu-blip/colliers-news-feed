import argparse
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from feedgen.feed import FeedGenerator

DATE_RE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b"
)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

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


def find_card_with_date(a, max_up: int = 10):
    """
    Walk up the DOM to find a container that includes a date.
    This avoids 'find_previous' mistakes and makes dates reliable.
    """
    node = a
    for _ in range(max_up):
        if not getattr(node, "get_text", None):
            break
        txt = node.get_text(" ", strip=True)
        if DATE_RE.search(txt):
            return node
        node = getattr(node, "parent", None)
        if node is None:
            break
    return a.parent if getattr(a, "parent", None) else a


def extract_date(node) -> datetime | None:
    if not node or not getattr(node, "get_text", None):
        return None
    txt = node.get_text(" ", strip=True)
    m = DATE_RE.search(txt)
    if not m:
        return None
    try:
        dt = dateparser.parse(m.group(0))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def extract_description(card, title: str) -> str:
    if not card or not getattr(card, "find_all", None):
        return ""
    for p in card.find_all("p"):
        t = p.get_text(" ", strip=True)
        if not t:
            continue
        low = t.lower().strip()
        if low in SKIP_TEXT:
            continue
        if t.strip() == title.strip():
            continue
        # ignore pure date lines
        if DATE_RE.fullmatch(t.strip()):
            continue
        # keep it reasonable
        if 10 <= len(t) <= 400:
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

    # Use dict so we can "upgrade" an item later if we find a better date/desc.
    by_url: dict[str, dict] = {}

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

        card = find_card_with_date(a)
        published = extract_date(card)
        desc = extract_description(card, title)

        existing = by_url.get(url)
        if not existing:
            by_url[url] = {
                "url": url,
                "title": title,
                "description": desc,
                "published": published,
            }
        else:
            # Keep the best/most complete fields we discover
            if existing.get("published") is None and published is not None:
                existing["published"] = published
            elif existing.get("published") is not None and published is not None:
                # If somehow different, keep the newer
                if published > existing["published"]:
                    existing["published"] = published

            if (not existing.get("description")) and desc:
                existing["description"] = desc

            # Prefer a longer/more "real" title if we ever got a weak one
            if len(title) > len(existing.get("title", "")):
                existing["title"] = title

    items = list(by_url.values())

    # Force newest-first in the XML
    items.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)

    return items[:limit]


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
