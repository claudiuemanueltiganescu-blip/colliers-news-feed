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

# The "real" title links on the page are not these:
SKIP_TEXT = {
    "read more",
    "view more",
    "view all news",
    "view all",
    "view podcasts",
    "view media mentions",
    "learn more",
}

# This is the section you want to keep; everything after Podcasts is ignored.
START_MARKERS = [
    "keep up with the latest commercial real estate news and trends",
    "keep up with the latest",
]
STOP_HEADINGS = {
    "podcasts",
    "media mentions",
    "press releases / announcements",
    "knowledge leader",
}

ARTICLE_RE = re.compile(
    r"^https://www\.colliers\.com/en/news/[^/?#]+/[^/?#]+/?$",
    re.IGNORECASE,
)


def normalize_url(base: str, href: str) -> str:
    u = urljoin(base, href)
    p = urlparse(u)
    return p._replace(query="", fragment="").geturl().rstrip("/")


def find_heading_containing(soup: BeautifulSoup, needles_lower: list[str]):
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
        txt = tag.get_text(" ", strip=True).lower()
        if any(n in txt for n in needles_lower):
            return tag
    return None


def find_next_stop_heading(start_tag, stop_set_lower: set[str]):
    if not start_tag:
        return None
    for el in start_tag.next_elements:
        if getattr(el, "name", None) in ["h1", "h2", "h3", "h4", "h5"]:
            t = el.get_text(" ", strip=True).lower()
            if t in stop_set_lower:
                return el
    return None


def iter_elements_between(start_tag, stop_tag):
    """
    Yield elements after start_tag until stop_tag is reached.
    If start_tag is None, start from the whole document.
    """
    if start_tag is None:
        # fall back: yield everything
        return []
    out = []
    for el in start_tag.next_elements:
        if el is stop_tag:
            break
        out.append(el)
    return out


def find_card_with_date(a, max_up: int = 10):
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


def extract_date(card) -> datetime | None:
    if not card or not getattr(card, "get_text", None):
        return None
    txt = card.get_text(" ", strip=True)
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
        if DATE_RE.fullmatch(t.strip()):
            continue
        if 10 <= len(t) <= 400:
            return t
    return ""


def get_items_from_news_section(listing_url: str, limit: int) -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Unofficial RSS generator)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(listing_url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    start = find_heading_containing(soup, START_MARKERS)
    stop = find_next_stop_heading(start, {s.lower() for s in STOP_HEADINGS})

    # If we couldn't find the start marker, fall back to "everything before Podcasts"
    if start is None:
        stop2 = find_heading_containing(soup, ["podcasts"])
        elements = []
        for el in soup.descendants:
            if el is stop2:
                break
            elements.append(el)
    else:
        elements = iter_elements_between(start, stop)

    by_url: dict[str, dict] = {}

    for el in elements:
        if getattr(el, "name", None) != "a":
            continue
        href = el.get("href")
        if not href:
            continue

        url = normalize_url(listing_url, href)
        if not ARTICLE_RE.match(url):
            continue

        title = el.get_text(" ", strip=True)
        if not title:
            continue
        if title.strip().lower() in SKIP_TEXT:
            continue

        card = find_card_with_date(el)
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
            # upgrade fields if we discover better info
            if existing.get("published") is None and published is not None:
                existing["published"] = published
            if (not existing.get("description")) and desc:
                existing["description"] = desc
            if len(title) > len(existing.get("title", "")):
                existing["title"] = title

    items = list(by_url.values())
    items.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return items[:limit]


def build_rss(listing_url: str, items: list[dict], out_file: str) -> None:
    fg = FeedGenerator()
    fg.title("Colliers News (unofficial) — Top News section only")
    fg.link(href=listing_url, rel="alternate")
    fg.description("Unofficial RSS feed generated from the top News section (excludes Podcasts, Media mentions, etc).")
    fg.language("en")

    for it in items:  # already newest-first
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

    items = get_items_from_news_section(args.listing, args.limit)
    build_rss(args.listing, items, args.out)
    print(f"Wrote {args.out} ({len(items)} items)")


if __name__ == "__main__":
    main()
