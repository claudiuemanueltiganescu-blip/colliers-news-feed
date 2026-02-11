import argparse
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from feedgen.feed import FeedGenerator

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# -----------------------
# Shared helpers
# -----------------------
def normalize_url(base: str, href: str) -> str:
    u = urljoin(base, href)
    p = urlparse(u)
    return p._replace(query="", fragment="").geturl().rstrip("/")


def uniq_preserve(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# -----------------------
# Colliers: top News section only (exclude Podcasts etc.)
# -----------------------
COLLIERS_DATE_RE = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b")

COLLIERS_SKIP_TEXT = {
    "read more",
    "view more",
    "view all news",
    "view all",
    "view podcasts",
    "view media mentions",
    "learn more",
}

COLLIERS_START_MARKERS = [
    "keep up with the latest commercial real estate news and trends",
    "keep up with the latest",
]
COLLIERS_STOP_HEADINGS = {
    "podcasts",
    "media mentions",
    "press releases / announcements",
    "knowledge leader",
}

COLLIERS_ARTICLE_RE = re.compile(
    r"^https://www\.colliers\.com/en/news/[^/?#]+/[^/?#]+/?$",
    re.IGNORECASE,
)


def colliers_find_heading_containing(soup: BeautifulSoup, needles_lower: list[str]):
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
        txt = tag.get_text(" ", strip=True).lower()
        if any(n in txt for n in needles_lower):
            return tag
    return None


def colliers_find_next_stop_heading(start_tag, stop_set_lower: set[str]):
    if not start_tag:
        return None
    for el in start_tag.next_elements:
        if getattr(el, "name", None) in ["h1", "h2", "h3", "h4", "h5"]:
            t = el.get_text(" ", strip=True).lower()
            if t in stop_set_lower:
                return el
    return None


def colliers_iter_elements_between(start_tag, stop_tag):
    if start_tag is None:
        return []
    out = []
    for el in start_tag.next_elements:
        if el is stop_tag:
            break
        out.append(el)
    return out


def colliers_find_card_with_date(a, max_up: int = 10):
    node = a
    for _ in range(max_up):
        if not getattr(node, "get_text", None):
            break
        txt = node.get_text(" ", strip=True)
        if COLLIERS_DATE_RE.search(txt):
            return node
        node = getattr(node, "parent", None)
        if node is None:
            break
    return a.parent if getattr(a, "parent", None) else a


def colliers_extract_date(card) -> datetime | None:
    if not card or not getattr(card, "get_text", None):
        return None
    txt = card.get_text(" ", strip=True)
    m = COLLIERS_DATE_RE.search(txt)
    if not m:
        return None
    try:
        dt = dateparser.parse(m.group(0))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except Exception:
        return None


def colliers_extract_description(card, title: str) -> str:
    if not card or not getattr(card, "find_all", None):
        return ""
    for p in card.find_all("p"):
        t = p.get_text(" ", strip=True)
        if not t:
            continue
        low = t.lower().strip()
        if low in COLLIERS_SKIP_TEXT:
            continue
        if t.strip() == title.strip():
            continue
        if COLLIERS_DATE_RE.fullmatch(t.strip()):
            continue
        if 10 <= len(t) <= 400:
            return t
    return ""


def get_colliers_items(listing_url: str, limit: int) -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Unofficial RSS generator)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(listing_url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    start = colliers_find_heading_containing(soup, COLLIERS_START_MARKERS)
    stop = colliers_find_next_stop_heading(start, {s.lower() for s in COLLIERS_STOP_HEADINGS})

    if start is None:
        # Fallback: everything before Podcasts heading
        podcasts_heading = colliers_find_heading_containing(soup, ["podcasts"])
        elements = []
        for el in soup.descendants:
            if el is podcasts_heading:
                break
            elements.append(el)
    else:
        elements = colliers_iter_elements_between(start, stop)

    by_url: dict[str, dict] = {}

    for el in elements:
        if getattr(el, "name", None) != "a":
            continue
        href = el.get("href")
        if not href:
            continue

        url = normalize_url(listing_url, href)
        if not COLLIERS_ARTICLE_RE.match(url):
            continue

        title = el.get_text(" ", strip=True)
        if not title:
            continue
        if title.strip().lower() in COLLIERS_SKIP_TEXT:
            continue

        card = colliers_find_card_with_date(el)
        published = colliers_extract_date(card)
        desc = colliers_extract_description(card, title)

        existing = by_url.get(url)
        if not existing:
            by_url[url] = {
                "url": url,
                "title": title,
                "description": desc,
                "published": published,
                "source": "Colliers",
            }
        else:
            if existing.get("published") is None and published is not None:
                existing["published"] = published
            if (not existing.get("description")) and desc:
                existing["description"] = desc
            if len(title) > len(existing.get("title", "")):
                existing["title"] = title

    items = list(by_url.values())
    items.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return items[:limit]


# -----------------------
# Northmarq: Transactions via "load more" endpoint (avoid robot-check page)
# -----------------------
NORTHMARQ_BASE_SITE = "https://www.northmarq.com"
NORTHMARQ_YM_RE = re.compile(r"-(\d{4})-(\d{2})(?:$|/)")


def northmarq_published_from_url(url: str) -> datetime | None:
    m = NORTHMARQ_YM_RE.search(url)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2))
    # Use 1st of month (safe ordering vs day-specific sources)
    return datetime(year, month, 1, tzinfo=UTC)


def get_northmarq_items(load_more_base: str, pages: int, limit: int) -> list[dict]:
    """
    load_more_base example:
      https://www.northmarq.com/northmarq_load_more/transactions/transactions
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Unofficial RSS generator)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    session = requests.Session()
    by_url: dict[str, dict] = {}

    for page in range(1, pages + 1):
        url = f"{load_more_base}/{page}"
        resp = session.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Each card is an <article> for a transaction node
        for article in soup.find_all("article"):
            a = article.find("a", href=True)
            if not a:
                continue

            href = a["href"]
            full_url = normalize_url(NORTHMARQ_BASE_SITE, href)

            if "/transactions/" not in full_url:
                continue

            title = (a.get("aria-label") or "").strip()
            if not title:
                # fallback to any h3 text
                h3 = article.find("h3")
                title = h3.get_text(" ", strip=True) if h3 else full_url

            # Build a compact description from the card fields (no detail-page fetch)
            deal_types = [x.get_text(" ", strip=True) for x in article.select(".field--name-field-deal-type .field__item")]
            deal_types = [x for x in deal_types if x]
            deal_types = ", ".join(uniq_preserve(deal_types))

            locality = article.select_one(".field--name-field-address .locality")
            admin = article.select_one(".field--name-field-address .administrative-area")
            location = ""
            if locality and admin:
                location = f"{locality.get_text(strip=True)}, {admin.get_text(strip=True)}"
            elif locality:
                location = locality.get_text(strip=True)

            price_el = article.select_one(".field--name-field-price")
            price = price_el.get_text(" ", strip=True) if price_el else ""

            parts = [p for p in [deal_types, location, price] if p]
            desc = " — ".join(parts)

            published = northmarq_published_from_url(full_url)

            if full_url not in by_url:
                by_url[full_url] = {
                    "url": full_url,
                    "title": title,
                    "description": desc,
                    "published": published,
                    "source": "Northmarq",
                }

            if len(by_url) >= limit:
                break

        if len(by_url) >= limit:
            break

        # Be polite
        time.sleep(0.7)

    items = list(by_url.values())
    items.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return items[:limit]


# -----------------------
# Merge + write RSS
# -----------------------
def merge_items(lists: list[list[dict]], total_limit: int) -> list[dict]:
    by_url: dict[str, dict] = {}
    for lst in lists:
        for it in lst:
            url = it["url"]
            existing = by_url.get(url)
            if not existing:
                by_url[url] = it
            else:
                # Keep the one with a date if the other doesn't
                if existing.get("published") is None and it.get("published") is not None:
                    by_url[url] = it

    merged = list(by_url.values())
    merged.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return merged[:total_limit]


def write_rss(items: list[dict], out_file: str, home_link: str) -> None:
    fg = FeedGenerator()
    fg.title("Colliers + Northmarq (unofficial)")
    fg.link(href=home_link, rel="alternate")
    fg.description("Unofficial combined feed: Colliers top News section + Northmarq recent transactions.")
    fg.language("en")

    for it in items:
        fe = fg.add_entry()
        fe.id(it["url"])
        fe.link(href=it["url"])
        # Prefix title so your reader clearly shows the source
        fe.title(f"[{it['source']}] {it['title']}")
        if it.get("published"):
            fe.published(it["published"])
        if it.get("description"):
            fe.description(it["description"])
        fe.category(term=it["source"])

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    fg.rss_file(out_file, pretty=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--colliers", required=True, help="Colliers listing page URL")
    ap.add_argument("--northmarq_base", required=True, help="Northmarq load-more base URL (no trailing /page)")
    ap.add_argument("--northmarq_pages", type=int, default=3)
    ap.add_argument("--colliers_limit", type=int, default=50)
    ap.add_argument("--northmarq_limit", type=int, default=50)
    ap.add_argument("--total_limit", type=int, default=50)
    ap.add_argument("--out", default="docs/colliers-news.xml")
    args = ap.parse_args()

    colliers_items = get_colliers_items(args.colliers, limit=args.colliers_limit)
    northmarq_items = get_northmarq_items(args.northmarq_base, pages=args.northmarq_pages, limit=args.northmarq_limit)

    combined = merge_items([colliers_items, northmarq_items], total_limit=args.total_limit)

    # Use Colliers listing as the "home" link for the feed
    write_rss(combined, out_file=args.out, home_link=args.colliers)

    print(
        f"Wrote {args.out} "
        f"(Colliers: {len(colliers_items)}, Northmarq: {len(northmarq_items)}, Combined: {len(combined)})"
    )


if __name__ == "__main__":
    main()
