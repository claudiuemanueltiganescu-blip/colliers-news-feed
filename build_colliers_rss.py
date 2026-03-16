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


def chrome_ua() -> str:
    # A boring, normal desktop Chrome UA (avoid “bot” words)
    return (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )


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
        "User-Agent": chrome_ua(),
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(listing_url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    start = colliers_find_heading_containing(soup, COLLIERS_START_MARKERS)
    stop = colliers_find_next_stop_heading(start, {s.lower() for s in COLLIERS_STOP_HEADINGS})

    if start is None:
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
# Northmarq: Transactions (handle 403 on GitHub Actions)
# -----------------------
NORTHMARQ_BASE = "https://www.northmarq.com"
NORTHMARQ_LISTING_URL = "https://www.northmarq.com/recent-closings-transactions"

# many transaction slugs include -YYYY-MM or -YYYY-MM-DD near the end
NORTHMARQ_DATE_IN_SLUG_RE = re.compile(r"-(\d{4})-(\d{2})(?:-(\d{2}))?(?:$|/)")


def northmarq_published_from_url(url: str) -> datetime | None:
    m = NORTHMARQ_DATE_IN_SLUG_RE.search(url)
    if not m:
        return None
    y = int(m.group(1))
    mo = int(m.group(2))
    day = int(m.group(3)) if m.group(3) else 1
    try:
        # noon UTC to avoid timezone edge cases
        return datetime(y, mo, day, 12, 0, 0, tzinfo=UTC)
    except ValueError:
        return None


def northmarq_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": chrome_ua(),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NORTHMARQ_LISTING_URL,
            "Origin": NORTHMARQ_BASE,
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    return s


def northmarq_items_from_card_html(html: str, limit: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    by_url: dict[str, dict] = {}

    for article in soup.find_all("article"):
        a = article.find("a", href=True)
        if not a:
            continue

        href = a["href"].strip()
        if not href.startswith("/transactions/"):
            continue

        full_url = normalize_url(NORTHMARQ_BASE, href)

        title = (a.get("aria-label") or "").strip()
        if not title:
            h3 = article.find("h3")
            title = h3.get_text(" ", strip=True) if h3 else a.get_text(" ", strip=True)
        title = title.strip() or full_url

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

        desc_parts = [p for p in [deal_types, location, price] if p]
        desc = " — ".join(desc_parts)

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

    items = list(by_url.values())
    items.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return items[:limit]


def northmarq_decode_load_more_response(resp: requests.Response) -> str:
    """
    The load_more endpoint typically returns JSON commands with HTML in a `data` field.
    Decoding JSON turns \\u003C into '<', so BeautifulSoup can parse it.
    """
    try:
        payload = resp.json()
    except Exception:
        return resp.text

    html_parts: list[str] = []
    if isinstance(payload, list):
        for obj in payload:
            if isinstance(obj, dict):
                data = obj.get("data")
                if isinstance(data, str) and data:
                    html_parts.append(data)
    elif isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, str) and data:
            html_parts.append(data)

    return "\n".join(html_parts) if html_parts else resp.text


def get_northmarq_items(load_more_base: str, pages: int, limit: int) -> list[dict]:
    s = northmarq_session()

    # Warm-up request to pick up any cookies / routing
    try:
        s.get(NORTHMARQ_LISTING_URL, timeout=30)
    except Exception:
        pass

    by_url: dict[str, dict] = {}

    for page in range(1, pages + 1):
        url = f"{load_more_base}/{page}"
        resp = s.get(url, timeout=30)

        # If blocked on the ajax endpoint, fall back to scraping the listing page (first page only)
        if resp.status_code == 403:
            print("[Northmarq] 403 on load_more endpoint from this runner. Falling back to listing page.")
            try:
                r2 = s.get(NORTHMARQ_LISTING_URL, timeout=30)
                if r2.status_code == 403:
                    print("[Northmarq] 403 on listing page too. Skipping Northmarq for this run.")
                    return []
                r2.raise_for_status()
                return northmarq_items_from_card_html(r2.text, limit=limit)
            except Exception as e:
                print(f"[Northmarq] Fallback failed: {e}. Skipping Northmarq for this run.")
                return []

        # Other non-OK
        try:
            resp.raise_for_status()
        except Exception as e:
            print(f"[Northmarq] HTTP error page={page}: {e}")
            continue

        html = northmarq_decode_load_more_response(resp)
        items = northmarq_items_from_card_html(html, limit=limit)

        for it in items:
            if it["url"] not in by_url:
                by_url[it["url"]] = it
            if len(by_url) >= limit:
                break

        if len(by_url) >= limit:
            break

        time.sleep(0.8)

    out = list(by_url.values())
    out.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return out[:limit]


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
                # prefer the one that has a published date
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
    ap.add_argument("--northmarq_pages", type=int, default=2)
    ap.add_argument("--colliers_limit", type=int, default=60)
    ap.add_argument("--northmarq_limit", type=int, default=60)
    ap.add_argument("--total_limit", type=int, default=120)
    ap.add_argument("--out", default="docs/colliers-news.xml")
    args = ap.parse_args()

    colliers_items = get_colliers_items(args.colliers, limit=args.colliers_limit)
    northmarq_items = get_northmarq_items(args.northmarq_base, pages=args.northmarq_pages, limit=args.northmarq_limit)

    combined = merge_items([colliers_items, northmarq_items], total_limit=args.total_limit)
    write_rss(combined, out_file=args.out, home_link=args.colliers)

    print(
        f"Wrote {args.out} "
        f"(Colliers: {len(colliers_items)}, Northmarq: {len(northmarq_items)}, Combined: {len(combined)})"
    )


if __name__ == "__main__":
    main()
