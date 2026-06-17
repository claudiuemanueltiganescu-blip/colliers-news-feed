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

COLLIERS_DEFAULT_URL = "https://www.colliers.com/en/countries/united-states/commercial-real-estate-news"
COLLIERS_HOME_URL = "https://www.colliers.com/en"
NORTHMARQ_LISTING_URL = "https://www.northmarq.com/recent-closings-transactions"
NORTHMARQ_BASE = "https://www.northmarq.com"

DATE_RE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b"
)

DATE_AT_START_RE = re.compile(
    r"^\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+)$"
)

NORTHMARQ_DATE_IN_SLUG_RE = re.compile(r"-(\d{4})-(\d{2})(?:-(\d{2}))?(?:$|/)")

COLLIERS_SKIP_TEXT = {
    "read more",
    "view more",
    "view all news",
    "view all",
    "view podcasts",
    "view media mentions",
    "learn more",
    "",
}

COLLIERS_ALLOWED_PATH_PARTS = (
    "/en/news/",
    "/en/research/",
    "/en/insights/",
)


def chrome_ua() -> str:
    return (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )


def normalize_url(base: str, href: str) -> str:
    u = urljoin(base, href)
    p = urlparse(u)
    return p._replace(query="", fragment="").geturl().rstrip("/")


def parse_date(date_text: str | None) -> datetime | None:
    if not date_text:
        return None
    try:
        dt = dateparser.parse(date_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except Exception:
        return None


def uniq_preserve(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def safe_get(session: requests.Session, url: str, timeout: int = 30) -> requests.Response | None:
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code >= 400:
            print(f"[HTTP] {url} returned {r.status_code}; trying fallback if available.")
            return None
        return r
    except Exception as e:
        print(f"[HTTP] {url} failed: {e}; trying fallback if available.")
        return None


# -----------------------
# Colliers
# -----------------------

def colliers_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": chrome_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def find_first_text_marker(soup: BeautifulSoup, markers: list[str]):
    markers_lower = [m.lower() for m in markers]

    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "p", "div", "section"]):
        text = tag.get_text(" ", strip=True).lower()
        if any(marker in text for marker in markers_lower):
            return tag

    return None


def find_next_stop_after(start_tag, stop_markers: list[str]):
    if not start_tag:
        return None

    stop_lower = [s.lower() for s in stop_markers]

    for el in start_tag.next_elements:
        if getattr(el, "name", None) not in ["h1", "h2", "h3", "h4", "h5", "section"]:
            continue
        text = el.get_text(" ", strip=True).lower()
        if any(stop in text for stop in stop_lower):
            return el

    return None


def elements_between(start_tag, stop_tag):
    if not start_tag:
        return []

    out = []
    for el in start_tag.next_elements:
        if stop_tag is not None and el is stop_tag:
            break
        out.append(el)

    return out


def colliers_find_card_with_date(anchor, max_up: int = 10):
    node = anchor

    for _ in range(max_up):
        if not getattr(node, "get_text", None):
            break

        text = node.get_text(" ", strip=True)
        if DATE_RE.search(text):
            return node

        node = getattr(node, "parent", None)
        if node is None:
            break

    return anchor.parent if getattr(anchor, "parent", None) else anchor


def colliers_extract_date_from_card(card) -> datetime | None:
    if not card or not getattr(card, "get_text", None):
        return None

    text = card.get_text(" ", strip=True)
    m = DATE_RE.search(text)
    if not m:
        return None

    return parse_date(m.group(0))


def colliers_clean_title(raw_title: str) -> tuple[str, datetime | None]:
    title = " ".join(raw_title.split()).strip()
    m = DATE_AT_START_RE.match(title)

    if m:
        dt = parse_date(m.group(1))
        title = m.group(2).strip()
        return title, dt

    return title, None


def colliers_extract_description(card, title: str) -> str:
    if not card or not getattr(card, "find_all", None):
        return ""

    for p in card.find_all(["p", "div"]):
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        low = text.lower().strip()

        if low in COLLIERS_SKIP_TEXT:
            continue
        if text.strip() == title.strip():
            continue
        if DATE_RE.fullmatch(text.strip()):
            continue
        if "view all news" in low:
            continue
        if "read more" == low:
            continue

        if 15 <= len(text) <= 500:
            return text

    return ""


def is_colliers_content_url(url: str) -> bool:
    parsed = urlparse(url)

    if not parsed.netloc.endswith("colliers.com"):
        return False

    return any(part in parsed.path for part in COLLIERS_ALLOWED_PATH_PARTS)


def colliers_extract_from_section(
    soup: BeautifulSoup,
    listing_url: str,
    start_markers: list[str],
    stop_markers: list[str],
    limit: int,
) -> list[dict]:
    start = find_first_text_marker(soup, start_markers)
    if not start:
        return []

    stop = find_next_stop_after(start, stop_markers)
    elements = elements_between(start, stop)

    by_url: dict[str, dict] = {}

    for el in elements:
        if getattr(el, "name", None) != "a":
            continue

        href = el.get("href")
        if not href:
            continue

        url = normalize_url(listing_url, href)

        if not is_colliers_content_url(url):
            continue

        raw_title = el.get_text(" ", strip=True)
        title, date_from_title = colliers_clean_title(raw_title)

        if not title:
            continue
        if title.lower().strip() in COLLIERS_SKIP_TEXT:
            continue
        if len(title) < 6:
            continue

        card = colliers_find_card_with_date(el)
        published = date_from_title or colliers_extract_date_from_card(card)
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
            if not existing.get("description") and desc:
                existing["description"] = desc
            if len(title) > len(existing.get("title", "")):
                existing["title"] = title

        if len(by_url) >= limit:
            break

    items = list(by_url.values())
    items.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return items[:limit]


def colliers_extract_items_from_page(html: str, listing_url: str, limit: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Original page section: top news only, stop before Podcasts.
    items = colliers_extract_from_section(
        soup=soup,
        listing_url=listing_url,
        start_markers=[
            "keep up with the latest commercial real estate news and trends",
            "latest commercial real estate news and trends",
        ],
        stop_markers=[
            "podcasts",
            "press releases / announcements",
            "media mentions",
            "knowledge leader",
        ],
        limit=limit,
    )

    if items:
        return items

    # Fallback: Colliers homepage "News & Research" section.
    items = colliers_extract_from_section(
        soup=soup,
        listing_url=listing_url,
        start_markers=[
            "news & research",
            "the latest commercial real estate news, insights and trends",
        ],
        stop_markers=[
            "enterprising to exceed expectations",
            "our people & expertise in action",
            "your needs",
            "knowledge leader",
        ],
        limit=limit,
    )

    return items


def get_colliers_items(primary_url: str | None, limit: int) -> list[dict]:
    urls = []

    if primary_url:
        urls.append(primary_url)

    urls.extend(
        [
            COLLIERS_DEFAULT_URL,
            COLLIERS_HOME_URL,
        ]
    )

    urls = uniq_preserve(urls)
    session = colliers_session()

    for url in urls:
        response = safe_get(session, url)
        if response is None:
            continue

        items = colliers_extract_items_from_page(response.text, url, limit)
        if items:
            print(f"[Colliers] Using {url} ({len(items)} items)")
            return items

        print(f"[Colliers] {url} loaded, but no usable items were found.")

    print("[Colliers] No Colliers items found. Continuing without Colliers for this run.")
    return []


# -----------------------
# Northmarq
# -----------------------

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


def northmarq_published_from_url(url: str) -> datetime | None:
    m = NORTHMARQ_DATE_IN_SLUG_RE.search(url)
    if not m:
        return None

    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3)) if m.group(3) else 1

    try:
        return datetime(year, month, day, 12, 0, 0, tzinfo=UTC)
    except ValueError:
        return None


def northmarq_decode_load_more_response(resp: requests.Response) -> str:
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

        title = " ".join(title.split()).strip() or full_url

        deal_types = [
            x.get_text(" ", strip=True)
            for x in article.select(".field--name-field-deal-type .field__item")
        ]
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


def get_northmarq_items(load_more_base: str | None, pages: int, limit: int) -> list[dict]:
    if not load_more_base:
        print("[Northmarq] No load_more base configured. Skipping Northmarq.")
        return []

    session = northmarq_session()

    try:
        session.get(NORTHMARQ_LISTING_URL, timeout=30)
    except Exception:
        pass

    by_url: dict[str, dict] = {}

    for page in range(1, pages + 1):
        url = f"{load_more_base.rstrip('/')}/{page}"

        try:
            resp = session.get(url, timeout=30)
        except Exception as e:
            print(f"[Northmarq] Page {page} failed: {e}")
            continue

        if resp.status_code == 403:
            print("[Northmarq] 403 on load_more endpoint. Trying listing-page fallback.")
            try:
                fallback = session.get(NORTHMARQ_LISTING_URL, timeout=30)
                if fallback.status_code >= 400:
                    print(f"[Northmarq] Listing-page fallback returned {fallback.status_code}. Skipping Northmarq.")
                    return []
                return northmarq_items_from_card_html(fallback.text, limit)
            except Exception as e:
                print(f"[Northmarq] Listing-page fallback failed: {e}. Skipping Northmarq.")
                return []

        if resp.status_code >= 400:
            print(f"[Northmarq] Page {page} returned {resp.status_code}; skipping this page.")
            continue

        html = northmarq_decode_load_more_response(resp)
        items = northmarq_items_from_card_html(html, limit)

        for item in items:
            if item["url"] not in by_url:
                by_url[item["url"]] = item

            if len(by_url) >= limit:
                break

        if len(by_url) >= limit:
            break

        time.sleep(0.8)

    out = list(by_url.values())
    out.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)

    print(f"[Northmarq] Found {len(out[:limit])} items")
    return out[:limit]


# -----------------------
# Merge + RSS writer
# -----------------------

def merge_items(lists: list[list[dict]], total_limit: int) -> list[dict]:
    by_url: dict[str, dict] = {}

    for item_list in lists:
        for item in item_list:
            url = item["url"]
            existing = by_url.get(url)

            if not existing:
                by_url[url] = item
                continue

            if existing.get("published") is None and item.get("published") is not None:
                by_url[url] = item

    merged = list(by_url.values())
    merged.sort(key=lambda x: x.get("published") or EPOCH, reverse=True)
    return merged[:total_limit]


def write_rss(items: list[dict], out_file: str, home_link: str) -> None:
    fg = FeedGenerator()
    fg.title("Colliers + Northmarq (unofficial)")
    fg.link(href=home_link, rel="alternate")
    fg.description("Unofficial combined feed: Colliers news/research + Northmarq recent transactions.")
    fg.language("en")
    fg.updated(datetime.now(UTC))

    for item in items:
        fe = fg.add_entry()
        fe.id(item["url"])
        fe.link(href=item["url"])
        fe.title(f"[{item['source']}] {item['title']}")

        if item.get("published"):
            fe.published(item["published"])

        if item.get("description"):
            fe.description(item["description"])
        else:
            fe.description(item["url"])

        fe.category(term=item["source"])

    out_dir = os.path.dirname(out_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    fg.rss_file(out_file, pretty=True)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--colliers", default=COLLIERS_DEFAULT_URL)
    parser.add_argument("--listing", default=None, help="Deprecated alias for --colliers")
    parser.add_argument("--northmarq_base", default="https://www.northmarq.com/northmarq_load_more/transactions/transactions")
    parser.add_argument("--northmarq_pages", type=int, default=3)

    parser.add_argument("--colliers_limit", type=int, default=80)
    parser.add_argument("--northmarq_limit", type=int, default=80)
    parser.add_argument("--total_limit", type=int, default=160)

    parser.add_argument("--out", default="docs/colliers-news.xml")

    args = parser.parse_args()

    colliers_url = args.colliers or args.listing or COLLIERS_DEFAULT_URL

    colliers_items = get_colliers_items(colliers_url, limit=args.colliers_limit)
    northmarq_items = get_northmarq_items(
        args.northmarq_base,
        pages=args.northmarq_pages,
        limit=args.northmarq_limit,
    )

    combined = merge_items([colliers_items, northmarq_items], total_limit=args.total_limit)

    home_link = colliers_url if colliers_items else COLLIERS_HOME_URL
    write_rss(combined, out_file=args.out, home_link=home_link)

    print(
        f"Wrote {args.out} "
        f"(Colliers: {len(colliers_items)}, Northmarq: {len(northmarq_items)}, Combined: {len(combined)})"
    )


if __name__ == "__main__":
    main()
