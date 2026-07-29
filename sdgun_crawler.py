#!/usr/bin/env python3
"""Fast, incremental crawler for SDGun second-hand sale threads.

Uses only Python's standard library.  A post is accepted only when the site's
own metadata identifies the toy marketplace and second-hand sale type.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import html
import json
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

BASE = "http://app.sdgun.com.cn"
DEFAULT_DB = Path(__file__).resolve().parent / "data" / "main" / "sdgun_market.db"
THREAD_URL = BASE + "/mag/circle/v1/forum/threadWapPage?tid={}"
THREAD_VIEW_URL = BASE + "/mag/circle/v1/forum/threadViewPage?tid={}"
COMMENT_URL = BASE + "/mag/circle/v1/Forum/commentList"
FORUM_LIST_URL = BASE + "/mag/circle/v1/Forum/threadList"
MARKET_FID = 176
SECONDHAND_SALE_TYPEID = 102
POST_TYPES = {
    101: "商家广告",
    102: "二手出售",
    103: "召集团购",
    104: "求购",
}
POST_TYPE_PREFIXES = {
    "【商家广告】": "商家广告",
    "【二手出售】": "二手出售",
    "【召集团购】": "召集团购",
    "【求购】": "求购",
}
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title\s*>", re.I | re.S)
ROW_RE = re.compile(r"\bvar\s+row\s*=\s*(\{.*?\})\s*;", re.S)
CREATE_TIME_RE = re.compile(r'"create_time"\s*:\s*"?(\d{10})"?')
CURL_TIMEOUT_RE = re.compile(rb"(?:cURL error 28|Operation timed out)", re.I)
URL_RE = re.compile(r"https?://[^\s<>\"'，。\[\]]+", re.I)
SOLD_RE = re.compile(r"(?:已出|已售|售出|已被秒|交易完成|已交易|sold|(?:^|\n)\s*出了\s*(?:$|\n))", re.I)
SOLD_QUESTION_RE = re.compile(r"(?:还没|未|没)\s*(?:出|售)|(?:已出|已售|售出|出了).{0,3}[吗嘛呢？?]", re.I)
SOLD_POLICY_RE = re.compile(
    r"(?:一经|一旦)\s*(?:售出|卖出|出售)[，,、\s]*(?:概不|不予|不)\s*(?:退|换|退换)"
    r"|(?:售出|卖出|出售)[，,、\s]*(?:概不|不予|不)\s*(?:退|换|退换)",
    re.I,
)
WANTED_RE = re.compile(r"(?:求购|求收|收一个|收个|蹲一个|想收|高价收|长期收|收[:：])", re.I)
SELL_RE = re.compile(r"(?:出售|出一个|出个|出手|转让|回血|明盘|包邮|不包邮|价格|￥|¥|\d+\s*元)", re.I)
ITEM_PREFIX_RE = re.compile(
    r"^\s*(?:(?:\d{1,2}|[一二三四五六七八九十]+)\s*[.、:：)）]|[-*•])\s*"
)
ITEM_LINE_PRICE_RE = re.compile(
    r"(?:明盘|售价|价格|小刀|包邮)\s*[:：]?\s*[￥¥]?\s*\d{2,6}|[￥¥]\s*\d{2,6}|\d{2,6}\s*(?:元|块)|\d{1,3}(?:\.\d{1,2})?\s*张", re.I
)
TRAILING_BARE_PRICE_RE = re.compile(r"(?<![\dA-Za-z-])(\d{1,6}(?:\.\d{1,2})?)\s*$")
REGION_SALE_PREFIX_RE = re.compile(
    r"^0\d{2,3}(?:\s*/\s*0\d{2,3})?\s*(?:(?:出|出售|出掉|出手)\s*)?",
    re.I,
)
# A number is a price only when accompanied by a currency marker.  This avoids
# treating dates, model names (PEQ-15) and quantities as prices.
PRICE_RE = re.compile(
    r"(?:[￥¥]\s*(\d{1,6}(?:\.\d{1,2})?)|(?<![\dA-Za-z-])(\d{1,6}(?:\.\d{1,2})?)\s*(?:元|块(?:钱)?|包邮|不包邮))",
    re.I,
)
ZHANG_PRICE_RE = re.compile(
    r"(?<![\dA-Za-z.-])(\d{1,3}(?:\.\d{1,2})?)\s*张(?![\u4e00-\u9fff])",
    re.I,
)
BBCODE_URL_RE = re.compile(
    r"\[url=(https?://[^\]]+)\].*?\[/url\]",
    re.I | re.S,
)
STOPWORDS = {
    "一个", "东西", "物品", "交易", "出售", "二手出售", "包邮", "不包邮", "价格",
    "联系", "可以", "没有", "这个", "那个", "需要", "直接", "闲鱼", "链接", "已经",
    "明盘", "已出", "自提", "几乎全新", "全新",
    "您的设备不支持视", "频标签",
}


class _TextHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.images: list[str] = []
        self.links: list[str] = []
        self.videos: list[str] = []
        self.video_posters: list[str] = []
        self._video_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_d = dict(attrs)
        tag = tag.lower()
        if tag == "img":
            src = attrs_d.get("data-original") or attrs_d.get("src")
            if src:
                self.images.append(src)
        if tag == "video":
            self._video_depth += 1
            if attrs_d.get("poster"):
                self.video_posters.append(attrs_d["poster"] or "")
            src = attrs_d.get("data-original") or attrs_d.get("data-src") or attrs_d.get("src")
            if src:
                self.videos.append(src)
        elif tag == "source" and self._video_depth:
            src = attrs_d.get("data-original") or attrs_d.get("data-src") or attrs_d.get("src")
            if src:
                self.videos.append(src)
        if tag == "a" and attrs_d.get("href"):
            self.links.append(attrs_d["href"] or "")
        if tag in {"br", "p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "video" and self._video_depth:
            self._video_depth -= 1

    def handle_data(self, data: str) -> None:
        # Text nested inside <video> is browser fallback UI, not post content.
        if self._video_depth:
            return
        self.parts.append(data)


def clean_rich_content(value: str) -> tuple[str, list[str], list[str], list[str], list[str]]:
    parser = _TextHTML()
    with contextlib.suppress(Exception):
        parser.feed(value or "")
    text = html.unescape("".join(parser.parts))
    text = BBCODE_URL_RE.sub(lambda match: match.group(1), text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    # Keep one blank line: paragraph boundaries are useful for splitting a
    # multi-item listing without any model/NLP call.
    text = re.sub(r"\n[ \t]*\n(?:[ \t]*\n)*", "\n\n", text).strip()
    found_urls = URL_RE.findall(text)
    videos = unique(parser.videos)
    posters = unique(parser.video_posters)
    return text, unique(parser.images), unique(parser.links + videos + found_urls), videos, posters


def clean_rich_text(value: str) -> tuple[str, list[str], list[str]]:
    """Return plain text, images and links; video sources are included in links."""
    text, images, links, _, _ = clean_rich_content(value)
    return text, images, links


def unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(v for v in values if v))


def absolute_url(url: str) -> str:
    return urllib.parse.urljoin(BASE + "/", html.unescape(url))


def forum_post_type(row: dict[str, Any]) -> str | None:
    """Return the site's post type, with its generated title as fallback."""
    typeid = row.get("typeid")
    if typeid not in (None, ""):
        try:
            return POST_TYPES.get(int(typeid))
        except (TypeError, ValueError):
            return None
    title = str(row.get("title") or "").strip()
    return next((name for marker, name in POST_TYPE_PREFIXES.items()
                 if title.startswith(marker)), None)


def is_market_post(row: dict[str, Any]) -> bool:
    """Accept ordinary typed posts in the toy marketplace.

    ``Forum/threadList`` exposes both ``fid`` and ``typeid``.  The detail page's
    embedded ``row`` omits ``typeid``, but still exposes ``fid`` and the
    server-generated title (whose prefix is derived from the selected type).
    """
    try:
        if int(row.get("fid")) != MARKET_FID:
            return False
    except (TypeError, ValueError):
        return False
    try:
        if int(row.get("is_top") or -1) == 1:
            return False
    except (TypeError, ValueError):
        return False
    return forum_post_type(row) is not None


def is_secondhand_sale(row: dict[str, Any], prefix: str = "【二手出售】") -> bool:
    """Return whether a market row belongs in the default sale view."""
    return is_market_post(row) and forum_post_type(row) == "二手出售"


def classify(title: str, body: str, comments: list[dict[str, Any]]) -> str:
    # Comments such as “已出” often contain the freshest transaction state.
    combined = "\n".join([title, body] + [str(x.get("content_text", "")) for x in comments])
    sold_evidence = SOLD_QUESTION_RE.sub("", SOLD_POLICY_RE.sub("", combined))
    if SOLD_RE.search(sold_evidence):
        return "已出"
    if WANTED_RE.search(title + "\n" + body):
        return "求购"
    if SELL_RE.search(title + "\n" + body) or title.startswith("【二手出售】"):
        return "出售"
    return "待确认"


def _item_blocks(body: str) -> list[str]:
    """Split listings by blank paragraphs and numbered/bulleted item lines."""
    blocks: list[str] = []
    for paragraph in re.split(r"\n\s*\n+", body):
        paragraph_lines = [re.sub(r"[ \t]+", " ", x).strip()
                           for x in paragraph.splitlines() if x.strip()]
        # Many sellers use one priced item per line without blank separators.
        # Treat each priced/numbered line as a new item and attach short
        # unpriced continuation lines to the previous item.
        structural_count = sum(bool(ITEM_PREFIX_RE.match(x) or ITEM_LINE_PRICE_RE.search(x)
                                    or SOLD_RE.search(SOLD_QUESTION_RE.sub("", SOLD_POLICY_RE.sub("", x)))
                                    or WANTED_RE.search(x)) for x in paragraph_lines)
        if len(paragraph_lines) > 1 and structural_count >= 2:
            current: list[str] = []
            for line in paragraph_lines:
                starts_item = bool(ITEM_PREFIX_RE.match(line) or ITEM_LINE_PRICE_RE.search(line)
                                   or SOLD_RE.search(SOLD_QUESTION_RE.sub("", SOLD_POLICY_RE.sub("", line)))
                                   or WANTED_RE.search(line))
                if starts_item and current:
                    blocks.append(" ".join(current))
                    current = []
                current.append(line)
            if current:
                blocks.append(" ".join(current))
            continue
        current: list[str] = []
        for line in paragraph_lines:
            if ITEM_PREFIX_RE.match(line) and current:
                blocks.append(" ".join(current))
                current = []
            current.append(line)
        if current:
            blocks.append(" ".join(current))
    return blocks


def _item_tokens(text: str) -> list[str]:
    cleaned = SOLD_RE.sub(
        "", ZHANG_PRICE_RE.sub("", PRICE_RE.sub("", ITEM_PREFIX_RE.sub("", text)))
    )
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9._+-]{1,20}|[\u4e00-\u9fff]{2,10}", cleaned)
    return [x.casefold() for x in tokens if x.casefold() not in STOPWORDS][:6]


def extract_item_prices(text: str) -> list[str]:
    values = extract_prices(text)
    for match in re.finditer(r"(?:明盘|售价|价格|小刀)\s*[:：]?\s*[￥¥]?\s*(\d{2,6}(?:\.\d{1,2})?)", text, re.I):
        values.append(match.group(1))
    return unique(values)


def _title_item_candidates(title_item: str) -> list[str]:
    """Split a title list after dropping a leading region-code sale marker."""
    cleaned = REGION_SALE_PREFIX_RE.sub("", title_item).strip()
    name_only = ZHANG_PRICE_RE.sub("", PRICE_RE.sub("", cleaned)).strip(" -—•\t")
    # VFC BCM is a product/platform phrase, not two separately advertised items.
    if re.fullmatch(r"vfc\s+bcm", name_only, re.I):
        return [cleaned]
    parts = [part.strip(" -—•\t") for part in re.split(r"\s+|[、；;]+", name_only)]
    parts = [part for part in parts if len(part) >= 2 and part.casefold() not in STOPWORDS]
    return parts if len(parts) >= 2 else ([cleaned] if cleaned else [])


def _normalized_item_name(text: str) -> str:
    """Reduce common seller/material descriptions to the advertised model name."""
    cleaned = URL_RE.sub("", ITEM_PREFIX_RE.sub("", text)).strip()
    cleaned = ZHANG_PRICE_RE.sub("", PRICE_RE.sub("", cleaned)).strip()
    cleaned = re.sub(r"[（(][^）)]*[）)]", "", cleaned)
    cleaned = TRAILING_BARE_PRICE_RE.sub("", cleaned).strip(" -—•\t")
    folded = cleaned.casefold()
    if re.search(r"\btango\s*6t\b", folded):
        return "tango 6t"
    if re.search(r"\bbcm\s*t2\s*支架", folded):
        return "bcm t2支架"
    if re.search(r"\bunity\s*(?:增高|支架)", folded):
        return "unity 支架"
    if (re.search(r"(?<![a-z0-9])kg(?![a-z0-9])", folded)
            and "radian" in folded):
        return "kg radian"
    return cleaned


def extract_item_details(title: str, body: str,
                         comments: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Extract and classify each listed item using cheap deterministic rules."""
    title_item = re.sub(r"^【二手出售】\s*", "", title).strip(" -—:：")
    raw_blocks = _item_blocks(body)
    structured_bare_prices = len(raw_blocks) >= 2
    candidates: list[str] = []
    for block in raw_blocks:
        compact = URL_RE.sub("", re.sub(r"\s+", " ", block)).strip(" -—•\t")
        if not 2 <= len(compact) <= 240:
            continue
        price_label = ZHANG_PRICE_RE.sub("", PRICE_RE.sub("", compact))
        price_label = price_label.strip(" -—•\t:：")
        if price_label in {"降价", "价格", "售价", "明盘", "小刀", "包邮", "不包邮"}:
            continue
        marked = bool(ITEM_PREFIX_RE.match(block))
        if (marked or PRICE_RE.search(compact) or ITEM_LINE_PRICE_RE.search(compact)
                or (structured_bare_prices and TRAILING_BARE_PRICE_RE.search(compact))
                or SOLD_RE.search(SOLD_POLICY_RE.sub("", compact)) or WANTED_RE.search(compact)):
            candidates.append(compact)
        if len(candidates) >= 20:
            break
    if not candidates and title_item:
        candidates = _title_item_candidates(title_item)
    compound_source = title + "\n" + body
    if re.search(r"北青\s*2011\s*c2", compound_source, re.I):
        candidates = [
            value for value in candidates
            if not re.search(r"北青\s*2011(?:\s*c2)?", value, re.I)
        ]
        candidates.append("北青2011c2")

    details: list[dict[str, Any]] = []
    for index, text in enumerate(unique(candidates), 1):
        evidence = SOLD_QUESTION_RE.sub("", SOLD_POLICY_RE.sub("", text))
        status = "已出" if SOLD_RE.search(evidence) else ("求购" if WANTED_RE.search(text) else "出售")
        details.append({
            "index": index,
            "name": _normalized_item_name(text),
            "text": text,
            "status": status,
            "prices": extract_item_prices(text),
        })

    merged: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    for detail in details:
        key = str(detail["name"]).casefold()
        if key in by_name:
            existing = by_name[key]
            existing["prices"] = unique(list(existing["prices"]) + list(detail["prices"]))
            if detail["status"] == "已出":
                existing["status"] = "已出"
            continue
        detail["index"] = len(merged) + 1
        by_name[key] = detail
        merged.append(detail)
    details = merged

    # A generic “已出” comment can close a one-item listing. For a multi-item
    # listing it only affects an item when the comment names it or its number.
    for comment in comments or []:
        comment_text = str(comment.get("content_text") or comment.get("content") or "")
        sold_text = SOLD_QUESTION_RE.sub("", SOLD_POLICY_RE.sub("", comment_text))
        if not SOLD_RE.search(sold_text):
            continue
        if len(details) == 1:
            details[0]["status"] = "已出"
            continue
        for detail in details:
            index = int(detail["index"])
            number_ref = re.search(rf"(?<!\d)(?:第\s*)?{index}\s*(?:号|件|个|款)(?!\d)", comment_text)
            token_ref = any(token in comment_text.casefold() for token in _item_tokens(detail["name"]))
            if number_ref or token_ref:
                detail["status"] = "已出"
    return details


def category_from_items(details: list[dict[str, Any]], fallback: str) -> str:
    if not details:
        return fallback
    statuses = Counter(str(x.get("status") or "待确认") for x in details)
    sale_items = statuses["出售"] + statuses["已出"]
    if sale_items and statuses["求购"]:
        return "出售"
    if sale_items and statuses["已出"] == sale_items and not statuses["求购"]:
        return "已出"
    if statuses["已出"]:
        return "部分已出"
    if statuses["求购"] and not statuses["出售"]:
        return "求购"
    return "出售"


def extract_items(title: str, body: str) -> list[str]:
    """Backward-compatible list of item labels."""
    return [x["name"] for x in extract_item_details(title, body)]


@dataclass
class Settings:
    timeout: float
    retries: int
    comments: bool
    comment_page_size: int
    max_comment_pages: int
    prefix: str
    keywords: tuple[str, ...]
    post_types: tuple[str, ...] = ()
    media: bool = True


class Crawler:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.local = threading.local()

    def _request(self, url: str) -> bytes:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; SDGunMarketCrawler/1.0)",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "identity",
            "Connection": "close",
        })
        last: Exception | None = None
        for attempt in range(self.s.retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.s.timeout) as response:
                    return response.read()
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
                if attempt < self.s.retries:
                    time.sleep(0.15 * (2**attempt))
        raise last or RuntimeError("request failed")

    def fetch_thread(self, tid: int) -> tuple[str, dict[str, Any] | None]:
        """Return status and post, using the site's own forum/type metadata."""
        self.local.last_create_time = None
        source_url = THREAD_URL.format(tid)
        try:
            try:
                raw = self._request(source_url)
            except Exception:
                source_url = THREAD_VIEW_URL.format(tid)
                raw = self._request(source_url)
            if CURL_TIMEOUT_RE.search(raw) and not ROW_RE.search(
                raw.decode("utf-8", errors="ignore")
            ):
                source_url = THREAD_VIEW_URL.format(tid)
                raw = self._request(source_url)
        except Exception:
            return "unreachable", None
        # JSON strings are ASCII escaped, while titles may contain UTF-8 Chinese.
        page = raw.decode("utf-8", errors="replace")
        create_match = CREATE_TIME_RE.search(page)
        if create_match:
            self.local.last_create_time = int(create_match.group(1))
        row_match = ROW_RE.search(page)
        if not row_match:
            return "no_row", None
        try:
            row = json.loads(row_match.group(1))
        except json.JSONDecodeError:
            return "bad_row", None
        post_type = forum_post_type(row)
        try:
            forum_id = int(row.get("fid"))
        except (TypeError, ValueError):
            return "skipped_forum", None
        if forum_id != MARKET_FID:
            return "skipped_forum", None
        try:
            if int(row.get("is_top") or -1) == 1:
                return "skipped_pinned", None
        except (TypeError, ValueError):
            return "skipped_forum", None
        if post_type is None:
            return "skipped_type_unknown", None
        if self.s.post_types and post_type not in self.s.post_types:
            return f"skipped_type:{post_type}", None
        title = str(row.get("title") or "").strip()
        if not title:
            match = TITLE_RE.search(page)
            if not match:
                return "no_title", None
            title = html.unescape(re.sub(r"<[^>]+>", "", match.group(1))).strip()
        folded_page = page.casefold()
        if self.s.keywords and not any(keyword_in_raw_page(k, folded_page) for k in self.s.keywords):
            return "skipped_keyword", None

        body, content_images, content_links, content_videos, content_video_posters = clean_rich_content(
            str(row.get("content", ""))
        )
        # row.pics belongs to the post. Never scrape arbitrary <img> elements.
        pic_urls: list[str] = []
        for pic in (row.get("pics") or []) if self.s.media else []:
            if isinstance(pic, str):
                pic_urls.append(pic)
            elif isinstance(pic, dict):
                for key in ("url", "pic", "src", "origin_url"):
                    if pic.get(key):
                        pic_urls.append(str(pic[key])); break
        # row.pics also contains <video poster> thumbnails. They belong to the
        # player and must not be rendered again in the image gallery.
        video_posters = set(absolute_url(x) for x in content_video_posters) if self.s.media else set()
        images = [x for x in unique_images(absolute_url(x) for x in pic_urls + content_images)
                  if x not in video_posters]
        if not self.s.media:
            images = []
        comments = self.fetch_comments(tid) if self.s.comments and int(row.get("reply_count") or 0) else []
        comment_links = [u for c in comments for u in c.get("links", [])]
        all_links = unique(absolute_url(x) for x in content_links + comment_links)
        videos = unique(absolute_url(x) for x in content_videos) if self.s.media else []
        xianyu_links = [x for x in all_links if any(h in x.casefold() for h in ("2.taobao.com", "m.tb.cn", "tb.cn", "goofish.com"))]
        item_details = extract_item_details(title, body, comments)
        fallback_category = "求购" if post_type == "求购" else classify(title, body, comments)
        category = category_from_items(item_details, fallback_category)
        post = {
            "tid": int(row.get("tid") or tid),
            "url": source_url,
            "title": title,
            "forum_id": int(row.get("fid") or MARKET_FID),
            "is_top": int(row.get("is_top") or -1) == 1,
            "post_type": post_type,
            "username": row.get("user_name") or "",
            "user_id": row.get("user_id") or "",
            "created_at": timestamp_text(row.get("create_time")),
            "content": body,
            "images": images,
            "videos": videos,
            "video_posters": list(video_posters),
            "links": all_links,
            "xianyu_links": xianyu_links,
            "comments": comments,
            "comments_loaded": self.s.comments,
            "media_loaded": self.s.media,
            "category": category,
            "is_sold": category == "已出",
            "items": [x["name"] for x in item_details],
            "item_details": item_details,
            "prices": unique(extract_prices(title + "\n" + body) +
                             [price for item in item_details for price in item.get("prices", [])]),
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }
        return "matched", post

    def fetch_comments(self, tid: int) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for page in range(1, self.s.max_comment_pages + 1):
            query = urllib.parse.urlencode({"tid": tid, "order": 2, "p": page,
                                            "step": self.s.comment_page_size, "authorid": 0})
            try:
                payload = json.loads(self._request(COMMENT_URL + "?" + query))
            except Exception:
                break
            if not payload.get("success"):
                break
            rows = payload.get("list") or []
            for item in rows:
                text, _, links = clean_rich_text(str(item.get("content", "")))
                quoted = item.get("to_comment") or {}
                quoted_text, _, quoted_links = clean_rich_text(str(quoted.get("content", "")))
                output.append({
                    "id": item.get("id") or item.get("comment_id"),
                    "username": item.get("user_name") or item.get("username") or "",
                    "user_id": item.get("user_id") or item.get("userid") or "",
                    "time": item.get("time") or item.get("create_time") or "",
                    "content": text,
                    "content_text": "\n".join(x for x in (text, quoted_text) if x),
                    "links": unique(absolute_url(x) for x in links + quoted_links),
                })
            count = int(payload.get("count") or len(output))
            if not rows or len(output) >= count or len(rows) < self.s.comment_page_size:
                break
        return output

    def fetch_latest_forum_tid(self, fid: int = 176, step: int = 100) -> int:
        """Return the newest ordinary typed market-post tid from the public list."""
        rows = self.fetch_forum_page(fid, 1, step)
        tids = [int(item["tid"]) for item in rows
                if item.get("tid") and is_market_post(item)]
        if not tids:
            raise RuntimeError("交易区列表没有返回普通分类帖子")
        return max(tids)

    def fetch_forum_page(self, fid: int = 176, page: int = 1,
                         step: int = 500) -> list[dict[str, Any]]:
        """Read one typed forum-list page without fetching thread details."""
        query = urllib.parse.urlencode({"fid": fid, "p": page, "step": step})
        payload = json.loads(self._request(FORUM_LIST_URL + "?" + query))
        if not payload.get("success"):
            raise RuntimeError(payload.get("msg") or "无法读取交易区最新帖子")
        return [item for item in payload.get("list") or [] if isinstance(item, dict)]


def timestamp_text(value: Any) -> str:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return str(value or "")


def keyword_in_raw_page(keyword: str, folded_page: str) -> bool:
    r"""Match literal UTF-8 text and JSON's common ``\uXXXX`` representation."""
    literal = keyword.casefold()
    escaped = json.dumps(keyword, ensure_ascii=True)[1:-1].casefold()
    return literal in folded_page or escaped in folded_page


def extract_prices(value: str) -> list[str]:
    values = [
        next(x for x in match.groups() if x is not None)
        for match in PRICE_RE.finditer(value)
    ]
    for match in ZHANG_PRICE_RE.finditer(value):
        try:
            amount = Decimal(match.group(1)) * 100
        except InvalidOperation:
            continue
        values.append(format(amount.normalize(), "f"))
    return unique(values)


def unique_images(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        # Qiniu resize/watermark parameters do not identify a different post image.
        key = urllib.parse.urlsplit(value)._replace(query="", fragment="").geturl()
        if key not in seen:
            seen.add(key)
            output.append(value)
    return output


def post_date_parts(value: str) -> tuple[int | None, int | None, str | None]:
    try:
        created = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        local = created.astimezone(timezone(timedelta(hours=8)))
        return local.year, local.month, local.date().isoformat()
    except (TypeError, ValueError):
        return None, None, None


def searchable_text(post: dict[str, Any]) -> str:
    """Text used for search; never includes link or image URL fields."""
    parts = [str(post.get("title", "")), str(post.get("username", "")),
             str(post.get("content", ""))]
    parts.extend(str(item.get("name", "")) for item in post.get("item_details", [])
                 if isinstance(item, dict))
    parts.extend(str(comment.get("content_text") or comment.get("content") or "")
                 for comment in post.get("comments", []) if isinstance(comment, dict))
    return "\n".join(URL_RE.sub("", part) for part in parts)


def monthly_db_path(main_db: Path, created_at: str) -> Path | None:
    year, month, _ = post_date_parts(created_at)
    if year is None or month is None:
        return None
    data_root = (main_db.parent.parent
                 if main_db.parent.name == "main" and main_db.parent.parent.name == "data"
                 else main_db.parent / "data")
    return data_root / "archive" / f"{year:04d}" / f"{month:02d}" / "market.db"


def save_monthly_post(main_db: Path, post: dict[str, Any]) -> None:
    """Mirror one target post into data/archive/YYYY/MM/market.db."""
    target = monthly_db_path(main_db, str(post.get("created_at", "")))
    if target is None:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    year, month, day = post_date_parts(str(post.get("created_at", "")))
    with contextlib.closing(sqlite3.connect(target)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS posts (
              tid INTEGER PRIMARY KEY, title TEXT, username TEXT, category TEXT,
              created_at TEXT, crawled_at TEXT, post_year INTEGER,
              post_month INTEGER, post_day TEXT, post_type TEXT,
              search_text TEXT, data TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_posts_day_time ON posts(post_day, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_posts_category ON posts(category);
        """)
        monthly_columns = {row[1] for row in db.execute("PRAGMA table_info(posts)")}
        if "search_text" not in monthly_columns:
            db.execute("ALTER TABLE posts ADD COLUMN search_text TEXT")
        if "post_type" not in monthly_columns:
            db.execute("ALTER TABLE posts ADD COLUMN post_type TEXT")
        db.execute("""INSERT OR REPLACE INTO posts
            (tid,title,username,category,created_at,crawled_at,post_year,post_month,post_day,post_type,search_text,data)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (post.get("tid"), post.get("title"), post.get("username"), post.get("category"),
             post.get("created_at"), post.get("crawled_at"), year, month, day,
             post.get("post_type") or "二手出售", searchable_text(post),
             json.dumps(post, ensure_ascii=False)))
        db.commit()


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS posts (
              tid INTEGER PRIMARY KEY, title TEXT, username TEXT, category TEXT,
              created_at TEXT, crawled_at TEXT, data TEXT NOT NULL,
              post_year INTEGER, post_month INTEGER, post_day TEXT,
              post_type TEXT, search_text TEXT
            );
            CREATE TABLE IF NOT EXISTS scan_state (
              tid INTEGER PRIMARY KEY, status TEXT NOT NULL, checked_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata (
              key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS daily_market (
              day TEXT PRIMARY KEY,
              timezone TEXT NOT NULL,
              first_tid INTEGER,
              last_tid INTEGER,
              first_post_time TEXT,
              last_post_time TEXT,
              posts INTEGER NOT NULL,
              selling INTEGER NOT NULL,
              sold INTEGER NOT NULL,
              wanted INTEGER NOT NULL,
              uncertain INTEGER NOT NULL,
              sold_rate REAL NOT NULL,
              active_users INTEGER NOT NULL,
              item_count INTEGER NOT NULL,
              price_samples INTEGER NOT NULL,
              average_price REAL,
              median_price REAL,
              minimum_price REAL,
              maximum_price REAL,
              listed_value_sum REAL,
              comments INTEGER NOT NULL,
              images INTEGER NOT NULL,
              xianyu_links INTEGER NOT NULL,
              post_change_pct REAL,
              median_price_change_pct REAL,
              fear_index REAL,
              fear_level TEXT,
              fear_confidence REAL,
              updated_at TEXT NOT NULL,
              data TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_posts_category ON posts(category);
            CREATE INDEX IF NOT EXISTS idx_daily_market_last_tid ON daily_market(last_tid);
        """)
        existing = {row[1] for row in self.db.execute("PRAGMA table_info(posts)")}
        for name, kind in (("post_year", "INTEGER"), ("post_month", "INTEGER"),
                           ("post_day", "TEXT"), ("post_type", "TEXT"),
                           ("search_text", "TEXT")):
            if name not in existing:
                self.db.execute(f"ALTER TABLE posts ADD COLUMN {name} {kind}")
        daily_existing = {row[1] for row in self.db.execute("PRAGMA table_info(daily_market)")}
        for name, kind in (("fear_index", "REAL"), ("fear_level", "TEXT"),
                           ("fear_confidence", "REAL")):
            if name not in daily_existing:
                self.db.execute(f"ALTER TABLE daily_market ADD COLUMN {name} {kind}")
        self.db.execute(
            "UPDATE posts SET post_type='二手出售' WHERE post_type IS NULL OR post_type=''"
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_posts_day_time ON posts(post_day, created_at DESC)")
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_posts_year_month ON posts(post_year, post_month, created_at DESC)")
        # One-time lightweight backfill for databases created before date partitions.
        for tid, created_at in self.db.execute(
                "SELECT tid, created_at FROM posts WHERE post_day IS NULL").fetchall():
            year, month, day = post_date_parts(created_at)
            self.db.execute("UPDATE posts SET post_year=?, post_month=?, post_day=? WHERE tid=?",
                            (year, month, day, tid))
        for tid, raw in self.db.execute(
                "SELECT tid, data FROM posts WHERE search_text IS NULL").fetchall():
            self.db.execute("UPDATE posts SET search_text=? WHERE tid=?",
                            (searchable_text(json.loads(raw)), tid))
        self.db.commit()
        shard_version = self.get_meta("monthly_shards_version")
        if shard_version != "1":
            for (raw,) in self.db.execute("SELECT data FROM posts").fetchall():
                save_monthly_post(self.path, json.loads(raw))
            self.set_meta("monthly_shards_version", 1)

    def save_result(self, tid: int, status: str, post: dict[str, Any] | None) -> None:
        now = int(time.time())
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO scan_state VALUES(?,?,?)", (tid, status, now))
            if post:
                year, month, day = post_date_parts(post.get("created_at", ""))
                self.db.execute("""INSERT OR REPLACE INTO posts
                    (tid,title,username,category,created_at,crawled_at,data,post_year,post_month,post_day,post_type,search_text)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (tid, post["title"], post["username"], post["category"], post["created_at"],
                     post["crawled_at"], json.dumps(post, ensure_ascii=False), year, month, day,
                     post.get("post_type") or "二手出售", searchable_text(post)))
                save_monthly_post(self.path, post)
            elif status in {"skipped_forum", "skipped_pinned", "skipped_type_unknown"}:
                self.db.execute("DELETE FROM posts WHERE tid=?", (tid,))

    def seen(self) -> set[int]:
        return {r[0] for r in self.db.execute("SELECT tid FROM scan_state")}

    def get_meta(self, key: str, default: str = "") -> str:
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_meta(self, key: str, value: Any) -> None:
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)", (key, str(value)))

    def close(self) -> None:
        with self.lock:
            self.db.close()


def scan(args: argparse.Namespace) -> int:
    settings = Settings(args.timeout, args.retries, not args.no_comments,
                        args.comment_page_size, args.max_comment_pages,
                        args.prefix, tuple(args.keyword or ()))
    crawler, store = Crawler(settings), Store(Path(args.db))
    tids = list(range(args.start, args.end + (1 if args.end >= args.start else -1),
                      1 if args.end >= args.start else -1))
    if args.new_only:
        seen = store.seen()
        tids = [x for x in tids if x not in seen]
    counts: Counter[str] = Counter()
    matches: list[dict[str, Any]] = []
    started = time.monotonic()

    def task(tid: int) -> tuple[int, str, dict[str, Any] | None]:
        status, post = crawler.fetch_thread(tid)
        return tid, status, post

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(task, tid): tid for tid in tids}
        for future in concurrent.futures.as_completed(futures):
            tid, status, post = future.result()
            store.save_result(tid, status, post)
            counts[status] += 1
            if post:
                matches.append(post)
                print(json.dumps(post, ensure_ascii=False), flush=True)
    elapsed = time.monotonic() - started
    summary = {"scanned": len(tids), "matched": len(matches), "seconds": round(elapsed, 2),
               "requests_per_second": round(len(tids) / elapsed, 2) if elapsed else 0,
               "statuses": counts}
    print(json.dumps(summary, ensure_ascii=False, default=dict), file=sys.stderr)
    if args.jsonl:
        with Path(args.jsonl).open("w", encoding="utf-8") as fh:
            for post in sorted(matches, key=lambda x: x["tid"], reverse=True):
                fh.write(json.dumps(post, ensure_ascii=False) + "\n")
    return 0


def report(args: argparse.Namespace) -> int:
    db = sqlite3.connect(args.db)
    posts = [json.loads(r[0]) for r in db.execute("SELECT data FROM posts ORDER BY tid DESC LIMIT ?", (args.limit,))]
    categories = Counter(x["category"] for x in posts)
    item_terms: Counter[str] = Counter()
    for post in posts:
        for item in post.get("items", []):
            for token in re.findall(r"[A-Za-z][A-Za-z0-9._+-]{1,20}|[\u4e00-\u9fff]{2,8}", item):
                if token.casefold() not in STOPWORDS:
                    item_terms[token.casefold()] += 1
    result = {"post_count": len(posts), "categories": categories.most_common(),
              "hot_terms": item_terms.most_common(args.top),
              "latest": [{k: x.get(k) for k in ("tid", "title", "category", "username", "prices", "url")}
                         for x in posts[:args.latest]]}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def watch(args: argparse.Namespace) -> int:
    """Poll the global tid frontier without permanently consuming future IDs."""
    settings = Settings(args.timeout, args.retries, not args.no_comments,
                        args.comment_page_size, args.max_comment_pages,
                        args.prefix, tuple(args.keyword or ()))
    crawler, store = Crawler(settings), Store(Path(args.db))
    saved = store.get_meta("watch_cursor")
    cursor = args.start if args.start is not None else int(saved or 4148800)
    cycle = 0
    transient = {"unreachable", "no_title", "no_row", "bad_row"}
    print(json.dumps({"watching_from": cursor, "batch": args.batch,
                      "poll_seconds": args.poll}, ensure_ascii=False), file=sys.stderr)
    try:
        while args.cycles == 0 or cycle < args.cycles:
            tids = list(range(cursor, cursor + args.batch))
            results: dict[int, tuple[str, dict[str, Any] | None]] = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {pool.submit(crawler.fetch_thread, tid): tid for tid in tids}
                for future in concurrent.futures.as_completed(futures):
                    tid = futures[future]
                    status, post = future.result()
                    results[tid] = (status, post)
                    # Network failures/future pages remain retryable at the frontier.
                    if status not in transient:
                        store.save_result(tid, status, post)
                    if post:
                        print(json.dumps(post, ensure_ascii=False), flush=True)

            # A missing id is an old gap only if a higher id already exists.  If
            # there is no higher definitive result, it is the future frontier.
            definitive = [tid for tid, (status, _) in results.items() if status not in transient]
            highest_existing = max(definitive, default=cursor - 1)
            next_cursor = cursor
            while next_cursor <= highest_existing:
                status, post = results[next_cursor]
                if status in transient:
                    store.save_result(next_cursor, "frontier_gap", None)
                next_cursor += 1
            cursor = next_cursor
            store.set_meta("watch_cursor", cursor)
            cycle += 1
            stats = Counter(status for status, _ in results.values())
            print(json.dumps({"cycle": cycle, "next_cursor": cursor, "statuses": stats},
                             ensure_ascii=False, default=dict), file=sys.stderr)
            if args.cycles == 0 or cycle < args.cycles:
                time.sleep(args.poll)
    except KeyboardInterrupt:
        print(json.dumps({"stopped": True, "next_cursor": cursor}, ensure_ascii=False), file=sys.stderr)
    return 0


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="SDGun 二手出售增量爬虫")
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("scan", help="并发扫描一个 tid 闭区间")
    s.add_argument("--start", type=int, required=True)
    s.add_argument("--end", type=int, required=True)
    s.add_argument("-k", "--keyword", action="append", help="关键词；可重复，任一命中即可")
    s.add_argument("--prefix", default="【二手出售】")
    s.add_argument("--workers", type=int, default=16)
    s.add_argument("--timeout", type=float, default=6)
    s.add_argument("--retries", type=int, default=0, help="默认不重试以追求吞吐")
    s.add_argument("--no-comments", action="store_true")
    s.add_argument("--comment-page-size", type=int, default=50)
    s.add_argument("--max-comment-pages", type=int, default=20)
    s.add_argument("--db", default=str(DEFAULT_DB))
    s.add_argument("--jsonl", help="另外导出本次命中为 JSONL")
    s.add_argument("--new-only", action=argparse.BooleanOptionalAction, default=True)
    s.set_defaults(func=scan)
    r = sub.add_parser("report", help="生成轻量趋势摘要")
    r.add_argument("--db", default=str(DEFAULT_DB))
    r.add_argument("--limit", type=int, default=1000)
    r.add_argument("--top", type=int, default=30)
    r.add_argument("--latest", type=int, default=20)
    r.set_defaults(func=report)
    w = sub.add_parser("watch", help="持续监控最新 tid；Ctrl+C 安全停止")
    w.add_argument("--start", type=int, help="首次起点；以后默认续接数据库游标")
    w.add_argument("-k", "--keyword", action="append", help="关键词；可重复，任一命中即可")
    w.add_argument("--prefix", default="【二手出售】")
    w.add_argument("--workers", type=int, default=8)
    w.add_argument("--batch", type=int, default=32, help="每轮向前探测的 tid 数")
    w.add_argument("--poll", type=float, default=20, help="轮询间隔秒数")
    w.add_argument("--cycles", type=int, default=0, help="0=持续运行；测试时可指定轮数")
    w.add_argument("--timeout", type=float, default=6)
    w.add_argument("--retries", type=int, default=0)
    w.add_argument("--no-comments", action="store_true")
    w.add_argument("--comment-page-size", type=int, default=50)
    w.add_argument("--max-comment-pages", type=int, default=20)
    w.add_argument("--db", default=str(DEFAULT_DB))
    w.set_defaults(func=watch)
    return p


if __name__ == "__main__":
    ns = parser().parse_args()
    raise SystemExit(ns.func(ns))
