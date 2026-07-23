#!/usr/bin/env python3
"""Local Web dashboard and API for the SDGun market crawler."""

from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import mimetypes
import re
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import median
from typing import Any

from sdgun_crawler import (MARKET_FID, Crawler, Settings, Store,
                           category_from_items, extract_item_details,
                           forum_post_type, is_market_post, save_monthly_post,
                           searchable_text)

ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
DEFAULT_DB = ROOT / "data" / "main" / "sdgun_market.db"
TRANSIENT = {"unreachable", "no_title", "no_row", "bad_row"}
TERM_RE = re.compile(r"[A-Za-z][A-Za-z0-9._+-]{1,20}|[\u4e00-\u9fff]{2,8}")
ITEM_TERM_STOPWORDS = {"二手出售", "包邮", "不包邮", "出售", "价格", "一个",
                       "明盘", "已出", "自提", "几乎全新", "全新",
                       "您的设备不支持视", "频标签"}
# Taiwan has used UTC+08:00 without daylight-saving changes in the period this
# crawler covers. A fixed offset avoids requiring the optional Windows tzdata package.
TAIPEI = timezone(timedelta(hours=8), "Asia/Taipei")
IMAGE_HOSTS = {"picapp.sdgun.net", "bbs.sdgun.com.cn", "sdgun.ymgames.com.cn",
               "shuidan.app1.magcloud.net"}
MAX_IMAGE_BYTES = 25 * 1024 * 1024
MAX_VIDEO_BYTES = 500 * 1024 * 1024


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, default=dict).encode("utf-8")


def parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


class TaskManager:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.state: dict[str, Any] = {
            "running": False, "mode": None, "message": "空闲", "processed": 0,
            "matched": 0, "started_at": None, "updated_at": None, "last_tid": None,
            "statuses": {}, "error": None,
        }

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            state = dict(self.state)
        if state["running"] and state["started_at"]:
            started = parse_iso(state["started_at"])
            if started:
                state["elapsed_seconds"] = int((datetime.now(timezone.utc) - started).total_seconds())
        return state

    def _update(self, **values: Any) -> None:
        with self.lock:
            self.state.update(values)
            self.state["updated_at"] = datetime.now(timezone.utc).isoformat()

    def start(self, mode: str, options: dict[str, Any]) -> tuple[bool, str]:
        with self.lock:
            if self.state["running"]:
                return False, "已有任务正在运行"
            self.stop_event.clear()
            self.state = {
                "running": True, "mode": mode, "message": "正在准备", "processed": 0,
                "matched": 0, "started_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(), "last_tid": None,
                "statuses": {}, "error": None,
            }
            target = {"watch": self._run_watch, "refresh": self._run_refresh}[mode]
            self.thread = threading.Thread(target=target, args=(options,), daemon=True,
                                           name=f"sdgun-{mode}")
            self.thread.start()
        return True, "任务已启动"

    def stop(self) -> bool:
        with self.lock:
            if not self.state["running"]:
                return False
            self.state["message"] = "正在停止"
            self.stop_event.set()
        return True

    @staticmethod
    def settings(options: dict[str, Any]) -> Settings:
        keywords = tuple(str(x).strip() for x in options.get("keywords", []) if str(x).strip())
        allowed_types = {"二手出售", "求购", "召集团购", "商家广告"}
        post_types = tuple(dict.fromkeys(
            str(x).strip() for x in options.get("post_types", ["二手出售"])
            if str(x).strip() in allowed_types
        ))
        return Settings(
            timeout=max(1.0, min(float(options.get("timeout", 6)), 30)),
            retries=max(0, min(int(options.get("retries", 0)), 3)),
            comments=bool(options.get("comments", True)),
            comment_page_size=50,
            max_comment_pages=max(1, min(int(options.get("max_comment_pages", 20)), 100)),
            prefix="【二手出售】",
            keywords=keywords,
            post_types=post_types,
            media=bool(options.get("media", False)),
        )

    def _process_range(self, tids: list[int], options: dict[str, Any], *, frontier: bool = False) -> tuple[dict[int, str], dict[int, int]]:
        import concurrent.futures

        crawler = Crawler(self.settings(options))
        store = Store(self.db_path)
        workers = max(1, min(int(options.get("workers", 8)), 32))
        statuses: dict[int, str] = {}
        create_times: dict[int, int] = {}
        counts: Counter[str] = Counter(self.snapshot().get("statuses", {}))
        processed = self.snapshot().get("processed", 0)
        matched = self.snapshot().get("matched", 0)
        def fetch_one(tid: int) -> tuple[str, dict[str, Any] | None, int | None]:
            status, post = crawler.fetch_thread(tid)
            return status, post, getattr(crawler.local, "last_create_time", None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch_one, tid): tid for tid in tids}
            for future in concurrent.futures.as_completed(futures):
                if self.stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                tid = futures[future]
                try:
                    status, post, create_time = future.result()
                except Exception:
                    status, post, create_time = "unreachable", None, None
                statuses[tid] = status
                if create_time is not None:
                    create_times[tid] = create_time
                # A temporary network/title failure must never overwrite a
                # previously confirmed result or advance persistent state.
                if status not in TRANSIENT:
                    store.save_result(tid, status, post)
                counts[status] += 1
                processed += 1
                matched += int(post is not None)
                self._update(message=f"已处理 tid {tid}", processed=processed, matched=matched,
                             last_tid=tid, statuses=dict(counts))
        store.close()
        return statuses, create_times

    def _finish(self, message: str, error: str | None = None) -> None:
        self._update(running=False, message=message, error=error)

    def _run_watch(self, options: dict[str, Any]) -> None:
        try:
            store = Store(self.db_path)
            poll = max(2.0, min(float(options.get("poll", 20)), 3600))
            while not self.stop_event.is_set():
                cursor = self._catch_up(options, store)
                Repository(self.db_path).rebuild_daily()
                self._update(message=f"等待新帖；上次记录 ID {cursor - 1}",
                             next_tid=cursor, last_recorded_id=cursor - 1)
                self.stop_event.wait(poll)
            self._finish("监控已停止")
            store.close()
        except Exception as exc:
            self._finish("监控失败", str(exc))

    def _run_refresh(self, options: dict[str, Any]) -> None:
        """Run one frontier probe, used on page open and by the refresh button."""
        try:
            options = dict(options)
            store = Store(self.db_path)
            cursor = self._catch_up(options, store)
            # Refresh a small set of recent market posts as well, so comments,
            # links and “已出” status represent the moment the user opened/refreshed.
            recent_limit = max(0, min(int(options.get("refresh_recent", 20)), 100))
            if recent_limit and not self.stop_event.is_set():
                with contextlib.closing(sqlite3.connect(self.db_path)) as db:
                    recent = [row[0] for row in db.execute(
                        "SELECT tid FROM posts ORDER BY created_at DESC, tid DESC LIMIT ?", (recent_limit,))]
                if recent:
                    self._process_range(recent, options)
            Repository(self.db_path).rebuild_daily()
            self._update(next_tid=cursor, last_recorded_id=cursor - 1)
            self._finish("刷新完成")
            store.close()
        except Exception as exc:
            self._finish("刷新失败", str(exc))

    def _catch_up(self, options: dict[str, Any], store: Store) -> int:
        """Discover typed market tids from forum pages, then fetch only selected ones."""
        sync_started = int(time.time())
        saved = store.get_meta("next_tid") or store.get_meta("watch_cursor")
        if saved:
            cursor = int(saved)
        else:
            with contextlib.closing(sqlite3.connect(self.db_path)) as db:
                row = db.execute("SELECT max(tid) FROM scan_state").fetchone()
            cursor = int(row[0] + 1) if row and row[0] is not None else 4148800
        crawler = Crawler(self.settings(options))
        selected_types = crawler.s.post_types
        previous_sync = int(store.get_meta("forum_sync_time") or 0)
        legacy_mode = (
            store.get_meta("post_type_backfill_version") != "2"
            and any(name != "二手出售" for name in selected_types)
        )
        with contextlib.closing(sqlite3.connect(self.db_path)) as db:
            known_status = {int(tid): status for tid, status in db.execute(
                "SELECT tid,status FROM scan_state"
            ).fetchall()}
            legacy_ids = {
                int(row[0]) for row in db.execute(
                    "SELECT tid FROM scan_state WHERE status='skipped_title'"
                ).fetchall()
            } if legacy_mode else set()

        list_step = max(100, min(int(options.get("forum_step", 500)), 500))
        max_pages = 10 if legacy_mode else 20
        discovered: dict[int, dict[str, Any]] = {}
        latest_tid = 0
        page = 1
        while page <= max_pages and not self.stop_event.is_set():
            rows = crawler.fetch_forum_page(MARKET_FID, page, list_step)
            if not rows:
                break
            ordinary = [row for row in rows if is_market_post(row)]
            for row in ordinary:
                tid = int(row["tid"])
                latest_tid = max(latest_tid, tid)
                created = int(row.get("create_time") or 0)
                is_new = created >= previous_sync if previous_sync else tid >= cursor
                if is_new or tid in legacy_ids or known_status.get(tid, "").startswith("skipped_type:"):
                    discovered[tid] = row
            activities = [
                int(row.get("last_reply_time") or row.get("create_time") or 0)
                for row in ordinary
            ]
            normal_done = (
                bool(previous_sync) and activities and max(activities) < previous_sync
            ) or (
                not previous_sync and page >= 2
                and not any(int(row.get("tid") or 0) >= cursor for row in ordinary)
            )
            if normal_done and (not legacy_mode or page >= 10):
                break
            if len(rows) < list_step:
                break
            page += 1
        if not latest_tid:
            raise RuntimeError("交易区列表没有返回普通分类帖子")

        target_tids: list[int] = []
        for tid, row in sorted(discovered.items()):
            post_type = forum_post_type(row)
            if post_type in selected_types:
                if known_status.get(tid) != "matched":
                    target_tids.append(tid)
            elif known_status.get(tid) != "matched":
                store.save_result(tid, f"skipped_type:{post_type}", None)

        fetch_options = dict(options)
        fetch_options.update(comments=False, media=False)
        batch = max(8, min(int(options.get("batch", 64)), 500))
        self._update(message=f"列表发现 {len(discovered)} 条；仅拉取 {len(target_tids)} 条目标帖子",
                     forum_latest_tid=latest_tid)
        for start in range(0, len(target_tids), batch):
            if self.stop_event.is_set():
                break
            tids = target_tids[start:start + batch]
            statuses, _ = self._process_range(tids, fetch_options, frontier=True)
            retry_tids = [tid for tid, status in statuses.items() if status in TRANSIENT]
            if retry_tids and not self.stop_event.is_set():
                self._process_range(retry_tids, fetch_options, frontier=True)
            self._update(message=f"正在拉取目标帖子；已完成 {min(start + batch, len(target_tids))}/{len(target_tids)}",
                         forum_latest_tid=latest_tid)
        if not self.stop_event.is_set():
            store.set_meta("forum_sync_time", sync_started)
            store.set_meta("post_type_backfill_version", 2)
            store.set_meta("next_tid", latest_tid + 1)
            store.set_meta("last_recorded_id", latest_tid)
            self._update(message=f"列表同步完成；最新交易区 ID {latest_tid}",
                         next_tid=latest_tid + 1, last_recorded_id=latest_tid,
                         forum_latest_tid=latest_tid)
        return latest_tid + 1


class Repository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        store = Store(db_path)  # Ensure schema exists.
        store.close()
        with contextlib.closing(sqlite3.connect(db_path)) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS favorites (
                tid INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                note TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(tid) REFERENCES posts(tid) ON DELETE CASCADE
            )""")
            favorite_columns = {row[1] for row in db.execute("PRAGMA table_info(favorites)")}
            if "note" not in favorite_columns:
                db.execute("ALTER TABLE favorites ADD COLUMN note TEXT NOT NULL DEFAULT ''")
            db.execute("UPDATE scan_state SET status='matched' WHERE tid IN (SELECT tid FROM posts)")
            version = db.execute("SELECT value FROM metadata WHERE key='item_schema_version'").fetchone()
            if not version or version[0] != "8":
                rows = db.execute("SELECT tid, data FROM posts").fetchall()
                for tid, raw in rows:
                    post = json.loads(raw)
                    details = extract_item_details(post.get("title", ""), post.get("content", ""),
                                                   post.get("comments", []))
                    if len(details) == 1 and post.get("category") in {"已出", "求购"}:
                        details[0]["status"] = post["category"]
                    post["item_details"] = details
                    post["items"] = [item["name"] for item in details]
                    post["prices"] = list(dict.fromkeys(
                        list(post.get("prices", [])) +
                        [price for item in details for price in item.get("prices", [])]
                    ))
                    post["category"] = category_from_items(details, post.get("category", "待确认"))
                    post["is_sold"] = post["category"] == "已出"
                    db.execute("UPDATE posts SET category=?, search_text=?, data=? WHERE tid=?",
                               (post["category"], searchable_text(post),
                                json.dumps(post, ensure_ascii=False), tid))
                    save_monthly_post(db_path, post)
                db.execute("INSERT OR REPLACE INTO metadata VALUES('item_schema_version','8')")
            db.commit()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        return db

    def posts(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = max(1, min(int(first(query, "limit", "50")), 500))
        offset = max(0, int(first(query, "offset", "0")))
        category, keyword = first(query, "category", ""), first(query, "q", "").strip()
        where, params = [], []
        post_type = first(query, "post_type", "").strip()
        allowed_post_types = {"二手出售", "求购", "召集团购", "商家广告"}
        if post_type and post_type != "全部":
            if post_type not in allowed_post_types:
                raise ValueError("帖子类型无效")
            where.append("post_type=?"); params.append(post_type)
        if category and category != "全部":
            where.append("category=?"); params.append(category)
        if keyword:
            keywords = unique_keywords(keyword)
            match_mode = first(query, "match", "any").lower()
            search_field = first(query, "field", "all").lower()
            if match_mode not in {"any", "all"}:
                raise ValueError("match 仅支持 any 或 all")
            if search_field not in {"all", "title", "author"}:
                raise ValueError("field 仅支持 all、title 或 author")
            columns = {
                "all": ("search_text",),
                "title": ("title",),
                "author": ("username",),
            }[search_field]
            keyword_clauses = []
            for word in keywords:
                keyword_clauses.append("(" + " OR ".join(
                    f"{column} LIKE ? ESCAPE '\\'" for column in columns
                ) + ")")
                token = f"%{escape_like(word)}%"
                params.extend([token] * len(columns))
            if keyword_clauses:
                joiner = " AND " if match_mode == "all" else " OR "
                where.append("(" + joiner.join(keyword_clauses) + ")")
        day = first(query, "day", "").strip()
        year = first(query, "year", "").strip()
        month = first(query, "month", "").strip()
        if day:
            where.append("post_day=?"); params.append(day)
        elif year:
            where.append("post_year=?"); params.append(int(year))
            if month:
                where.append("post_month=?"); params.append(int(month))
        clause = " WHERE " + " AND ".join(where) if where else ""
        with contextlib.closing(self.connect()) as db:
            total = db.execute("SELECT count(*) FROM posts" + clause, params).fetchone()[0]
            # Always reflect the forum's actual post time, never crawl/import time.
            rows = db.execute("SELECT data FROM posts" + clause +
                              " ORDER BY created_at DESC, tid DESC LIMIT ? OFFSET ?",
                              params + [limit, offset]).fetchall()
        items = [json.loads(row[0]) for row in rows]
        for post in items:
            post.setdefault("post_type", "二手出售")
        with contextlib.closing(self.connect()) as db:
            favorite_ids = {row[0] for row in db.execute("SELECT tid FROM favorites").fetchall()}
        for post in items:
            post["is_favorite"] = int(post["tid"]) in favorite_ids
        if keyword:
            for post in items:
                annotate_search_context(post, unique_keywords(keyword),
                                        first(query, "match", "any").lower(),
                                        first(query, "field", "all").lower())
        return {"total": total, "limit": limit, "offset": offset,
                "items": items,
                "excluded_on_page": sum(bool(post.get("search_excluded")) for post in items)}

    def post(self, tid: int, hydrate: bool = False) -> dict[str, Any] | None:
        with contextlib.closing(self.connect()) as db:
            row = db.execute("SELECT data FROM posts WHERE tid=?", (tid,)).fetchone()
            favorite = db.execute("SELECT 1 FROM favorites WHERE tid=?", (tid,)).fetchone()
        if not row:
            return None
        post = json.loads(row[0])
        post.setdefault("post_type", "二手出售")
        post["is_favorite"] = bool(favorite)
        if hydrate and not (post.get("comments_loaded") and post.get("media_loaded")):
            options = {
                "timeout": 10, "retries": 1, "comments": True, "media": True,
                "max_comment_pages": 5,
                "post_types": ["二手出售", "求购", "召集团购", "商家广告"],
            }
            crawler = Crawler(TaskManager.settings(options))
            status, hydrated = crawler.fetch_thread(tid)
            if status == "matched" and hydrated:
                store = Store(self.db_path)
                try:
                    store.save_result(tid, status, hydrated)
                finally:
                    store.close()
                hydrated["is_favorite"] = bool(favorite)
                return hydrated
        return post

    def set_favorite(self, tid: int, favorite: bool) -> dict[str, Any] | None:
        with contextlib.closing(self.connect()) as db:
            if not db.execute("SELECT 1 FROM posts WHERE tid=?", (tid,)).fetchone():
                return None
            if favorite:
                db.execute("INSERT OR IGNORE INTO favorites(tid) VALUES(?)", (tid,))
            else:
                db.execute("DELETE FROM favorites WHERE tid=?", (tid,))
            db.commit()
        return {"tid": tid, "is_favorite": favorite}

    def favorites(self) -> dict[str, Any]:
        with contextlib.closing(self.connect()) as db:
            rows = db.execute(
                """SELECT p.data, f.note, f.created_at AS favorited_at
                   FROM favorites f JOIN posts p ON p.tid=f.tid
                   ORDER BY f.created_at DESC, f.tid DESC"""
            ).fetchall()
        items = []
        for row in rows:
            post = json.loads(row["data"])
            post.setdefault("post_type", "二手出售")
            post.update(is_favorite=True, note=row["note"], favorited_at=row["favorited_at"])
            items.append(post)
        return {"total": len(items), "items": items}

    def set_favorite_note(self, tid: int, note: str) -> dict[str, Any] | None:
        note = str(note).strip()
        if len(note) > 1000:
            raise ValueError("备注不能超过 1000 字")
        with contextlib.closing(self.connect()) as db:
            if not db.execute("SELECT 1 FROM favorites WHERE tid=?", (tid,)).fetchone():
                return None
            db.execute("UPDATE favorites SET note=? WHERE tid=?", (note, tid))
            db.commit()
        return {"tid": tid, "note": note}

    def update_post_status(self, tid: int, status: str) -> dict[str, Any] | None:
        status = str(status).strip()
        allowed = {"出售", "出售+求购", "部分已出", "已出", "求购", "待确认"}
        if status not in allowed:
            raise ValueError("交易状态无效")

        with contextlib.closing(self.connect()) as db:
            row = db.execute("SELECT data FROM posts WHERE tid=?", (tid,)).fetchone()
            if not row:
                return None
            post = json.loads(row[0])
            post["category"] = status
            post["is_sold"] = status == "已出"
            if status in {"出售", "已出", "求购", "待确认"}:
                for item in post.get("item_details", []):
                    if isinstance(item, dict):
                        item["status"] = status
            db.execute(
                "UPDATE posts SET category=?, search_text=?, data=? WHERE tid=?",
                (status, searchable_text(post), json.dumps(post, ensure_ascii=False), tid),
            )
            db.commit()

        save_monthly_post(self.db_path, post)
        self.rebuild_daily()
        return post

    def refresh_post(self, tid: int) -> dict[str, Any] | None:
        previous = self.post(tid)
        if not previous:
            return None
        crawler = Crawler(TaskManager.settings({
            "timeout": 10, "retries": 1, "comments": True, "max_comment_pages": 20,
            "media": True,
            "post_types": ["二手出售", "求购", "召集团购", "商家广告"],
        }))
        fetch_status, post = crawler.fetch_thread(tid)
        if fetch_status != "matched" or not post:
            messages = {
                "unreachable": "暂时无法连接原帖", "no_title": "原帖不存在或无法读取",
                "no_row": "原帖内容暂时无法解析", "bad_row": "原帖返回了无效数据",
                "skipped_title": "原帖标题已不符合交易帖规则",
                "skipped_forum": "原帖已不属于玩具交易区",
                "skipped_pinned": "原帖属于置顶固定展示位",
                "skipped_type_unknown": "网站没有返回可识别的帖子类型",
                "skipped_keyword": "原帖已不符合关键词规则",
            }
            raise RuntimeError(messages.get(fetch_status, f"更新失败：{fetch_status}"))
        store = Store(self.db_path)
        try:
            store.save_result(tid, fetch_status, post)
        finally:
            store.close()
        self.rebuild_daily()
        refreshed = self.post(tid) or post
        old_status = previous.get("category", "待确认")
        new_status = refreshed.get("category", "待确认")
        return {
            "post": refreshed, "previous_status": old_status, "current_status": new_status,
            "status_changed": old_status != new_status,
            "message": (f"状态已由{old_status}更新为{new_status}"
                        if old_status != new_status else f"状态仍为{new_status}"),
        }

    def all_posts(self, limit: int | None = 10000,
                  post_type: str | None = None) -> list[dict[str, Any]]:
        with contextlib.closing(self.connect()) as db:
            where = " WHERE post_type=?" if post_type else ""
            params: list[Any] = [post_type] if post_type else []
            if limit is None:
                rows = db.execute(
                    "SELECT data FROM posts" + where + " ORDER BY tid DESC", params
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT data FROM posts" + where + " ORDER BY tid DESC LIMIT ?",
                    params + [limit],
                ).fetchall()
        posts = [json.loads(row[0]) for row in rows]
        for post in posts:
            post.setdefault("post_type", "二手出售")
        return posts

    def rebuild_daily(self) -> int:
        rows = build_daily_market(self.all_posts(None, "二手出售"))
        columns = (
            "day", "timezone", "first_tid", "last_tid", "first_post_time", "last_post_time",
            "posts", "selling", "sold", "wanted", "uncertain", "sold_rate", "active_users",
            "item_count", "price_samples", "average_price", "median_price", "minimum_price",
            "maximum_price", "listed_value_sum", "comments", "images", "xianyu_links",
            "post_change_pct", "median_price_change_pct", "updated_at", "data",
        )
        placeholders = ",".join("?" for _ in columns)
        with contextlib.closing(self.connect()) as db:
            db.execute("DELETE FROM daily_market")
            db.executemany(
                f"INSERT INTO daily_market ({','.join(columns)}) VALUES ({placeholders})",
                [[row.get(column) if column != "data" else json.dumps(row, ensure_ascii=False)
                  for column in columns] for row in rows],
            )
            db.commit()
        return len(rows)

    def daily(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = max(1, min(int(first(query, "limit", "90")), 3650))
        with contextlib.closing(self.connect()) as db:
            total = db.execute("SELECT count(*) FROM daily_market").fetchone()[0]
            rows = db.execute("SELECT data FROM daily_market ORDER BY day DESC LIMIT ?", (limit,)).fetchall()
        return {"total": total, "items": [json.loads(row[0]) for row in rows]}

    def summary(self, query: dict[str, list[str]]) -> dict[str, Any]:
        days = max(1, min(int(first(query, "days", "30")), 3650))
        all_posts = self.all_posts(post_type="二手出售")
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        posts = [p for p in all_posts if (parse_iso(p.get("created_at", "")) or cutoff) >= cutoff]
        post_categories = Counter(p.get("category", "待确认") for p in posts)
        categories = Counter(item.get("status", "待确认") for p in posts for item in post_items(p))
        sale_base = categories["出售"] + categories["已出"]
        prices = [float(x) for p in posts for x in p.get("prices", []) if number(x)]
        daily: defaultdict[str, Counter[str]] = defaultdict(Counter)
        users: Counter[str] = Counter()
        terms: Counter[str] = Counter()
        xianyu_count = 0
        image_count = 0
        comment_count = 0
        for post in posts:
            dt = parse_iso(post.get("created_at", ""))
            if dt:
                daily[dt.date().isoformat()].update(item.get("status", "待确认") for item in post_items(post))
            users[post.get("username") or "未知用户"] += 1
            xianyu_count += len(post.get("xianyu_links", []))
            image_count += len(post.get("images", []))
            comment_count += len(post.get("comments", []))
            for item in post.get("items", []):
                for term in TERM_RE.findall(item):
                    if len(term) >= 2 and term not in ITEM_TERM_STOPWORDS:
                        terms[term.casefold()] += 1
        dates = [(cutoff.date() + timedelta(days=i)).isoformat() for i in range(days + 1)]
        return {
            "period_days": days,
            "kpis": {
                "posts": len(posts), "items": sum(categories.values()),
                "selling": categories["出售"], "sold": categories["已出"],
                "wanted": categories["求购"],
                "partial_posts": post_categories["部分已出"],
                "sold_rate": round(100 * categories["已出"] / sale_base, 1) if sale_base else 0,
                "median_price": round(median(prices), 2) if prices else None,
                "average_price": round(sum(prices) / len(prices), 2) if prices else None,
                "users": len(users), "xianyu_links": xianyu_count,
                "images": image_count, "comments": comment_count,
            },
            "categories": [{"name": key, "value": categories[key]} for key in ("出售", "已出", "求购", "待确认")],
            "daily": [{"date": date, **{key: daily[date][key] for key in ("出售", "已出", "求购")}}
                      for date in dates],
            "top_users": [{"name": name, "value": value} for name, value in users.most_common(10)],
            "hot_terms": [{"name": name, "value": value} for name, value in terms.most_common(16)],
            "price_bands": price_bands(prices),
            "latest_tid": all_posts[0]["tid"] if all_posts else None,
        }

    def scan_stats(self) -> dict[str, Any]:
        with contextlib.closing(self.connect()) as db:
            rows = db.execute("SELECT status, count(*) AS count FROM scan_state GROUP BY status").fetchall()
            total = sum(row["count"] for row in rows)
            meta = db.execute("SELECT key, value FROM metadata WHERE key IN ('next_tid','last_recorded_id','watch_cursor')").fetchall()
        values = {row["key"]: row["value"] for row in meta}
        last_id = values.get("last_recorded_id")
        if last_id is None:
            next_id = values.get("next_tid") or values.get("watch_cursor")
            last_id = str(int(next_id) - 1) if next_id else None
        return {"total_scanned": total, "last_recorded_id": int(last_id) if last_id else None,
                "statuses": {row["status"]: row["count"] for row in rows}}


def first(query: dict[str, list[str]], key: str, default: str) -> str:
    return query.get(key, [default])[0]


def unique_keywords(value: str) -> list[str]:
    return list(dict.fromkeys(x.casefold() for x in re.split(r"[\s,，、;；]+", value) if x.strip()))[:20]


def annotate_search_context(post: dict[str, Any], keywords: list[str],
                            match_mode: str, search_field: str) -> None:
    if search_field == "title":
        text = str(post.get("title", ""))
    elif search_field == "author":
        text = str(post.get("username", ""))
    else:
        text = searchable_text(post)
    positive: list[bool] = []
    excluded_terms: list[str] = []
    for keyword in keywords:
        escaped = re.escape(keyword)
        negative = re.compile(
            rf"(?:不含|不带|没有|没带|不要|不收|不出|不包括)\s*[^，。；;\n]{{0,4}}?{escaped}|"
            rf"无(?!刷)\s*{escaped}|"
            rf"{escaped}\s*(?:除外|不要)", re.I)
        has_negative = bool(negative.search(text))
        positive_text = negative.sub("", text)
        has_positive = keyword in positive_text.casefold()
        positive.append(has_positive)
        if has_negative and not has_positive:
            excluded_terms.append(keyword)
    is_positive = all(positive) if match_mode == "all" else any(positive)
    post["search_excluded"] = not is_positive
    post["search_excluded_terms"] = excluded_terms


def escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def post_items(post: dict[str, Any]) -> list[dict[str, Any]]:
    details = post.get("item_details")
    if isinstance(details, list) and details:
        return [x for x in details if isinstance(x, dict)]
    status = post.get("category", "待确认")
    return [{"index": i, "name": name, "text": name, "status": status, "prices": []}
            for i, name in enumerate(post.get("items", []), 1)]


def number(value: Any) -> bool:
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


def price_bands(prices: list[float]) -> list[dict[str, Any]]:
    bands = [(0, 200, "0–199"), (200, 500, "200–499"), (500, 1000, "500–999"),
             (1000, 2000, "1k–2k"), (2000, 5000, "2k–5k"), (5000, float("inf"), "5k+")]
    return [{"name": label, "value": sum(low <= p < high for p in prices)} for low, high, label in bands]


def percent_change(current: float | int | None, previous: float | int | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round((float(current) - float(previous)) / float(previous) * 100, 1)


def build_daily_market(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: defaultdict[str, list[tuple[datetime, dict[str, Any]]]] = defaultdict(list)
    for post in posts:
        created = parse_iso(post.get("created_at", ""))
        if created:
            local = created.astimezone(TAIPEI)
            grouped[local.date().isoformat()].append((local, post))

    result: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for day in sorted(grouped):
        records = sorted(grouped[day], key=lambda item: (item[0], int(item[1].get("tid") or 0)))
        day_posts = [item[1] for item in records]
        categories = Counter(item.get("status", "待确认") for post in day_posts for item in post_items(post))
        prices = [float(value) for post in day_posts for value in post.get("prices", []) if number(value)]
        users = {post.get("user_id") or post.get("username") for post in day_posts}
        terms: Counter[str] = Counter()
        for post in day_posts:
            for item in post.get("items", []):
                for term in TERM_RE.findall(item):
                    if len(term) >= 2 and term not in ITEM_TERM_STOPWORDS:
                        terms[term.casefold()] += 1
        sale_base = categories["出售"] + categories["已出"]
        row: dict[str, Any] = {
            "day": day, "timezone": "Asia/Taipei",
            "first_tid": int(day_posts[0]["tid"]), "last_tid": int(day_posts[-1]["tid"]),
            "first_post_time": records[0][0].isoformat(), "last_post_time": records[-1][0].isoformat(),
            "posts": len(day_posts), "selling": categories["出售"], "sold": categories["已出"],
            "wanted": categories["求购"], "uncertain": categories["待确认"],
            "sold_rate": round(categories["已出"] / sale_base * 100, 1) if sale_base else 0,
            "active_users": len(users), "item_count": sum(len(post_items(post)) for post in day_posts),
            "price_samples": len(prices), "average_price": round(sum(prices) / len(prices), 2) if prices else None,
            "median_price": round(median(prices), 2) if prices else None,
            "minimum_price": min(prices) if prices else None, "maximum_price": max(prices) if prices else None,
            "listed_value_sum": round(sum(prices), 2) if prices else None,
            "comments": sum(len(post.get("comments", [])) for post in day_posts),
            "images": sum(len(post.get("images", [])) for post in day_posts),
            "xianyu_links": sum(len(post.get("xianyu_links", [])) for post in day_posts),
            "top_terms": [{"name": name, "value": count} for name, count in terms.most_common(10)],
            "top_users": [{"name": name, "value": count} for name, count in
                          Counter(post.get("username") or "未知用户" for post in day_posts).most_common(5)],
            "post_change_pct": percent_change(len(day_posts), previous.get("posts") if previous else None),
            "median_price_change_pct": percent_change(
                round(median(prices), 2) if prices else None,
                previous.get("median_price") if previous else None,
            ),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        result.append(row)
        previous = row
    return result


class HunterManager:
    """Run each keyword watcher as an independent, concurrently managed task."""

    def __init__(self, repo: Repository, tasks: TaskManager) -> None:
        self.repo = repo
        self.tasks = tasks
        self.lock = threading.Lock()
        self.jobs: dict[str, dict[str, Any]] = {}

    @staticmethod
    def normalize(options: dict[str, Any]) -> dict[str, Any]:
        query = str(options.get("q", "")).strip()
        if not unique_keywords(query):
            raise ValueError("请至少输入一个猎手关键词")
        match = str(options.get("match", "any")).lower()
        field = str(options.get("field", "all")).lower()
        category = str(options.get("category", "全部"))
        post_type = str(options.get("post_type", "二手出售"))
        if match not in {"any", "all"}:
            raise ValueError("命中方式无效")
        if field not in {"all", "title", "author"}:
            raise ValueError("检索范围无效")
        if category not in {"全部", "出售", "出售+求购", "部分已出", "已出", "求购"}:
            raise ValueError("交易状态无效")
        if post_type not in {"全部", "二手出售", "求购", "召集团购", "商家广告"}:
            raise ValueError("帖子类型无效")
        interval = max(10, min(int(options.get("interval", 60)), 86400))
        return {"q": query, "match": match, "field": field, "post_type": post_type,
                "category": category, "interval": interval}

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            jobs = [self._copy_job(job) for job in self.jobs.values()]
        jobs.sort(key=lambda job: job.get("created_at", ""), reverse=True)
        active = sum(bool(job["active"]) for job in jobs)
        return {"active": active > 0, "active_count": active, "total": len(jobs), "tasks": jobs}

    @staticmethod
    def _copy_job(job: dict[str, Any]) -> dict[str, Any]:
        return {
            key: ([dict(x) for x in value] if key in {"results", "latest_hits"} else
                  dict(value) if key == "config" else value)
            for key, value in job.items()
            if key not in {"event", "thread", "seen_ids"}
        }

    def search(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        query = {key: [str(config[key])]
                 for key in ("q", "match", "field", "category", "post_type")}
        query["limit"] = ["100"]
        posts = self.repo.posts(query)["items"]
        keys = ("tid", "title", "username", "post_type", "category",
                "created_at", "prices", "url", "items",
                "search_excluded", "search_excluded_terms")
        return [{key: post.get(key) for key in keys} for post in posts]

    @staticmethod
    def published_after(posts: list[dict[str, Any]], started: datetime) -> list[dict[str, Any]]:
        return [post for post in posts
                if (parse_iso(post.get("created_at", "")) or started) >= started]

    def start_task(self, options: dict[str, Any]) -> dict[str, Any]:
        config = self.normalize(options)
        # Forum timestamps are second-precision. Floor the boundary to a whole
        # second, then use the baseline ID set to disambiguate same-second posts.
        started = datetime.now(timezone.utc).replace(microsecond=0)
        baseline = self.search(config)
        historical_ids = {
            int(post["tid"]) for post in baseline
            if (parse_iso(post.get("created_at", "")) or started) <= started
        }
        task_id = uuid.uuid4().hex[:12]
        event = threading.Event()
        name = str(options.get("name", "")).strip()[:60] or config["q"]
        job: dict[str, Any] = {
            "id": task_id, "name": name, "active": True,
            "status": "scheduled", "message": "已建立当前起点，等待新帖",
            "config": config, "created_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started.isoformat(), "completed_at": None,
            "checks": 0, "total_matches": 0, "new_hits": 0,
            "alert_id": 0, "results": [], "last_check_at": None,
            "next_check_at": None, "latest_hits": [], "error": None,
            "event": event, "seen_ids": historical_ids,
        }
        with self.lock:
            self.jobs[task_id] = job
            thread = threading.Thread(target=self._run, args=(task_id,), daemon=True,
                                      name=f"sdgun-hunter-{task_id}")
            job["thread"] = thread
            thread.start()
        return self._copy_job(job)

    def start(self, options: dict[str, Any]) -> tuple[bool, str]:
        job = self.start_task(options)
        return True, f"猎手任务“{job['name']}”已启动"

    def edit_task(self, task_id: str, options: dict[str, Any]) -> dict[str, Any]:
        config = self.normalize(options)
        name = str(options.get("name", "")).strip()[:60] or config["q"]
        with self.lock:
            if task_id not in self.jobs:
                raise ValueError("猎手任务不存在")
            current = self.jobs[task_id]
            old_config = dict(current["config"])
            active = bool(current["active"])
        search_changed = any(
            old_config.get(key) != config.get(key)
            for key in ("q", "match", "field", "post_type", "category")
        )
        baseline: list[dict[str, Any]] = self.search(config) if search_changed else []
        edited_at = datetime.now(timezone.utc).replace(microsecond=0)
        with self.lock:
            job = self.jobs[task_id]
            job["name"] = name
            job["config"] = config
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            if search_changed:
                job["started_at"] = edited_at.isoformat()
                job["seen_ids"] = {
                    int(post["tid"]) for post in baseline
                    if (parse_iso(post.get("created_at", "")) or edited_at) <= edited_at
                }
                job["results"] = []
                job["latest_hits"] = []
                job["total_matches"] = 0
                job["new_hits"] = 0
                job["message"] = "规则已更新，已从当前时间重新建立追踪起点"
            elif active:
                job["message"] = "任务设置已更新"
            return self._copy_job(job)

    def stop(self, task_id: str | None = None) -> bool:
        with self.lock:
            if task_id:
                if task_id not in self.jobs:
                    return False
                targets = [self.jobs[task_id]]
            else:
                targets = [job for job in self.jobs.values() if job["active"]]
            if not targets:
                return False
            for job in targets:
                if job["active"]:
                    job["message"] = "正在停止猎手任务"
                    job["status"] = "stopping"
                    job["event"].set()
        return True

    def _refresh(self, event: threading.Event) -> None:
        task = self.tasks.snapshot()
        if not task.get("running"):
            self.tasks.start("refresh", {
                "batch": 64, "workers": 8, "comments": True,
                "max_comment_pages": 5, "refresh_recent": 0,
            })
            task = self.tasks.snapshot()
        if task.get("mode") == "watch":
            return
        deadline = time.monotonic() + 180
        while self.tasks.snapshot().get("running") and time.monotonic() < deadline:
            if event.wait(0.5):
                return

    def _run(self, task_id: str) -> None:
        with self.lock:
            job = self.jobs[task_id]
            event = job["event"]
        while not event.is_set():
            try:
                with self.lock:
                    config = dict(job["config"])
                    job["status"] = "running"
                self._refresh(event)
                if event.is_set():
                    break
                results = self.search(config)
                started = parse_iso(job.get("started_at", "")) or datetime.now(timezone.utc)
                eligible = self.published_after(results, started)
                unseen = [post for post in eligible if int(post["tid"]) not in job["seen_ids"]]
                new_results = [post for post in unseen if not post.get("search_excluded")]
                job["seen_ids"].update(int(post["tid"]) for post in eligible)
                now = datetime.now(timezone.utc)
                next_check = now + timedelta(seconds=int(config["interval"]))
                with self.lock:
                    previous = {int(post["tid"]): post for post in job.get("results", [])}
                    previous.update({int(post["tid"]): post for post in unseen})
                    tracked = sorted(previous.values(),
                                     key=lambda post: (post.get("created_at") or "", int(post["tid"])),
                                     reverse=True)[:50]
                    job.update({
                        "checks": int(job["checks"]) + 1,
                        "total_matches": sum(not post.get("search_excluded") for post in tracked),
                        "excluded_count": sum(bool(post.get("search_excluded")) for post in tracked),
                        "new_hits": len(new_results),
                        "results": tracked, "last_check_at": now.isoformat(),
                        "next_check_at": next_check.isoformat(), "error": None,
                        "message": (f"启动后命中 {sum(not post.get('search_excluded') for post in tracked)} 条；"
                                    f"本轮新增 {len(new_results)} 条"),
                    })
                    if new_results:
                        job["alert_id"] = int(job["alert_id"]) + 1
                        job["latest_hits"] = new_results[:20]
                if event.wait(int(config["interval"])):
                    break
            except Exception as exc:
                with self.lock:
                    job["error"] = str(exc)
                    job["message"] = "本轮检索失败，将自动重试"
                    job["next_check_at"] = (
                        datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
                if event.wait(30):
                    break
        with self.lock:
            job["active"] = False
            job["status"] = "completed"
            job["next_check_at"] = None
            job["completed_at"] = datetime.now(timezone.utc).isoformat()
            job["message"] = "猎手任务已停止"
            job["latest_hits"] = []
            job["alert_id"] = int(job["alert_id"]) + 1


class Handler(BaseHTTPRequestHandler):
    server_version = "SDGunDashboard/1.0"

    @property
    def app(self) -> "DashboardServer":
        return self.server  # type: ignore[return-value]

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def send_data(self, data: bytes, content_type: str, status: int = 200,
                  extra_headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        if "Cache-Control" not in (extra_headers or {}):
            self.send_header("Cache-Control", "no-store" if content_type.startswith("application/json") else "public, max-age=60")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, value: Any, status: int = 200) -> None:
        self.send_data(json_bytes(value), "application/json; charset=utf-8", status)

    def read_json(self) -> dict[str, Any]:
        length = min(int(self.headers.get("Content-Length", "0")), 1_000_000)
        value = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(value, dict):
            raise ValueError("请求内容必须是 JSON 对象")
        return value

    def update_post_status(self) -> bool:
        path = urllib.parse.urlsplit(self.path).path
        parts = path.strip("/").split("/")
        if len(parts) != 4 or parts[:2] != ["api", "posts"] or parts[3] != "status":
            return False
        tid = int(parts[2])
        payload = self.read_json()
        if "status" not in payload:
            raise ValueError("缺少 status 字段")
        result = self.app.repo.update_post_status(tid, payload["status"])
        self.send_json(result or {"error": "帖子不存在"}, 200 if result else 404)
        return True

    def do_GET(self) -> None:
        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)
        try:
            if url.path == "/favicon.ico":
                return self.serve_static("/favicon.svg")
            if url.path == "/api/summary":
                payload = self.app.repo.summary(query)
                payload["scan"] = self.app.repo.scan_stats()
                return self.send_json(payload)
            if url.path == "/api/posts":
                return self.send_json(self.app.repo.posts(query))
            if url.path == "/api/favorites":
                return self.send_json(self.app.repo.favorites())
            if url.path.startswith("/api/posts/"):
                post = self.app.repo.post(int(url.path.rsplit("/", 1)[1]))
                return self.send_json(post or {"error": "帖子不存在"}, 200 if post else 404)
            if url.path == "/api/task":
                return self.send_json(self.app.tasks.snapshot())
            if url.path == "/api/hunter":
                return self.send_json(self.app.hunter.snapshot())
            if url.path == "/api/image":
                return self.proxy_image(first(query, "url", ""))
            if url.path == "/api/video":
                return self.proxy_video(first(query, "url", ""))
            if url.path == "/api/daily":
                return self.send_json(self.app.repo.daily(query))
            if url.path == "/api/export.jsonl":
                data = b"".join(json_bytes(p) + b"\n" for p in self.app.repo.all_posts())
                return self.send_data(data, "application/x-ndjson; charset=utf-8", extra_headers={
                    "Content-Disposition": 'attachment; filename="sdgun-posts.jsonl"'})
            if url.path == "/api/export.csv":
                out = io.StringIO()
                writer = csv.writer(out)
                writer.writerow(["tid", "时间", "帖子类型", "交易状态", "标题", "用户名", "价格", "物品", "链接"])
                for p in self.app.repo.all_posts():
                    writer.writerow([p.get("tid"), p.get("created_at"), p.get("post_type"),
                                     p.get("category"), p.get("title"),
                                     p.get("username"), " | ".join(p.get("prices", [])),
                                     " | ".join(p.get("items", [])), p.get("url")])
                data = ("\ufeff" + out.getvalue()).encode("utf-8")
                return self.send_data(data, "text/csv; charset=utf-8", extra_headers={
                    "Content-Disposition": 'attachment; filename="sdgun-posts.csv"'})
            if url.path == "/api/export-daily.csv":
                out = io.StringIO()
                writer = csv.writer(out)
                writer.writerow(["日期", "时区", "首TID", "末TID", "发帖量", "出售", "已出", "求购",
                                 "公开售出率%", "活跃用户", "物品条目", "价格样本", "均价", "中位数",
                                 "最低价", "最高价", "标价合计", "评论", "图片", "闲鱼链接",
                                 "发帖量环比%", "中位价环比%", "首帖时间", "末帖时间"])
                for row in self.app.repo.daily({"limit": ["3650"]})["items"]:
                    writer.writerow([row.get(key) for key in (
                        "day", "timezone", "first_tid", "last_tid", "posts", "selling", "sold", "wanted",
                        "sold_rate", "active_users", "item_count", "price_samples", "average_price",
                        "median_price", "minimum_price", "maximum_price", "listed_value_sum", "comments",
                        "images", "xianyu_links", "post_change_pct", "median_price_change_pct",
                        "first_post_time", "last_post_time")])
                data = ("\ufeff" + out.getvalue()).encode("utf-8")
                return self.send_data(data, "text/csv; charset=utf-8", extra_headers={
                    "Content-Disposition": 'attachment; filename="sdgun-daily-market.csv"'})
            return self.serve_static(url.path)
        except Exception as exc:
            return self.send_json({"error": str(exc)}, 500)

    def do_POST(self) -> None:
        try:
            if self.update_post_status():
                return
            path = urllib.parse.urlsplit(self.path).path
            parts = path.strip("/").split("/")
            if len(parts) == 4 and parts[:2] == ["api", "posts"] and parts[3] == "refresh":
                result = self.app.repo.refresh_post(int(parts[2]))
                return self.send_json(result or {"error": "帖子不存在"}, 200 if result else 404)
            if len(parts) == 4 and parts[:2] == ["api", "posts"] and parts[3] == "hydrate":
                result = self.app.repo.post(int(parts[2]), hydrate=True)
                return self.send_json(result or {"error": "帖子不存在"}, 200 if result else 404)
            if self.path == "/api/tasks/watch":
                ok, message = self.app.tasks.start("watch", self.read_json())
                return self.send_json({"ok": ok, "message": message}, 202 if ok else 409)
            if self.path == "/api/tasks/refresh":
                ok, message = self.app.tasks.start("refresh", self.read_json())
                return self.send_json({"ok": ok, "message": message}, 202 if ok else 409)
            if self.path == "/api/tasks/stop":
                ok = self.app.tasks.stop()
                return self.send_json({"ok": ok, "message": "停止信号已发送" if ok else "当前没有任务"})
            if self.path == "/api/hunter/start":
                job = self.app.hunter.start_task(self.read_json())
                return self.send_json({
                    "ok": True, "message": f"猎手任务“{job['name']}”已启动",
                    "task": job,
                }, 202)
            if self.path == "/api/hunter/edit":
                payload = self.read_json()
                task_id = str(payload.pop("task_id", "")).strip()
                if not task_id:
                    raise ValueError("缺少猎手任务 ID")
                job = self.app.hunter.edit_task(task_id, payload)
                return self.send_json({
                    "ok": True, "message": f"猎手任务“{job['name']}”已更新",
                    "task": job,
                })
            if self.path == "/api/hunter/stop":
                task_id = str(self.read_json().get("task_id", "")).strip() or None
                ok = self.app.hunter.stop(task_id)
                return self.send_json({"ok": ok, "message": "猎手任务正在停止" if ok else "猎手任务未运行"})
            if self.path.startswith("/api/favorites/"):
                tid = int(self.path.rsplit("/", 1)[1])
                payload = self.read_json()
                if "note" in payload:
                    result = self.app.repo.set_favorite_note(tid, payload["note"])
                else:
                    result = self.app.repo.set_favorite(tid, bool(payload.get("favorite", True)))
                return self.send_json(result or {"error": "帖子不存在"}, 200 if result else 404)
            return self.send_json({"error": "接口不存在"}, 404)
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            return self.send_json({"error": str(exc)}, 400)
        except RuntimeError as exc:
            return self.send_json({"error": str(exc)}, 502)

    def do_PATCH(self) -> None:
        try:
            if self.update_post_status():
                return
            return self.send_json({"error": "接口不存在"}, 404)
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            return self.send_json({"error": str(exc)}, 400)

    def do_PUT(self) -> None:
        return self.do_PATCH()

    def proxy_image(self, source: str) -> None:
        parsed = urllib.parse.urlsplit(source)
        hostname = (parsed.hostname or "").lower()
        if parsed.scheme not in {"http", "https"} or hostname not in IMAGE_HOSTS:
            return self.send_json({"error": "不允许的图片地址"}, 403)
        request = urllib.request.Request(source, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://app.sdgun.com.cn/",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        })
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                final_host = (urllib.parse.urlsplit(response.geturl()).hostname or "").lower()
                if final_host not in IMAGE_HOSTS:
                    return self.send_json({"error": "图片跳转到了不允许的地址"}, 403)
                content_type = response.headers.get_content_type()
                if not content_type.startswith("image/"):
                    return self.send_json({"error": "上游返回的不是图片"}, 502)
                declared = int(response.headers.get("Content-Length") or 0)
                if declared > MAX_IMAGE_BYTES:
                    return self.send_json({"error": "图片超过25MB限制"}, 413)
                data = response.read(MAX_IMAGE_BYTES + 1)
                if len(data) > MAX_IMAGE_BYTES:
                    return self.send_json({"error": "图片超过25MB限制"}, 413)
        except Exception as exc:
            return self.send_json({"error": f"图片读取失败: {exc}"}, 502)
        return self.send_data(data, content_type, extra_headers={
            "Cache-Control": "public, max-age=86400",
            "X-Content-Type-Options": "nosniff",
        })

    def proxy_video(self, source: str) -> None:
        parsed = urllib.parse.urlsplit(source)
        hostname = (parsed.hostname or "").lower()
        if parsed.scheme not in {"http", "https"} or hostname not in IMAGE_HOSTS:
            return self.send_json({"error": "不允许的视频地址"}, 403)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://app.sdgun.com.cn/",
            "Accept": "video/*,*/*;q=0.8",
        }
        requested_range = self.headers.get("Range")
        if requested_range:
            headers["Range"] = requested_range
        request = urllib.request.Request(source, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                final_host = (urllib.parse.urlsplit(response.geturl()).hostname or "").lower()
                if final_host not in IMAGE_HOSTS:
                    return self.send_json({"error": "视频跳转到了不允许的地址"}, 403)
                content_type = response.headers.get_content_type()
                if not content_type.startswith("video/") and content_type != "application/octet-stream":
                    return self.send_json({"error": "上游返回的不是视频"}, 502)
                declared = int(response.headers.get("Content-Length") or 0)
                if declared > MAX_VIDEO_BYTES:
                    return self.send_json({"error": "视频超过500MB限制"}, 413)
                data = response.read(MAX_VIDEO_BYTES + 1)
                if len(data) > MAX_VIDEO_BYTES:
                    return self.send_json({"error": "视频超过500MB限制"}, 413)
                response_headers = {
                    "Cache-Control": "public, max-age=86400",
                    "Accept-Ranges": response.headers.get("Accept-Ranges", "bytes"),
                    "X-Content-Type-Options": "nosniff",
                }
                if response.headers.get("Content-Range"):
                    response_headers["Content-Range"] = response.headers["Content-Range"]
                status = getattr(response, "status", 200)
        except Exception as exc:
            return self.send_json({"error": f"视频读取失败: {exc}"}, 502)
        return self.send_data(data, content_type, status=status, extra_headers=response_headers)

    def serve_static(self, path: str) -> None:
        relative = "index.html" if path in {"", "/"} else path.lstrip("/")
        target = (WEB_ROOT / relative).resolve()
        if WEB_ROOT.resolve() not in target.parents and target != WEB_ROOT.resolve():
            return self.send_json({"error": "非法路径"}, 403)
        if not target.is_file():
            return self.send_json({"error": "页面不存在"}, 404)
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            content_type += "; charset=utf-8"
        self.send_data(target.read_bytes(), content_type)


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], db_path: Path) -> None:
        self.repo = Repository(db_path)
        self.repo.rebuild_daily()
        self.tasks = TaskManager(db_path)
        self.hunter = HunterManager(self.repo, self.tasks)
        super().__init__(address, Handler)


def main() -> int:
    parser = argparse.ArgumentParser(description="SDGun 市场情报 Web 控制台")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    server = DashboardServer((args.host, args.port), args.db.resolve())
    print(f"SDGun Dashboard: http://{args.host}:{args.port}")
    print(f"Database: {args.db.resolve()}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.hunter.stop()
        server.tasks.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
