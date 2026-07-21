import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sdgun_crawler import Store
from web_app import HunterManager, Repository, TaskManager


def sample(tid, created_at, category="出售", prices=None):
    return {
        "tid": tid, "url": f"http://example/{tid}", "title": f"【二手出售】物品{tid}",
        "username": "测试用户", "user_id": "1", "created_at": created_at,
        "content": "正文", "images": [], "links": [], "xianyu_links": [],
        "comments": [], "category": category, "is_sold": category == "已出",
        "items": [f"物品{tid}"], "prices": prices or [], "crawled_at": "2030-01-01T00:00:00+00:00",
    }


class RepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "test.db"
        store = Store(self.path)
        # Higher tid is intentionally older: sorting must use actual post time.
        store.save_result(20, "matched", sample(20, "2026-07-19T10:00:00+00:00"))
        store.save_result(10, "matched", sample(10, "2026-07-20T10:00:00+00:00", "已出", ["300"]))
        store.close()
        self.repo = Repository(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def test_posts_are_sorted_by_post_time_not_tid_or_crawl_time(self):
        result = self.repo.posts({"limit": ["10"]})
        self.assertEqual([x["tid"] for x in result["items"]], [10, 20])

    def test_posts_support_multiple_keywords_and_day_partition(self):
        result = self.repo.posts({"q": ["物品10,不存在"], "day": ["2026-07-20"]})
        self.assertEqual([x["tid"] for x in result["items"]], [10])
        db = self.repo.connect()
        try:
            row = db.execute("SELECT post_year, post_month, post_day FROM posts WHERE tid=10").fetchone()
        finally:
            db.close()
        self.assertEqual(tuple(row), (2026, 7, "2026-07-20"))
        monthly = self.path.parent / "data" / "2026" / "07" / "market.db"
        self.assertTrue(monthly.exists())

    def test_keyword_match_mode_and_search_field(self):
        any_title = self.repo.posts({"q": ["物品10,不存在"], "match": ["any"], "field": ["title"]})
        self.assertEqual([x["tid"] for x in any_title["items"]], [10])
        all_title = self.repo.posts({"q": ["二手出售 物品10"], "match": ["all"], "field": ["title"]})
        self.assertEqual([x["tid"] for x in all_title["items"]], [10])
        missing_title = self.repo.posts({"q": ["物品10 不存在"], "match": ["all"], "field": ["title"]})
        self.assertEqual(missing_title["total"], 0)
        author_only = self.repo.posts({"q": ["测试 用户"], "match": ["all"], "field": ["author"]})
        self.assertEqual(author_only["total"], 2)
        wrong_field = self.repo.posts({"q": ["测试用户"], "match": ["all"], "field": ["title"]})
        self.assertEqual(wrong_field["total"], 0)

    def test_search_marks_negative_context_and_ignores_link_urls(self):
        store = Store(self.path)
        negative = sample(30, "2026-07-20T11:00:00+00:00")
        negative["content"] = "本体不含手电"
        positive = sample(31, "2026-07-20T12:00:00+00:00")
        positive["content"] = "附送手电"
        link_only = sample(32, "2026-07-20T13:00:00+00:00")
        link_only["content"] = "详情 https://example.com/手电"
        for post in (negative, positive, link_only):
            store.save_result(post["tid"], "matched", post)
        store.close()
        result = self.repo.posts({"q": ["手电"], "match": ["any"], "field": ["all"], "limit": ["20"]})
        by_tid = {post["tid"]: post for post in result["items"]}
        self.assertFalse(by_tid[31]["search_excluded"])
        self.assertTrue(by_tid[30]["search_excluded"])
        self.assertEqual(by_tid[30]["search_excluded_terms"], ["手电"])
        self.assertNotIn(32, by_tid)

    def test_market_hunter_reuses_all_search_modes(self):
        config = HunterManager.normalize({
            "q": "二手出售 物品10", "match": "all", "field": "title",
            "category": "已出", "interval": 2,
        })
        self.assertEqual(config["interval"], 10)
        hunter = HunterManager(self.repo, TaskManager(self.path))
        results = hunter.search(config)
        self.assertEqual([x["tid"] for x in results], [10])
        with self.assertRaises(ValueError):
            HunterManager.normalize({"q": "   "})

    def test_market_hunter_excludes_posts_before_start(self):
        started = datetime(2026, 7, 20, 10, 0, tzinfo=timezone.utc)
        posts = [
            {"tid": 1, "created_at": "2026-07-20T09:59:59+00:00"},
            {"tid": 2, "created_at": "2026-07-20T10:00:00+00:00"},
            {"tid": 3, "created_at": "2026-07-20T10:00:01+00:00"},
        ]
        self.assertEqual([x["tid"] for x in HunterManager.published_after(posts, started)], [2, 3])

    def test_summary_quantifies_status_and_price(self):
        result = self.repo.summary({"days": ["3650"]})
        self.assertEqual(result["kpis"]["posts"], 2)
        self.assertEqual(result["kpis"]["sold"], 1)
        self.assertEqual(result["kpis"]["median_price"], 300.0)

    def test_refresh_stops_at_forum_latest_published_tid(self):
        store = Store(self.path)
        store.set_meta("next_tid", 100)
        manager = TaskManager(self.path)
        starts = []

        def fake_process(tids, options, frontier=False):
            starts.append(tids[0])
            return {tid: "skipped_title" for tid in tids}, {}

        manager._process_range = fake_process
        with patch("web_app.Crawler.fetch_latest_forum_tid", return_value=108):
            cursor = manager._catch_up({"batch": 8}, store)
        self.assertEqual(cursor, 109)
        self.assertEqual(starts, [100, 108])
        self.assertEqual(store.get_meta("last_recorded_id"), "108")
        store.close()

    def test_daily_market_is_persisted_and_quantified(self):
        self.assertEqual(self.repo.rebuild_daily(), 2)
        rows = self.repo.daily({"limit": ["10"]})["items"]
        self.assertEqual([row["day"] for row in rows], ["2026-07-20", "2026-07-19"])
        latest = rows[0]
        self.assertEqual(latest["posts"], 1)
        self.assertEqual(latest["sold"], 1)
        self.assertEqual(latest["sold_rate"], 100.0)
        self.assertEqual(latest["median_price"], 300.0)
        self.assertEqual(latest["active_users"], 1)


if __name__ == "__main__":
    unittest.main()
