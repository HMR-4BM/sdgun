import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sdgun_crawler import Store
from web_app import HunterManager, Repository, TaskManager, enrich_market_indices


def sample(tid, created_at, category="出售", prices=None):
    return {
        "tid": tid, "url": f"http://example/{tid}", "title": f"【二手出售】物品{tid}",
        "post_type": "二手出售",
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

    def test_posts_can_be_filtered_by_native_post_type(self):
        wanted = sample(30, "2026-07-20T11:00:00+00:00", "求购")
        wanted.update(post_type="求购", title="【求购】物品30")
        store = Store(self.path)
        store.save_result(30, "matched", wanted)
        store.close()
        self.assertEqual(
            [x["tid"] for x in self.repo.posts({"post_type": ["求购"]})["items"]],
            [30],
        )
        self.assertEqual(
            [x["tid"] for x in self.repo.posts({"post_type": ["二手出售"]})["items"]],
            [10, 20],
        )

    def test_favorites_can_be_saved_noted_listed_and_removed(self):
        self.assertFalse(self.repo.post(10)["is_favorite"])
        self.assertEqual(self.repo.set_favorite(10, True), {"tid": 10, "is_favorite": True})
        self.assertTrue(self.repo.post(10)["is_favorite"])
        self.assertEqual(self.repo.set_favorite_note(10, "价格合适，待联系"),
                         {"tid": 10, "note": "价格合适，待联系"})
        result = self.repo.favorites()
        self.assertEqual([post["tid"] for post in result["items"]], [10])
        self.assertTrue(result["items"][0]["is_favorite"])
        self.assertEqual(result["items"][0]["note"], "价格合适，待联系")
        self.repo.set_favorite(10, False)
        self.assertEqual(self.repo.favorites()["total"], 0)
        self.assertIsNone(self.repo.set_favorite(999, True))
        self.assertIsNone(self.repo.set_favorite_note(999, "不存在"))

    def test_posts_support_multiple_keywords_and_day_partition(self):
        result = self.repo.posts({"q": ["物品10,不存在"], "day": ["2026-07-20"]})
        self.assertEqual([x["tid"] for x in result["items"]], [10])
        db = self.repo.connect()
        try:
            row = db.execute("SELECT post_year, post_month, post_day FROM posts WHERE tid=10").fetchone()
        finally:
            db.close()
        self.assertEqual(tuple(row), (2026, 7, "2026-07-20"))
        monthly = self.path.parent / "data" / "archive" / "2026" / "07" / "market.db"
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
        # Excluded matches remain in their original chronological position.
        negative = sample(30, "2026-07-20T13:00:00+00:00")
        negative["content"] = "本体不含手电"
        positive = sample(31, "2026-07-20T12:00:00+00:00")
        positive["content"] = "附送手电"
        link_only = sample(32, "2026-07-20T13:00:00+00:00")
        link_only["content"] = "详情 https://example.com/手电"
        for post in (negative, positive, link_only):
            store.save_result(post["tid"], "matched", post)
        store.close()
        result = self.repo.posts({"q": ["手电"], "match": ["any"], "field": ["all"], "limit": ["20"]})
        self.assertEqual([post["tid"] for post in result["items"]], [30, 31])
        by_tid = {post["tid"]: post for post in result["items"]}
        self.assertFalse(by_tid[31]["search_excluded"])
        self.assertTrue(by_tid[30]["search_excluded"])
        self.assertEqual(by_tid[30]["search_excluded_terms"], ["手电"])
        self.assertNotIn(32, by_tid)

    def test_product_term_wushua_does_not_exclude_later_model_keywords(self):
        store = Store(self.path)
        post = sample(4152176, "2026-07-21T10:53:53+00:00")
        post["title"] = "【二手出售】023，LDT无刷ATMurgi"
        store.save_result(post["tid"], "matched", post)
        store.close()
        for keyword in ("ATM", "urgi"):
            result = self.repo.posts({"q": [keyword], "field": ["all"]})
            match = next(item for item in result["items"] if item["tid"] == 4152176)
            self.assertFalse(match["search_excluded"], keyword)

    def test_market_hunter_reuses_all_search_modes(self):
        config = HunterManager.normalize({
            "q": "二手出售 物品10", "match": "all", "field": "title",
            "category": "已出", "interval": 2,
        })
        self.assertEqual(config["interval"], 10)
        self.assertEqual(HunterManager.normalize({"q": "物品10"})["interval"], 60)
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

    def test_market_hunters_are_independent_concurrent_tasks(self):
        hunter = HunterManager(self.repo, TaskManager(self.path))

        def wait_until_stopped(manager, task_id):
            with manager.lock:
                event = manager.jobs[task_id]["event"]
            event.wait(1)

        with patch.object(HunterManager, "_run", wait_until_stopped):
            first = hunter.start_task({"name": "任务 A", "q": "物品10"})
            second = hunter.start_task({"name": "任务 B", "q": "物品20"})
            snapshot = hunter.snapshot()
            self.assertEqual(snapshot["active_count"], 2)
            self.assertEqual({job["id"] for job in snapshot["tasks"]},
                             {first["id"], second["id"]})
            edited = hunter.edit_task(first["id"], {
                "name": "任务 A（已编辑）", "q": "物品20", "interval": 90,
            })
            self.assertEqual(edited["name"], "任务 A（已编辑）")
            self.assertEqual(edited["config"]["interval"], 90)
            self.assertEqual(edited["results"], [])
            self.assertTrue(hunter.stop(first["id"]))
            self.assertFalse(hunter.stop("missing-task"))
            self.assertFalse(hunter.jobs[second["id"]]["event"].is_set())
            hunter.stop()

    def test_market_hunter_tasks_persist_resume_immediately_and_delete(self):
        resumed = []

        def wait_until_stopped(manager, task_id):
            resumed.append(task_id)
            with manager.lock:
                event = manager.jobs[task_id]["event"]
            event.wait(1)

        with patch.object(HunterManager, "_run", wait_until_stopped):
            first_manager = HunterManager(self.repo, TaskManager(self.path))
            created = first_manager.start_task({"name": "持久任务", "q": "物品10"})
            first_manager.close()

            resumed.clear()
            second_manager = HunterManager(self.repo, TaskManager(self.path))
            for _ in range(20):
                if resumed:
                    break
                threading.Event().wait(0.01)
            self.assertEqual(resumed, [created["id"]])
            restored = second_manager.snapshot()["tasks"][0]
            self.assertTrue(restored["active"])
            self.assertIn("立即同步", restored["message"])
            self.assertTrue(second_manager.delete_task(created["id"]))
            self.assertEqual(second_manager.snapshot()["tasks"], [])

            third_manager = HunterManager(self.repo, TaskManager(self.path))
            self.assertEqual(third_manager.snapshot()["tasks"], [])

    def test_summary_quantifies_status_and_price(self):
        result = self.repo.summary({"days": ["3650"]})
        self.assertEqual(result["kpis"]["posts"], 2)
        self.assertEqual(result["kpis"]["sold"], 1)
        self.assertEqual(result["kpis"]["median_price"], 300.0)

    def test_transaction_words_are_not_item_hot_terms(self):
        store = Store(self.path)
        post = sample(40, "2026-07-20T14:00:00+00:00")
        post["items"] = ["明盘 已出 自提 几乎全新 全新 您的设备不支持视 频标签 配件"]
        store.save_result(post["tid"], "matched", post)
        store.close()
        terms = {item["name"] for item in self.repo.summary({"days": ["3650"]})["hot_terms"]}
        self.assertTrue({"明盘", "已出", "自提", "几乎全新", "全新",
                         "您的设备不支持视", "频标签"}.isdisjoint(terms))
        self.assertIn("配件", terms)

    def test_update_specific_post_status_persists_all_views(self):
        updated = self.repo.update_post_status(20, "已出")
        self.assertEqual(updated["category"], "已出")
        self.assertTrue(updated["is_sold"])
        self.assertEqual(self.repo.post(20)["category"], "已出")
        self.assertEqual(self.repo.summary({"days": ["3650"]})["kpis"]["sold"], 2)

        monthly = self.path.parent / "data" / "archive" / "2026" / "07" / "market.db"
        root_db = self.repo.connect()
        try:
            root_status = root_db.execute("SELECT category FROM posts WHERE tid=20").fetchone()[0]
        finally:
            root_db.close()
        monthly_db = sqlite3.connect(monthly)
        try:
            monthly_status = monthly_db.execute(
                "SELECT category FROM posts WHERE tid=20"
            ).fetchone()[0]
        finally:
            monthly_db.close()
        self.assertEqual((root_status, monthly_status), ("已出", "已出"))

    def test_update_specific_post_status_validates_input_and_missing_post(self):
        with self.assertRaisesRegex(ValueError, "交易状态无效"):
            self.repo.update_post_status(20, "已删除")
        self.assertIsNone(self.repo.update_post_status(999999, "已出"))

    def test_refresh_specific_post_reloads_status_from_source(self):
        refreshed = sample(20, "2026-07-19T10:00:00+00:00", "已出")
        refreshed["item_details"] = [{
            "index": 1, "name": "物品20", "text": "物品20 已出",
            "status": "已出", "prices": [],
        }]
        with patch("web_app.Crawler.fetch_thread", return_value=("matched", refreshed)):
            result = self.repo.refresh_post(20)
        self.assertEqual(result["previous_status"], "出售")
        self.assertEqual(result["current_status"], "已出")
        self.assertTrue(result["status_changed"])
        self.assertEqual(self.repo.post(20)["category"], "已出")

    def test_opening_post_lazily_loads_media_and_comments(self):
        hydrated = sample(10, "2026-07-20T10:00:00+00:00", "已出", ["300"])
        hydrated.update(
            media_loaded=True,
            comments_loaded=True,
            images=["http://pic.example/a.jpg"],
            comments=[{"content": "已出", "content_text": "已出"}],
        )
        with patch("web_app.Crawler.fetch_thread", return_value=("matched", hydrated)) as fetch:
            result = self.repo.post(10, hydrate=True)
        fetch.assert_called_once_with(10)
        self.assertTrue(result["media_loaded"])
        self.assertTrue(result["comments_loaded"])
        self.assertEqual(result["images"], ["http://pic.example/a.jpg"])

    def test_refresh_fetches_only_selected_tids_discovered_by_forum_list(self):
        store = Store(self.path)
        store.set_meta("next_tid", 100)
        manager = TaskManager(self.path)
        starts = []

        def fake_process(tids, options, frontier=False):
            starts.extend(tids)
            self.assertFalse(options["comments"])
            self.assertFalse(options["media"])
            return {tid: "matched" for tid in tids}, {}

        manager._process_range = fake_process
        forum_rows = [
            {"tid": "108", "fid": "176", "typeid": "102", "is_top": -1,
             "create_time": "200", "last_reply_time": "200"},
            {"tid": "107", "fid": "176", "typeid": "104", "is_top": -1,
             "create_time": "199", "last_reply_time": "199"},
            {"tid": "999", "fid": "176", "typeid": "102", "is_top": 1,
             "create_time": "198", "last_reply_time": "198"},
        ]
        with patch("web_app.Crawler.fetch_forum_page", return_value=forum_rows):
            cursor = manager._catch_up({"batch": 8}, store)
        self.assertEqual(cursor, 109)
        self.assertEqual(starts, [108])
        self.assertEqual(store.get_meta("last_recorded_id"), "108")
        status = store.db.execute(
            "SELECT status FROM scan_state WHERE tid=107"
        ).fetchone()[0]
        self.assertEqual(status, "skipped_type:求购")
        store.close()

    def test_selecting_other_type_backfills_legacy_skipped_titles(self):
        store = Store(self.path)
        store.save_result(90, "skipped_title", None)
        store.set_meta("next_tid", 109)
        manager = TaskManager(self.path)
        processed = []

        def fake_process(tids, options, frontier=False):
            processed.extend(tids)
            return {tid: "matched" for tid in tids}, {}

        manager._process_range = fake_process
        forum_rows = [
            {"tid": "108", "fid": "176", "typeid": "102", "is_top": -1,
             "create_time": "200", "last_reply_time": "200"},
            {"tid": "90", "fid": "176", "typeid": "104", "is_top": -1,
             "create_time": "100", "last_reply_time": "100"},
        ]
        with patch("web_app.Crawler.fetch_forum_page", return_value=forum_rows):
            manager._catch_up({"post_types": ["求购"], "batch": 8}, store)
        self.assertIn(90, processed)
        self.assertEqual(store.get_meta("post_type_backfill_version"), "2")
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
        self.assertEqual(latest["fear_index"], 50.0)
        self.assertEqual(latest["fear_level"], "中性")
        self.assertEqual(latest["fear_confidence"], 4.7)
        self.assertEqual(latest["market_index"], 50.0)
        self.assertEqual(latest["market_level"], "平衡")
        self.assertEqual(latest["fear_baseline_days"], 1)
        self.assertEqual(set(latest["fear_components"]), {
            "liquidity_pressure", "supply_pressure", "price_pressure", "activity_shock",
        })

    def test_market_indices_use_rolling_baseline_and_detect_stress_trend(self):
        def market_day(day, posts=20, selling=12, sold=6, wanted=2,
                       sold_rate=33.3, price_change=0.0):
            return {
                "day": f"2026-07-{day:02d}", "posts": posts, "selling": selling,
                "sold": sold, "wanted": wanted, "sold_rate": sold_rate,
                "price_samples": 10, "median_price_change_pct": price_change,
            }

        rows = [market_day(day) for day in range(1, 15)]
        rows.append(market_day(
            15, posts=40, selling=38, sold=2, wanted=0,
            sold_rate=5.0, price_change=-20.0,
        ))
        enriched = enrich_market_indices(rows)
        stable = enriched[13]
        stressed = enriched[14]
        self.assertAlmostEqual(stable["fear_index"], 50.0, delta=0.1)
        self.assertGreater(stressed["fear_index"], 80)
        self.assertEqual(stressed["fear_level"], "极度恐惧")
        self.assertEqual(stressed["market_level"], "低迷")
        self.assertEqual(stressed["fear_trend"], "升温")
        self.assertEqual(stressed["market_trend"], "走弱")
        self.assertEqual(stressed["fear_baseline_days"], 14)
        self.assertEqual(stressed["fear_confidence"], 100.0)
        self.assertGreater(stressed["fear_ma7"], stable["fear_ma7"])

    def test_unmarked_sold_status_cannot_trigger_fear_by_itself(self):
        rows = [{
            "day": f"2026-07-{day:02d}", "posts": 20, "selling": 14,
            "sold": 6, "wanted": 0, "sold_rate": 30.0,
            "price_samples": 10, "median_price_change_pct": 0.0,
        } for day in range(1, 15)]
        rows.append({
            **rows[-1], "day": "2026-07-15", "selling": 20,
            "sold": 0, "sold_rate": 0.0,
        })
        latest = enrich_market_indices(rows)[-1]
        self.assertLess(latest["fear_index"], 60)
        self.assertNotIn(latest["fear_level"], {"恐惧", "极度恐惧"})


if __name__ == "__main__":
    unittest.main()
