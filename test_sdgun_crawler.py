import unittest
import json

from sdgun_crawler import (
    Crawler,
    Settings,
    classify,
    category_from_items,
    clean_rich_content,
    clean_rich_text,
    extract_item_details,
    extract_prices,
    keyword_in_raw_page,
    unique_images,
)


class ParserTests(unittest.TestCase):
    def test_content_only_images_and_links(self):
        text, images, links = clean_rich_text(
            '<p>正文 <a href="https://m.tb.cn/a">闲鱼</a></p>'
            '<img data-original="http://pic.example/a.jpg">'
        )
        self.assertEqual(text, "正文 闲鱼")
        self.assertEqual(images, ["http://pic.example/a.jpg"])
        self.assertEqual(links, ["https://m.tb.cn/a"])

    def test_image_variants_are_deduplicated(self):
        self.assertEqual(unique_images([
            "http://pic.example/a.jpg",
            "http://pic.example/a.jpg?imageView2/3/w/600",
        ]), ["http://pic.example/a.jpg"])

    def test_video_and_nested_source_tags_are_parsed(self):
        text, images, links, videos, posters = clean_rich_content(
            '<p>演示视频</p>'
            '<video data-src="/media/demo.mp4" poster="/media/demo.jpg">'
            '<source src="https://cdn.example/demo.webm" type="video/webm">'
            '<source src="https://cdn.example/demo.webm">'
            '您的设备不支持视频标签'
            '</video>'
            '<source src="https://cdn.example/not-in-video.mp4">'
        )
        self.assertEqual(text, "演示视频")
        self.assertEqual(images, [])
        self.assertEqual(videos, ["/media/demo.mp4", "https://cdn.example/demo.webm"])
        self.assertEqual(links, videos)
        self.assertEqual(posters, ["/media/demo.jpg"])

    def test_video_posters_are_not_duplicated_as_post_images(self):
        crawler = Crawler(Settings(1, 0, False, 50, 1, "【二手出售】", ()))
        row = {
            "tid": 123,
            "create_time": 1784642106,
            "content": (
                '<video poster="http://pic.example/video-cover.jpg">'
                '<source src="http://pic.example/demo.mp4" type="video/mp4">'
                '</video><img src="http://pic.example/real-photo.jpg">'
            ),
            "pics": [
                "http://pic.example/video-cover.jpg",
                "http://pic.example/real-photo.jpg",
            ],
        }
        page = '<title>【二手出售】视频帖</title><script>var row=' + json.dumps(row) + ';</script>'
        crawler._request = lambda url: page.encode("utf-8")
        status, post = crawler.fetch_thread(123)
        self.assertEqual(status, "matched")
        self.assertEqual(post["videos"], ["http://pic.example/demo.mp4"])
        self.assertEqual(post["images"], ["http://pic.example/real-photo.jpg"])

    def test_prices_require_currency_context(self):
        text = "PEQ-15，2026-02-25，售价1380元，另一个¥280，4件38000"
        self.assertEqual(extract_prices(text), ["1380", "280"])

    def test_sold_has_priority_but_question_does_not(self):
        self.assertEqual(classify("【二手出售】配件", "100元", [{"content_text": "已出"}]), "已出")
        self.assertEqual(classify("【二手出售】配件", "100元", [{"content_text": "已出了吗？"}]), "出售")
        self.assertEqual(classify("【二手出售】配件", "所以900出了", []), "出售")

    def test_numbered_and_blank_lines_are_classified_per_item(self):
        body = "1. 锦明波箱 300元 已出\n\n2、红点 200元\n\n3）求购握把 100元"
        details = extract_item_details("【二手出售】多个配件", body, [])
        self.assertEqual([x["status"] for x in details], ["已出", "出售", "求购"])
        self.assertEqual(category_from_items(details, "出售"), "出售+求购")

    def test_partially_sold_without_wanted_is_not_mixed(self):
        details = [
            {"name": "红点", "status": "已出"},
            {"name": "握把", "status": "出售"},
        ]
        self.assertEqual(category_from_items(details, "出售"), "部分已出")

    def test_generic_sold_comment_does_not_sell_every_item(self):
        body = "1. 锦明波箱 300元\n\n2. 红点 200元"
        generic = extract_item_details("【二手出售】多个配件", body, [{"content_text": "已出"}])
        self.assertEqual([x["status"] for x in generic], ["出售", "出售"])
        specific = extract_item_details("【二手出售】多个配件", body, [{"content_text": "2号已出"}])
        self.assertEqual([x["status"] for x in specific], ["出售", "已出"])

    def test_one_priced_item_per_line_without_blank_lines(self):
        body = "创研星辰大海，明盘3000\n原厂精击4，价格400\n乐辉MPX，明盘800"
        details = extract_item_details("【二手出售】多件", body, [])
        self.assertEqual(len(details), 3)
        self.assertEqual([x["prices"] for x in details], [["3000"], ["400"], ["800"]])

    def test_region_code_sale_prefix_is_not_an_item(self):
        expected = ["任翔金属弹匣", "司骏尼龙弹匣", "10寸前鱼骨"]
        for region in ("0311", "020", "0769"):
            with self.subTest(region=region):
                details = extract_item_details(
                    f"【二手出售】{region}出 任翔金属弹匣 司骏尼龙弹匣 10寸前鱼骨",
                    "20 20 50 到付", [],
                )
                self.assertEqual([item["name"] for item in details], expected)

    def test_keyword_matches_json_unicode_escape(self):
        raw = r'{"content":"\u9526\u660e\u6ce2\u7bb1"}'.casefold()
        self.assertTrue(keyword_in_raw_page("锦明", raw))
        self.assertFalse(keyword_in_raw_page("不存在", raw))

    def test_non_market_page_only_exposes_second_precision_boundary_time(self):
        crawler = Crawler(Settings(1, 0, False, 50, 1, "【二手出售】", ()))
        crawler._request = lambda url: (
            '<title>【其他】普通帖子</title><script>var row={"create_time":"1784524303"};</script>'
        ).encode("utf-8")
        status, post = crawler.fetch_thread(123)
        self.assertEqual(status, "skipped_title")
        self.assertIsNone(post)
        self.assertEqual(crawler.local.last_create_time, 1784524303)

    def test_latest_forum_tid_ignores_pinned_threads(self):
        crawler = Crawler(Settings(1, 0, False, 50, 1, "【二手出售】", ()))
        crawler._request = lambda url: json.dumps({
            "success": True,
            "list": [
                {"tid": "9999999", "is_top": 1},
                {"tid": "4149040", "is_top": -1},
                {"tid": "4149051", "is_top": -1},
            ],
        }).encode("utf-8")
        self.assertEqual(crawler.fetch_latest_forum_tid(), 4149051)


if __name__ == "__main__":
    unittest.main()
