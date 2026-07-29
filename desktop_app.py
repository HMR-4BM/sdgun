#!/usr/bin/env python3
"""Desktop host for the SDGun dashboard.

The HTTP server lives in this process and is stopped when the WebView window
closes.  Importing pywebview lazily keeps the normal web/server mode free of
third-party dependencies.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

from web_app import DashboardServer


def application_dir() -> Path:
    """Return the portable data directory beside the script or packaged exe."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def main() -> int:
    try:
        import webview
    except ImportError:
        print("缺少桌面组件，请运行: python -m pip install pywebview")
        return 1

    db_path = application_dir() / "data" / "main" / "sdgun_market.db"
    server = DashboardServer(("127.0.0.1", 0), db_path)
    port = server.server_address[1]
    server_thread = threading.Thread(
        target=server.serve_forever,
        name="sdgun-dashboard-server",
        daemon=True,
    )
    server_thread.start()

    try:
        webview.create_window(
            "SDGun 二手交易市场工具",
            f"http://127.0.0.1:{port}",
            width=1440,
            height=900,
            min_size=(960, 640),
        )
        webview.start()
    finally:
        # Stop request handling first, then allow managed jobs to save/exit.
        server.shutdown()
        server.hunter.close()
        server.tasks.stop()
        server.server_close()
        server_thread.join(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
