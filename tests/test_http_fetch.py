"""Network regressions run against a local server, never against production data."""

import contextlib
import http.server
import io
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.request

from script.crawling_OSDB_list import crawling_OSDB_list_soup
from script.crawling_OSDB_infos import crawling_OSDB_infos_soup, inspect_dbms_info_soup
from script.http_fetch import FetchError, fetch_html


BROWSE_HTML = b'''<html><form id="mainsearch"><table id="results-table">
<tr class="browse-row"><td><a class="r-name" href="/db/example">Example</a></td></tr>
</table></form></html>'''


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    counts = {}

    def log_message(self, *args):
        pass

    def do_GET(self):
        path = self.path
        self.counts[path] = self.counts.get(path, 0) + 1
        attempt = self.counts[path]
        if path == "/missing":
            self.send_error(404)
            return
        if path == "/busy" and attempt == 1:
            self.send_error(503)
            return
        if path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/ok")
            self.end_headers()
            return

        body = BROWSE_HTML if path in ("/browse", "/broken-browse") else b"<html>complete</html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        chunked = path in ("/chunks", "/midchunk", "/broken-browse", "/trickle")
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
        else:
            size = len(body) + (100 if path == "/short" and attempt == 1 else 0)
            self.send_header("Content-Length", str(size))
        self.end_headers()
        try:
            if path == "/stall":
                time.sleep(0.2)
            if path == "/trickle":
                for _ in range(20):
                    self.wfile.write(b"1\r\nx\r\n")
                    self.wfile.flush()
                    time.sleep(0.015)
                self.wfile.write(b"0\r\n\r\n")
            elif path == "/midchunk" and attempt == 1:
                self.wfile.write(b"100\r\npartial")
            elif chunked:
                self.wfile.write(f"{len(body):x}\r\n".encode() + body + b"\r\n")
                if path != "/broken-browse" and attempt > 1:
                    self.wfile.write(b"0\r\n\r\n")
                # First /chunks response loses the final chunk marker.
            else:
                self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        self.close_connection = True


class DownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.root = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        Handler.counts = {}
        # User/system proxies must not intercept the local fault-injection server.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.urlopen_patch = patch("script.http_fetch.urllib.request.urlopen", side_effect=opener.open)
        self.urlopen_patch.start()
        self.output = io.StringIO()
        self.capture = contextlib.redirect_stdout(self.output)
        self.capture.__enter__()

    def tearDown(self):
        self.capture.__exit__(None, None, None)
        self.urlopen_patch.stop()

    def fetch(self, path, **options):
        return fetch_html(self.root + path, {}, retry_backoff=0, **options)

    def test_chunked_disconnect_is_retried_from_scratch(self):
        html, _ = self.fetch("/chunks")
        self.assertEqual(html, "<html>complete</html>")
        self.assertEqual(Handler.counts["/chunks"], 2)
        self.assertIn("partial response discarded", self.output.getvalue())

    def test_short_content_length_is_retried(self):
        self.assertEqual(self.fetch("/short")[0], "<html>complete</html>")
        self.assertEqual(Handler.counts["/short"], 2)

    def test_interruption_inside_chunk_is_retried(self):
        self.assertEqual(self.fetch("/midchunk")[0], "<html>complete</html>")
        self.assertEqual(Handler.counts["/midchunk"], 2)

    def test_temporary_http_error_is_retried(self):
        self.assertEqual(self.fetch("/busy")[0], "<html>complete</html>")
        self.assertEqual(Handler.counts["/busy"], 2)

    def test_404_is_not_retried(self):
        with self.assertRaisesRegex(FetchError, "HTTP Error 404"):
            self.fetch("/missing")
        self.assertEqual(Handler.counts["/missing"], 1)

    def test_socket_timeout_has_finite_retries(self):
        with self.assertRaises(FetchError):
            self.fetch("/stall", timeout=0.03, max_attempts=2)
        self.assertEqual(Handler.counts["/stall"], 2)

    def test_trickling_response_exceeds_read_budget(self):
        with self.assertRaisesRegex(FetchError, "read budget"):
            self.fetch("/trickle", timeout=0.2, max_read_seconds=0.04, max_attempts=1)

    def test_resolved_url_is_preserved(self):
        _, resolved_url = self.fetch("/redirect")
        self.assertEqual(resolved_url, self.root + "/ok")

    def test_complete_browse_page_is_saved(self):
        with tempfile.TemporaryDirectory() as folder:
            dest = Path(folder) / "list.csv"
            crawling_OSDB_list_soup(self.root + "/browse", {},
                                   {"main_contents": ["form", {"id": "mainsearch"}]},
                                   dest, url_root=self.root, retry_backoff=0)
            self.assertIn(self.root + "/db/example", dest.read_text(encoding="utf-8"))

    def test_failed_download_or_invalid_page_preserves_existing_csv(self):
        with tempfile.TemporaryDirectory() as folder:
            dest = Path(folder) / "list.csv"
            dest.write_text("existing monthly data", encoding="utf-8")
            for path, error in (("/broken-browse", FetchError), ("/ok", ValueError)):
                with self.subTest(path=path), self.assertRaises(error):
                    crawling_OSDB_list_soup(self.root + path, {},
                                           {"main_contents": ["form", {"id": "mainsearch"}]},
                                           dest, max_attempts=2, retry_backoff=0)
                self.assertEqual(dest.read_text(encoding="utf-8"), "existing monthly data")

    def test_empty_detail_page_is_reported_as_failure(self):
        result = inspect_dbms_info_soup(self.root + "/ok", {}, retry_backoff=0)
        self.assertFalse(result["ok"])
        self.assertIn("No DBMS detail content", result["error"])

    def test_keyboard_interrupt_is_not_swallowed(self):
        with patch("script.crawling_OSDB_infos.fetch_dbms_info_soup", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                inspect_dbms_info_soup(self.root + "/ok", {})

    def test_detail_download_failure_is_recorded_and_next_record_continues(self):
        import pandas as pd

        with tempfile.TemporaryDirectory() as folder:
            dest = Path(folder) / "details.csv"
            records = pd.DataFrame({"db_names": ["Missing", "Empty"],
                                    "urls": [self.root + "/missing", self.root + "/ok"]})
            with patch("script.crawling_OSDB_infos.time.sleep"):
                crawling_OSDB_infos_soup(records, [{}],
                                        {"main_contents": ["form", {"id": "mainsearch"}]},
                                        dest, max_attempts=1, retry_backoff=0)
            result = pd.read_csv(dest)
            self.assertEqual(result["Name"].tolist(), ["missing", "ok"])
            self.assertTrue(result["Crawl Error"].str.len().gt(0).all())


if __name__ == "__main__":
    unittest.main()
