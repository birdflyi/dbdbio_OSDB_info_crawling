"""Download complete HTML responses with bounded waits and visible retries."""

import http.client
import time
import urllib.error
import urllib.request


DEFAULT_TIMEOUT = 30
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_MAX_READ_SECONDS = 120
DEFAULT_RETRY_BACKOFF = 2
RETRYABLE_HTTP_STATUS = {408, 429, 500, 502, 503, 504}


class FetchError(RuntimeError):
    """A page could not be downloaded completely."""


def fetch_html(url, headers, *, timeout=DEFAULT_TIMEOUT,
               max_attempts=DEFAULT_MAX_ATTEMPTS,
               max_read_seconds=DEFAULT_MAX_READ_SECONDS,
               retry_backoff=DEFAULT_RETRY_BACKOFF):
    """Return (HTML, resolved URL); discard partial bodies before retrying.

    timeout limits blocking socket operations. The body budget is checked
    between reads, so a pending read can exceed it by up to timeout seconds.
    """
    if timeout <= 0 or max_read_seconds <= 0 or max_attempts < 1 or retry_backoff < 0:
        raise ValueError("Timeouts and max_attempts must be positive; retry_backoff must be nonnegative.")

    request_headers = dict(headers or {})
    # We decode HTML ourselves, so request an uncompressed response.
    request_headers = {key: value for key, value in request_headers.items()
                       if key.lower() != "accept-encoding"}
    request_headers["Accept-Encoding"] = "identity"

    for attempt in range(1, max_attempts + 1):
        started = time.monotonic()
        print(f"[HTTP {attempt}/{max_attempts}] GET {url} (timeout={timeout}s)", flush=True)
        try:
            request = urllib.request.Request(url, headers=request_headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                chunks = []
                received = 0
                last_progress = started
                while True:
                    if time.monotonic() - started > max_read_seconds:
                        raise TimeoutError(f"Response exceeded the {max_read_seconds}s read budget")
                    chunk = response.read1(64 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    received += len(chunk)
                    now = time.monotonic()
                    if now - last_progress >= 10:
                        print(f"[HTTP] Received {received:,} bytes from {url} ({now - started:.1f}s)", flush=True)
                        last_progress = now

                # read1() can return EOF without raising for a truncated
                # Content-Length response. Chunk framing is checked by urllib.
                content_length = response.headers.get("Content-Length")
                if content_length is not None and not response.chunked:
                    expected = int(content_length)
                    if received != expected:
                        raise http.client.IncompleteRead(b"", max(0, expected - received))
                if not received:
                    raise http.client.IncompleteRead(b"", 1)
                body = b"".join(chunks)
                encoding = response.headers.get_content_charset() or "utf-8"
                html = body.decode(encoding, errors="replace")
                resolved_url = response.geturl()
            print(f"[HTTP] Complete: {received:,} bytes in {time.monotonic() - started:.1f}s", flush=True)
            return html, resolved_url
        except urllib.error.HTTPError as exc:
            retryable = exc.code in RETRYABLE_HTTP_STATUS
            exc.close()
            error = exc
        except (urllib.error.URLError, http.client.HTTPException, OSError) as exc:
            retryable = True
            error = exc

        reason = f"{type(error).__name__}: {error}"
        if not retryable or attempt == max_attempts:
            raise FetchError(f"Failed to download {url} after {attempt} attempt(s): {reason}") from error
        delay = retry_backoff * (2 ** (attempt - 1))
        print(f"[HTTP] {reason}; retrying in {delay}s (partial response discarded)", flush=True)
        time.sleep(delay)


def fetch_options(options):
    """Pass the same optional network settings through all crawler entry points."""
    return {key: options[key] for key in
            ("timeout", "max_attempts", "max_read_seconds", "retry_backoff")
            if key in options}
