"""REST client for Kalshi's public trade-api v2 market listing endpoints.

These endpoints (series/markets) are read-only and require no authentication.

Kalshi's rate limiter is token-bucket based, not a hard request cap --
per its docs, a 429 costs no penalty or cooldown, the bucket just keeps
refilling and the next request succeeds once balance covers its cost. So
_get_with_retry below backs off and retries on 429 rather than raising, and
paginated calls sleep briefly between pages -- relevant here because a
single fetch_trades call on a busy near-expiry market, or a fetch_markets
scan over weeks of settled history, can run to dozens of pages back-to-back.

Also retries on plain connection failures (reset, timeout, DNS blip) --
distinct from 429s (those are the server explicitly saying "slow down";
these are transient network noise), but over a scan spanning hours and tens
of thousands of requests, both are eventually guaranteed to happen at least
once. Found live 2026-08-24: an unretried ConnectionResetError killed an
unattended overnight backtest.py run after ~900 of ~134,000 markets, with
no partial progress saved -- see backtest.py's checkpointing for the other
half of that fix.
"""

import time

import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def _get_with_retry(url: str, params: dict, max_retries: int = 8) -> requests.Response:
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=20)
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(2.0 * (attempt + 1))
                continue
            raise
        if response.status_code == 429 and attempt < max_retries - 1:
            time.sleep(2.0 * (attempt + 1))
            continue
        response.raise_for_status()  # last attempt's 429 (or other HTTP error) surfaces here
        return response


def fetch_series(category: str | None = None, limit: int = 200) -> list[dict]:
    """Lists market series (e.g. KXFED, KXBTCD), optionally filtered by category."""
    params = {"limit": limit}
    if category:
        params["category"] = category
    return _get_with_retry(f"{BASE_URL}/series", params).json().get("series", [])


def fetch_markets(
    series_ticker: str | None = None,
    status: str | None = "open",
    limit: int = 200,
    max_pages: int = 10,
    page_delay: float = 0.1,
) -> list[dict]:
    """Fetches markets, following the cursor until exhausted or max_pages is
    hit. max_pages=10 at limit=200 (the defaults) caps this at 2000 markets
    -- fine for "what's open now", but a status="settled" scan over a long
    lookback can need far more (a busy hourly series can settle ~200
    markets/hour, so a 30-day window needs on the order of 700+ pages) --
    raise both max_pages and, ideally, pair this with a cutoff-aware caller
    that stops once a page's results are already older than the window it
    cares about, rather than relying on max_pages alone.
    """
    markets: list[dict] = []
    cursor = None

    for page in range(max_pages):
        params = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor

        payload = _get_with_retry(f"{BASE_URL}/markets", params).json()

        markets.extend(payload.get("markets", []))
        cursor = payload.get("cursor")
        if not cursor:
            break
        if page_delay and page < max_pages - 1:
            time.sleep(page_delay)

    return markets


def fetch_orderbook(ticker: str, depth: int | None = None) -> dict:
    """Fetches the current order book (yes/no price levels) for a single market."""
    params = {}
    if depth:
        params["depth"] = depth
    return _get_with_retry(f"{BASE_URL}/markets/{ticker}/orderbook", params).json().get("orderbook_fp", {})


def fetch_trades(
    ticker: str,
    min_ts: int | None = None,
    max_ts: int | None = None,
    limit: int = 1000,
    max_pages: int = 20,
    page_delay: float = 0.1,
) -> list[dict]:
    """Fetches historical executed trades for a single market ticker (unlike
    fetch_orderbook, this IS real history -- the order book itself has no
    historical endpoint, but individual fills do). Each trade dict has
    yes_price_dollars/no_price_dollars, taker_side, count_fp, created_time
    (ISO-8601 UTC, variable-precision fractional seconds), trade_id.
    min_ts/max_ts are unix seconds, both optional. Follows the cursor until
    exhausted or max_pages is hit, pausing page_delay seconds between pages
    (a market can print hundreds of trades in its last few seconds, which
    without a delay would fire a burst of back-to-back page requests).
    """
    trades: list[dict] = []
    cursor = None

    for page in range(max_pages):
        params = {"ticker": ticker, "limit": limit}
        if min_ts is not None:
            params["min_ts"] = int(min_ts)
        if max_ts is not None:
            params["max_ts"] = int(max_ts)
        if cursor:
            params["cursor"] = cursor

        payload = _get_with_retry(f"{BASE_URL}/markets/trades", params).json()

        trades.extend(payload.get("trades", []))
        cursor = payload.get("cursor")
        if not cursor:
            break
        if page_delay and page < max_pages - 1:
            time.sleep(page_delay)

    return trades


if __name__ == "__main__":
    fed_markets = fetch_markets(series_ticker="KXFED")
    print(f"Fetched {len(fed_markets)} KXFED markets")
    for market in fed_markets[:5]:
        print(market["ticker"], "|", market["title"])

    if fed_markets:
        orderbook = fetch_orderbook(fed_markets[0]["ticker"])
        print(f"Orderbook for {fed_markets[0]['ticker']}: {orderbook}")
