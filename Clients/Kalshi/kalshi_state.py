"""Cross-process coordination for callers that share one Kalshi account.

All three trading experiments under expirments/ (resolution_alpha,
btc_implied_prob, golf_field_alpha) authenticate with the same
KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH -- there is exactly one Kalshi
account and balance behind however many of these processes happen to be
running. Each experiment's own OrderManager used to build its own bare
KalshiTradingClient and call get_balance()/place_order() directly, which is
fine for exactly one process but breaks down the moment a second one is
running against the same account at the same time:

  - Kelly sizing (each experiment's _kelly_contracts) reads "the account
    balance" and sizes as if it owns the whole thing. Two experiments
    signaling in the same few seconds both size off the same starting
    balance, so the combined intended stake can run well past what either
    strategy's own KELLY_FRACTION was tuned to risk.
  - Every experiment fires its own uncoordinated GET/POST calls at Kalshi's
    API. N experiments polling on their own schedules add up to N times the
    request rate against the one account -- with no shared view of that,
    nothing backs off if that starts tripping rate limits.

KalshiStateManager fixes both by giving every experiment process on this
machine a shared, file-backed (SQLite) view of two things: a request
schedule (so concurrent processes' Kalshi API calls interleave into one
system-wide rate instead of stacking), and a capital allocation (so Kelly
sizing can be told "you get 40% of the account", not "you get all of it").
It deliberately does NOT unify local position tracking -- see
README.md's "What this does not do".

See README.md (this directory) for the full write-up and a worked example
of wiring this into a new experiment.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from pathlib import Path

from live_execution import KalshiTradingClient

logger = logging.getLogger("kalshi_state")

_DEFAULT_DB_PATH = Path(__file__).resolve().parent / "state.db"
_DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.2
_DEFAULT_BALANCE_CACHE_SECONDS = 2.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rate_limit (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next_slot REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS balance_cache (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload_json TEXT NOT NULL,
    fetched_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS order_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment TEXT NOT NULL,
    ticker TEXT NOT NULL,
    favored_side TEXT NOT NULL,
    api_side TEXT NOT NULL,
    count TEXT NOT NULL,
    price TEXT NOT NULL,
    dry_run INTEGER NOT NULL,
    response_json TEXT,
    created_at REAL NOT NULL
);
"""


def _float_env(name: str, default: float) -> float:
    val = os.environ.get(name)
    return float(val) if val is not None else default


class _ThrottledClient:
    """Wraps one process's KalshiTradingClient so every call reserves its
    slot in the shared, cross-process request schedule first (see
    KalshiStateManager._throttle). Same method surface as
    live_execution.KalshiTradingClient -- a drop-in replacement anywhere that
    held a bare KalshiTradingClient before.
    """

    def __init__(self, state: "KalshiStateManager"):
        self._state = state
        self._client = state._raw_client

    def get_balance(self) -> dict:
        self._state._throttle()
        return self._client.get_balance()

    def get_positions(self, ticker: str | None = None, event_ticker: str | None = None) -> dict:
        self._state._throttle()
        return self._client.get_positions(ticker=ticker, event_ticker=event_ticker)

    def get_orders(self, status: str | None = None, ticker: str | None = None) -> dict:
        self._state._throttle()
        return self._client.get_orders(status=status, ticker=ticker)

    def place_order(self, **kwargs) -> dict:
        self._state._throttle()
        return self._client.place_order(**kwargs)

    def cancel_order(self, order_id: str) -> dict:
        self._state._throttle()
        return self._client.cancel_order(order_id)


class KalshiStateManager:
    """Per-experiment handle onto the shared, cross-process Kalshi state.

    One instance per experiment process (typically built once in that
    experiment's OrderManager.__init__ and held for the process lifetime).
    `experiment` is a short, stable name (e.g. "resolution_alpha") -- it
    tags every row this instance writes to the shared order_log, and has no
    other effect (the rate limiter and balance cache are shared across ALL
    experiments by design, not partitioned per name).

    `capital_fraction` / `capital_cap_dollars` control apply_capital_allocation
    below -- see that method and README.md's "Capital allocation" section.
    Both default to "no limit" (fraction=1.0, cap=None), so an experiment
    that never sets these behaves exactly as if this class didn't exist.
    """

    def __init__(
        self,
        experiment: str,
        *,
        dry_run: bool,
        capital_fraction: float | None = 1.0,
        capital_cap_dollars: float | None = None,
        db_path: str | Path | None = None,
        min_request_interval_seconds: float | None = None,
        balance_cache_seconds: float | None = None,
    ):
        self.experiment = experiment
        self.dry_run = dry_run
        self.capital_fraction = capital_fraction
        self.capital_cap_dollars = capital_cap_dollars
        self.db_path = Path(db_path or os.environ.get("KALSHI_STATE_DB_PATH") or _DEFAULT_DB_PATH)
        self.min_request_interval_seconds = (
            min_request_interval_seconds
            if min_request_interval_seconds is not None
            else _float_env("KALSHI_STATE_MIN_REQUEST_INTERVAL_SECONDS", _DEFAULT_MIN_REQUEST_INTERVAL_SECONDS)
        )
        self.balance_cache_seconds = (
            balance_cache_seconds
            if balance_cache_seconds is not None
            else _float_env("KALSHI_STATE_BALANCE_CACHE_SECONDS", _DEFAULT_BALANCE_CACHE_SECONDS)
        )

        # Same posture as every experiment's existing OrderManager: no real
        # client is ever constructed in dry-run, so dry-run never requires
        # credentials to be configured.
        self._raw_client = None if dry_run else KalshiTradingClient()

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None (autocommit) hands transaction boundaries
        # entirely to explicit BEGIN/COMMIT below -- _throttle relies on
        # BEGIN IMMEDIATE taking the write lock atomically with its read, and
        # the sqlite3 module's own implicit-transaction behavior (silently
        # opening one before the first DML statement) would otherwise start
        # it a statement too late for that.
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _throttle(self) -> None:
        """Reserves this call's send slot in the shared request schedule and
        sleeps until it arrives. Every live Kalshi API call from every
        experiment on this machine funnels through here (via get_client()),
        so however many processes are polling concurrently, they add up to
        one system-wide request cadence (one call at most every
        min_request_interval_seconds) instead of each independently assuming
        the whole request budget is theirs. The DB write that reserves a
        slot is a single short transaction -- the actual HTTP call happens
        after, without holding any lock, so one process waiting on Kalshi's
        network doesn't block another's turn to reserve its own slot.
        """
        now = time.time()
        conn = self._connect()
        try:
            # BEGIN IMMEDIATE takes the write lock up front, before the SELECT
            # -- a bare SELECT takes no lock at all, so without this two
            # processes calling _throttle at nearly the same moment could
            # both read the same next_slot and both compute the same
            # send_at, colliding onto one reserved slot instead of getting
            # two serialized ones.
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT next_slot FROM rate_limit WHERE id = 1").fetchone()
            next_slot = row[0] if row else 0.0
            send_at = max(now, next_slot)
            conn.execute(
                "INSERT INTO rate_limit (id, next_slot) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET next_slot = excluded.next_slot",
                (send_at + self.min_request_interval_seconds,),
            )
            conn.commit()
        finally:
            conn.close()
        delay = send_at - time.time()
        if delay > 0:
            time.sleep(delay)

    def get_client(self) -> _ThrottledClient:
        """A KalshiTradingClient-alike whose calls are paced against the
        shared request schedule. Only valid when dry_run is False (same
        restriction as the raw client this wraps -- there's nothing to call
        otherwise).
        """
        if self._raw_client is None:
            raise RuntimeError(f"{self.experiment}: no Kalshi client in dry-run mode")
        return _ThrottledClient(self)

    def get_balance(self, *, force_refresh: bool = False) -> dict:
        """The account's raw /portfolio/balance payload, cached for
        balance_cache_seconds and shared across every experiment process on
        this machine. Several experiments polling on similar cadences
        collapse into one real request per cache window instead of one each.
        Only valid when dry_run is False.
        """
        if not force_refresh:
            cached = self._read_balance_cache()
            if cached is not None:
                return cached
        payload = self.get_client().get_balance()
        self._write_balance_cache(payload)
        return payload

    def _read_balance_cache(self) -> dict | None:
        with self._connect() as conn:
            row = conn.execute("SELECT payload_json, fetched_at FROM balance_cache WHERE id = 1").fetchone()
        if row is None:
            return None
        payload_json, fetched_at = row
        if time.time() - fetched_at > self.balance_cache_seconds:
            return None
        return json.loads(payload_json)

    def _write_balance_cache(self, payload: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO balance_cache (id, payload_json, fetched_at) VALUES (1, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET payload_json = excluded.payload_json, "
                "fetched_at = excluded.fetched_at",
                (json.dumps(payload), time.time()),
            )

    def apply_capital_allocation(self, total_balance_dollars: float) -> float:
        """Scales a real total-account balance down to what THIS experiment
        may treat as its own bankroll for sizing. Defaults (fraction=1.0,
        cap=None) are a no-op -- an experiment only sees a reduced number
        once its config explicitly sets RESOLUTION_ALPHA_CAPITAL_FRACTION
        (or the btc_implied_prob/golf_field_alpha equivalent) below 1.0, or a
        dollar cap. See README.md's "Capital allocation" section for how to
        pick a value when running more than one experiment live at once.
        """
        allocated = total_balance_dollars
        if self.capital_fraction is not None:
            allocated *= self.capital_fraction
        if self.capital_cap_dollars is not None:
            allocated = min(allocated, self.capital_cap_dollars)
        return allocated

    def record_order(
        self,
        *,
        ticker: str,
        favored_side: str,
        api_side: str,
        count,
        price,
        dry_run: bool,
        response: dict,
    ) -> None:
        """Appends one row to the shared order_log -- purely an audit trail
        (every order any experiment placed or simulated, in one place, for
        debugging cross-experiment activity) with no effect on control flow.
        Never raises: a logging failure must not be able to block or fail a
        real trade.
        """
        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO order_log "
                    "(experiment, ticker, favored_side, api_side, count, price, dry_run, response_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        self.experiment, ticker, favored_side, api_side, str(count), str(price),
                        int(dry_run), json.dumps(response, default=str), time.time(),
                    ),
                )
        except Exception:
            logger.exception("kalshi_state: failed to record order for %s (non-fatal)", ticker)
