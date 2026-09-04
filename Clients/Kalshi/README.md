# kalshi_state.py

Shared, cross-process coordination for callers -- currently the three
trading experiments under `expirments/` -- that all trade the same Kalshi
account. Every experiment authenticates with the same `KALSHI_API_KEY_ID` /
`KALSHI_PRIVATE_KEY_PATH` -- there is exactly one account and one real
balance behind however many of resolution_alpha, btc_implied_prob, and
golf_field_alpha happen to be running at once. `kalshi_state.py` lives here
rather than under `expirments/` because it's really an extension of this
directory's own trading client (`live_execution.py`), not something
specific to any one experiment.

## The problem this solves

Each experiment's `OrderManager` used to build its own bare
`KalshiTradingClient` and call `get_balance()` / `place_order()` directly.
That's correct for exactly one process. Run a second experiment against the
same account at the same time and two things break:

1. **Kelly sizing reads the full account balance.** Every experiment's
   `_kelly_contracts` (or equivalent) sizes a position off "the account
   balance" as if it owns the whole thing. If resolution_alpha and
   btc_implied_prob both signal within the same few seconds, both size
   against the same starting balance -- the combined intended stake can run
   well past what either strategy's own `KELLY_FRACTION` was tuned to risk,
   even though each one individually looks correctly sized.
2. **Nothing paces API calls across processes.** Each experiment fires its
   own GET/POST calls at Kalshi's API on its own schedule. N experiments
   polling independently add up to N times the request rate against the one
   account, with no shared view of that -- nothing backs off if it starts
   tripping rate limits.

`KalshiStateManager` gives every experiment process on the machine a shared,
file-backed view (a small SQLite DB, `state.db` in this directory --
gitignored, created on first use) of a request schedule and a capital
allocation, so both problems become configuration instead of surprises.

## What it provides

- **Cross-process request pacing** (`get_client()`): every live Kalshi API
  call from every experiment funnels through one shared schedule, so
  concurrent processes' calls interleave into a single system-wide rate
  (`KALSHI_STATE_MIN_REQUEST_INTERVAL_SECONDS`, default 0.2s between any two
  calls, from any experiment) instead of stacking. Implemented as an atomic
  "reserve the next slot, then sleep until it arrives" against the shared DB
  -- the DB lock is only held for the reservation, not for the network call
  itself, so one process waiting on Kalshi doesn't block another's turn to
  reserve its own slot.
- **Capital allocation** (`apply_capital_allocation`, used by each
  experiment's `OrderManager.get_balance_dollars`): scales the real account
  balance down to what one experiment is allowed to treat as its own
  bankroll for sizing, via a fraction of the account and/or a hard dollar
  cap. See "Capital allocation" below.
- **Shared balance cache** (`get_balance`): the real `/portfolio/balance`
  response is cached for `KALSHI_STATE_BALANCE_CACHE_SECONDS` (default 2s)
  and shared across every experiment process, so several experiments polling
  on similar cadences collapse into one real request per cache window
  instead of one each.
- **Order audit log** (`record_order`): every order any experiment places or
  simulates (dry-run included) is appended to a shared `order_log` table --
  ticker, side, count, price, which experiment, live or dry-run, the raw
  response. Purely additive, never raises (a logging failure must not be
  able to block or fail a real trade) -- for after-the-fact visibility into
  what every bot on the account has been doing, e.g.:

  ```python
  import sqlite3
  conn = sqlite3.connect("prediction_market_scraper/Clients/Kalshi/state.db")
  conn.execute("SELECT * FROM order_log ORDER BY created_at DESC LIMIT 20").fetchall()
  ```

## What this does NOT do

- **It does not unify local position tracking.** btc_implied_prob and
  golf_field_alpha each still persist their own `open_positions` dict to
  their own `positions_state.json` via their own `positions_store.py`, and
  reconcile it against the real account on startup. That file exists to stop
  ONE process from re-buying a ticker it already holds across ticks/restarts
  -- it's process-local by design, not something other experiments need to
  read. Real cross-account position truth already has a single source: the
  Kalshi account itself. `KalshiTradingClient.get_positions()` returns every
  position on the account regardless of which experiment opened it -- that's
  already the shared, authoritative view; there's no separate ledger to keep
  in sync with it.
- **It does not stop two experiments from trading the same ticker.** If two
  strategies' scopes genuinely overlap (e.g. both resolution_alpha and
  btc_implied_prob trade Kalshi's BTC/ETH interval markets), each still
  decides independently whether to buy. Capital allocation limits *how much*
  each can risk, not *which tickers* they're allowed to touch. Keeping
  strategies' market scopes disjoint, or accepting the overlap deliberately,
  is a design decision for each experiment -- not something this module
  arbitrates.
- **It does not coordinate across machines.** `state.db` is a local file;
  two experiments on two different hosts (or a dev machine and the Pi) don't
  share a rate limit or a capital split. Coordination only applies to
  processes running on the same machine, pointed at the same `state.db`.

## Capital allocation

`CAPITAL_FRACTION` (fraction of the real balance, default `1.0`) and
`CAPITAL_CAP_DOLLARS` (hard dollar ceiling, default unset) live in each
experiment's own `config.py`, env-overridable the same way every other knob
there is (`RESOLUTION_ALPHA_CAPITAL_FRACTION`,
`BTC_IMPLIED_PROB_CAPITAL_FRACTION`, `GOLF_FIELD_ALPHA_CAPITAL_FRACTION`, and
the `*_CAPITAL_CAP_DOLLARS` equivalents). Both default to a no-op -- an
experiment run alone behaves exactly as if this module didn't exist. They
only need setting once more than one experiment is trading the account live
at the same time: e.g. running resolution_alpha and golf_field_alpha
together, `RESOLUTION_ALPHA_CAPITAL_FRACTION=0.7` and
`GOLF_FIELD_ALPHA_CAPITAL_FRACTION=0.3` partitions the account 70/30 for
Kelly sizing purposes. The fraction is applied to the REAL balance at query
time (not a one-time snapshot), so it tracks the account's actual equity as
it moves -- if you'd rather cap what one experiment can ever spend
regardless of how big the account gets, set `CAPITAL_CAP_DOLLARS` instead
(or alongside; whichever number is smaller wins).

## Using this in an existing experiment

All three current experiments already do this -- their `order_manager.py`
constructs a `KalshiStateManager` in `OrderManager.__init__` and routes
`get_balance_dollars` / the live client through it. Use that as the
reference implementation for the wiring below.

## Wiring a new experiment

1. In your experiment's `config.py`, add the same three knobs every other
   experiment has (swap the env prefix for your experiment's own):

   ```python
   EXPERIMENT_NAME = "my_new_experiment"
   CAPITAL_FRACTION = _float_env("MY_NEW_EXPERIMENT_CAPITAL_FRACTION", 1.0)
   _capital_cap_dollars_env = os.environ.get("MY_NEW_EXPERIMENT_CAPITAL_CAP_DOLLARS")
   CAPITAL_CAP_DOLLARS = float(_capital_cap_dollars_env) if _capital_cap_dollars_env else None
   ```

2. In your `order_manager.py`, use the same sys.path bootstrap your
   `kalshi_gateway.py` already uses to reach this directory (that package
   uses bare same-directory imports internally, so the directory has to be
   on `sys.path`, not imported as `prediction_market_scraper.Clients.Kalshi`),
   then build a `KalshiStateManager` once, in `__init__`:

   ```python
   import sys
   from pathlib import Path

   from config import CAPITAL_CAP_DOLLARS, CAPITAL_FRACTION, DRY_RUN, EXPERIMENT_NAME

   _REPO_ROOT = Path(__file__).resolve().parents[2]
   _KALSHI_CLIENT_DIR = _REPO_ROOT / "prediction_market_scraper" / "Clients" / "Kalshi"
   if str(_KALSHI_CLIENT_DIR) not in sys.path:
       sys.path.insert(0, str(_KALSHI_CLIENT_DIR))

   from kalshi_state import KalshiStateManager


   class OrderManager:
       _state: KalshiStateManager | None = None

       def __init__(self, dry_run: bool = DRY_RUN, state: KalshiStateManager | None = None):
           self.dry_run = dry_run
           self._state = state or KalshiStateManager(
               EXPERIMENT_NAME, dry_run=dry_run,
               capital_fraction=CAPITAL_FRACTION, capital_cap_dollars=CAPITAL_CAP_DOLLARS,
           )
           self._client = None if dry_run else self._state.get_client()

       def get_balance_dollars(self) -> float:
           total = float(self._state.get_balance()["balance_dollars"])  # or ["balance"] / 100.0 -- check which key your account actually returns, see order_manager.py in other experiments
           return self._state.apply_capital_allocation(total)

       def buy_favored_side(self, ticker, side, contracts, limit_price):
           ...  # build api_side / price_str / count_str for your market's tick size
           if self.dry_run:
               response = {"dry_run": True, ...}
           else:
               response = self._client.place_order(...)
           if self._state is not None:
               self._state.record_order(
                   ticker=ticker, favored_side=side, api_side=api_side,
                   count=count_str, price=price_str, dry_run=self.dry_run, response=response,
               )
           return response
   ```

   The `_state: KalshiStateManager | None = None` class attribute matters if
   you write unit tests that build an `OrderManager` via
   `OrderManager.__new__(OrderManager)` to inject a fake `_client` (see
   `expirments/resolution_alpha/tests/test_order_manager.py`'s
   `TestPlaceOrderWiring`) -- without it, an instance built that way has no
   `_state` attribute at all and `buy_favored_side` raises `AttributeError`
   the first time it tries to record to it.

3. Accept an optional `state` constructor param (as above) rather than
   always building your own `KalshiStateManager` -- lets tests inject a fake
   one, and lets a future runner that wants to share one `KalshiStateManager`
   across multiple `OrderManager`-like objects in the same process do so.

4. That's it -- your experiment now paces its Kalshi calls against every
   other experiment on the machine, and its `get_balance_dollars()` respects
   whatever capital split gets configured if it's ever run alongside another
   live experiment. Nothing about `strategy.py` / `runner.py` call sites
   needs to change; they already just call `manager.get_balance_dollars()`
   and `manager.buy_favored_side(...)`.

## Config reference

| Env var | Default | Meaning |
|---|---|---|
| `KALSHI_STATE_DB_PATH` | `prediction_market_scraper/Clients/Kalshi/state.db` | Shared SQLite DB path. Override if you want an experiment to coordinate against a different (or per-deployment) state file. |
| `KALSHI_STATE_MIN_REQUEST_INTERVAL_SECONDS` | `0.2` | Minimum spacing between any two Kalshi API calls, system-wide across every experiment on this machine. |
| `KALSHI_STATE_BALANCE_CACHE_SECONDS` | `2.0` | How long a fetched `/portfolio/balance` response is reused before the next caller triggers a fresh one. |
| `<PREFIX>_CAPITAL_FRACTION` | `1.0` | Fraction of the real balance this experiment's `get_balance_dollars()` returns. |
| `<PREFIX>_CAPITAL_CAP_DOLLARS` | unset | Hard dollar ceiling on top of the fraction; the smaller of the two wins. |

`<PREFIX>` is `RESOLUTION_ALPHA`, `BTC_IMPLIED_PROB`, or `GOLF_FIELD_ALPHA`
for the existing experiments.

## Local dev / tests

`KalshiStateManager(dry_run=True)` never constructs a real
`KalshiTradingClient` (same rule every experiment's `OrderManager` already
followed), so dry-run and unit tests need no Kalshi credentials. It does
still create/touch the local `state.db` (SQLite, gitignored) to record dry
run orders and hold the rate-limit/balance-cache tables -- harmless, and the
same file every experiment on the machine shares, so a dry-run test run and
a live process technically touch the same DB if pointed at the same
default path. Point `KALSHI_STATE_DB_PATH` at a temp file for tests that
need real isolation from a live deployment's state.
