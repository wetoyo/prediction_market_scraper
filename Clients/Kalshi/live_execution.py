"""REST client for Kalshi's authenticated portfolio/order endpoints (the
Execution Loop's trading side) -- placing/cancelling orders and viewing
balance, positions, and open orders.

Every request here is signed the same way as live_datastream.py's websocket
handshake. Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH (PEM RSA key)
in the environment to use this.
"""

import os
import time
import uuid

import requests

from live_datastream import _load_private_key, _sign_request

# The V2 event-market order endpoints (`/portfolio/events/orders`, POST/DELETE)
# are only served from the "Production Trade API" host below. The older
# "Shared API" host (api.elections.kalshi.com) still answers the read-only
# portfolio endpoints but 404s on order placement/cancellation, so use the
# trade host for everything. Signing is over the path only, so API_PATH_PREFIX
# is unchanged and host-independent.
BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
API_PATH_PREFIX = "/trade-api/v2"


def _auth_headers(api_key_id: str, private_key, method: str, path: str) -> dict:
    timestamp_ms = str(int(time.time() * 1000))
    signature = _sign_request(private_key, timestamp_ms, method, path)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


class KalshiTradingClient:
    """Thin wrapper around Kalshi's authenticated portfolio/order REST endpoints."""

    def __init__(self, api_key_id: str | None = None, private_key_path: str | None = None):
        self.api_key_id = api_key_id or os.environ["KALSHI_API_KEY_ID"]
        self.private_key = _load_private_key(private_key_path or os.environ["KALSHI_PRIVATE_KEY_PATH"])

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> dict:
        headers = _auth_headers(self.api_key_id, self.private_key, method, f"{API_PATH_PREFIX}{path}")
        response = requests.request(
            method, f"{BASE_URL}{path}", headers=headers, params=params, json=json_body, timeout=20
        )
        if not response.ok:
            # raise_for_status() drops the response body; Kalshi puts the actual
            # reason there (`{"error":{"code":"insufficient_balance",...}}` etc.)
            # and a bare "400 Bad Request" with no detail has cost real
            # debugging time. Keep it an HTTPError with `.response` set so
            # existing `except HTTPError` / `.response.status_code` callers are
            # unaffected.
            raise requests.exceptions.HTTPError(
                f"{response.status_code} {response.reason} for {method} {path}: {response.text[:600]}",
                response=response,
            )
        return response.json()

    def get_balance(self) -> dict:
        """Returns available balance and portfolio value, in cents."""
        return self._request("GET", "/portfolio/balance")

    def get_positions(self, ticker: str | None = None, event_ticker: str | None = None) -> dict:
        """Returns open market_positions and event_positions (non-zero position or total_traded)."""
        params = {}
        if ticker:
            params["ticker"] = ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        return self._request("GET", "/portfolio/positions", params=params)

    def get_orders(self, status: str | None = None, ticker: str | None = None) -> dict:
        """Lists orders. status filters to 'resting', 'canceled', or 'executed'."""
        params = {}
        if status:
            params["status"] = status
        if ticker:
            params["ticker"] = ticker
        return self._request("GET", "/portfolio/orders", params=params)

    def place_order(
        self,
        ticker: str,
        side: str,
        count: str,
        price: str,
        time_in_force: str = "good_till_canceled",
        client_order_id: str | None = None,
        exchange_index: int | None = None,
        reduce_only: bool | None = None,
    ) -> dict:
        """Submits a limit order. side is 'bid' or 'ask'; count/price are fixed-point
        dollar strings (e.g. count="10.00", price="0.5600").

        exchange_index (Kalshi Exchange Sharding, docs.kalshi.com/getting_started/
        exchange_sharding): which exchange shard to route the order to. Markets
        carry an `exchange_index` on GET /markets; collateral is per-shard and an
        order routed to a shard the account holds no balance on is rejected
        `404 {"error":{"code":"user_not_found"}}`. None omits the field, which
        makes Kalshi auto-route by ticker (billed to the unscoped write bucket
        plus every nonzero shard's); passing the market's own index targets that
        shard's write bucket directly. reduce_only, when True, forbids the order
        from opening or increasing a position (only closing one) -- for
        best-effort exit sells that must never oversell into an opposite position.
        """
        body = {
            "ticker": ticker,
            "side": side,
            "count": count,
            "price": price,
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if exchange_index is not None:
            body["exchange_index"] = exchange_index
        if reduce_only is not None:
            body["reduce_only"] = reduce_only
        return self._request("POST", "/portfolio/events/orders", json_body=body)

    def cancel_order(self, order_id: str) -> dict:
        return self._request("DELETE", f"/portfolio/events/orders/{order_id}")


if __name__ == "__main__":
    client = KalshiTradingClient()
    print("Balance:", client.get_balance())
    print("Positions:", client.get_positions())
    print("Open orders:", client.get_orders(status="resting"))
