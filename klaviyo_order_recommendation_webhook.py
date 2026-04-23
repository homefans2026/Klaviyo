#!/usr/bin/env python3
"""Turn an order webhook into a Klaviyo recommendation-ready event.

The recommendation source of truth is upsell_recommendations_wide.csv.
This script can run as a tiny HTTP webhook receiver or as a CLI dry run.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import hashlib
import html
import json
import os
import re
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


DEFAULT_CSV = Path(__file__).with_name("upsell_recommendations_wide.csv")
DEFAULT_EVENT_NAME = "Placed Order Recommendation Ready"
DEFAULT_REVISION = "2024-07-15"
KLAVIYO_EVENTS_URL = "https://a.klaviyo.com/api/events"
MAX_WEBHOOK_BYTES = 1024 * 1024
DEFAULT_UTM_SOURCE = "klaviyo"
DEFAULT_UTM_MEDIUM = "email"
DEFAULT_UTM_CAMPAIGN = "post_purchase_upsell"

TRUE_VALUES = {"1", "true", "yes", "y", "on"}
GENERIC_PRODUCT_TITLES = {
    "ticket sale",
    "tickets sale",
    "ticket",
    "tickets",
    "general ticket sale",
}


@dataclass(frozen=True)
class Config:
    csv_path: Path
    event_name: str
    api_key: str
    revision: str
    dry_run: bool
    min_recommendations: int
    webhook_secret: str
    generic_fallback: bool


@dataclass(frozen=True)
class ProductMatch:
    row: dict[str, str]
    input_title: str
    matched_title: str
    match_type: str
    score: float


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in TRUE_VALUES


def normalize_title(value: Any) -> str:
    """Normalize titles so full order names can match truncated CSV labels."""
    text = html.unescape(str(value or ""))
    text = text.replace("\u2026", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("&", " and ")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^A-Za-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def is_generic_product_title(value: str) -> bool:
    normalized = normalize_title(value)
    return normalized in GENERIC_PRODUCT_TITLES


def clean_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def first_nonempty(*values: Any) -> str:
    for value in values:
        cleaned = clean_string(value)
        if cleaned:
            return cleaned
    return ""


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        cleaned = clean_string(value)
        key = normalize_title(cleaned)
        if cleaned and key and key not in seen:
            seen.add(key)
            unique.append(cleaned)
    return unique


def deep_get(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def find_first_key(payload: Any, target_keys: set[str]) -> Any:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in target_keys and clean_string(value):
                return value
        for value in payload.values():
            found = find_first_key(value, target_keys)
            if clean_string(found):
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_first_key(item, target_keys)
            if clean_string(found):
                return found
    return None


def extract_email(payload: dict[str, Any]) -> str:
    candidates = [
        payload.get("email"),
        payload.get("customer_email"),
        payload.get("billing_email"),
        deep_get(payload, ("billing", "email")),
        deep_get(payload, ("customer", "email")),
        deep_get(payload, ("contact", "email")),
        deep_get(payload, ("order", "email")),
        deep_get(payload, ("order", "billing", "email")),
        deep_get(payload, ("data", "email")),
        deep_get(payload, ("data", "billing", "email")),
        deep_get(payload, ("data", "customer", "email")),
        find_first_key(payload, {"email", "customer_email", "billing_email"}),
    ]
    return first_nonempty(*candidates).lower()


def extract_order_id(payload: dict[str, Any]) -> str:
    candidates = [
        payload.get("order_id"),
        payload.get("orderId"),
        payload.get("id"),
        payload.get("number"),
        deep_get(payload, ("order", "id")),
        deep_get(payload, ("order", "order_id")),
        deep_get(payload, ("data", "id")),
        deep_get(payload, ("data", "order_id")),
    ]
    return first_nonempty(*candidates)


def extract_line_item_titles(container: Any) -> list[str]:
    title_keys = (
        "product_title",
        "productTitle",
        "product_name",
        "productName",
        "title",
        "name",
    )
    titles: list[str] = []
    if not isinstance(container, list):
        return titles
    for item in container:
        if not isinstance(item, dict):
            if clean_string(item):
                titles.append(clean_string(item))
            continue
        title = first_nonempty(*(item.get(key) for key in title_keys))
        if title:
            titles.append(title)
    return titles


def extract_product_titles(payload: dict[str, Any], include_generic: bool = False) -> list[str]:
    titles: list[str] = []
    direct_keys = (
        "product_title",
        "productTitle",
        "product_name",
        "productName",
        "purchased_product",
    )
    titles.extend(clean_string(payload.get(key)) for key in direct_keys)

    for key in ("product_titles", "productTitles", "purchased_products"):
        value = payload.get(key)
        if isinstance(value, list):
            titles.extend(clean_string(item) for item in value)

    item_list_keys = ("line_items", "lineItems", "items", "products", "cart_items")
    containers: list[Any] = []
    for key in item_list_keys:
        containers.append(payload.get(key))
        containers.append(deep_get(payload, ("order", key)))
        containers.append(deep_get(payload, ("data", key)))

    for container in containers:
        titles.extend(extract_line_item_titles(container))

    unique_titles = dedupe(titles)
    if include_generic:
        return unique_titles
    return [title for title in unique_titles if not is_generic_product_title(title)]


class RecommendationIndex:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.rows = self._load_rows(csv_path)
        self.exact_index: dict[str, ProductMatch] = {}
        self.candidates: list[tuple[str, str, dict[str, str], str]] = []
        self._build_index()

    @staticmethod
    def _load_rows(csv_path: Path) -> list[dict[str, str]]:
        if not csv_path.exists():
            raise FileNotFoundError(f"Recommendation CSV not found: {csv_path}")
        with csv_path.open(newline="", encoding="utf-8-sig") as handle:
            return list(csv.DictReader(handle))

    def _build_index(self) -> None:
        for row in self.rows:
            title_sources = {
                "base_product": row.get("base_product", ""),
                "base_product_live_title": row.get("base_product_live_title", ""),
            }
            for source, title in title_sources.items():
                cleaned = clean_string(title)
                normalized = normalize_title(cleaned)
                if not normalized or is_generic_product_title(cleaned):
                    continue
                self.candidates.append((normalized, cleaned, row, source))
                existing = self.exact_index.get(normalized)
                current = ProductMatch(row, cleaned, cleaned, f"exact:{source}", 1.0)
                if existing is None or self.available_recommendations(row) > self.available_recommendations(existing.row):
                    self.exact_index[normalized] = current

    def generic_recommendation_rows(self, excluded_titles: list[str] | None = None, limit: int = 3) -> list[dict[str, str]]:
        excluded = {
            normalize_title(title)
            for title in (excluded_titles or [])
            if normalize_title(title)
        }
        eligible = [
            row
            for row in self.rows
            if row.get("base_product_trigger_eligible") == "yes"
            and clean_string(row.get("base_product_live_title"))
            and clean_string(row.get("base_product_url"))
            and not is_generic_product_title(row.get("base_product_live_title", ""))
            and normalize_title(row.get("base_product_live_title", "")) not in excluded
            and normalize_title(row.get("base_product", "")) not in excluded
        ]
        eligible.sort(key=self._generic_sort_key, reverse=True)

        selected: list[dict[str, str]] = []
        seen_locations: set[str] = set()
        for row in eligible:
            location_key = normalize_title(
                first_nonempty(row.get("base_product_locality"), row.get("base_product_country"))
            )
            if location_key and location_key in seen_locations:
                continue
            selected.append(row)
            if location_key:
                seen_locations.add(location_key)
            if len(selected) == limit:
                return selected

        selected_ids = {id(row) for row in selected}
        for row in eligible:
            if id(row) in selected_ids:
                continue
            selected.append(row)
            if len(selected) == limit:
                return selected
        return selected

    @staticmethod
    def available_recommendations(row: dict[str, str]) -> int:
        count = 0
        for rank in range(1, 4):
            if clean_string(row.get(f"suggestion_{rank}_url")) and clean_string(
                row.get(f"suggestion_{rank}_live_title") or row.get(f"suggestion_{rank}")
            ):
                count += 1
        return count

    @staticmethod
    def _generic_sort_key(row: dict[str, str]) -> tuple[float, float, str]:
        orders_last_12m = float(row.get("base_product_orders_last_12m") or 0)
        recent_weight = float(row.get("base_product_recent_activity_weight") or 0)
        last_seen = row.get("base_product_last_seen_date") or ""
        return orders_last_12m, recent_weight, last_seen

    def find(self, input_title: str) -> ProductMatch | None:
        normalized = normalize_title(input_title)
        if not normalized or is_generic_product_title(input_title):
            return None

        exact = self.exact_index.get(normalized)
        if exact is not None:
            return ProductMatch(exact.row, input_title, exact.matched_title, exact.match_type, 1.0)

        best: ProductMatch | None = None
        for candidate_norm, candidate_title, row, source in self.candidates:
            score = self._match_score(normalized, candidate_norm)
            if score < 0.86:
                continue
            match_type = "fuzzy"
            if self._is_prefix_or_contains_match(normalized, candidate_norm):
                match_type = "partial"
            contender = ProductMatch(row, input_title, candidate_title, f"{match_type}:{source}", score)
            if best is None or self._is_better_match(contender, best):
                best = contender
        return best

    @staticmethod
    def _is_prefix_or_contains_match(left: str, right: str) -> bool:
        if min(len(left), len(right)) < 12:
            return False
        return (
            left.startswith(right)
            or right.startswith(left)
            or (min(len(left), len(right)) >= 18 and (left in right or right in left))
        )

    def _match_score(self, input_norm: str, candidate_norm: str) -> float:
        if input_norm == candidate_norm:
            return 1.0
        if self._is_prefix_or_contains_match(input_norm, candidate_norm):
            length_ratio = min(len(input_norm), len(candidate_norm)) / max(len(input_norm), len(candidate_norm))
            return 0.92 + min(length_ratio, 1.0) * 0.07
        return difflib.SequenceMatcher(None, input_norm, candidate_norm).ratio()

    def _is_better_match(self, contender: ProductMatch, current: ProductMatch) -> bool:
        if contender.score != current.score:
            return contender.score > current.score
        contender_recs = self.available_recommendations(contender.row)
        current_recs = self.available_recommendations(current.row)
        if contender_recs != current_recs:
            return contender_recs > current_recs
        contender_live = contender.match_type.endswith("base_product_live_title")
        current_live = current.match_type.endswith("base_product_live_title")
        return contender_live and not current_live


def recommendation_properties(match: ProductMatch) -> tuple[dict[str, Any], int]:
    row = match.row
    properties: dict[str, Any] = {
        "purchased_product": match.input_title,
        "matched_product": row.get("base_product_live_title") or row.get("base_product"),
        "matched_product_url": row.get("base_product_url"),
        "matched_product_locality": row.get("base_product_locality"),
        "matched_product_country": row.get("base_product_country"),
        "match_type": match.match_type,
        "match_score": round(match.score, 4),
        "recommendation_source": "upsell_recommendations_wide.csv",
        "recommendation_mode": "mapped",
        "recommendations_generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }

    available = 0
    for rank in range(1, 4):
        title = first_nonempty(row.get(f"suggestion_{rank}_live_title"), row.get(f"suggestion_{rank}"))
        url = clean_string(row.get(f"suggestion_{rank}_url"))
        if title and url:
            available += 1

        properties[f"recommended_{rank}_title"] = title
        properties[f"recommended_{rank}_url"] = url
        properties[f"recommended_{rank}_basis"] = row.get(f"suggestion_{rank}_basis")
        properties[f"recommended_{rank}_location_proximity"] = row.get(f"suggestion_{rank}_location_proximity")
        properties[f"recommended_{rank}_locality"] = row.get(f"suggestion_{rank}_locality")
        properties[f"recommended_{rank}_country"] = row.get(f"suggestion_{rank}_country")
        properties[f"recommended_{rank}_confidence"] = row.get(f"suggestion_{rank}_confidence")
        properties[f"recommended_{rank}_phi"] = row.get(f"suggestion_{rank}_phi")
        properties[f"recommended_{rank}_lift"] = row.get(f"suggestion_{rank}_lift")
        properties[f"recommended_{rank}_recency_weighted_score"] = row.get(
            f"suggestion_{rank}_recency_weighted_score"
        )

    properties["available_recommendations"] = available
    return properties, available


def add_utm(url: str, content: str) -> str:
    parts = urllib.parse.urlsplit(url)
    query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    query.update(
        {
            "utm_source": DEFAULT_UTM_SOURCE,
            "utm_medium": DEFAULT_UTM_MEDIUM,
            "utm_campaign": DEFAULT_UTM_CAMPAIGN,
            "utm_content": content,
        }
    )
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), parts.fragment)
    )


def generic_recommendation_properties(
    index: RecommendationIndex,
    purchased_product: str,
    reason: str,
) -> tuple[dict[str, Any], int]:
    rows = index.generic_recommendation_rows([purchased_product], limit=3)
    properties: dict[str, Any] = {
        "purchased_product": purchased_product or "Unknown product",
        "matched_product": "",
        "matched_product_url": "",
        "matched_product_locality": "",
        "matched_product_country": "",
        "match_type": "generic_fallback",
        "match_score": 0,
        "recommendation_source": "upsell_recommendations_wide.csv",
        "recommendation_mode": "generic",
        "generic_reason": reason,
        "recommendations_generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }

    for rank in range(1, 4):
        row = rows[rank - 1] if rank <= len(rows) else {}
        title = clean_string(row.get("base_product_live_title"))
        url = clean_string(row.get("base_product_url"))

        properties[f"recommended_{rank}_title"] = title
        properties[f"recommended_{rank}_url"] = add_utm(url, f"generic_suggestion_{rank}") if url else ""
        properties[f"recommended_{rank}_basis"] = "generic_top_recent_active"
        properties[f"recommended_{rank}_location_proximity"] = "generic"
        properties[f"recommended_{rank}_locality"] = row.get("base_product_locality", "")
        properties[f"recommended_{rank}_country"] = row.get("base_product_country", "")
        properties[f"recommended_{rank}_confidence"] = "generic"
        properties[f"recommended_{rank}_phi"] = ""
        properties[f"recommended_{rank}_lift"] = ""
        properties[f"recommended_{rank}_recency_weighted_score"] = row.get("base_product_recent_activity_weight", "")

    available = len(rows)
    properties["available_recommendations"] = available
    return properties, available


def stable_unique_id(email: str, order_id: str, purchased_product: str) -> str:
    if order_id:
        base = f"homefans-order-rec|{order_id}|{normalize_title(purchased_product)}"
    else:
        # Without an order id, include a timestamp so separate purchases do not dedupe each other.
        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        base = f"homefans-order-rec|{email}|{normalize_title(purchased_product)}|{now}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]
    return f"homefans-order-rec-{digest}"


def build_klaviyo_payload(
    *,
    email: str,
    event_name: str,
    order_id: str,
    properties: dict[str, Any],
) -> dict[str, Any]:
    properties = dict(properties)
    properties["email"] = email

    return {
        "data": {
            "type": "event",
            "attributes": {
                "metric": {
                    "data": {
                        "type": "metric",
                        "attributes": {"name": event_name},
                    }
                },
                "profile": {
                    "data": {
                        "type": "profile",
                        "attributes": {"email": email},
                    }
                },
                "properties": properties,
                "unique_id": stable_unique_id(email, order_id, properties["purchased_product"]),
                "time": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
        }
    }


def send_klaviyo_event(payload: dict[str, Any], config: Config) -> dict[str, Any]:
    if config.dry_run:
        return {"sent": False, "dry_run": True, "klaviyo_status": None}
    if not config.api_key:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY. Use --dry-run for local testing.")

    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        KLAVIYO_EVENTS_URL,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Klaviyo-API-Key {config.api_key}",
            "accept": "application/vnd.api+json",
            "content-type": "application/vnd.api+json",
            "revision": config.revision,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            return {
                "sent": True,
                "dry_run": False,
                "klaviyo_status": response.status,
                "klaviyo_response": response_body,
            }
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Klaviyo API error {exc.code}: {error_body}") from exc


def process_order(payload: dict[str, Any], index: RecommendationIndex, config: Config) -> dict[str, Any]:
    email = extract_email(payload)
    raw_product_titles = extract_product_titles(payload, include_generic=True)
    product_titles = [title for title in raw_product_titles if not is_generic_product_title(title)]
    fallback_purchased_product = first_nonempty(*(raw_product_titles or ["Unknown product"]))
    order_id = extract_order_id(payload)

    if not email:
        return {"status": "error", "reason": "missing_email"}
    if not product_titles:
        return process_generic_fallback(
            email=email,
            order_id=order_id,
            purchased_product=fallback_purchased_product,
            reason="missing_or_generic_product_title",
            index=index,
            config=config,
        )

    attempted: list[dict[str, Any]] = []
    for product_title in product_titles:
        match = index.find(product_title)
        if match is None:
            attempted.append({"product_title": product_title, "reason": "no_recommendation_row"})
            continue

        properties, available = recommendation_properties(match)
        attempted.append(
            {
                "product_title": product_title,
                "matched_product": properties.get("matched_product"),
                "available_recommendations": available,
                "match_score": properties.get("match_score"),
                "match_type": properties.get("match_type"),
            }
        )
        if available < config.min_recommendations:
            continue

        event_payload = build_klaviyo_payload(
            email=email,
            event_name=config.event_name,
            order_id=order_id,
            properties=properties,
        )
        send_result = send_klaviyo_event(event_payload, config)
        return {
            "status": "event_ready" if config.dry_run else "sent",
            "event_name": config.event_name,
            "email": email,
            "order_id": order_id,
            "purchased_product": product_title,
            "matched_product": properties.get("matched_product"),
            "available_recommendations": available,
            **send_result,
            "klaviyo_payload": event_payload,
        }

    result = process_generic_fallback(
        email=email,
        order_id=order_id,
        purchased_product=fallback_purchased_product,
        reason="no_product_with_enough_mapped_recommendations",
        index=index,
        config=config,
    )
    result["attempted_products"] = attempted
    return result


def process_generic_fallback(
    *,
    email: str,
    order_id: str,
    purchased_product: str,
    reason: str,
    index: RecommendationIndex,
    config: Config,
) -> dict[str, Any]:
    if not config.generic_fallback:
        return {
            "status": "no_event",
            "reason": reason,
            "email": email,
            "order_id": order_id,
            "generic_fallback": False,
        }

    properties, available = generic_recommendation_properties(index, purchased_product, reason)
    if available < config.min_recommendations:
        return {
            "status": "no_event",
            "reason": "generic_fallback_missing_recommendations",
            "email": email,
            "order_id": order_id,
            "available_recommendations": available,
            "min_recommendations": config.min_recommendations,
        }

    event_payload = build_klaviyo_payload(
        email=email,
        event_name=config.event_name,
        order_id=order_id,
        properties=properties,
    )
    send_result = send_klaviyo_event(event_payload, config)
    return {
        "status": "event_ready" if config.dry_run else "sent",
        "event_name": config.event_name,
        "email": email,
        "order_id": order_id,
        "purchased_product": purchased_product,
        "available_recommendations": available,
        "recommendation_mode": "generic",
        "generic_reason": reason,
        **send_result,
        "klaviyo_payload": event_payload,
    }


class RecommendationWebhookHandler(BaseHTTPRequestHandler):
    index: RecommendationIndex
    config: Config

    def do_GET(self) -> None:
        if self.path in {"/", "/health", "/healthz"}:
            self.write_json(200, {"status": "ok", "event_name": self.config.event_name})
            return
        self.write_json(404, {"status": "not_found"})

    def do_POST(self) -> None:
        if self.path not in {"/", "/webhook", "/orders"}:
            self.write_json(404, {"status": "not_found"})
            return

        if self.config.webhook_secret:
            supplied_secret = self.headers.get("X-Webhook-Secret", "")
            if supplied_secret != self.config.webhook_secret:
                self.write_json(401, {"status": "error", "reason": "invalid_webhook_secret"})
                return

        try:
            payload = self.read_json_body()
            result = process_order(payload, self.index, self.config)
        except json.JSONDecodeError as exc:
            self.write_json(400, {"status": "error", "reason": f"invalid_json: {exc}"})
            return
        except ValueError as exc:
            self.write_json(400, {"status": "error", "reason": str(exc)})
            return
        except Exception as exc:  # Keep webhook failures visible to the caller.
            self.write_json(500, {"status": "error", "reason": str(exc)})
            return

        status_code = 202 if result.get("status") in {"event_ready", "sent"} else 200
        self.write_json(status_code, result)

    def read_json_body(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError:
            length = 0
        if length <= 0:
            raise json.JSONDecodeError("empty body", "", 0)
        if length > MAX_WEBHOOK_BYTES:
            raise ValueError("webhook payload is too large")
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("webhook payload must be a JSON object")
        return payload

    def write_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))


def config_from_args(args: argparse.Namespace) -> Config:
    return Config(
        csv_path=Path(args.csv).expanduser().resolve(),
        event_name=args.event_name,
        api_key=os.getenv("KLAVIYO_PRIVATE_API_KEY", ""),
        revision=args.revision,
        dry_run=args.dry_run,
        min_recommendations=args.min_recommendations,
        webhook_secret=os.getenv("WEBHOOK_SECRET", ""),
        generic_fallback=args.generic_fallback,
    )


def load_payload_from_args(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.payload_file:
        with Path(args.payload_file).expanduser().open(encoding="utf-8") as handle:
            payload = json.load(handle)
    elif args.stdin:
        payload = json.load(sys.stdin)
    elif args.email or args.product_title:
        payload = {
            "email": args.email,
            "product_titles": args.product_title or [],
        }
    else:
        return None

    if not isinstance(payload, dict):
        raise ValueError("Input payload must be a JSON object")
    return payload


def serve(args: argparse.Namespace, config: Config, index: RecommendationIndex) -> None:
    handler = RecommendationWebhookHandler
    handler.index = index
    handler.config = config
    server = ThreadingHTTPServer((args.host, args.port), handler)
    mode = "dry run" if config.dry_run else "live"
    print(f"Listening on http://{args.host}:{args.port}/webhook ({mode})", file=sys.stderr)
    server.serve_forever()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send Klaviyo recommendation-ready events from order webhooks."
    )
    parser.add_argument("--serve", action="store_true", help="Run an HTTP webhook server.")
    parser.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8080")))
    parser.add_argument("--csv", default=os.getenv("RECOMMENDATIONS_CSV", str(DEFAULT_CSV)))
    parser.add_argument("--event-name", default=os.getenv("KLAVIYO_EVENT_NAME", DEFAULT_EVENT_NAME))
    parser.add_argument("--revision", default=os.getenv("KLAVIYO_REVISION", DEFAULT_REVISION))
    parser.add_argument(
        "--min-recommendations",
        type=int,
        default=int(os.getenv("MIN_RECOMMENDATIONS", "3")),
        help="Minimum recommendation slots required before sending an event.",
    )
    parser.add_argument("--dry-run", action="store_true", default=env_bool("DRY_RUN"))
    parser.add_argument(
        "--generic-fallback",
        dest="generic_fallback",
        action="store_true",
        default=env_bool("GENERIC_FALLBACK", True),
        help="Send a generic recommendation event when a product is unmapped.",
    )
    parser.add_argument(
        "--no-generic-fallback",
        dest="generic_fallback",
        action="store_false",
        help="Keep old behavior and skip unmapped products.",
    )
    parser.add_argument("--stdin", action="store_true", help="Read one webhook JSON object from stdin.")
    parser.add_argument("--payload-file", help="Read one webhook JSON object from a file.")
    parser.add_argument("--email", help="Quick CLI test email.")
    parser.add_argument(
        "--product-title",
        action="append",
        help="Quick CLI test product title. Can be supplied more than once.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = config_from_args(args)
    index = RecommendationIndex(config.csv_path)

    if args.serve:
        serve(args, config, index)
        return 0

    payload = load_payload_from_args(args)
    if payload is None:
        parser.print_help(sys.stderr)
        return 2

    result = process_order(payload, index, config)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "error":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
