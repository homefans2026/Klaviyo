#!/usr/bin/env python3
"""Backfill Klaviyo recommendation events for skipped flow activity rows.

This script reconciles a Klaviyo Flow Split Activity CSV with one or more
WooCommerce order exports, then replays the recommendation event for matched
external customer orders.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from klaviyo_order_recommendation_webhook import (
    DEFAULT_CSV,
    DEFAULT_EVENT_NAME,
    DEFAULT_REVISION,
    Config,
    RecommendationIndex,
    clean_string,
    is_blocked_email,
    is_valid_email,
    process_order,
)


DEFAULT_SKIPPED_CSV = Path("/Users/ronanliedmeier/Downloads/Klaviyo Flow Split Activity.csv")
DEFAULT_ORDERS_CSVS = [
    Path("/Users/ronanliedmeier/Downloads/wc_order-export-2026-05-16.csv"),
    Path("/Users/ronanliedmeier/Downloads/wc_order-export-2026-05-16 (1).csv"),
]
DEFAULT_OUTPUT_CSV = Path(__file__).with_name("backfill_skipped_klaviyo_events_2026-05-16.csv")
VALID_ORDER_STATUSES = {"booking confirmed", "completed"}


@dataclass(frozen=True)
class SkippedActivity:
    row_number: int
    email: str
    activity_date: dt.datetime | None
    raw: dict[str, str]


@dataclass(frozen=True)
class OrderRow:
    email: str
    order_label: str
    order_id: str
    order_date: dt.date | None
    status: str
    products: str
    raw: dict[str, str]


def parse_skip_datetime(value: str) -> dt.datetime | None:
    try:
        return dt.datetime.strptime(clean_string(value), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def parse_order_date(value: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(clean_string(value), "%b %d, %Y").date()
    except ValueError:
        return None


def extract_order_id(order_label: str) -> str:
    match = re.search(r"#?(\d+)", order_label)
    return match.group(1) if match else clean_string(order_label)


def split_products(products: str) -> list[str]:
    return [part.strip() for part in clean_string(products).split(",") if part.strip()]


def load_skipped(path: Path) -> tuple[list[SkippedActivity], Counter[str]]:
    skipped: list[SkippedActivity] = []
    counters: Counter[str] = Counter()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            email = clean_string(row.get("Email")).lower()
            if not email:
                counters["skipped_blank_email"] += 1
                continue
            if not is_valid_email(email):
                counters["skipped_invalid_email"] += 1
                continue
            if is_blocked_email(email):
                counters["skipped_homefans_email"] += 1
                continue
            skipped.append(
                SkippedActivity(
                    row_number=row_number,
                    email=email,
                    activity_date=parse_skip_datetime(row.get("Date", "")),
                    raw=row,
                )
            )
            counters["eligible_external_activity_rows"] += 1
    return skipped, counters


def load_orders(paths: list[Path]) -> tuple[dict[str, list[OrderRow]], Counter[str]]:
    by_email: dict[str, list[OrderRow]] = defaultdict(list)
    counters: Counter[str] = Counter()
    seen: set[tuple[str, str, str, str, str]] = set()
    for path in paths:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                email = clean_string(row.get("Email")).lower()
                products = clean_string(row.get("Products"))
                status = clean_string(row.get("Status"))
                order_label = clean_string(row.get("Order"))
                key = (email, order_label, products, clean_string(row.get("Date")), status)
                if key in seen:
                    counters["duplicate_order_rows"] += 1
                    continue
                seen.add(key)

                if not email or not is_valid_email(email):
                    counters["order_rows_missing_or_invalid_email"] += 1
                    continue
                if is_blocked_email(email):
                    counters["order_rows_homefans_email"] += 1
                    continue
                if not products:
                    counters["order_rows_missing_products"] += 1
                    continue

                order = OrderRow(
                    email=email,
                    order_label=order_label,
                    order_id=extract_order_id(order_label),
                    order_date=parse_order_date(row.get("Date", "")),
                    status=status,
                    products=products,
                    raw=row,
                )
                by_email[email].append(order)
                counters["loaded_order_rows"] += 1

    for orders in by_email.values():
        orders.sort(
            key=lambda order: (
                order.status.strip().lower() in VALID_ORDER_STATUSES,
                order.order_date or dt.date.min,
            ),
            reverse=True,
        )
    return by_email, counters


def choose_order(activity: SkippedActivity, orders: list[OrderRow]) -> tuple[OrderRow | None, str]:
    valid_orders = [
        order
        for order in orders
        if order.status.strip().lower() in VALID_ORDER_STATUSES and split_products(order.products)
    ]
    if not orders:
        return None, "no_order_for_email"
    if not valid_orders:
        return None, "no_confirmed_or_completed_order"
    if activity.activity_date is None:
        return valid_orders[0], "latest_valid_order_no_activity_date"

    activity_day = activity.activity_date.date()
    same_day = [order for order in valid_orders if order.order_date == activity_day]
    if same_day:
        return same_day[0], "same_day_valid_order"

    previous = [
        order for order in valid_orders if order.order_date is not None and order.order_date <= activity_day
    ]
    if previous:
        return min(previous, key=lambda order: abs((activity_day - order.order_date).days)), "closest_previous_valid_order"

    return min(
        valid_orders,
        key=lambda order: abs((activity_day - (order.order_date or activity_day)).days),
    ), "closest_valid_order"


def build_backfill_payload(activity: SkippedActivity, order: OrderRow) -> dict[str, Any]:
    return {
        "email": activity.email,
        "order_id": order.order_id,
        "product_titles": split_products(order.products),
        "backfill_source": "klaviyo_flow_split_activity",
        "backfill_activity_date": activity.raw.get("Date", ""),
        "backfill_order_label": order.order_label,
    }


def backfill(args: argparse.Namespace) -> int:
    skipped, skipped_counters = load_skipped(Path(args.skipped_csv).expanduser())
    order_csvs = args.orders_csv or [str(path) for path in DEFAULT_ORDERS_CSVS]
    orders_by_email, order_counters = load_orders([Path(path).expanduser() for path in order_csvs])
    index = RecommendationIndex(Path(args.recommendations_csv).expanduser())
    config = Config(
        csv_path=Path(args.recommendations_csv).expanduser(),
        event_name=args.event_name,
        api_key=os.getenv("KLAVIYO_PRIVATE_API_KEY", ""),
        revision=args.revision,
        dry_run=not args.live,
        min_recommendations=args.min_recommendations,
        generic_fallback=True,
        log_incoming_payload=False,
    )

    if args.live and not config.api_key:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY; refusing live backfill.")

    output_rows: list[dict[str, Any]] = []
    seen_events: set[tuple[str, str, str]] = set()
    counters = skipped_counters + order_counters

    for activity in skipped:
        order, match_reason = choose_order(activity, orders_by_email.get(activity.email, []))
        if order is None:
            counters[match_reason] += 1
            output_rows.append(
                {
                    "status": "not_sent",
                    "reason": match_reason,
                    "email": activity.email,
                    "activity_date": activity.raw.get("Date", ""),
                    "order": "",
                    "order_date": "",
                    "order_status": "",
                    "products": "",
                    "recommendation_mode": "",
                    "purchased_product": "",
                }
            )
            continue

        event_key = (activity.email, order.order_id, order.products)
        if event_key in seen_events:
            counters["duplicate_activity_for_same_order"] += 1
            output_rows.append(
                {
                    "status": "not_sent",
                    "reason": "duplicate_activity_for_same_order",
                    "email": activity.email,
                    "activity_date": activity.raw.get("Date", ""),
                    "order": order.order_label,
                    "order_date": order.raw.get("Date", ""),
                    "order_status": order.status,
                    "products": order.products,
                    "recommendation_mode": "",
                    "purchased_product": "",
                }
            )
            continue
        seen_events.add(event_key)

        payload = build_backfill_payload(activity, order)
        try:
            result = process_order(payload, index, config)
        except Exception as exc:
            counters["result_error"] += 1
            output_rows.append(
                {
                    "status": "error",
                    "reason": str(exc),
                    "email": activity.email,
                    "activity_date": activity.raw.get("Date", ""),
                    "order": order.order_label,
                    "order_id": order.order_id,
                    "order_date": order.raw.get("Date", ""),
                    "order_status": order.status,
                    "match_reason": match_reason,
                    "products": order.products,
                    "recommendation_mode": "",
                    "purchased_product": "",
                    "matched_product": "",
                    "available_recommendations": "",
                    "klaviyo_status": "",
                    "sent": "",
                }
            )
            if args.live:
                break
            continue
        counters[f"result_{result.get('status')}"] += 1
        if result.get("recommendation_mode"):
            counters[f"mode_{result.get('recommendation_mode')}"] += 1
        if result.get("reason"):
            counters[f"reason_{result.get('reason')}"] += 1

        output_rows.append(
            {
                "status": result.get("status", ""),
                "reason": result.get("reason", ""),
                "email": activity.email,
                "activity_date": activity.raw.get("Date", ""),
                "order": order.order_label,
                "order_id": order.order_id,
                "order_date": order.raw.get("Date", ""),
                "order_status": order.status,
                "match_reason": match_reason,
                "products": order.products,
                "recommendation_mode": result.get("recommendation_mode", ""),
                "purchased_product": result.get("purchased_product", ""),
                "matched_product": result.get("matched_product", ""),
                "available_recommendations": result.get("available_recommendations", ""),
                "klaviyo_status": result.get("klaviyo_status", ""),
                "sent": result.get("sent", ""),
            }
        )

    output_path = Path(args.output_csv).expanduser()
    fieldnames = [
        "status",
        "reason",
        "email",
        "activity_date",
        "order",
        "order_id",
        "order_date",
        "order_status",
        "match_reason",
        "products",
        "recommendation_mode",
        "purchased_product",
        "matched_product",
        "available_recommendations",
        "klaviyo_status",
        "sent",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    print("mode", "live" if args.live else "dry_run")
    print("output_csv", output_path)
    for key, value in counters.most_common():
        print(key, value)

    failed = [
        row
        for row in output_rows
        if row.get("status") not in {"event_ready", "sent", "not_sent"}
    ]
    return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill skipped Klaviyo recommendation events.")
    parser.add_argument("--skipped-csv", default=str(DEFAULT_SKIPPED_CSV))
    parser.add_argument(
        "--orders-csv",
        action="append",
        help="WooCommerce order export CSV. Repeat to provide multiple files. Defaults to the April/May exports.",
    )
    parser.add_argument("--recommendations-csv", default=str(DEFAULT_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    parser.add_argument("--event-name", default=os.getenv("KLAVIYO_EVENT_NAME", DEFAULT_EVENT_NAME))
    parser.add_argument("--revision", default=os.getenv("KLAVIYO_REVISION", DEFAULT_REVISION))
    parser.add_argument("--min-recommendations", type=int, default=int(os.getenv("MIN_RECOMMENDATIONS", "3")))
    parser.add_argument("--live", action="store_true", help="Send events to Klaviyo. Default is dry run.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return backfill(args)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
