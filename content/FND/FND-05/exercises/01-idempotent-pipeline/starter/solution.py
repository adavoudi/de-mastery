"""Starter for FND-05/01-idempotent-pipeline. See README.md."""

from __future__ import annotations


def dedup(events, key_fields) -> list[dict]:
    # TODO: keep the first row per key tuple, preserving order.
    raise NotImplementedError


def apply_idempotent(state, events, key_fields) -> dict:
    # TODO: first-write-wins upsert into a NEW dict keyed by the dedup key.
    raise NotImplementedError


def revenue_by_product(events) -> list[dict]:
    # TODO: dedup on (company_id, order_item_id), then sum/count per
    #       (company_id, product_code).
    raise NotImplementedError
