"""
Idempotency/state tracking using DynamoDB (optional).
"""

import time
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from data_slacklake.config import AWS_REGION, IDEMPOTENCY_TABLE_NAME, logger


DEFAULT_TTL_SECONDS = 60 * 60 * 6  # 6h


def _table():
    if not IDEMPOTENCY_TABLE_NAME:
        return None
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    return dynamodb.Table(IDEMPOTENCY_TABLE_NAME)


def try_claim_event(event_id: str, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> bool:
    """
    Attempts to claim an event_id for processing (idempotency).
    Returns True if claimed, False if it already exists.
    If table is not configured, always returns True (best-effort).
    """
    table = _table()
    if table is None:
        return True

    now = int(time.time())
    ttl = now + ttl_seconds

    try:
        table.put_item(
            Item={
                "event_id": event_id,
                "status": "RECEIVED",
                "created_at": now,
                "updated_at": now,
                "ttl": ttl,
            },
            ConditionExpression="attribute_not_exists(event_id)",
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def update_state(event_id: str, **attributes: Any) -> None:
    """
    Updates the event state (best-effort).
    """
    table = _table()
    if table is None:
        return

    now = int(time.time())
    attributes = {k: v for k, v in attributes.items() if v is not None}
    attributes["updated_at"] = now

    update_expr_parts = []
    expr_attr_names: Dict[str, str] = {}
    expr_attr_values: Dict[str, Any] = {}

    for i, (k, v) in enumerate(attributes.items()):
        name_key = f"#k{i}"
        value_key = f":v{i}"
        expr_attr_names[name_key] = k
        expr_attr_values[value_key] = v
        update_expr_parts.append(f"{name_key} = {value_key}")

    update_expression = "SET " + ", ".join(update_expr_parts)

    table.update_item(
        Key={"event_id": event_id},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values,
    )


def get_state(event_id: str) -> Optional[Dict[str, Any]]:
    table = _table()
    if table is None:
        return None
    resp = table.get_item(Key={"event_id": event_id})
    return resp.get("Item")


def is_done(event_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    item = get_state(event_id)
    if not item:
        return False, None
    return item.get("status") == "DONE", item

