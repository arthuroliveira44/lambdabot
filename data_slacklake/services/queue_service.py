"""
Queue service responsible for enqueueing jobs for async processing.
"""

import json
from typing import Any, Dict

import boto3

from data_slacklake.config import AWS_REGION, SQS_QUEUE_URL, logger


def enqueue_job(job: Dict[str, Any]) -> str:
    """
    Enqueue a job payload to SQS.
    Returns the SQS MessageId.
    """
    if not SQS_QUEUE_URL:
        raise ValueError("SQS_QUEUE_URL não configurado (async não habilitado).")

    sqs = boto3.client("sqs", region_name=AWS_REGION)
    resp = sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(job, ensure_ascii=False),
    )
    message_id = resp.get("MessageId", "")
    logger.info("Job enfileirado no SQS", extra={"message_id": message_id})
    return message_id

