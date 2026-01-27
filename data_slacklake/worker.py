"""
SQS worker that processes Slack jobs asynchronously and replies back to Slack.

Expected SQS message body (JSON):
{
  "event_id": "...",
  "channel": "C123",
  "user": "U123",
  "text": "pergunta normalizada",
  "reply_thread_ts": "12345.6789"
}
"""

import json
from typing import Any, Dict

from data_slacklake.config import logger
from data_slacklake.services.ai_service import process_question
from data_slacklake.services.idempotency_service import is_done, update_state
from data_slacklake.services.slack_service import get_client, post_message, update_message


def _handle_job(job: Dict[str, Any]) -> None:
    event_id = job.get("event_id")
    channel = job.get("channel")
    reply_thread_ts = job.get("reply_thread_ts")
    user = job.get("user")
    text = job.get("text")

    if not event_id or not channel or not reply_thread_ts or not text:
        raise ValueError(f"Job inválido: {job}")

    done, state = is_done(event_id)
    if done:
        logger.info("Job já concluído; ignorando duplicata", extra={"event_id": event_id})
        return

    client = get_client()

    processing_ts = None
    if state:
        processing_ts = state.get("processing_message_ts")

    try:
        update_state(event_id, status="PROCESSING")

        if not processing_ts:
            resp = post_message(
                client,
                channel=channel,
                thread_ts=reply_thread_ts,
                text=f"Olá <@{user}>! Estou processando sua pergunta…",
            )
            processing_ts = resp.get("ts")
            update_state(event_id, processing_message_ts=processing_ts)

        resposta, sql_debug = process_question(text)

        # Update the processing message with the final answer
        update_message(client, channel=channel, ts=processing_ts, text=resposta)

        # Optional debug message in the same thread
        if sql_debug:
            post_message(
                client,
                channel=channel,
                thread_ts=reply_thread_ts,
                text=f"*Debug SQL:* ```{sql_debug}```",
            )

        update_state(event_id, status="DONE")

    except Exception as e:
        logger.error("Erro no worker", extra={"event_id": event_id, "error": str(e)}, exc_info=True)
        if processing_ts:
            update_message(
                client,
                channel=channel,
                ts=processing_ts,
                text=f"Desculpe, ocorreu um erro ao processar sua solicitação. Detalhe: {str(e)}",
            )
        else:
            post_message(
                client,
                channel=channel,
                thread_ts=reply_thread_ts,
                text=f"Desculpe, ocorreu um erro ao processar sua solicitação. Detalhe: {str(e)}",
            )
        update_state(event_id, status="FAILED", last_error=str(e))
        raise


def handler(event, context):  # pylint: disable=unused-argument
    """
    AWS Lambda entrypoint for SQS events.
    """
    records = event.get("Records", [])
    for record in records:
        body = record.get("body", "")
        job = json.loads(body) if body else {}
        _handle_job(job)

    return {"statusCode": 200}

