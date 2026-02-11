import base64
import json
import logging
from typing import Any, Callable

from slack_bolt import App
from slack_bolt.request import BoltRequest
from slack_bolt.response import BoltResponse

from data_slacklake.config import SLACK_BOT_TOKEN, SLACK_SIGNING_SECRET


def _configure_logger() -> logging.Logger:
    configured_logger = logging.getLogger()
    if configured_logger.handlers:
        for existing_handler in list(configured_logger.handlers):
            configured_logger.removeHandler(existing_handler)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return configured_logger


def _extract_question_from_mention(message_text: str) -> str:
    if ">" in message_text:
        return message_text.split(">", 1)[1].strip()
    return message_text.strip()


def _build_conversation_key(event_payload: dict[str, Any]) -> str:
    """
    Gera uma chave estável por canal/thread/usuário para memória conversacional.
    """
    channel_id = event_payload.get("channel", "unknown-channel")
    thread_ts = event_payload.get("thread_ts") or event_payload.get("ts") or "no-thread"
    user_id = event_payload.get("user", "unknown-user")
    return f"slack:{channel_id}:{thread_ts}:{user_id}"


def _lowercase_headers(raw_headers: dict[str, Any] | None) -> dict[str, str]:
    if not raw_headers:
        return {}
    return {str(key).lower(): str(value) for key, value in raw_headers.items()}


def _ok_response() -> dict[str, Any]:
    return {"statusCode": 200, "body": "OK"}


def _decode_request_body(event: dict[str, Any]) -> str:
    body_content = event.get("body", "")
    is_base64_encoded = bool(event.get("isBase64Encoded", False))

    if is_base64_encoded and body_content:
        try:
            return base64.b64decode(body_content).decode("utf-8")
        except Exception as exc:
            raise ValueError("Bad Request: Invalid Base64") from exc
    return body_content


def _handle_url_verification_if_present(body_content: str) -> dict[str, Any] | None:
    if not body_content:
        return None

    try:
        body_json = json.loads(body_content)
    except json.JSONDecodeError:
        return None

    if body_json.get("type") != "url_verification":
        return None

    logger.info("Detectado url_verification. Respondendo manualmente.")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"challenge": body_json["challenge"]}),
    }


logger = _configure_logger()


app = App(
    token=SLACK_BOT_TOKEN,
    signing_secret=SLACK_SIGNING_SECRET,
    process_before_response=True,
)


@app.event("app_mention")
def handle_app_mentions(body: dict[str, Any], say: Callable[..., Any]) -> None:
    """
    Listener for when the bot is mentioned in the channels.
    """
    event_payload = body.get("event", {})
    message_text = event_payload.get("text", "")
    user_id = event_payload.get("user", "Desconhecido")
    event_ts = event_payload.get("ts")
    thread_ts = event_payload.get("thread_ts") or event_ts
    user_question = _extract_question_from_mention(message_text)

    if not user_question:
        say(f"Olá <@{user_id}>! Como posso ajudar?", thread_ts=thread_ts)
        return

    logger.info("Pergunta de %s: %s", user_id, user_question)
    say(f"Olá <@{user_id}>! Processando sua pergunta: *'{user_question}'*...", thread_ts=thread_ts)

    try:
        from data_slacklake.services.ai_service import process_question

        conversation_key = _build_conversation_key(event_payload)
        answer_text, sql_debug = process_question(user_question, conversation_key=conversation_key)
        say(answer_text, thread_ts=thread_ts)
        if sql_debug:
            say(f"*Debug SQL:* ```{sql_debug}```", thread_ts=thread_ts)
    except Exception as exc:
        logger.error("Erro ao processar menção: %s", exc, exc_info=True)
        say(f"Erro crítico: {str(exc)}", thread_ts=thread_ts)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Entrypoint do AWS Lambda.
    """
    del context  # Contexto não utilizado; mantido por compatibilidade da assinatura Lambda.

    logger.info("EVENTO RECEBIDO: %s", json.dumps(event))

    headers = event.get("headers", {})
    headers_lower = _lowercase_headers(headers)

    if "elb-healthchecker" in headers_lower.get("user-agent", ""):
        return _ok_response()

    if "x-slack-retry-num" in headers_lower:
        logger.info("Retry do Slack detectado. Ignorando para evitar duplicidade.")
        return _ok_response()

    try:
        body_content = _decode_request_body(event)
    except ValueError as exc:
        logger.warning("Falha ao decodificar body da requisição: %s", exc)
        return {"statusCode": 400, "body": str(exc)}

    url_verification_response = _handle_url_verification_if_present(body_content)
    if url_verification_response:
        return url_verification_response

    bolt_req = BoltRequest(
        body=body_content,
        query=event.get("queryStringParameters", {}),
        headers=headers,
    )

    bolt_resp: BoltResponse = app.dispatch(bolt_req)

    logger.info("STATUS DO BOLT: %s", bolt_resp.status)

    return {
        "statusCode": bolt_resp.status,
        "body": bolt_resp.body,
        "headers": bolt_resp.headers,
    }
