import base64
import json
import logging
import time
from threading import Lock
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


PROCESSED_EVENT_TTL_SECONDS = 60 * 60
IN_FLIGHT_EVENT_TTL_SECONDS = 5 * 60
_SLACK_EVENT_STATES: dict[str, dict[str, Any]] = {}
_PROCESSED_EVENTS_LOCK = Lock()
_EVENT_STATE_IN_FLIGHT = "in_flight"
_EVENT_STATE_PROCESSED = "processed"
_SENSITIVE_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "set-cookie",
        "x-slack-signature",
        "x-forwarded-for",
        "cf-connecting-ip",
    }
)
_HEADERS_TO_LOG = (
    "user-agent",
    "x-amzn-trace-id",
    "x-slack-request-timestamp",
    "x-slack-retry-num",
    "x-slack-retry-reason",
    "x-slack-signature",
)


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


def _build_genie_usage_message() -> str:
    # Import tardio para evitar custo de import no cold start antes de uso real.
    from data_slacklake.services.ai_service import list_configured_genie_commands

    commands = list_configured_genie_commands()
    if commands:
        commands_text = ", ".join(commands)
        first_command = commands[0]
        return (
            "Me envie uma pergunta para consultar a Genie.\n"
            f"Comandos configurados: {commands_text}\n"
            f"Exemplo: `{first_command} quanto operações tivemos esse ano?`"
        )

    return "Me envie uma pergunta para consultar a Genie. Exemplo: `quanto operações tivemos esse ano?`"


def _lowercase_headers(raw_headers: dict[str, Any] | None) -> dict[str, str]:
    if not raw_headers:
        return {}
    return {str(key).lower(): str(value) for key, value in raw_headers.items()}


def _ok_response() -> dict[str, Any]:
    return {"statusCode": 200, "body": "OK"}


def _parse_json_body(body_content: str) -> dict[str, Any] | None:
    if not body_content:
        return None
    try:
        parsed_body = json.loads(body_content)
    except json.JSONDecodeError:
        return None
    return parsed_body if isinstance(parsed_body, dict) else None


def _decode_request_body(event: dict[str, Any]) -> str:
    body_content = event.get("body", "")
    is_base64_encoded = bool(event.get("isBase64Encoded", False))

    if is_base64_encoded and body_content:
        try:
            return base64.b64decode(body_content).decode("utf-8")
        except Exception as exc:
            raise ValueError("Bad Request: Invalid Base64") from exc
    return body_content


def _redact_header_value(header_name: str, header_value: str) -> str:
    if header_name in _SENSITIVE_HEADERS:
        return "[REDACTED]"
    return header_value


def _build_event_log_summary(
    event: dict[str, Any], headers_lower: dict[str, str], body_json: dict[str, Any] | None
) -> dict[str, Any]:
    event_payload = body_json.get("event", {}) if body_json else {}
    headers_summary = {
        header_name: _redact_header_value(header_name, headers_lower.get(header_name, ""))
        for header_name in _HEADERS_TO_LOG
        if header_name in headers_lower
    }
    return {
        "requestContext": {"path": event.get("path"), "httpMethod": event.get("httpMethod")},
        "headers": headers_summary,
        "slack_event": {
            "type": body_json.get("type") if body_json else None,
            "event_id": body_json.get("event_id") if body_json else None,
            "event_type": event_payload.get("type"),
            "team_id": body_json.get("team_id") if body_json else None,
            "channel": event_payload.get("channel"),
            "user": event_payload.get("user"),
            "thread_ts": event_payload.get("thread_ts") or event_payload.get("ts"),
        },
    }


def _prune_processed_event_ids(now_timestamp: float) -> None:
    processed_expiration_limit = now_timestamp - PROCESSED_EVENT_TTL_SECONDS
    in_flight_expiration_limit = now_timestamp - IN_FLIGHT_EVENT_TTL_SECONDS
    expired_event_ids = []
    for event_id, state_data in _SLACK_EVENT_STATES.items():
        updated_at = float(state_data.get("updated_at", 0.0))
        status = str(state_data.get("status", ""))
        if status == _EVENT_STATE_PROCESSED and updated_at < processed_expiration_limit:
            expired_event_ids.append(event_id)
        elif status == _EVENT_STATE_IN_FLIGHT and updated_at < in_flight_expiration_limit:
            expired_event_ids.append(event_id)

    for event_id in expired_event_ids:
        _SLACK_EVENT_STATES.pop(event_id, None)


def _is_duplicate_slack_event(body_json: dict[str, Any] | None) -> tuple[bool, str | None, str | None]:
    if not body_json or body_json.get("type") != "event_callback":
        return False, None, None

    event_id = str(body_json.get("event_id", "")).strip()
    if not event_id:
        return False, None, None

    now_timestamp = time.time()
    with _PROCESSED_EVENTS_LOCK:
        _prune_processed_event_ids(now_timestamp)
        state_data = _SLACK_EVENT_STATES.get(event_id) or {}
        status = str(state_data.get("status", ""))
        if status == _EVENT_STATE_PROCESSED:
            return True, event_id, _EVENT_STATE_PROCESSED
        if status == _EVENT_STATE_IN_FLIGHT:
            return True, event_id, _EVENT_STATE_IN_FLIGHT

        _SLACK_EVENT_STATES[event_id] = {"status": _EVENT_STATE_IN_FLIGHT, "updated_at": now_timestamp}

    return False, event_id, None


def _finalize_slack_event_processing(event_id: str | None, was_successful: bool) -> None:
    if not event_id:
        return

    now_timestamp = time.time()
    with _PROCESSED_EVENTS_LOCK:
        _prune_processed_event_ids(now_timestamp)
        if was_successful:
            _SLACK_EVENT_STATES[event_id] = {"status": _EVENT_STATE_PROCESSED, "updated_at": now_timestamp}
            return
        _SLACK_EVENT_STATES.pop(event_id, None)


def _handle_url_verification_if_present(body_json: dict[str, Any] | None) -> dict[str, Any] | None:
    if not body_json:
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
        usage_message = _build_genie_usage_message()
        say(f"Olá <@{user_id}>! {usage_message}", thread_ts=thread_ts)
        return

    logger.info("Pergunta de %s: %s", user_id, user_question)
    say(f"Olá <@{user_id}>! Consultando a Genie...", thread_ts=thread_ts)

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
    request_id = getattr(context, "aws_request_id", "unknown-request-id")
    started_at = time.perf_counter()
    response_status = 500
    tracked_event_id: str | None = None
    should_finalize_tracked_event = False
    tracked_event_success = False

    headers = event.get("headers", {})
    headers_lower = _lowercase_headers(headers)

    try:
        if "elb-healthchecker" in headers_lower.get("user-agent", ""):
            response_status = 200
            return _ok_response()

        if "x-slack-retry-num" in headers_lower:
            logger.info(
                "Retry do Slack detectado (request_id=%s, retry_num=%s, retry_reason=%s).",
                request_id,
                headers_lower.get("x-slack-retry-num"),
                headers_lower.get("x-slack-retry-reason"),
            )

        try:
            body_content = _decode_request_body(event)
        except ValueError as exc:
            logger.warning("Falha ao decodificar body da requisição: %s", exc)
            response_status = 400
            return {"statusCode": 400, "body": str(exc)}

        body_json = _parse_json_body(body_content)
        logger.info(
            "EVENTO RECEBIDO: %s",
            json.dumps(_build_event_log_summary(event, headers_lower, body_json), ensure_ascii=False),
        )

        url_verification_response = _handle_url_verification_if_present(body_json)
        if url_verification_response:
            response_status = int(url_verification_response.get("statusCode", 200))
            return url_verification_response

        is_duplicate, event_id, duplicate_status = _is_duplicate_slack_event(body_json)
        if is_duplicate:
            logger.info("event_id=%s já está em status='%s'. Ignorando duplicidade.", event_id, duplicate_status)
            response_status = 200
            return _ok_response()

        tracked_event_id = event_id
        should_finalize_tracked_event = bool(event_id)

        bolt_req = BoltRequest(
            body=body_content,
            query=event.get("queryStringParameters", {}),
            headers=headers,
        )

        bolt_resp: BoltResponse = app.dispatch(bolt_req)

        logger.info("STATUS DO BOLT: %s", bolt_resp.status)

        response_status = bolt_resp.status
        tracked_event_success = 200 <= int(bolt_resp.status) < 300
        return {
            "statusCode": bolt_resp.status,
            "body": bolt_resp.body,
            "headers": bolt_resp.headers,
        }
    finally:
        if should_finalize_tracked_event:
            _finalize_slack_event_processing(tracked_event_id, tracked_event_success)

        duration_ms = (time.perf_counter() - started_at) * 1000
        logger.info(
            "FIM REQUEST (request_id=%s, status=%s, duration_ms=%.2f)",
            request_id,
            response_status,
            duration_ms,
        )
