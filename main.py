import json
import base64
import logging
from slack_bolt import App
from slack_bolt.adapter.aws_lambda import SlackRequestHandler
from slack_bolt.request import BoltRequest
from slack_bolt.response import BoltResponse
from data_slacklake.config import SLACK_BOT_TOKEN, SLACK_SIGNING_SECRET

logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


app = App(
    token=SLACK_BOT_TOKEN,
    signing_secret=SLACK_SIGNING_SECRET,
    process_before_response=True
)

slack_handler = SlackRequestHandler(app)


@app.event("app_mention")
def handle_app_mentions(body, say):
    """
    Listener for when the bot is mentioned in the channels.
    """
    event = body["event"]
    text = event.get("text", "")
    user = event.get("user", "Desconhecido")
    ts = event.get("ts")

    if ">" in text:
        pergunta = text.split(">", 1)[1].strip()
    else:
        pergunta = text.strip()

    if not pergunta:
        say(f"Olá <@{user}>! Como posso ajudar?")
        return

    logger.info(f"Pergunta de {user}: {pergunta}")
    say(f"Olá <@{user}>! Processando sua pergunta: *'{pergunta}'*...")

    try:
        from data_slacklake.services.ai_service import process_question
        resposta, sql_debug = process_question(pergunta)
        say(resposta)
        if sql_debug:
            say(f"*Debug SQL:* ```{sql_debug}```", thread_ts=ts)
    except Exception as e:
        logger.error(f"Erro: {str(e)}", exc_info=True)
        say(f"Erro: {str(e)}")


def handler(event, context):
    """
    Entrypoint do AWS Lambda.
    """

    logger.info(f"EVENTO RECEBIDO: {json.dumps(event)}")

    headers = event.get('headers', {})

    headers_lower = {k.lower(): v for k, v in headers.items()}

    if 'elb-healthchecker' in headers_lower.get('user-agent', ''):
        return {"statusCode": 200, "body": "OK"}

    if 'x-slack-retry-num' in headers_lower:
        logger.info("Retry do Slack detectado. Ignorando para evitar duplicidade.")
        return {"statusCode": 200, "body": "OK"}

    body_content = event.get('body', '')
    if event.get('isBase64Encoded', False) and body_content:
        try:
            body_content = base64.b64decode(body_content).decode('utf-8')
        except Exception as e:
            return {"statusCode": 400, "body": "Bad Request: Invalid Base64"}

    try:
        if body_content:
            body_json = json.loads(body_content)
            if body_json.get('type') == 'url_verification':
                logger.info("Detectado url_verification. Respondendo manualmente.")
                return {
                    "statusCode": 200,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"challenge": body_json['challenge']})
                }
    except Exception:
        pass

    bolt_req = BoltRequest(
        body=body_content,
        query=event.get("queryStringParameters", {}),
        headers=headers
    )

    bolt_resp: BoltResponse = app.dispatch(bolt_req)

    logger.info(f"STATUS DO BOLT: {bolt_resp.status}")

    return {
        "statusCode": bolt_resp.status,
        "body": bolt_resp.body,
        "headers": bolt_resp.headers
    }
