"""Testes unitários para o Lambda worker."""

from unittest.mock import patch

from worker import handler


@patch("worker.process_app_mention_event")
@patch("worker._get_slack_user_display_name", return_value="Arthur Oliveira")
def test_worker_processa_evento_de_app_mention(_mock_get_user_name, mock_process_event):
    """Worker deve processar payload válido com sucesso."""
    event = {
        "event_id": "EvWorker123",
        "event_payload": {
            "type": "app_mention",
            "channel": "C123",
            "user": "U123",
            "ts": "111.222",
            "text": "<@BOT> oi",
        },
    }

    response = handler(event, context={})

    assert response["statusCode"] == 200
    mock_process_event.assert_called_once()
    payload_enviado = mock_process_event.call_args.args[0]
    assert payload_enviado["username"] == "Arthur Oliveira"


@patch("worker.process_app_mention_event", side_effect=RuntimeError("falha worker"))
@patch("worker._get_slack_user_display_name", return_value=None)
def test_worker_retorna_500_quando_falha(_mock_get_user_name, mock_process_event):
    """Worker deve retornar erro quando processamento levantar exceção."""
    event = {
        "event_id": "EvWorkerError",
        "event_payload": {
            "type": "app_mention",
            "channel": "C123",
            "user": "U123",
            "ts": "111.222",
            "text": "<@BOT> oi",
        },
    }

    response = handler(event, context={})

    assert response["statusCode"] == 500
    assert response["body"] == "Internal Server Error"
    mock_process_event.assert_called_once()
