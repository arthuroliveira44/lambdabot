from unittest.mock import patch


@patch("worker.process_app_mention_event")
def test_worker_processa_evento_de_app_mention(mock_process_event):
    from worker import handler

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


@patch("worker.process_app_mention_event", side_effect=RuntimeError("falha worker"))
def test_worker_retorna_500_quando_falha(mock_process_event):
    from worker import handler

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
    assert "falha worker" in response["body"]
    mock_process_event.assert_called_once()
