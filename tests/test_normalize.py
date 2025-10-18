from app.templates import spot_message
from datetime import datetime, timezone

def test_spot_message_smoke():
    msg = spot_message("KUCOIN", "RVV", datetime(2025,10,18,13,34,tzinfo=timezone.utc), 2, "KuCoin symbols API", "https://example")
    assert "KUCOIN SPOT LISTING ALERT" in msg