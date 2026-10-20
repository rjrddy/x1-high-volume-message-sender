import pytest
from main import DELIVER, enqueue, server


def test_enqueue_conflict_and_sink_deduplication():
    with server() as (client, _):
        assert enqueue(client, "a", "synthetic")
        assert not enqueue(client, "a", "synthetic")
        with pytest.raises(ValueError):
            enqueue(client, "a", "different")
        assert client.eval(DELIVER, 0, "a", 1) == 1
        assert client.eval(DELIVER, 0, "a", 2) == 0
        assert client.hget("sends", "a") == "1"
