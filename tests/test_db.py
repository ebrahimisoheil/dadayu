from dadayu.db import get_ch_client


def test_get_ch_client_returns_client(monkeypatch):
    import clickhouse_connect

    monkeypatch.setenv("CLICKHOUSE_HOST", "localhost")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
    monkeypatch.setenv("CLICKHOUSE_DB", "dadayu")
    monkeypatch.setenv("CLICKHOUSE_USER", "dadayu")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "")

    calls = []

    def fake_get_client(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(clickhouse_connect, "get_client", fake_get_client)

    client = get_ch_client()

    assert client is not None
    assert calls[0]["host"] == "localhost"
    assert calls[0]["port"] == 8123
    assert calls[0]["database"] == "dadayu"
    assert calls[0]["username"] == "dadayu"
