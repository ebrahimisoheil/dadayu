from dadayu.db import get_pg_client


def test_get_pg_client_returns_client(monkeypatch):
    import psycopg

    monkeypatch.setenv("DADAYU_PG_HOST", "localhost")
    monkeypatch.setenv("DADAYU_PG_PORT", "5432")
    monkeypatch.setenv("DADAYU_PG_DB", "dadayu")
    monkeypatch.setenv("DADAYU_PG_USER", "dadayu")
    monkeypatch.setenv("DADAYU_PG_PASSWORD", "secret")

    calls = []

    class FakeConnection:
        autocommit = True

        def close(self):
            pass

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return FakeConnection()

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    client = get_pg_client()

    assert client is not None
    assert calls[0]["host"] == "localhost"
    assert calls[0]["port"] == 5432
    assert calls[0]["dbname"] == "dadayu"
    assert calls[0]["user"] == "dadayu"
    assert calls[0]["password"] == "secret"
