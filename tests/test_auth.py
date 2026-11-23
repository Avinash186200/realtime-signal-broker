import jwt
import pytest

from sigbroker.auth import AuthError, mint_token, new_secret, secret_from_env, verify_token


def test_round_trip_and_topic_patterns(secret):
    c = verify_token(secret, mint_token(secret, "desk-a", "subscriber", ["signals.*", "alerts.fx"]))
    assert c.sub == "desk-a" and c.role == "subscriber"
    assert c.may_access("signals.rates") and c.may_access("alerts.fx")
    assert not c.may_access("alerts.rates") and not c.may_access("signalsXrates")


def test_wrong_secret_expired_and_tampered(secret):
    tok = mint_token(secret, "a", "subscriber", ["x"])
    with pytest.raises(AuthError):
        verify_token(new_secret(), tok)
    old = mint_token(secret, "a", "subscriber", ["x"], ttl_s=10, now=1_000_000)
    with pytest.raises(AuthError):
        verify_token(secret, old)
    head, body, sig = tok.split(".")
    with pytest.raises(AuthError):
        verify_token(secret, f"{head}.{body}x.{sig}")


def test_unsigned_and_wrong_algorithm_tokens_rejected(secret):
    claims = {"iss": "sigbroker", "sub": "a", "role": "publisher", "topics": ["*"], "iat": 1, "exp": 2**31}
    none_tok = jwt.encode(claims, key=None, algorithm="none")
    with pytest.raises(AuthError):
        verify_token(secret, none_tok)
    hs512 = jwt.encode(claims, secret, algorithm="HS512")
    with pytest.raises(AuthError):
        verify_token(secret, hs512)


def test_bad_claims_rejected(secret):
    claims = {"iss": "sigbroker", "sub": "a", "role": "admin", "topics": ["*"], "iat": 1, "exp": 2**31}
    with pytest.raises(AuthError):
        verify_token(secret, jwt.encode(claims, secret, algorithm="HS256"))


def test_secret_rules(monkeypatch):
    with pytest.raises(ValueError):
        mint_token("short", "a", "subscriber", ["x"])
    with pytest.raises(ValueError):
        mint_token(new_secret(), "a", "root", ["x"])
    monkeypatch.delenv("SIGBROKER_SECRET", raising=False)
    with pytest.raises(AuthError):
        secret_from_env()
    s = new_secret()
    monkeypatch.setenv("SIGBROKER_SECRET", s)
    assert secret_from_env() == s and len(s) == 64
