"""Token auth: HS256 JWTs with a role and a topic allow-list.

The secret comes from the ``SIGBROKER_SECRET`` environment variable. Tests and the demo
generate a random secret at runtime; no secret is stored in the repo.
"""
from __future__ import annotations

import fnmatch
import os
import secrets
import time
from dataclasses import dataclass

import jwt

ALGORITHM = "HS256"
ISSUER = "sigbroker"
MIN_SECRET_BYTES = 32
ROLES = ("subscriber", "publisher")


class AuthError(Exception):
    pass


def new_secret() -> str:
    """A fresh random secret (64 hex chars). Used by tests and the demo."""
    return secrets.token_hex(32)


def secret_from_env(var: str = "SIGBROKER_SECRET") -> str:
    value = os.environ.get(var, "")
    if len(value.encode()) < MIN_SECRET_BYTES:
        raise AuthError(f"{var} must be set to at least {MIN_SECRET_BYTES} bytes")
    return value


@dataclass(frozen=True)
class Claims:
    sub: str
    role: str
    topics: tuple[str, ...]  # fnmatch patterns, e.g. "signals.*"
    exp: int

    def may_access(self, topic: str) -> bool:
        return any(fnmatch.fnmatchcase(topic, pat) for pat in self.topics)


def mint_token(secret: str, sub: str, role: str, topics: list[str] | tuple[str, ...],
               ttl_s: int = 3600, now: float | None = None) -> str:
    if role not in ROLES:
        raise ValueError(f"role must be one of {ROLES}")
    if len(secret.encode()) < MIN_SECRET_BYTES:
        raise ValueError(f"secret must be at least {MIN_SECRET_BYTES} bytes")
    iat = int(now if now is not None else time.time())
    claims = {"iss": ISSUER, "sub": sub, "role": role, "topics": list(topics),
              "iat": iat, "exp": iat + int(ttl_s)}
    return jwt.encode(claims, secret, algorithm=ALGORITHM)


def verify_token(secret: str, token: str, leeway_s: int = 5) -> Claims:
    try:
        data = jwt.decode(token, secret, algorithms=[ALGORITHM], issuer=ISSUER, leeway=leeway_s,
                          options={"require": ["exp", "iat", "sub", "iss"]})
    except jwt.PyJWTError as e:
        raise AuthError(f"invalid token: {type(e).__name__}") from None
    role = data.get("role")
    topics = data.get("topics")
    if role not in ROLES or not isinstance(topics, list) or not all(isinstance(t, str) for t in topics):
        raise AuthError("invalid token: bad role or topics claim")
    return Claims(sub=str(data["sub"]), role=role, topics=tuple(topics), exp=int(data["exp"]))
