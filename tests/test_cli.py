import os
import subprocess
import sys

import pytest

from conftest import run
from sigbroker.auth import new_secret, verify_token
from sigbroker.cli import main
from sigbroker.demo import run_demo


def test_token_command(monkeypatch, capsys):
    s = new_secret()
    monkeypatch.setenv("SIGBROKER_SECRET", s)
    main(["token", "--sub", "desk-a", "--role", "subscriber", "--topics", "signals.*"])
    tok = capsys.readouterr().out.strip()
    c = verify_token(s, tok)
    assert c.sub == "desk-a" and c.topics == ("signals.*",)


def test_token_command_needs_a_secret(monkeypatch):
    monkeypatch.delenv("SIGBROKER_SECRET", raising=False)
    with pytest.raises(SystemExit):
        main(["token", "--sub", "a", "--role", "publisher", "--topics", "x"])


def test_demo_runs_clean(capsys):
    out = run(run_demo(30, new_secret()), timeout=60)
    assert out["desk"]["exact"] and out["dashboard"]["exact"]
    assert out["desk"]["reconnects"] >= 2
    assert "in order with no gap or duplicate: True" in capsys.readouterr().out


def test_serve_prints_port_and_stops(tmp_path):
    env = dict(os.environ, SIGBROKER_SECRET=new_secret())
    p = subprocess.Popen([sys.executable, "-m", "sigbroker.cli", "serve", "--port", "0", "--backend",
                          f"sqlite:{tmp_path / 'log.sqlite'}"], env=env, stdout=subprocess.PIPE, text=True)
    try:
        line = p.stdout.readline()
        assert line.startswith("PORT ") and int(line.split()[1]) > 0
    finally:
        p.terminate()
        p.wait(10)
