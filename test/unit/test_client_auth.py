import importlib.util
import sys
from unittest.mock import Mock

import pytest
import requests
import websockets
from websockets.asyncio.server import serve

import apsis.cli
import apsis.ctl
from apsis.service import DEFAULT_PORT
from apsis.service.client import (
    AUTH_ERROR,
    NO_HTTPS_ERROR,
    NO_KERBEROS_ERROR,
    APIError,
    Client,
    get_negotiate_headers,
    get_ws_msgs,
)


def _response(status: int, content: bytes = b"{}") -> requests.Response:
    resp = requests.Response()
    resp.status_code = status
    resp._content = content
    return resp


@pytest.mark.parametrize(
    "flags, url, error",
    [
        ([], "https://apsis.example.com/api/v1/jobs/j", AUTH_ERROR),
        (["--host", "h"], "http://h:6000/api/v1/jobs/j", NO_HTTPS_ERROR),
    ],
)
def test_cli_url_and_auth_error(monkeypatch, capsys, flags, url, error):
    monkeypatch.setattr("apsis.service.client.HTTPSPNEGOAuth", Mock())
    monkeypatch.setenv("APSIS_URL", "https://apsis.example.com")
    monkeypatch.setenv("APSIS_HOST", "other:6000")
    urls = []
    monkeypatch.setattr(
        "requests.request", lambda method, url, **kw: urls.append(url) or _response(401)
    )
    monkeypatch.setattr("sys.argv", ["apsis", *flags, "job", "j"])
    with pytest.raises(SystemExit) as exc:
        apsis.cli.main()
    assert exc.value.code == 1
    assert urls == [url]
    assert error in " ".join(capsys.readouterr().out.split())


@pytest.mark.parametrize(
    "argv, url, out",
    [
        (
            ["version"],
            "https://apsis.example.com/api/control/version",
            "server address: https://apsis.example.com",
        ),
        (["--host", "h", "version"], "http://h:6000/api/control/version", "server address: h:6000"),
    ],
)
def test_ctl_client_url(monkeypatch, capsys, argv, url, out):
    monkeypatch.setenv("APSIS_URL", "https://apsis.example.com")
    monkeypatch.setenv("APSIS_HOST", "other:6000")
    urls = []
    body = {"version": "v"}
    monkeypatch.setattr(
        "requests.request",
        lambda method, url, **kw: (
            urls.append(url.split("?")[0]) or Mock(status_code=200, json=lambda: body)
        ),
    )
    monkeypatch.setattr("sys.argv", ["apsisctl", *argv])
    with pytest.raises(SystemExit) as exc:
        apsis.ctl.main()
    assert exc.value.code == 0
    assert urls == [url]
    assert out in capsys.readouterr().out


def test_ctl_serve_binds_serve_defaults(monkeypatch):
    monkeypatch.setenv("APSIS_URL", "https://apsis.example.com")
    monkeypatch.setattr("apsis.config.load", lambda path: {})
    binds = []
    monkeypatch.setattr(
        "apsis.service.main.serve", lambda cfg, host, port, debug, **kw: binds.append((host, port))
    )
    monkeypatch.setattr("sys.argv", ["apsisctl", "serve"])
    with pytest.raises(SystemExit):
        apsis.ctl.main()
    assert binds == [("127.0.0.1", DEFAULT_PORT)]


@pytest.mark.parametrize(
    "var, value, base",
    [
        ("APSIS_URL", "https://apsis.example.com/", ("https", "wss", "apsis.example.com")),
        ("APSIS_HOST", "other:6000", ("http", "ws", "other:6000")),
    ],
)
def test_ws_scheme_follows_base_url(monkeypatch, var, value, base):
    http, ws, netloc = base
    monkeypatch.delenv("APSIS_URL", raising=False)
    monkeypatch.setenv(var, value)
    client = Client()
    assert client._Client__url("/api/v1/runs") == f"{http}://{netloc}/api/v1/runs"
    assert client._Client__url("/api/v1/runs", ws=True) == f"{ws}://{netloc}/api/v1/runs"


def _creds(lifetime: int):
    return lambda **kw: Mock(lifetime=lifetime)


def _gss_error(**kw):
    import gssapi

    raise gssapi.exceptions.GSSError(0, 0)


@pytest.mark.parametrize(
    "url, creds, authed",
    [
        ("https://apsis.example.com", _creds(1), True),
        ("https://apsis.example.com", _creds(0), False),
        ("https://apsis.example.com", _gss_error, False),
        ("http://apsis.example.com", _creds(1), False),
    ],
)
def test_spnego_auth_with_requests_gssapi(monkeypatch, url, creds, authed):
    gssapi = pytest.importorskip("gssapi")
    requests_gssapi = pytest.importorskip("requests_gssapi")
    monkeypatch.setenv("APSIS_URL", url)
    monkeypatch.setattr(gssapi, "Credentials", creds)
    auths = []
    monkeypatch.setattr(
        "requests.request", lambda method, url, auth, **kw: auths.append(auth) or _response(200)
    )
    Client().stats()
    (auth,) = auths
    if authed:
        assert isinstance(auth, requests_gssapi.HTTPSPNEGOAuth)
        assert auth.opportunistic_auth
        assert auth.mutual_authentication == requests_gssapi.DISABLED
    else:
        assert auth is None


def test_cli_spnego_failure_logs_nothing(monkeypatch, capsys, caplog):
    gssapi = pytest.importorskip("gssapi")
    pytest.importorskip("requests_gssapi")
    monkeypatch.setenv("APSIS_URL", "https://apsis.example.com")
    monkeypatch.setattr(gssapi, "Credentials", _creds(1))
    monkeypatch.setattr(gssapi, "SecurityContext", _gss_error)

    def send(adapter, request, **kw):
        resp = _response(401)
        resp.request = request
        return resp

    monkeypatch.setattr("requests.adapters.HTTPAdapter.send", send)
    monkeypatch.setattr("sys.argv", ["apsis", "job", "j"])
    with pytest.raises(SystemExit):
        apsis.cli.main()
    assert AUTH_ERROR in " ".join(capsys.readouterr().out.split())
    assert [r for r in caplog.records if r.name.startswith("requests_gssapi")] == []


def test_no_kerberos_without_gssapi_packages(monkeypatch):
    monkeypatch.setitem(sys.modules, "gssapi", None)
    monkeypatch.setitem(sys.modules, "requests_gssapi", None)
    spec = importlib.util.find_spec("apsis.service.client")
    client = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(client)
    auths = []
    monkeypatch.setattr(
        "requests.request", lambda method, url, auth, **kw: auths.append(auth) or _response(401)
    )
    with pytest.raises(client.APIError) as exc:
        client.Client(("localhost", 1)).stats()
    assert str(exc.value) == NO_KERBEROS_ERROR
    assert auths == [None]
    assert client.get_negotiate_headers("wss://apsis.example.com/x") == {}


@pytest.mark.parametrize("get", [Client.get_outputs, lambda c, r: c.get_output(r, "output")])
@pytest.mark.parametrize(
    "content, error",
    [(b"<html>", NO_HTTPS_ERROR), (b'{"error": "missing X-Remote-User"}', "missing X-Remote-User")],
)
def test_raw_gets_raise_auth_error(monkeypatch, get, content, error):
    monkeypatch.setattr("apsis.service.client.HTTPSPNEGOAuth", Mock())
    monkeypatch.setattr("requests.request", lambda method, url, **kw: _response(401, content))
    with pytest.raises(APIError) as exc:
        get(Client(("localhost", 1)), "r1")
    assert exc.value.status == 401
    assert str(exc.value) == error


def test_negotiate_header_only_for_wss(monkeypatch):
    gssapi = pytest.importorskip("gssapi")
    monkeypatch.setattr(gssapi, "Credentials", _creds(1))

    class Context:
        def __init__(self, **kw):
            pass

        def step(self) -> bytes:
            return b"tok"

    monkeypatch.setattr(gssapi, "SecurityContext", Context)
    assert get_negotiate_headers("ws://apsis.example.com/x") == {}
    assert get_negotiate_headers("wss://apsis.example.com/x") == {"Authorization": "Negotiate dG9r"}


@pytest.mark.parametrize("failing", ["Credentials", "SecurityContext"])
def test_negotiate_header_dropped_on_gss_error(monkeypatch, failing):
    gssapi = pytest.importorskip("gssapi")
    monkeypatch.setattr(gssapi, "Credentials", _creds(1))
    monkeypatch.setattr(gssapi, failing, _gss_error)
    assert get_negotiate_headers("wss://apsis.example.com/x") == {}


def _port(server) -> int:
    return server.sockets[0].getsockname()[1]


@pytest.mark.asyncio
async def test_ws_msgs_send_negotiate_header(monkeypatch):
    monkeypatch.setattr(
        "apsis.service.client.get_negotiate_headers",
        lambda url: {"Authorization": "Negotiate dG9r"},
    )
    headers = []

    async def handler(conn):
        headers.append(conn.request.headers.get("Authorization"))
        await conn.send("hi")

    async with serve(handler, "127.0.0.1", 0) as server:
        async with get_ws_msgs(f"ws://127.0.0.1:{_port(server)}/x") as msgs:
            assert [m async for m in msgs] == ["hi"]
    assert headers == ["Negotiate dG9r"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, error, match",
    [(401, APIError, NO_HTTPS_ERROR), (500, websockets.exceptions.InvalidHandshake, None)],
)
async def test_ws_status_error(monkeypatch, status, error, match):
    monkeypatch.setattr("apsis.service.client.HTTPSPNEGOAuth", Mock())
    async with serve(
        None, "127.0.0.1", 0, process_request=lambda conn, req: conn.respond(status, "")
    ) as server:
        with pytest.raises(error, match=match):
            async with get_ws_msgs(f"ws://127.0.0.1:{_port(server)}/x"):
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates, path",
    [
        (lambda c: c.get_run_updates("r1"), "/api/v1/runs/r1/updates"),
        (
            lambda c: c.get_output_data_updates("r1", "output"),
            "/api/v1/runs/r1/output/output/updates",
        ),
    ],
)
async def test_updates_connect_over_ws(updates, path):
    paths = []

    async def handler(conn):
        paths.append(conn.request.path)

    async with serve(handler, "127.0.0.1", 0) as server:
        async with updates(Client(("127.0.0.1", _port(server)))) as msgs:
            assert [m async for m in msgs] == []
    assert paths == [path]
