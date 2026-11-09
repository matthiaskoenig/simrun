"""The downloads of the tools of the k3d tests try a server error again.

No cluster needed: these run with the unit tests.
"""

import io
import urllib.error
from email.message import Message
from unittest import mock

import k3d_cluster
import pytest


def _error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x", code, "error", Message(), io.BytesIO())


def _response(body: bytes) -> mock.MagicMock:
    response = mock.MagicMock()
    response.__enter__.return_value.read.return_value = body
    return response


def test_a_gateway_timeout_is_tried_again() -> None:
    sleeps: list[float] = []
    with mock.patch.object(
        k3d_cluster.urllib.request,
        "urlopen",
        side_effect=[_error(504), urllib.error.URLError("reset"), _response(b"k3d")],
    ):
        assert k3d_cluster._fetch("https://x", sleeps.append) == b"k3d"
    assert sleeps == [k3d_cluster.DOWNLOAD_RETRY_S, 2 * k3d_cluster.DOWNLOAD_RETRY_S]


def test_a_client_error_is_not_tried_again() -> None:
    sleeps: list[float] = []
    with (
        mock.patch.object(
            k3d_cluster.urllib.request, "urlopen", side_effect=[_error(404)]
        ),
        pytest.raises(urllib.error.HTTPError),
    ):
        k3d_cluster._fetch("https://x", sleeps.append)
    assert sleeps == []


def test_the_last_server_error_is_raised() -> None:
    sleeps: list[float] = []
    errors = [_error(503) for _ in range(k3d_cluster.DOWNLOAD_ATTEMPTS)]
    with (
        mock.patch.object(k3d_cluster.urllib.request, "urlopen", side_effect=errors),
        pytest.raises(urllib.error.HTTPError),
    ):
        k3d_cluster._fetch("https://x", sleeps.append)
    assert len(sleeps) == k3d_cluster.DOWNLOAD_ATTEMPTS - 1
