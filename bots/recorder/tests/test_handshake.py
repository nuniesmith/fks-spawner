import pytest
import websocket._handshake as hs

from recorder import core


def test_the_patch_drops_only_the_connection_check(monkeypatch):
    monkeypatch.setattr(hs, "_HEADERS_TO_CHECK", {"upgrade": "websocket", "connection": "upgrade"})
    core.apply_handshake_patch()
    assert hs._HEADERS_TO_CHECK == {"upgrade": "websocket"}
    core.apply_handshake_patch()  # a second call is a no-op
    assert hs._HEADERS_TO_CHECK == {"upgrade": "websocket"}


def test_the_pinned_library_still_has_the_shape_the_patch_expects():
    # requirements.txt pins websocket-client 1.9.2 for this. If a bump changes
    # its internals, this fails here instead of at the recorder's startup.
    import websocket

    assert websocket.__version__ == "1.9.2"


def test_it_refuses_a_library_whose_internals_changed(monkeypatch):
    monkeypatch.setattr(hs, "_HEADERS_TO_CHECK", {"upgrade": "websocket", "connection": "upgrade", "x": "y"})
    with pytest.raises(RuntimeError, match="refusing to run"):
        core.apply_handshake_patch()
