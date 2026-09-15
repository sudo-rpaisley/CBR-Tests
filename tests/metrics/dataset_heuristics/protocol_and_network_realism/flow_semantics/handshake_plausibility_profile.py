"""Compatibility imports for the former protocol/network metric location."""

from cbr_tests.metrics.protocol_network.flow_semantics import handshake_plausibility_profile as _impl

globals().update(
    {name: getattr(_impl, name) for name in dir(_impl) if not name.startswith("__")}
)

__all__ = [name for name in globals() if not name.startswith("__")]
