"""Compatibility imports for the former slice-representation metric location."""

from cbr_tests.metrics import slice_representation as _impl

globals().update(
    {
        name: getattr(_impl, name)
        for name in dir(_impl)
        if not name.startswith("__")
    }
)

__all__ = [name for name in globals() if not name.startswith("__")]
