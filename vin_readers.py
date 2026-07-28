"""VIN acquisition helpers for ELM327-compatible OBD-II readers."""

from __future__ import annotations

import os
import re
import select
import termios
import time
from collections.abc import Callable


VIN_PATTERN = re.compile(r"[A-HJ-NPR-Z0-9]{17}")


class VinReaderError(RuntimeError):
    """Raised when a reader cannot return a usable VIN."""


def parse_obd_vin(response: str) -> str:
    """Extract a VIN from plain text or an ELM327 Mode 09 PID 02 response."""
    direct = VIN_PATTERN.search(response.upper())
    if direct:
        return direct.group()

    payload = bytearray()
    for raw_line in response.upper().replace(">", "").splitlines():
        tokens = re.findall(r"(?<![0-9A-F])[0-9A-F]{2}(?![0-9A-F])", raw_line)
        if not tokens:
            continue
        # With headers enabled, discard the CAN identifier. ELM output normally
        # renders it as three hex digits, so it is not captured by the regex.
        values = [int(token, 16) for token in tokens]
        if values and (values[0] & 0xF0) in (0x10, 0x20):
            values = values[2:] if (values[0] & 0xF0) == 0x10 else values[1:]
        payload.extend(values)
    marker = payload.find(b"\x49\x02\x01")
    if marker >= 0:
        payload = payload[marker + 3 :]
    decoded = "".join(chr(value) if 32 <= value < 127 else "" for value in payload)
    match = VIN_PATTERN.search(decoded)
    if not match:
        raise VinReaderError("The OBD reader response did not contain a 17-character VIN.")
    return match.group()


def _serial_transport(device: str, baud: int, command: str, timeout: float) -> str:
    if not hasattr(termios, f"B{baud}"):
        raise VinReaderError(f"Unsupported serial baud rate: {baud}")
    try:
        descriptor = os.open(device, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    except OSError as exc:
        raise VinReaderError(f"Unable to open OBD reader {device}: {exc}") from exc
    try:
        attributes = termios.tcgetattr(descriptor)
        attributes[0] = attributes[1] = attributes[3] = 0
        attributes[2] = termios.CS8 | termios.CLOCAL | termios.CREAD
        speed = getattr(termios, f"B{baud}")
        attributes[4] = attributes[5] = speed
        termios.tcsetattr(descriptor, termios.TCSANOW, attributes)
        termios.tcflush(descriptor, termios.TCIOFLUSH)
        os.write(descriptor, f"{command}\r".encode("ascii"))
        deadline, chunks = time.monotonic() + timeout, []
        while time.monotonic() < deadline:
            readable, _, _ = select.select(
                [descriptor], [], [], max(0, min(0.2, deadline - time.monotonic()))
            )
            if readable:
                chunk = os.read(descriptor, 4096)
                chunks.append(chunk)
                if b">" in chunk:
                    break
        return b"".join(chunks).decode("ascii", errors="replace")
    finally:
        os.close(descriptor)


def read_elm327_vin(
    device: str,
    *,
    baud: int = 38400,
    timeout: float = 5,
    transport: Callable[[str, int, str, float], str] = _serial_transport,
) -> str:
    """Request VIN service 09/PID 02 from a USB or Bluetooth serial ELM327."""
    if timeout <= 0:
        raise VinReaderError("Reader timeout must be greater than zero.")
    for command in ("ATZ", "ATE0", "ATL0", "ATS1", "ATH0", "ATSP0"):
        transport(device, baud, command, timeout)
    response = transport(device, baud, "0902", timeout)
    return parse_obd_vin(response)
