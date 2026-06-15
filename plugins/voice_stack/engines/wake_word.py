"""Wake Word Engine — keyword spotting.

Concrete engines:
- PorcupineEngine: Picovoice Porcupine
- OpenWakeWordEngine: OpenWakeWord
- CommandWWEngine: Generic CLI wake-word wrapper
- DisabledWakeWordEngine: explicit no-op / unavailable state

Design goals:
- Imports stay lazy so missing optional dependencies do not crash Hermes.
- A broken wake-word backend should disable only the local voice loop, not the
  Home Assistant websocket bridge or the rest of the gateway.
- External detectors such as MiroWakeWord can integrate through the generic
  command wrapper.
"""

from __future__ import annotations

import logging
import os
import queue
import shlex
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Optional

logger = logging.getLogger(__name__)


class WakeWordEngine(ABC):
    """Abstract wake word engine interface."""

    @abstractmethod
    def available(self) -> bool:
        """Return True if this engine's dependencies are installed."""
        ...

    @abstractmethod
    def listen(self, timeout_seconds: float = 60.0) -> bool:
        """Block until the wake word is detected or timeout."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop listening (for graceful shutdown)."""
        ...

    def list_wake_words(self) -> List[str]:
        return []


class DisabledWakeWordEngine(WakeWordEngine):
    """No-op engine used when wake-word support is intentionally disabled."""

    def __init__(self, reason: str = "disabled") -> None:
        self.reason = reason

    def available(self) -> bool:
        return False

    def listen(self, timeout_seconds: float = 60.0) -> bool:
        time.sleep(min(timeout_seconds, 0.25))
        return False

    def stop(self) -> None:
        return None


class _RawMicStream:
    """Small sounddevice wrapper that yields raw int16 PCM frames."""

    def __init__(self, sample_rate: int, blocksize: int) -> None:
        self.sample_rate = sample_rate
        self.blocksize = blocksize
        self._queue: "queue.Queue[bytes]" = queue.Queue()
        self._stream = None
        self._closed = False

    def __enter__(self) -> "_RawMicStream":
        import sounddevice as sd

        def _callback(indata, frames, time_info, status) -> None:
            if status:
                logger.debug("Wake-word audio status: %s", status)
            if not self._closed:
                self._queue.put(bytes(indata))

        self._stream = sd.RawInputStream(
            samplerate=self.sample_rate,
            blocksize=self.blocksize,
            channels=1,
            dtype="int16",
            callback=_callback,
        )
        self._stream.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._closed = True
        if self._stream is not None:
            try:
                self._stream.stop()
            except Exception:
                pass
            try:
                self._stream.close()
            except Exception:
                pass
            self._stream = None

    def read(self, timeout: float = 1.0) -> Optional[bytes]:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None


class PorcupineEngine(WakeWordEngine):
    """Porcupine wake word engine via pvporcupine.

    Uses sounddevice for microphone capture so a missing PyAudio install does
    not sink the whole feature.
    """

    def __init__(
        self,
        access_key: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        sensitivities: Optional[List[float]] = None,
    ) -> None:
        self._access_key = access_key or os.getenv("PORCUPINE_ACCESS_KEY", "").strip()
        self._keywords = keywords or ["computer"]
        self._sensitivities = sensitivities or [0.5] * len(self._keywords)
        self._porcupine = None
        self._stop = False

    def available(self) -> bool:
        if not self._access_key:
            return False
        try:
            import pvporcupine  # noqa: F401
            import sounddevice  # noqa: F401
            return True
        except ImportError:
            return False

    def listen(self, timeout_seconds: float = 60.0) -> bool:
        import pvporcupine
        import struct

        if not self._access_key:
            raise RuntimeError("PORCUPINE_ACCESS_KEY not set")

        self._porcupine = pvporcupine.create(
            access_key=self._access_key,
            keywords=self._keywords,
            sensitivities=self._sensitivities,
        )
        self._stop = False

        try:
            with _RawMicStream(
                sample_rate=self._porcupine.sample_rate,
                blocksize=self._porcupine.frame_length,
            ) as mic:
                start = time.monotonic()
                while not self._stop:
                    if time.monotonic() - start > timeout_seconds:
                        return False
                    chunk = mic.read(timeout=1.0)
                    if chunk is None:
                        continue
                    pcm = struct.unpack_from("h" * self._porcupine.frame_length, chunk)
                    keyword_index = self._porcupine.process(pcm)
                    if keyword_index >= 0:
                        logger.info("Wake word detected: %s", self._keywords[keyword_index])
                        return True
                return False
        finally:
            self._cleanup()

    def stop(self) -> None:
        self._stop = True

    def _cleanup(self) -> None:
        if self._porcupine is not None:
            try:
                self._porcupine.delete()
            except Exception:
                pass
            self._porcupine = None

    def list_wake_words(self) -> List[str]:
        return [
            "computer", "jarvis", "alexa", "hey google", "hey siri",
            "ok google", "porcupine", "terminator", "blueberry", "bumblebee",
            "grapefruit", "grasshopper", "hey barista", "hey edison",
            "picovoice", "pico clock",
        ]


class OpenWakeWordEngine(WakeWordEngine):
    """OpenWakeWord engine backed by sounddevice + numpy."""

    def __init__(
        self,
        model_paths: Optional[List[str]] = None,
        wake_words: Optional[List[str]] = None,
        threshold: float = 0.5,
    ) -> None:
        self._model_paths = [p for p in (model_paths or []) if p]
        self._wake_words = [w for w in (wake_words or []) if w]
        self._threshold = threshold
        self._models: List[Any] = []
        self._stop = False

    def available(self) -> bool:
        try:
            import openwakeword  # noqa: F401
            import numpy  # noqa: F401
            import sounddevice  # noqa: F401
            return True
        except ImportError:
            return False

    def listen(self, timeout_seconds: float = 60.0) -> bool:
        import numpy as np
        from openwakeword.model import Model

        model_kwargs: dict[str, Any] = {}
        if self._model_paths:
            model_kwargs["wakeword_models"] = self._model_paths
        elif self._wake_words:
            model_kwargs["wakeword_models"] = self._wake_words
        else:
            model_kwargs["wakeword_models"] = ["alexa"]

        self._models = [Model(**model_kwargs)]
        self._stop = False
        sample_rate = 16000
        blocksize = 1280  # 80ms at 16kHz

        with _RawMicStream(sample_rate=sample_rate, blocksize=blocksize) as mic:
            start = time.monotonic()
            while not self._stop:
                if time.monotonic() - start > timeout_seconds:
                    return False
                chunk = mic.read(timeout=1.0)
                if chunk is None:
                    continue
                pcm = np.frombuffer(chunk, dtype=np.int16)
                for model in self._models:
                    predictions = model.predict(pcm)
                    for wake_word, score in predictions.items():
                        if score >= self._threshold:
                            logger.info("Wake word '%s' detected (score: %.2f)", wake_word, score)
                            return True
            return False

    def stop(self) -> None:
        self._stop = True


class CommandWWEngine(WakeWordEngine):
    """Generic CLI-based wake word engine.

    The command should exit 0 when the wake word is detected, non-zero on
    timeout or error. `{timeout}` placeholders are expanded at runtime.
    """

    def __init__(self, command: List[str]) -> None:
        self._command = command
        self._proc = None

    def available(self) -> bool:
        return bool(self._command) and shutil.which(self._command[0]) is not None

    def listen(self, timeout_seconds: float = 60.0) -> bool:
        if not self.available():
            raise RuntimeError("Wake-word command not available")
        cmd = [part.replace("{timeout}", str(int(timeout_seconds))) for part in self._command]
        self._proc = subprocess.Popen(cmd)
        try:
            return self._proc.wait(timeout=timeout_seconds + 5) == 0
        except subprocess.TimeoutExpired:
            self.stop()
            return False
        finally:
            self._proc = None

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.terminate()
            except Exception:
                pass


def _normalize_engine_name(engine_type: str) -> str:
    return str(engine_type or "").strip().lower().replace("-", "").replace("_", "")


def _parse_command(value: Any) -> List[str]:
    if isinstance(value, (list, tuple)):
        return [str(part) for part in value if str(part).strip()]
    if isinstance(value, str) and value.strip():
        return shlex.split(value)
    return []


def _configured_command(command: Any = None) -> List[str]:
    parsed = _parse_command(command)
    if parsed:
        return parsed
    for env_name in ("HERMES_WAKE_WORD_COMMAND", "HERMES_MIROWAKEWORD_COMMAND"):
        env_value = os.getenv(env_name, "").strip()
        if env_value:
            parsed = _parse_command(env_value)
            if parsed:
                return parsed
    return []


def _pick_auto_engine(
    access_key: Optional[str],
    keywords: Optional[List[str]],
    sensitivities: Optional[List[float]],
    model_paths: Optional[List[str]],
    wake_words: Optional[List[str]],
    threshold: float,
    command: Any,
) -> WakeWordEngine:
    porcupine = PorcupineEngine(
        access_key=access_key,
        keywords=keywords,
        sensitivities=sensitivities,
    )
    if porcupine.available():
        return porcupine

    open_wake_word = OpenWakeWordEngine(
        model_paths=model_paths,
        wake_words=wake_words,
        threshold=threshold,
    )
    if open_wake_word.available():
        return open_wake_word

    cmd = _configured_command(command)
    if cmd:
        command_engine = CommandWWEngine(cmd)
        if command_engine.available():
            return command_engine

    return DisabledWakeWordEngine("No wake-word backend available")


def create_wake_word_engine(engine_type: str = "auto", **kwargs: Any) -> WakeWordEngine:
    """Create a wake word engine instance by name.

    Supported names:
    - auto
    - porcupine
    - openwakeword
    - mirowakeword (alias for command-based external detector)
    - command
    - disabled/off/none
    """
    normalized = _normalize_engine_name(engine_type)
    keywords = kwargs.get("keywords") or ["computer"]
    sensitivities = kwargs.get("sensitivities")
    access_key = kwargs.get("access_key")
    model_paths = kwargs.get("model_paths")
    wake_words = kwargs.get("wake_words") or keywords
    threshold = float(kwargs.get("threshold", 0.5))
    command = kwargs.get("command")

    if normalized in {"", "auto"}:
        return _pick_auto_engine(
            access_key=access_key,
            keywords=keywords,
            sensitivities=sensitivities,
            model_paths=model_paths,
            wake_words=wake_words,
            threshold=threshold,
            command=command,
        )
    if normalized in {"disabled", "disable", "off", "none", "false"}:
        return DisabledWakeWordEngine("Wake-word engine disabled by config")
    if normalized == "porcupine":
        return PorcupineEngine(
            access_key=access_key,
            keywords=keywords,
            sensitivities=sensitivities,
        )
    if normalized == "openwakeword":
        return OpenWakeWordEngine(
            model_paths=model_paths,
            wake_words=wake_words,
            threshold=threshold,
        )
    if normalized == "mirowakeword":
        return CommandWWEngine(_configured_command(command))
    if normalized == "command":
        return CommandWWEngine(_configured_command(command))
    raise ValueError(
        f"Unknown wake word engine '{engine_type}'. Valid: auto, porcupine, openwakeword, mirowakeword, command, disabled"
    )
