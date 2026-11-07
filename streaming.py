"""Renderer live per Telegram: status bar animata + effetto macchina da scrivere.

Nessuna chiamata al modello: solo edit del messaggio, ~1/sec (flood control Telegram).
Due limiti reali, aggirati senza mentire all'utente:
  1) `opencode run --format json` non manda i token, manda il blocco `text` a step finito.
     Quindi il typewriter parte quando il testo arriva, non prima.
  2) Telegram accetta ~1 edit/secondo per chat: il reveal e' a colpi, non a singoli caratteri.
"""

from __future__ import annotations

import asyncio
import math
import re
import time

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
WAVE = "▁▂▃▄▅▆▇█▇▆▅▄▃▂▁"
CURSOR = "▌"

EDIT_INTERVAL = 1.05   # cadenza edit (Telegram: oltre ~1/s scatta il flood control)
MAX_LIVE = 3200        # oltre questo, il resto va in messaggi successivi
TG_LIMIT = 4000
MODEL_PREFIX = re.compile(r"^\[([^\]]+)\]\n")


def _bar(ratio: float, width: int = 20) -> str:
    """Barra con gradiente: █▓▒░ + percentuale."""
    ratio = 0.0 if ratio < 0 else min(1.0, ratio)
    n = int(ratio * width)
    frac = ratio * width - n
    if n >= width:
        body = "█" * width
    elif n <= 0:
        body = "░" * width
    else:
        body = "█" * n + ("▓" if frac > 0.5 else "▒") + "░" * (width - n - 1)
    return f"{body} {int(ratio * 100):3d}%"


def _wave(tick: int, width: int = 20) -> str:
    return "".join(WAVE[(tick + i) % len(WAVE)] for i in range(width))


def _k(n: int) -> str:
    return f"{n / 1000:.1f}k" if n >= 1000 else str(n)


class Streamer:
    """Un solo messaggio Telegram che si aggiorna: stato, barra, testo a macchina."""

    def __init__(self, message, model: str = ""):
        self.msg = message
        self.model = model.split("/")[-1]
        self.phase = "connessione"
        self.tools: list[str] = []
        self.text = ""       # testo ricevuto
        self.shown = 0       # caratteri gia' mostrati
        self.tokens_in = 0
        self.tokens_out = 0
        self.t0 = time.monotonic()
        self.done = False
        self.tick = 0
        self._task: asyncio.Task | None = None
        self._chunk = 0
        self._last_payload = ""

    # --- input dal processo opencode ---
    def set_model(self, model: str):
        """Inizio tentativo su un modello: azzera il testo del tentativo fallito."""
        self.model = model.split("/")[-1]
        self.phase = "invio"
        self.tools.clear()
        self.text = ""
        self.shown = 0
        self._chunk = 0

    def feed(self, event: dict):
        etype = (event or {}).get("type", "")
        part = (event or {}).get("part") or {}
        if etype == "text":
            chunk = part.get("text") or event.get("text") or ""
            if chunk:
                self.text += chunk if self.text.endswith("\n") or not self.text else "\n" + chunk
                self.phase = "scrive"
        elif etype in ("step_start", "step-start"):
            self.phase = "pensa"
        elif etype == "reasoning":
            self.phase = "ragiona"
        elif "tool" in etype:
            name = part.get("tool") or part.get("name") or "tool"
            self.phase = f"usa {name}"[:18]
            if name not in self.tools:
                self.tools.append(str(name))
        elif etype == "step_finish":
            tokens = part.get("tokens") or {}
            if tokens:
                self.tokens_in += int(tokens.get("input", 0) or 0)
                self.tokens_out += int(tokens.get("output", 0) or 0)

    # --- ciclo di render ---
    def start(self):
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        while True:
            await self._tick()
            if self.done and self.shown >= min(len(self.text), MAX_LIVE):
                break
            await asyncio.sleep(EDIT_INTERVAL)

    def _advance(self):
        total = len(self.text)
        if self.shown >= total:
            return
        if not self.done:
            backlog = total - self.shown
            # rivela mentre arriva, ma lascia un piccolo arretrato: si vede il typewriter
            self.shown = total if backlog < 120 else self.shown + max(60, backlog // 2)
            return
        if not self._chunk:
            target = 1.8 + total / 900          # durata reveal ~2-6s
            edits = max(2, min(6, round(target / EDIT_INTERVAL)))
            self._chunk = max(12, math.ceil(total / edits))
        self.shown = min(total, self.shown + self._chunk)

    def _typing(self) -> bool:
        return not self.done or self.shown < min(len(self.text), MAX_LIVE)

    def _header(self, elapsed: float) -> str:
        """Status bar a 3 righe: fase, barra/token, strumenti."""
        name = self.model or "free"
        if not self._typing():
            line1 = f"✔ {elapsed:.1f}s · {name}"
            line2 = _bar(1.0) + (f" · ↑{_k(self.tokens_in)} ↓{_k(self.tokens_out)}" if self.tokens_in or self.tokens_out else "")
            line3 = ", ".join(self.tools[-4:])
            return "\n".join(x for x in (line1, line2, line3) if x)
        spin = SPINNER[self.tick % len(SPINNER)]
        line1 = f"{spin} {self.phase} · {elapsed:.1f}s · {name}"
        if self.text:
            ratio = self.shown / max(1, len(self.text))
            line2 = _bar(ratio) + (f" · ↑{_k(self.tokens_in)} ↓{_k(self.tokens_out)}" if self.tokens_in or self.tokens_out else "")
        else:
            line2 = _wave(self.tick)
        line3 = ", ".join(self.tools[-4:])
        return "\n".join(x for x in (line1, line2, line3) if x)

    def _payload(self) -> str:
        elapsed = time.monotonic() - self.t0
        head = self._header(elapsed)
        body = self.text[:MAX_LIVE]
        shown = body[: self.shown] if self.shown < len(body) else body
        if not shown:
            return head
        tail = CURSOR if self._typing() else ""
        return f"{head}\n\n{shown}{tail}"[:TG_LIMIT]

    async def _tick(self):
        self.tick += 1
        self._advance()
        payload = self._payload()
        if payload == self._last_payload:
            return
        try:
            await self.msg.edit_text(payload)
            self._last_payload = payload
        except Exception as exc:  # flood / not modified / rete: si riprova al tick dopo
            retry = getattr(exc, "retry_after", None)
            if retry:
                await asyncio.sleep(float(retry) + 0.5)

    # --- chiusura ---
    async def finish(self, full_text: str) -> str:
        """Rivela il testo definitivo. Ritorna l'eventuale coda oltre MAX_LIVE."""
        text = full_text or "(vuoto)"
        m = MODEL_PREFIX.match(text)
        if m:
            self.model = m.group(1)
            text = text[m.end():]
        self.text = text
        self.phase = "stampa"
        self.done = True
        self._chunk = 0
        if self._task:
            try:
                await self._task
            except Exception:
                pass
        return self.text[MAX_LIVE:]

    async def abort(self, text: str = "Errore durante la risposta."):
        self.text = text
        self.done = True
        self.shown = len(text)
        if self._task:
            self._task.cancel()
        await self._tick()
