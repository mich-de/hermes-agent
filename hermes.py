"""Hermes Agent — orchestrator: Telegram bot → Open Code free models → fritz-reconnect IP rotation."""

import asyncio
import contextlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters, ContextTypes

from streaming import Streamer

TG_LIMIT = 4000
RUN_TIMEOUT = 60
DANGEROUS = re.compile(
    r"\b(shutdown|restart-computer|stop-computer|format|mkfs|diskpart|bcdedit|"
    r"reg\s+delete|remove-item\s+-recurse|rd\s+/s|del\s+/[fsq]|rm\s+-rf|git\s+push)\b",
    re.IGNORECASE,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "hermes.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("hermes")

CFG_PATH = Path(__file__).parent / "config.json"
RATE_LIMIT_RE = re.compile(
    r"(usage|free|quota|rate)[ _-]*(limit|cap)|limit reached|remaining usage|429|too many requests",
    re.IGNORECASE,
)
SYSTEM_PROMPT_PATH = Path(__file__).parent / "system-prompt.md"
SYSTEM_PREFIX = ""


def _load_system_prefix() -> str:
    if SYSTEM_PROMPT_PATH.exists():
        return SYSTEM_PROMPT_PATH.read_text("utf-8").strip() + "\n\n"
    return "[Rispondi in modo conciso e completo.]\n\n"

cfg: dict = {}
model_failures: dict[str, float] = {}
fritz_last_run: float = 0
# Token tracking per sessione: {session_id: {"total_in": N, "total_out": N, "turns": N, "model": str}}
session_tokens: dict[str, dict] = {}


def load_config():
    global cfg, SYSTEM_PREFIX, KITT_PREFIX
    cfg = json.loads(CFG_PATH.read_text("utf-8"))
    SYSTEM_PREFIX = _load_system_prefix()
    KITT_PREFIX = _load_kitt_prefix()
    if not cfg["telegram_token"]:
        sys.exit("Compila telegram_token in config.json")
    if not cfg["allowed_chat_ids"]:
        sys.exit("Compila allowed_chat_ids in config.json (il tuo chat ID Telegram)")


def all_models_exhausted() -> bool:
    now = time.time()
    return all(now - model_failures.get(m, 0) <= 600 for m in cfg["models"])


def _extract_tokens(raw: str) -> dict:
    """Estrae i conteggi token dal JSON streaming di opencode."""
    for line in reversed(raw.strip().splitlines()):
        try:
            obj = json.loads(line.strip())
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, dict) and obj.get("type") == "step_finish":
            tokens = obj.get("part", {}).get("tokens", {})
            return {
                "input": tokens.get("input", 0),
                "output": tokens.get("output", 0),
                "cached": tokens.get("cache", {}).get("read", 0),
            }
    return {}


def _extract_response(raw: str) -> str:
    """Estrae il testo dalla risposta JSON streaming di opencode run."""
    texts = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            texts.append(line)
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("type") == "text":
            part = obj.get("part", {})
            if isinstance(part, dict) and part.get("text"):
                texts.append(part["text"])
            elif obj.get("text"):
                texts.append(obj["text"])
        elif obj.get("type") == "tool-result":
            part = obj.get("part", {})
            if isinstance(part, dict) and part.get("output"):
                texts.append(f"[tool] {part['output'][:500]}")
    return "\n".join(texts) if texts else ""


def _get_session_key(chat_id: int, chat_data: dict) -> str:
    return chat_data.get("session_key", "") or f"hermes-{chat_id}"


def _update_token_stats(session_id: str, token_info: dict, model: str):
    """Aggiorna le statistiche token per la sessione."""
    if not token_info:
        return
    stats = session_tokens.setdefault(session_id, {"total_in": 0, "total_out": 0, "turns": 0, "model": model})
    stats["total_in"] += token_info.get("input", 0)
    stats["total_out"] += token_info.get("output", 0)
    stats["turns"] += 1
    stats["model"] = model


def _check_session_health(session_id: str) -> tuple[str | None, bool]:
    """Soglie in percentuale sulla quota free configurata (`quota_tokens`).
    Ritorna (warning_msg, needs_fritz). needs_fritz=True → cambio IP proattivo."""
    stats = session_tokens.get(session_id)
    if not stats:
        return None, False
    total = stats["total_in"] + stats["total_out"]
    quota = int(cfg.get("quota_tokens") or 150_000)
    pct = total / quota * 100
    if pct >= 90:
        return (
            f"🚨 {pct:.0f}% quota (~{total:,}/{quota:,} token). "
            "Stop al lavoro pesante: cambio IP e nuova sessione.",
            True,
        )
    if pct >= 60:
        return (
            f"⚠️ {pct:.0f}% quota (~{total:,}/{quota:,} token). Al 90% cambio IP automatico.",
            False,
        )
    if stats["turns"] > 30:
        return "📊 30+ turni. Scrivi HANDOFF.md e usa /new per un contesto pulito.", False
    return None, False


def _build_context_summary(session_id: str) -> str:
    """Costruisce un riassunto del contesto per il passaggio tra modelli."""
    stats = session_tokens.get(session_id)
    if not stats or stats["turns"] < 2:
        return ""
    return (
        f"[Contesto precedente: {stats['turns']} turni, "
        f"{stats['total_in']+stats['total_out']} token su {stats['model']}. "
        "Continua la conversazione in corso.]\n\n"
    )


async def run_opencode(
    prompt: str,
    model: str,
    continue_session: bool = True,
    on_event=None,
) -> tuple[str, bool, dict]:
    """Esegue opencode run leggendo gli eventi JSON in streaming.
    on_event riceve ogni evento mentre arriva (render live lato Telegram)."""
    env = {
        **os.environ, **cfg["opencode_env"],
        "HERMES_AGENT": "1",
        "HERMES_MODEL": model,
        "HERMES_PID": str(os.getpid()),
        "HERMES_SESSION": "continue" if continue_session else "new",
    }
    cmd = [
        cfg["opencode_bin"], "--standalone", "run", "--auto",
        "--format", "json",
        "-m", model,
    ]
    if continue_session:
        cmd.append("-c")
    cmd.append(prompt)
    log.info("opencode run -m %s: %.80s...", model, prompt)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=cfg["working_dir"],
        )
    except Exception as e:
        return f"Errore lancio opencode: {e}", False, {}

    out_buf: list[str] = []
    err_buf: list[str] = []

    async def pump(stream, buf: list[str], emit: bool):
        while True:
            line = await stream.readline()
            if not line:
                return
            text = line.decode("utf-8", errors="replace")
            buf.append(text)
            if not emit or on_event is None:
                continue
            stripped = text.strip()
            if not stripped.startswith("{"):
                continue
            try:
                event = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                continue
            try:
                res = on_event(event)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as exc:
                log.warning("on_event: %s", exc)

    try:
        await asyncio.wait_for(
            asyncio.gather(
                pump(proc.stdout, out_buf, True),
                pump(proc.stderr, err_buf, False),
                proc.wait(),
            ),
            timeout=cfg["response_timeout_seconds"],
        )
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
            await proc.wait()
        return "Timeout: il modello non ha risposto in tempo.", False, {}

    out = "".join(out_buf)
    err = "".join(err_buf)
    combined = out + err
    token_info = _extract_tokens(out)

    if RATE_LIMIT_RE.search(combined):
        log.warning("Rate limit su %s", model)
        return combined, True, token_info

    response = _extract_response(out)
    if not response:
        response = out.strip() or err.strip() or "(nessuna risposta)"
    return response, False, token_info


async def fritz_reconnect() -> bool:
    """Esegue fritz-reconnect per cambiare IP. Ritorna True se ha funzionato."""
    global fritz_last_run
    now = time.time()
    cooldown = cfg["fritz_cooldown_minutes"] * 60
    if now - fritz_last_run < cooldown:
        remaining = int((cooldown - (now - fritz_last_run)) / 60)
        log.info("fritz-reconnect in cooldown, %d min rimasti", remaining)
        return False

    log.info("Lancio fritz-reconnect...")
    fritz_last_run = now
    try:
        proc = await asyncio.create_subprocess_exec(
            "pwsh", "-NoProfile", "-File", cfg["fritz_reconnect_bin"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
        output = stdout.decode("utf-8", errors="replace")
        log.info("fritz-reconnect output: %s", output.strip())
        if proc.returncode == 0:
            model_failures.clear()
            return True
        log.error("fritz-reconnect exit %d: %s", proc.returncode, stderr.decode())
    except asyncio.TimeoutError:
        log.error("fritz-reconnect timeout 5 min")
    except Exception as e:
        log.error("fritz-reconnect errore: %s", e)
    return False


HANDOFF_MAX = 2500
HANDOFF_FRESH_SECONDS = 2 * 3600


def _boot_handoff(chat_data: dict) -> str:
    """Boot sequence: se HANDOFF.md e' fresco (<2h) lo inietta nel primo prompt."""
    if chat_data.get("booted"):
        return ""
    chat_data["booted"] = True
    base = Path(cfg.get("working_dir") or ".")
    for name in ("HANDOFF.md", "handover.md"):
        path = base / name
        try:
            if path.exists() and time.time() - path.stat().st_mtime < HANDOFF_FRESH_SECONDS:
                body = path.read_text("utf-8", errors="replace")[:HANDOFF_MAX]
                return f"[Boot: riprendi da {name}, modificato <2h]\n{body}\n\n"
        except OSError:
            continue
    return ""


async def handle_prompt(
    prompt: str,
    chat_id: int,
    chat_data: dict,
    on_model=None,
    on_event=None,
) -> tuple[str, str | None, bool]:
    """Prova i modelli in ordine; se tutti 429, fritz-reconnect e riprova.
    Ritorna (risposta, warning_opzionale, needs_fritz)."""
    session_key = _get_session_key(chat_id, chat_data)
    continue_session = not chat_data.get("force_new", False)
    chat_data["force_new"] = False
    full_prompt = SYSTEM_PREFIX + _boot_handoff(chat_data) + prompt
    switching_model = False
    # Routing: modello bloccato per la sessione (si cambia solo con /new)
    candidates = list(cfg["models"])
    locked = chat_data.get("model_locked")
    if locked in candidates:
        candidates.remove(locked)
        candidates.insert(0, locked)

    for _ in range(2):
        for model in candidates:
            if time.time() - model_failures.get(model, 0) <= 600:
                continue

            actual_prompt = full_prompt
            if switching_model:
                ctx_summary = _build_context_summary(session_key)
                if ctx_summary:
                    actual_prompt = ctx_summary + full_prompt

            if on_model:
                on_model(model)
            response, limited, token_info = await run_opencode(
                actual_prompt, model, continue_session, on_event=on_event
            )

            if not limited:
                _update_token_stats(session_key, token_info, model)
                chat_data["model_locked"] = model
                warning, needs_fritz = _check_session_health(session_key)
                model_name = model.split("/")[-1]
                return f"[{model_name}]\n{response}", warning, needs_fritz

            model_failures[model] = time.time()
            switching_model = True
            log.info("Modello %s esaurito, passo al prossimo", model)

        if not all_models_exhausted():
            continue

        ok = await fritz_reconnect()
        if not ok:
            return (
                "Tutti i modelli free sono in rate limit e fritz-reconnect "
                "non è disponibile (cooldown o errore). Riprova tra qualche minuto.",
                None, False,
            )

    return "Nessun modello disponibile dopo i tentativi. Riprova più tardi.", None, False


# --- Telegram handlers ---

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "OdissREA attivo.\n"
        f"Modelli free: {len(cfg['models'])}\n\n"
        "Scrivi in linguaggio naturale, oppure:\n"
        + "\n".join(f"/{c} — {d}" for c, d in COMMANDS)
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    now = time.time()
    lines = ["Stato modelli:"]
    for m in cfg["models"]:
        name = m.split("/")[-1]
        last = model_failures.get(m, 0)
        if now - last > 600:
            lines.append(f"  ✓ {name}")
        else:
            rem = int((600 - (now - last)) / 60)
            lines.append(f"  ✗ {name} (cooldown ~{rem}min)")
    fritz_cd = cfg["fritz_cooldown_minutes"] * 60
    if now - fritz_last_run > fritz_cd:
        lines.append("\nFritz-reconnect: disponibile")
    else:
        rem = int((fritz_cd - (now - fritz_last_run)) / 60)
        lines.append(f"\nFritz-reconnect: cooldown ~{rem}min")

    sk = _get_session_key(update.message.chat_id, ctx.chat_data)
    stats = session_tokens.get(sk)
    if stats:
        total = stats["total_in"] + stats["total_out"]
        lines.append(f"\nSessione: {stats['turns']} turni, ~{total:,} token, modello: {stats['model']}")
    await update.message.reply_text("\n".join(lines))


async def cmd_fritz(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Lancio fritz-reconnect (internet giù ~1-2 min)...")
    ok = await fritz_reconnect()
    if ok:
        await update.message.reply_text("IP cambiato, tutti i modelli resettati.")
    else:
        await update.message.reply_text("fritz-reconnect fallito o in cooldown.")


async def cmd_new(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reset sessione: la prossima conversazione parte da zero."""
    old_key = _get_session_key(update.message.chat_id, ctx.chat_data)
    stats = session_tokens.pop(old_key, None)
    new_key = f"hermes-{update.message.chat_id}-{int(time.time())}"
    ctx.chat_data["session_key"] = new_key
    ctx.chat_data["force_new"] = True
    ctx.chat_data.pop("model_locked", None)
    ctx.chat_data.pop("booted", None)
    msg = "Nuova conversazione avviata."
    if stats:
        total = stats["total_in"] + stats["total_out"]
        msg += f"\n(Sessione precedente: {stats['turns']} turni, ~{total:,} token)"
    await update.message.reply_text(msg)


def _reset_session(chat_id: int, chat_data: dict) -> dict | None:
    old_key = _get_session_key(chat_id, chat_data)
    stats = session_tokens.pop(old_key, None)
    chat_data["session_key"] = f"hermes-{chat_id}-{int(time.time())}"
    chat_data["force_new"] = True
    chat_data.pop("model_locked", None)
    chat_data.pop("booted", None)
    return stats


async def cmd_clear(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Azzera la sessione e cancella i messaggi del bot (Telegram: solo <48h)."""
    chat_id = update.message.chat_id
    deleted = 0
    for mid in _sent_ids.pop(chat_id, []):
        try:
            await ctx.bot.delete_message(chat_id, mid)
            deleted += 1
        except Exception:
            pass
    _reset_session(chat_id, ctx.chat_data)
    note = (
        "Chat pulita.\n"
        f"Messaggi miei eliminati: {deleted}.\n"
        "Sessione azzerata: contesto riparte da zero.\n"
        "Nota: i tuoi messaggi e quelli oltre le 48h restano — Telegram non li fa cancellare ai bot."
    )
    await update.message.reply_text(note)


async def cmd_tokens(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mostra uso token della sessione corrente."""
    sk = _get_session_key(update.message.chat_id, ctx.chat_data)
    stats = session_tokens.get(sk)
    if not stats:
        await update.message.reply_text("Nessun dato token per questa sessione.")
        return
    total = stats["total_in"] + stats["total_out"]
    bar_len = min(20, max(1, total // 5000))
    bar = "█" * bar_len + "░" * (20 - bar_len)
    lines = [
        f"Sessione: {sk[-20:]}",
        f"Turni: {stats['turns']}",
        f"Token in: {stats['total_in']:,}",
        f"Token out: {stats['total_out']:,}",
        f"Totale: {total:,}",
        f"[{bar}] {'~' + str(total//1000) + 'k'}",
        f"Modello: {stats['model']}",
    ]
    warning, _ = _check_session_health(sk)
    if warning:
        lines.append(f"\n{warning}")
    await update.message.reply_text("\n".join(lines))


async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    names = [m.split("/")[-1] for m in cfg["models"]]
    await update.message.reply_text("Modelli:\n" + "\n".join(f"  {i+1}. {n}" for i, n in enumerate(names)))


# --- Comandi utensile ---

def _resolve(arg: str | None) -> Path:
    base = Path(cfg["working_dir"])
    if not arg:
        return base
    p = Path(arg.strip().strip('"'))
    return p if p.is_absolute() else base / p


async def _reply_chunked(update: Update, text: str):
    text = text or "(vuoto)"
    for i in range(0, len(text), TG_LIMIT):
        await _reply(update, text[i:i + TG_LIMIT])


# ID dei messaggi inviati dal bot, per /clear (Telegram: cancelabili solo <48h)
_sent_ids: dict[int, list[int]] = {}


async def _reply(update: Update, text: str):
    msg = await update.message.reply_text(text)
    ids = _sent_ids.setdefault(update.message.chat_id, [])
    ids.append(msg.message_id)
    del ids[:-200]
    return msg


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "OdissREA — strumenti disponibili (Telegram).\n"
        "Comandi slash: /start /help /status /tokens /models /new /fritz /ip /ls /read /run /search\n\n"
        "In linguaggio naturale (senza slash):\n"        "  leggi <file> [righe]       → legge file (es. leggi config.json)\n"
        "  elenca [cartella]          → elenca file (default cartella lavoro)\n"
        "  esegui <comando>           → esegui shell (60s, distruttivi bloccati)\n"
        "  cerca <domanda>            → ricerca web in italiano\n"
        "  stato / ip / token / modelli → stato sessione\n"
        "  nuova                      → nuova conversazione\n"
        "  aiuto / comandi / strumenti → questo messaggio\n\n"
        "Esempi reali:\n"
        "  leggi hermes.py 100      → primi 100 righe di hermes.py\n"
        "  esegui git status        → stato repo Git\n"
        "  cerca errore 429         → cosa significa e come risolvere\n\n"
        "Nota: dopo cambiamenti, chiudi e rilancia start.cmd (polling esclusivo).\n"
        "Audio: invia una nota vocale o un file audio, la trascrivo e rispondo."
    )


async def cmd_ls(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    target = _resolve(" ".join(ctx.args) if ctx.args else None)
    if not target.exists():
        await update.message.reply_text(f"Non esiste: {target}")
        return
    if target.is_file():
        await update.message.reply_text(f"{target} ({target.stat().st_size:,} byte)")
        return
    rows = sorted(target.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))[:100]
    lines = [f"{target}"]
    for p in rows:
        if p.is_dir():
            lines.append(f"  [dir]  {p.name}")
        else:
            try:
                size = f"{p.stat().st_size:,}"
            except OSError:
                size = "?"
            lines.append(f"  {size:>10}  {p.name}")
    await _reply_chunked(update, "\n".join(lines))


async def cmd_read(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Uso: /read <percorso> [righe]")
        return
    target = _resolve(ctx.args[0])
    max_lines = 200
    if len(ctx.args) > 1 and ctx.args[1].isdigit():
        max_lines = min(int(ctx.args[1]), 800)
    try:
        raw = target.read_bytes()
    except OSError as e:
        await update.message.reply_text(f"Errore lettura: {e}")
        return
    if len(raw) > 2_000_000:
        await update.message.reply_text(f"File troppo grande ({len(raw):,} byte). Usa /run con head/tail.")
        return
    lines = raw.decode("utf-8", errors="replace").splitlines()
    head = "\n".join(lines[:max_lines])
    suffix = f"\n...[troncato, {len(lines)} righe totali]" if len(lines) > max_lines else ""
    await _reply_chunked(update, f"{target}\n```\n{head[:3500]}\n```{suffix}")


async def cmd_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cmd = " ".join(ctx.args).strip()
    if not cmd:
        await update.message.reply_text("Uso: /run <comando>")
        return
    if DANGEROUS.search(cmd):
        await update.message.reply_text("Bloccato: comando distruttivo o irreversibile. Eseguilo a mano.")
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            "pwsh", "-NoProfile", "-Command", cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=cfg["working_dir"],
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=RUN_TIMEOUT)
    except asyncio.TimeoutError:
        await update.message.reply_text(f"Timeout {RUN_TIMEOUT}s.")
        return
    except Exception as e:
        await update.message.reply_text(f"Errore: {e}")
        return
    body = out.decode("utf-8", errors="replace").strip()
    await _reply_chunked(update, f"exit {proc.returncode}\n```\n{body[:3500]}\n```")


async def cmd_ip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        proc = await asyncio.create_subprocess_exec(
            "pwsh", "-NoProfile", "-File", cfg["fritz_reconnect_bin"], "-Status",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
    except Exception as e:
        await update.message.reply_text(f"Errore: {e}")
        return
    text = out.decode("utf-8", errors="replace").strip()
    m = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)
    await update.message.reply_text(f"IP pubblico: {m.group(0)}" if m else text[:1000] or "(nessun output)")


async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = " ".join(ctx.args).strip()
    if not query:
        await update.message.reply_text("Uso: /search <domanda>")
        return
    await update.message.reply_text("🔎")
    response, warning, _ = await handle_prompt(
        "Cerca sul web le informazioni aggiornate e rispondi in italiano, citando le fonti: " + query,
        update.message.chat_id, ctx.chat_data,
    )
    await _reply_chunked(update, response)
    if warning:
        await update.message.reply_text(warning)


async def cmd_unknown(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Comando sconosciuto. /help per la lista.")


def _kill_other_hermes() -> list[int]:
    """Termina ogni altra istanza di hermes.py (python/pythonw). Ritorna i PID chiusi."""
    me = os.getpid()
    ps = (
        f"$me={me}; "
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match '^pythonw?\\.exe$' -and "
        "$_.CommandLine -like '*hermes.py*' -and $_.ProcessId -ne $me } | "
        "Select-Object -ExpandProperty ProcessId"
    )
    try:
        out = subprocess.run(
            ["pwsh", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=25,
        ).stdout
    except Exception as exc:
        log.warning("Scansione istanze hermes fallita: %s", exc)
        return []
    pids = [int(x) for x in out.split() if x.strip().isdigit()]
    for pid in pids:
        try:
            subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                           capture_output=True, timeout=15)
        except Exception as exc:
            log.warning("Kill PID %d fallito: %s", pid, exc)
    # Pulisci anche opencode orfani lanciati da hermes (env HERMES_AGENT=1)
    orphans = _kill_orphan_opencode()
    if orphans:
        log.info("Opencode orfani di hermes chiusi: %s", orphans)
    return pids


def _kill_orphan_opencode() -> list[int]:
    """Killa processi opencode.exe il cui parent non esiste più (orfani di hermes)."""
    ps = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -eq 'opencode.exe' } | "
        "ForEach-Object { $p = $_.ParentProcessId; "
        "  $alive = $null -ne (Get-Process -Id $p -ErrorAction SilentlyContinue); "
        "  if (-not $alive) { $_.ProcessId } }"
    )
    try:
        out = subprocess.run(
            ["pwsh", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=25,
        ).stdout
    except Exception:
        return []
    pids = [int(x) for x in out.split() if x.strip().isdigit()]
    for pid in pids:
        try:
            subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                           capture_output=True, timeout=15)
        except Exception:
            pass
    return pids


def _spawn_hermes_detached(delay: int = 3):
    """Rilancia il bot tra `delay` secondi in un processo staccato: sopravvive a questo."""
    script = str(Path(__file__).resolve())
    flags = 0
    if os.name == "nt":
        flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 0
        )
    cmd = f'timeout /t {delay} /nobreak >nul & "{sys.executable}" "{script}"'
    try:
        subprocess.Popen(
            ["cmd.exe", "/c", cmd],
            cwd=str(Path(script).parent),
            creationflags=flags,
            close_fds=True,
        )
        log.info("Rilancio programmato tra %ds", delay)
    except Exception as exc:
        log.error("Rilancio fallito: %s", exc)


async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Auto-restart: killa ogni altra istanza hermes e rilancia una sola copia pulita."""
    if update.message.chat_id not in cfg["allowed_chat_ids"]:
        return
    killed = _kill_other_hermes()
    _spawn_hermes_detached(delay=3)
    detail = f" (chiusi {len(killed)} duplicati)" if killed else ""
    await update.message.reply_text(f"Riavvio in corso{detail}... online tra ~10s.")
    log.info("Restart richiesto da chat %s — duplicati chiusi: %s", update.message.chat_id, killed)
    await asyncio.sleep(1)  # lascia spedire la reply
    os._exit(0)


# --- Routing in linguaggio naturale: stessi handler, senza slash ---

class _Ctx:
    """Contenitore minimale compatibile con ContextTypes (args + chat_data)."""
    def __init__(self, args: list[str], chat_data: dict):
        self.args = args
        self.chat_data = chat_data


def _split_read(text: str) -> list[str]:
    """<percorso> [righe]: le righe sono l'ultimo token solo se numerico."""
    parts = text.rsplit(None, 1)
    if len(parts) == 2 and parts[1].isdigit():
        return [parts[0], parts[1]]
    return [text] if text else []


INTENTS: list[tuple[re.Pattern, object, str]] = [
    (re.compile(r"^(?:leggi|read)\s+(.+)$", re.I), cmd_read, "read"),
    (re.compile(r"^(?:esegui|exec|run|shell|terminale)\s+(.+)$", re.I), cmd_run, "split"),
    (re.compile(r"^(?:elenca|lista|dir|ls)\s*(.*)$", re.I), cmd_ls, "split"),
    (re.compile(r"^(?:cerca|search|google)\s+(.+)$", re.I), cmd_search, "split"),
    (re.compile(r"^(?:stato|status)$", re.I), cmd_status, "none"),
    (re.compile(r"^(?:ip|ip pubblico)$", re.I), cmd_ip, "none"),
    (re.compile(r"^(?:tokens?|token)$", re.I), cmd_tokens, "none"),
    (re.compile(r"^(?:modelli?|models)$", re.I), cmd_models, "none"),
    (re.compile(r"^(?:aiuto|help|comandi|strumenti)$", re.I), cmd_help, "none"),
    (re.compile(r"^(?:riavvia|restart|reboot|ricomincia il bot)$", re.I), cmd_restart, "none"),
    (re.compile(r"^(?:nuova|new|reset|cambia argomento)$", re.I), cmd_new, "none"),
    (re.compile(r"^(?:clear|pulisci|pulisci chat|cancella chat|cancella messaggi)$", re.I), cmd_clear, "none"),
    (re.compile(r"^(?:team|squadra|multi[- ]?agent)\s+(.+)$", re.I), "cmd_team", "split"),
]


async def try_natural_tool(text: str, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """Se il testo corrisponde a uno strumento, lo esegue e ritorna True."""
    stripped = text.strip()
    for pattern, handler, mode in INTENTS:
        m = pattern.match(stripped)
        if not m:
            continue
        payload = m.group(1) if m.groups() else ""
        if mode == "none":
            args: list[str] = []
        elif mode == "split":
            args = payload.split()
        elif mode == "read":
            args = _split_read(payload)
        else:
            args = payload.split()
        fn = globals()[handler] if isinstance(handler, str) else handler
        await fn(update, _Ctx(args, ctx.chat_data))
        return True
    return False


AGENT_ROLES = [
    ("analyst", "🔍", "Analizza il problema in dettaglio: individua requisiti, vincoli e rischi. Rispondi in italiano, max 300 parole."),
    ("coder", "💻", "Scrivi il codice o la soluzione tecnica. Rispondi in italiano con codice funzionante e conciso."),
    ("reviewer", "🔎", "Rivedi criticamente il task: trova punti deboli, errori logici e migliorie. Rispondi in italiano, max 200 parole."),
]

# Cache risultati agenti per inline keyboard (chat_id -> {team_id -> {role -> text}})
_team_results: dict[int, dict[str, dict]] = {}
_team_counter = 0


def _bar(done: bool, running: bool) -> str:
    if done:
        return "▓▓▓▓▓▓▓▓ ✅"
    if running:
        return "▓▓▓▓░░░░ ⚙️"
    return "░░░░░░░░ ⏳"


def _render_dashboard(task: str, agents: list[dict], elapsed: float, synthesis: str | None = None) -> str:
    """Genera il testo della dashboard live."""
    t = task[:80] + ("..." if len(task) > 80 else "")
    lines = [f"🤖 <b>TEAM</b> — {t}", ""]
    for i, a in enumerate(agents):
        prefix = "┌" if i == 0 else ("└" if i == len(agents) - 1 else "├")
        model_short = a["model"].split("/")[-1][:20] if a["model"] else "—"
        bar = _bar(a["done"], a["running"])
        elapsed_a = f" {a['elapsed']:.0f}s" if a["done"] else ""
        lines.append(f"{prefix} {a['icon']} <b>{a['role'].upper()}</b>  {bar}  <code>{model_short}</code>{elapsed_a}")

    lines.append("")
    if synthesis == "running":
        lines.append("🧠 <b>SINTESI</b>  ▓▓▓▓░░░░ ⚙️")
    elif synthesis:
        lines.append("🧠 <b>SINTESI</b>  ▓▓▓▓▓▓▓▓ ✅")
    else:
        lines.append("🧠 <b>SINTESI</b>  ░░░░░░░░ ⏳")

    lines.append(f"\n⏱ {elapsed:.0f}s")
    return "\n".join(lines)


async def cmd_team(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Comando /team: lancia multi-agent con dashboard live."""
    if update.message.chat_id not in cfg["allowed_chat_ids"]:
        return
    task = update.message.text
    if hasattr(ctx, 'args') and ctx.args:
        task = " ".join(ctx.args)
    else:
        task = (task or "").split(None, 1)[1] if (task and " " in task) else ""
    if not task:
        await update.message.reply_text(
            "🤖 <b>Multi-Agent Team</b>\n\n"
            "Uso: <code>/team descrizione task</code>\n\n"
            "Lancia 3 agenti in parallelo:\n"
            "🔍 <b>Analyst</b> — analisi e requisiti\n"
            "💻 <b>Coder</b> — soluzione tecnica\n"
            "🔎 <b>Reviewer</b> — revisione critica\n"
            "🧠 <b>Sintesi</b> — risposta finale integrata",
            parse_mode="HTML",
        )
        return

    global _team_counter
    _team_counter += 1
    team_id = f"t{_team_counter}"
    chat_id = update.message.chat_id

    available = [m for m in cfg["models"] if time.time() - model_failures.get(m, 0) > 600]
    if not available:
        await update.message.reply_text("Nessun modello disponibile.")
        return

    agents = []
    for i, (role, icon, _system) in enumerate(AGENT_ROLES):
        agents.append({
            "role": role, "icon": icon,
            "model": available[i % len(available)],
            "done": False, "running": False, "elapsed": 0.0, "result": "",
        })

    t0 = time.time()
    dashboard = await update.message.reply_text(
        _render_dashboard(task, agents, 0),
        parse_mode="HTML",
    )

    async def _update_dash(synthesis=None):
        try:
            await dashboard.edit_text(
                _render_dashboard(task, agents, time.time() - t0, synthesis),
                parse_mode="HTML",
            )
        except Exception:
            pass

    async def _run_agent(idx: int):
        role, _icon, system = AGENT_ROLES[idx]
        agents[idx]["running"] = True
        await _update_dash()
        at0 = time.time()
        prompt = f"[RUOLO: {role.upper()}]\n{system}\n\nTASK: {task}"
        resp, limited, _ = await run_opencode(prompt, agents[idx]["model"], continue_session=False)
        agents[idx]["elapsed"] = time.time() - at0
        agents[idx]["done"] = True
        agents[idx]["running"] = False
        if limited:
            model_failures[agents[idx]["model"]] = time.time()
            agents[idx]["result"] = f"(rate limit su {agents[idx]['model']})"
        else:
            agents[idx]["result"] = resp
        await _update_dash()

    await asyncio.gather(*[_run_agent(i) for i in range(len(AGENT_ROLES))])

    # Sintesi
    await _update_dash(synthesis="running")
    synthesis_prompt = (
        "Sei un coordinatore. Sintetizza le risposte di 3 agenti specializzati.\n"
        f"Task: {task}\n\n"
    )
    for a in agents:
        synthesis_prompt += f"--- {a['role'].upper()} ---\n{a['result']}\n\n"
    synthesis_prompt += (
        "Rispondi in italiano con una risposta finale integrata. "
        "Incorpora i punti migliori, correggi errori del reviewer. Max 500 parole."
    )

    synth_model = available[0]
    synth_resp, limited, _ = await run_opencode(synthesis_prompt, synth_model, continue_session=False)
    if limited:
        synth_resp = "\n\n".join(f"<b>{a['role'].upper()}</b>:\n{a['result']}" for a in agents)

    await _update_dash(synthesis="done")

    # Salva risultati per callback inline
    _team_results.setdefault(chat_id, {})[team_id] = {
        "task": task,
        "agents": {a["role"]: {"text": a["result"], "model": a["model"], "time": a["elapsed"]} for a in agents},
        "synthesis": synth_resp,
    }

    # Inline keyboard per espandere risultati singoli agenti
    buttons = []
    for a in agents:
        buttons.append(InlineKeyboardButton(f"{a['icon']} {a['role'].upper()}", callback_data=f"team:{team_id}:{a['role']}"))
    keyboard = InlineKeyboardMarkup([buttons])

    clean_synth = synth_resp[:TG_LIMIT - 200]
    await update.message.reply_text(
        f"🧠 <b>Sintesi finale</b>\n\n{clean_synth}",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


async def on_team_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Gestisce click sui bottoni inline dei risultati team."""
    query = update.callback_query
    if not query.data.startswith("team:"):
        return
    await query.answer()
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        return
    _, team_id, role = parts
    chat_id = query.message.chat_id

    team_data = _team_results.get(chat_id, {}).get(team_id)
    if not team_data:
        await query.answer("Risultati scaduti.", show_alert=True)
        return

    agent_data = team_data["agents"].get(role)
    if not agent_data:
        return

    icon = {"analyst": "🔍", "coder": "💻", "reviewer": "🔎"}.get(role, "🤖")
    model_short = agent_data["model"].split("/")[-1]
    header = f"{icon} <b>{role.upper()}</b>  ·  <code>{model_short}</code>  ·  {agent_data['time']:.0f}s\n\n"
    text = header + agent_data["text"][:TG_LIMIT - len(header)]
    await query.message.reply_text(text, parse_mode="HTML")


COMMANDS = [
    ("start", "avvia il bot"),
    ("help", "lista comandi"),
    ("status", "stato modelli, IP e sessione"),
    ("tokens", "uso token della sessione"),
    ("models", "modelli free disponibili"),
    ("new", "nuova conversazione"),
    ("clear", "pulisci chat e sessione"),
    ("restart", "riavvia il bot da solo"),
    ("fritz", "cambia IP pubblico"),
    ("ip", "mostra IP pubblico attuale"),
    ("ls", "elenca file: /ls [percorso]"),
    ("read", "leggi file: /read <percorso> [righe]"),
    ("run", "esegui comando shell: /run <comando>"),
    ("search", "ricerca web: /search <domanda>"),
    ("team", "multi-agent: /team <task>"),
]


async def process_text(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    prompt: str,
    skip_tools: bool = False,
):
    """Pipeline unica testo: prima strumenti, poi modello (render live)."""
    if not skip_tools and await try_natural_tool(prompt, update, ctx):
        return
    status = await update.message.reply_text("⏳ avvio...")
    ids = _sent_ids.setdefault(update.message.chat_id, [])
    ids.append(status.message_id)
    del ids[:-200]
    streamer = Streamer(status)
    streamer.start()
    try:
        response, warning, needs_fritz = await handle_prompt(
            prompt,
            update.message.chat_id,
            ctx.chat_data,
            on_model=streamer.set_model,
            on_event=streamer.feed,
        )
    except Exception as exc:
        log.exception("handle_prompt fallito")
        await streamer.abort(f"Errore: {exc}")
        return
    rest = await streamer.finish(response)
    if rest:
        await _reply_chunked(update, rest)
    if warning:
        await _reply(update, warning)
    if needs_fritz:
        await _reply(update, "🔄 Cambio IP proattivo per rinnovare la quota...")
        ok = await fritz_reconnect()
        if ok:
            _reset_session(update.message.chat_id, ctx.chat_data)
            await _reply(
                update,
                "✅ IP cambiato, quota resettata, nuova sessione avviata.\n"
                "Continua a scrivere normalmente."
            )
        else:
            await _reply(
                update,
                "⚠️ Fritz-reconnect non disponibile (cooldown). "
                "Usa /new per risparmiare token con un contesto pulito."
            )


# --- Audio: voice/audio Telegram → Whisper (sherpa-onnx) → testo ---

_stt_recognizer = None


def _get_stt():
    """Carica il recognizer Whisper ONNX una sola volta (~1-2s, poi in cache)."""
    global _stt_recognizer
    if _stt_recognizer is None:
        import sherpa_onnx
        d = Path(cfg["stt_model_dir"])
        if not d.is_absolute():
            d = Path(__file__).parent / d
        enc = next(d.glob("*-encoder*.onnx"))
        dec = next(d.glob("*-decoder*.onnx"))
        tok = next(d.glob("*tokens.txt"))
        log.info("Carico STT: %s", d.name)
        _stt_recognizer = sherpa_onnx.OfflineRecognizer.from_whisper(
            encoder=str(enc),
            decoder=str(dec),
            tokens=str(tok),
            language=cfg.get("stt_language", "it"),
            task="transcribe",
            num_threads=4,
        )
    return _stt_recognizer


def _stt_run(wav_path: str) -> str:
    import wave

    import numpy as np

    with wave.open(wav_path, "rb") as f:
        sr = f.getframerate()
        data = np.frombuffer(f.readframes(f.getnframes()), dtype=np.int16)
    samples = data.astype(np.float32) / 32768.0
    rec = _get_stt()
    stream = rec.create_stream()
    stream.accept_waveform(sr, samples)
    rec.decode_stream(stream)
    return stream.result.text.strip()


async def transcribe(media_path: Path) -> str:
    """ffmpeg → 16k mono wav → Whisper. Ritorna stringa vuota se fallisce."""
    wav_path = media_path.with_name(media_path.stem + ".16k.wav")
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(media_path),
            "-ar", "16000", "-ac", "1", "-f", "wav", str(wav_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.communicate(), timeout=60)
        if not wav_path.exists():
            return ""
        return await asyncio.to_thread(_stt_run, str(wav_path))
    except Exception as e:
        log.error("Trascrizione fallita: %s", e)
        return ""
    finally:
        wav_path.unlink(missing_ok=True)


KITT_PROMPT_PATH = Path(__file__).parent / "kitt-prompt.md"
KITT_PREFIX = ""


def _load_kitt_prefix() -> str:
    if KITT_PROMPT_PATH.exists():
        return KITT_PROMPT_PATH.read_text("utf-8").strip() + "\n\n"
    return ""


async def tts_kitt(text: str) -> Path | None:
    """Sintetizza testo con voce KITT via subprocess (venv-voice). Ritorna il path OGG o None."""
    if not cfg.get("kitt_voice_enabled"):
        return None
    wav = Path(tempfile.gettempdir()) / f"kitt-{int(time.time()*1000)}.wav"
    ogg = wav.with_suffix(".ogg")
    try:
        proc = await asyncio.create_subprocess_exec(
            cfg["kitt_python"], cfg["kitt_tts_script"], text, str(wav),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0 or not wav.exists():
            log.error("TTS KITT fallito: %s", err.decode("utf-8", errors="replace")[:500])
            return None
        proc2 = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(wav),
            "-c:a", "libopus", "-b:a", "48k", "-ar", "48000", "-ac", "1",
            str(ogg),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc2.communicate(), timeout=30)
        if not ogg.exists():
            return None
        return ogg
    except asyncio.TimeoutError:
        log.error("TTS KITT timeout")
        return None
    except Exception as e:
        log.error("TTS KITT errore: %s", e)
        return None
    finally:
        wav.unlink(missing_ok=True)


async def on_voice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.chat_id not in cfg["allowed_chat_ids"]:
        return
    msg = update.message
    media = msg.voice or msg.audio or msg.video_note
    if not media:
        return
    await msg.reply_text("🎙️ trascrivo...")
    tmp = Path(tempfile.gettempdir()) / f"hermes-{msg.message_id}.ogg"
    try:
        tg_file = await media.get_file()
        await tg_file.download_to_drive(str(tmp))
        text = await transcribe(tmp)
    except Exception as e:
        log.error("Download audio fallito: %s", e)
        await msg.reply_text("Audio non leggibile.")
        return
    finally:
        tmp.unlink(missing_ok=True)

    if not text:
        await msg.reply_text("Non ho capito l'audio (o STT non configurato).")
        return
    await msg.reply_text(f"🎙️ {text}")

    # Risposta con persona KITT se voce abilitata, altrimenti testo normale
    if cfg.get("kitt_voice_enabled") and KITT_PREFIX:
        await msg.reply_text("⏳ KITT pensa...")
        session_key = _get_session_key(msg.chat_id, ctx.chat_data)
        continue_session = not ctx.chat_data.get("force_new", False)
        ctx.chat_data["force_new"] = False
        kitt_prompt = KITT_PREFIX + text
        # Usa il primo modello disponibile
        response_text = ""
        for model in cfg["models"]:
            if time.time() - model_failures.get(model, 0) <= 600:
                continue
            resp, limited, token_info = await run_opencode(kitt_prompt, model, continue_session)
            if not limited:
                _update_token_stats(session_key, token_info, model)
                response_text = resp
                break
            model_failures[model] = time.time()
        if not response_text:
            await msg.reply_text("Modelli non disponibili.")
            return
        # Rimuovi tag modello e markdown per la voce
        clean = re.sub(r"^\[.*?\]\n", "", response_text).strip()
        clean = re.sub(r"[*_`#\[\]]", "", clean)
        await msg.reply_text(f"🗣️ {clean}")
        # TTS con voce KITT
        ogg = await tts_kitt(clean)
        if ogg:
            try:
                with open(ogg, "rb") as f:
                    await msg.reply_voice(f)
            finally:
                ogg.unlink(missing_ok=True)
    else:
        await process_text(update, ctx, text)


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.chat_id not in cfg["allowed_chat_ids"]:
        return
    prompt = update.message.text
    if not prompt:
        return
    await process_text(update, ctx, prompt)


SCRIPTS_DIR = Path(__file__).parent / "scripts"


async def _download_file(update: Update, file_obj, suffix: str) -> Path:
    tg_file = await file_obj.get_file()
    dest = Path(tempfile.mkdtemp(prefix="hermes-")) / f"input{suffix}"
    await tg_file.download_to_drive(str(dest))
    return dest


async def _run_script(name: str, *args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(SCRIPTS_DIR / name), *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(proc.communicate(), timeout=180)
    return proc.returncode, (out + err).decode("utf-8", errors="replace").strip()


async def on_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """PDF → testo locale (pypdf), poi il modello. Nessun PDF mandato al modello."""
    if update.message.chat_id not in cfg["allowed_chat_ids"]:
        return
    doc = update.message.document
    name = doc.file_name or "documento.pdf"
    if not name.lower().endswith(".pdf"):
        await _reply(update, "Leggo solo PDF, per ora. Altri formati: /run con uno script.")
        return
    status = await update.message.reply_text("📄 estraggo il testo...")
    try:
        src = await _download_file(update, doc, ".pdf")
        dst = src.with_suffix(".txt")
        code, msg = await _run_script("extract_pdf.py", str(src), str(dst))
    except Exception as exc:
        await status.edit_text(f"Errore estrazione PDF: {exc}"[:3500])
        return
    if code != 0 or not dst.exists():
        hint = "PDF di sole immagini: serve OCR (tesseract)." if code == 3 else msg
        await status.edit_text(hint[:3500])
        return
    body = dst.read_text("utf-8", errors="replace")
    caption = update.message.caption or "Riassumi i punti chiave di questo documento."
    prompt = f"{caption}\n\n[PDF: {name}, {len(body)} caratteri]\n\n{body[:9000]}"
    await process_text(update, ctx, prompt, skip_tools=True)


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Screenshot/immagine → OCR locale (tesseract), poi il modello."""
    if update.message.chat_id not in cfg["allowed_chat_ids"]:
        return
    photo = update.message.photo[-1]
    status = await update.message.reply_text("🖼️ OCR in corso...")
    try:
        src = await _download_file(update, photo, ".jpg")
        dst = src.with_suffix(".txt")
        code, msg = await _run_script("extract_image_ocr.py", str(src), str(dst))
    except Exception as exc:
        await status.edit_text(f"Errore OCR: {exc}"[:3500])
        return
    if code != 0 or not dst.exists():
        hint = "tesseract non installato: serve per l'OCR." if "tesseract" in msg else msg
        await status.edit_text(hint[:3500])
        return
    body = dst.read_text("utf-8", errors="replace")
    caption = update.message.caption or "Rispondi a questa immagine."
    await process_text(update, ctx, f"{caption}\n\n[Testo OCR]\n\n{body[:9000]}", skip_tools=True)


async def post_init(app: Application):
    await app.bot.set_my_commands([BotCommand(c, d[:256]) for c, d in COMMANDS])
    log.info("Comandi Telegram registrati: %d", len(COMMANDS))


def main():
    load_config()
    stale = _kill_other_hermes()
    if stale:
        log.info("Istanze hermes duplicate chiuse all'avvio: %s", stale)
    log.info("Hermes Agent avviato — %d modelli, chat_ids=%s", len(cfg["models"]), cfg["allowed_chat_ids"])

    app = (
        Application.builder()
        .token(cfg["telegram_token"])
        .post_init(post_init)
        .connect_timeout(30)
        .read_timeout(60)
        .write_timeout(30)
        .get_updates_connect_timeout(30)
        .get_updates_read_timeout(30)
        .build()
    )
    auth = filters.Chat(cfg["allowed_chat_ids"])

    handlers = {
        "start": cmd_start, "help": cmd_help, "status": cmd_status,
        "tokens": cmd_tokens, "models": cmd_models, "new": cmd_new,
        "clear": cmd_clear,
        "fritz": cmd_fritz, "ip": cmd_ip, "ls": cmd_ls,
        "read": cmd_read, "run": cmd_run, "search": cmd_search,
        "restart": cmd_restart,
        "team": cmd_team,
    }
    for name, fn in handlers.items():
        app.add_handler(CommandHandler(name, fn, filters=auth))
    app.add_handler(CallbackQueryHandler(on_team_callback, pattern=r"^team:"))
    app.add_handler(MessageHandler(auth & filters.COMMAND, cmd_unknown))
    app.add_handler(MessageHandler(auth & (filters.VOICE | filters.AUDIO | filters.VIDEO_NOTE), on_voice))
    app.add_handler(MessageHandler(auth & filters.Document.PDF, on_document))
    app.add_handler(MessageHandler(auth & filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(auth & filters.TEXT & ~filters.COMMAND, on_message))

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
