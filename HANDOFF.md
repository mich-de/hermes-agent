# HANDOFF — Hermes Agent / OdissREA (2026-11-08)

## 1. Obiettivo
Bot Telegram (@OdissREA_bot) che orchestra modelli AI free via Open Code CLI con multi-agent team, voce KITT, rotazione IP Fritz-reconnect.

## 2. Stato attuale
- **Bot attivo** su pythonw, polling OK, **16 comandi** registrati
- **Repo GitHub**: https://github.com/mich-de/hermes-agent (public, commit iniziale)
- **Multi-agent `/team`** (`run_team`): dispatcher → 2-4 sottotask JSON (fallback ai 3 ruoli fissi) → worker paralleli con `_run_with_retry` (max 3 modelli) → verifier (snapshot mtime di working_dir, risponde `OK`/`KO n: motivo`, non scrive codice) → 1 giro correzione → sintesi. Dashboard live + bottoni inline per sottotask. **Non testato e2e.**
- **`/schedule`**: `HH:MM <prompt>` o `HH:MM /team <task>`, `list`, `del N`. Ciclo asyncio ogni 30s (`_scheduler_loop`, avviato in `post_init`), salvato su `.schedules.json`. Ogni run riceve le 5 note più recenti del vault (`vault/`, config `vault_dir`). Nessuna dipendenza nuova (JobQueue scartato: richiede APScheduler). **Non testato e2e.**
- Self-check parser: `python test_team.py`
- **KITT voice**: pipeline completa (STT Whisper → Open Code → TTS Pocket → voice Telegram), non testato e2e
- **Restart**: `/restart` scrive `_relaunch.cmd` (ping delay + start pythonw), lanciato con `cmd.exe /c start /min` + `CREATE_NEW_CONSOLE`, sopravvive a `os._exit(0)`. Post-init manda feedback "✅ online" via `.restart_chat` flag. **Da testare e2e su Telegram.**
- **Token tracking**: persistente su `.tokens.json`, `/tokens` e `/quota` per statistiche
- **Env var opencode**: `HERMES_AGENT=1`, `HERMES_MODEL`, `HERMES_PID`, `HERMES_SESSION`
- **Cleanup orfani**: `_kill_orphan_opencode()` all'avvio
- **Streaming**: render live a 3 righe, typewriter ~1 edit/s
- **Quota**: soglie % su 150k token, 60% avviso, 90% stop + cambio IP

## 3. Tentativi falliti e problemi risolti
- `telegram.error.TimedOut` bootstrap → timeout builder: connect 30, read 60, write 30, get_updates 30
- `409 Conflict` istanze duplicate → `_kill_other_hermes()` all'avvio
- `cmd_team` in INTENTS prima della definizione → lazy lookup via stringa + `globals()`
- **`_spawn_hermes_detached` (3 tentativi)**:
  1. `cmd.exe timeout` → non funziona senza console (DETACHED_PROCESS)
  2. `pwsh Start-Sleep + Start-Process` con DETACHED_PROCESS → `os._exit(0)` uccide il figlio pwsh
  3. **Fix finale**: scrive `_relaunch.cmd` su disco (ping delay + start pythonw + del self), lanciato con `cmd.exe /c start /min` + `CREATE_NEW_CONSOLE` — processo completamente indipendente
- Bot killato dal task runner Claude Code → pythonw (processo indipendente)
- opencode.exe orfano da 5 giorni (242MB) → cleanup automatico + kill manuale
- `Streamer.start` collisione attributo → rinominato `t0`
- pypdf assente → installato, aggiunto a requirements.txt
- opencode-quota plugin → "No provider data available" (free tier non espone quota residua)

## 4. Decisioni prese
- Herdr scartato: emulazione x64 su ARM64 inaccettabile, multi-agent nativo Python con asyncio.gather
- TTS locale Pocket TTS per voce KITT (nessun cloud free supporta voice cloning)
- pythonw per processo bot, logging su hermes.log con FileHandler
- Un solo messaggio editato per risposta, ~1 edit/s (flood control Telegram)

## 5. File toccati (ultime sessioni)
`hermes.py` (multi-agent, dashboard, restart cmd fix, env var, cleanup, quota, token persistence), `config.example.json`, `.gitignore` (aggiunto `_relaunch.cmd`), pushati su GitHub.

## 6. Prossimi passi
1. **Testare `/restart`** end-to-end su Telegram — verificare che il bot torni online e mandi feedback
2. **Testare `/team`** end-to-end (nuova pipeline dispatcher/retry/verifier) e `/schedule` con orario a 2 minuti
3. **Testare KITT voice** — vocale → STT → LLM → TTS → voice message
4. **Escape HTML** nelle risposte OpenCode (`<>&`) prima di parse_mode="HTML"
5. **Path STT**: config ha `models/sherpa-onnx-whisper-base` (relativo) ma modello è `C:\npu\models\whisper-small`
6. **TTL su `_team_results`**: i risultati inline si accumulano senza limite
7. **Servizio Windows** o auto-start per persistenza bot
8. Push aggiornamenti su GitHub
