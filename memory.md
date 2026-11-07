# memory.md — Hermes Agent (OdissREA)

## Fatti stabili
- Bot Telegram `hermes.py` → `opencode run --format json` → modelli free, con rotazione e `fritz-reconnect` su rate limit.
- Python: `C:\Users\mich-de\AppData\Local\Programs\Python\Python312-arm64\python.exe` (python nel PATH).
- Avvio: `start.cmd` (loop di riavvio ogni 5s).
- `opencode run --format json` emette JSONL: `step_start`, `text` (blocco intero a step finito), `tool*`, `step_finish` con `tokens`. **Non** fa streaming token-per-token.
- Telegram: ~1 edit/secondo per chat → il typewriter è a colpi, non carattere per carattere.

## Decisioni
- Render live in `streaming.py` (classe `Streamer`): status bar a 3 righe (fase / barra o onda / strumenti), typewriter a colpi, nessuna chiamata extra al modello.
- `run_opencode` legge stdout riga per riga e chiama `on_event` (callback sincrona o async).
- `handle_prompt(..., on_model, on_event)`; `process_text` mostra un solo messaggio che viene editato, poi ancora messaggi in coda oltre 3200 caratteri vivi.

## Convenzioni
- Protocollo Save Your Tokens in `system-prompt.md`: boot da `HANDOFF.md` (<2h), lock modello per sessione, ASCII gate, CLI over MCP, `curl.exe`, determinismo, subagent solo bulk.
- `HANDOFF.md` (6 sezioni fisse) è il checkpoint; niente compact, si riparte con `/new`.
- Risposte in italiano, dirette; un solo messaggio Telegram editato invece di "⏳" + risposta finale.
