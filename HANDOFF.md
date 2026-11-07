# HANDOFF — Hermes Agent / OdissREA (2026-11-07)

## 1. Obiettivo
Bot personale Telegram → `opencode run` con modelli free; render live (status bar + typewriter), PDF/OCR locali, riavvio a istanza singola; protocollo Save Your Tokens applicato.

## 2. Stato attuale
- `streaming.py`: render a 3 righe (fase / barra-o-onda / strumenti), typewriter a colpi 1/s, cursore `▌`, header finale con token.
- `hermes.py`: eventi opencode in streaming, un solo messaggio editato, lock modello per sessione, boot da `HANDOFF.md` (<2h), `/restart` kill-all + rilancio staccato, guardia anti-duplicati all'avvio.
- PDF: `filters.Document.PDF` → `scripts/extract_pdf.py` (pypdf 7 installato) → testo al modello, mai il PDF. Foto: `scripts/extract_image_ocr.py` (tesseract **mancante**).
- Quota: soglie in % su `quota_tokens` (150k): 60% avviso, 90% stop lavoro pesante + cambio IP.
- Nomi: `netutil.ps1` (copia di fritz-reconnect) puntato da `config.json`; originale intatto per non rompere altri tool.
- Verificato: compile OK, health 94%/61%, extract_pdf su PDF senza testo → exit 3 con messaggio OCR.

## 3. Tentativi falliti e problemi risolti
- `start.cmd` via `Start-Process cmd /c` non trovato → `/c "C:\hermes-agent\start.cmd"`, poi `restart.ps1`.
- Due istanze → `Conflict: terminated by other getUpdates` → kill-all all'avvio e in `/restart`.
- `Streamer.start` collideva con l'attributo → rinominato `t0`.
- `opencode run --format json` non manda token: typewriter al blocco `text` (limite reale).
- `pypdf` assente → `pip install pypdf` (7.0.0), aggiunto a `requirements.txt`.

## 4. Decisioni prese
- Un solo messaggio editato per risposta, ~1 edit/s (flood control Telegram).
- Nessun compact: `HANDOFF.md` + `/new`.
- `chat_data["model_locked"]`, azzerato da `/new`, `/clear`, cambio IP.
- Contenuti (PDF/immagini) estratti in locale prima del modello; `skip_tools=True` per non far scattare i tool sul testo estratto.
- Riavvio sempre a istanza singola via `restart.ps1`.

## 5. File toccati
`hermes.py`, `streaming.py`, `system-prompt.md`, `config.json`, `requirements.txt`, `restart.ps1`, `scripts/extract_pdf.py`, `scripts/extract_image_ocr.py`, `memory.md`, `HANDOFF.md`, `D:\CentroAI\bin\netutil.ps1`.

## 6. Prossimi passi
1. Installare tesseract OCR (ita+eng) per attivare `on_photo`.
2. Streaming token-per-token vero via `opencode serve` + SSE.
3. Sostituire del tutto `fritz-reconnect.ps1` con `netutil.ps1` quando nessun altro tool lo usa.
4. Audit periodico skill/comandi.
