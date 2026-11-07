#!/usr/bin/env python3
"""OCR di uno screenshot di testo in un file di testo, senza usare il modello.

Uso: python extract_image_ocr.py <immagine> [<uscita.txt>]   (senza uscita: <immagine>.txt accanto all'immagine)
Richiede il programma tesseract (non serve pytesseract). Usa ita+eng, oppure le lingue installate tra queste due.
Codici di uscita: 0 fatto, 1 argomenti, file o tesseract mancanti, 2 errore di tesseract, 3 nessun testo trovato.
"""
import shutil
import subprocess
import sys
from pathlib import Path

PERCORSI_WINDOWS = [r"C:\Program Files\Tesseract-OCR\tesseract.exe", r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"]
LINGUE = ["ita", "eng"]


def errore(messaggio, codice):
    print(f"extract_image_ocr: {messaggio}", file=sys.stderr)
    return codice


def trova_tesseract():
    trovato = shutil.which("tesseract")
    if trovato:
        return trovato
    return next((p for p in PERCORSI_WINDOWS if Path(p).is_file()), None)


def esegui(comando):
    return subprocess.run(comando, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)


def main(argv):
    if len(argv) not in (2, 3):
        return errore("uso: extract_image_ocr.py <immagine> [<uscita.txt>]", 1)
    immagine = Path(argv[1])
    if not immagine.is_file():
        return errore(f"file non trovato: {immagine}", 1)
    uscita = Path(argv[2]) if len(argv) == 3 else immagine.with_suffix(".txt")
    tesseract = trova_tesseract()
    if not tesseract:
        return errore("tesseract non trovato: installarlo o metterlo nel PATH", 1)
    # tesseract --list-langs scrive l'elenco su stdout o su stderr a seconda della versione
    elenco = esegui([tesseract, "--list-langs"])
    installate = set((elenco.stdout + "\n" + elenco.stderr).split())
    lingue = [l for l in LINGUE if l in installate]
    if not lingue:
        return errore(f"nessuna lingua tra {LINGUE} installata in tesseract", 1)
    try:
        r = esegui([tesseract, str(immagine), "stdout", "-l", "+".join(lingue)])
    except subprocess.TimeoutExpired:
        return errore(f"tesseract interrotto dopo 120 secondi: {immagine}", 2)
    if r.returncode != 0:
        return errore(f"tesseract non riuscito: {r.stderr.strip()[-300:]}", 2)
    testo = r.stdout.strip()
    uscita.parent.mkdir(parents=True, exist_ok=True)
    uscita.write_text(testo, encoding="utf-8")
    print(f"{uscita}: {len(testo)} caratteri, lingue {'+'.join(lingue)}")
    if not testo:
        return errore("nessun testo trovato nell'immagine", 3)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
