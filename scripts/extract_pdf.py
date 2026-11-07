#!/usr/bin/env python3
"""Estrae il testo di un PDF in un file di testo, senza usare il modello.

Uso: python extract_pdf.py <file.pdf> [<uscita.txt>]   (senza uscita: <file>.txt accanto al PDF)
Richiede: pip install pypdf
Codici di uscita: 0 fatto, 1 argomenti o file sbagliati, 2 PDF illeggibile, 3 nessun testo (PDF fatto di immagini).
"""
import sys
from pathlib import Path


def errore(messaggio, codice):
    print(f"extract_pdf: {messaggio}", file=sys.stderr)
    return codice


def main(argv):
    if len(argv) not in (2, 3):
        return errore("uso: extract_pdf.py <file.pdf> [<uscita.txt>]", 1)
    pdf = Path(argv[1])
    if not pdf.is_file():
        return errore(f"file non trovato: {pdf}", 1)
    uscita = Path(argv[2]) if len(argv) == 3 else pdf.with_suffix(".txt")
    try:
        from pypdf import PdfReader
    except ImportError:
        return errore("manca pypdf: pip install pypdf", 1)
    try:
        lettore = PdfReader(pdf)
        if lettore.is_encrypted and not lettore.decrypt(""):
            return errore(f"PDF protetto da password: {pdf}", 2)
        pagine = [p.extract_text() or "" for p in lettore.pages]
    except Exception as e:
        return errore(f"PDF illeggibile: {pdf}: {e}", 2)
    uscita.parent.mkdir(parents=True, exist_ok=True)
    uscita.write_text("\n\n".join(f"--- Pagina {i} ---\n{t}" for i, t in enumerate(pagine, 1)), encoding="utf-8")
    vuote = sum(1 for t in pagine if not t.strip())
    print(f"{uscita}: {len(pagine)} pagine, {vuote} senza testo")
    if vuote == len(pagine):
        return errore("nessun testo estratto: il PDF e' fatto di immagini, serve l'OCR", 3)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
