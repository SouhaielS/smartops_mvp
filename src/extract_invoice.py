from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

from pdfminer.high_level import extract_text

logging.warning("DEBUG_EXTRACTOR_FILE: %s", __file__)


def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n|\r", "\n", text)
    return text.strip()


def _find_first(patterns, text: str) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    return None


def _parse_amount(raw: str) -> Optional[float]:
    if not raw:
        return None
    s = raw.strip()
    s = re.sub(r"[^\d,.\- ]", "", s).replace(" ", "")

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def _extract_fields_from_text(text: str) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    po_patterns = [
        r"\bPO\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bP\.?O\.?\s*Number\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bBon\s+de\s+commande\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
    ]

    inv_patterns = [
        r"\bInvoice\s*(?:No\.?|Number)?\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bFacture\s*(?:N°|No\.?|Num(?:éro)?)\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\b(INV[\-\/]?[0-9A-Z]+)\b",
    ]

    amount_patterns = [
        r"\bTotal\s*(?:Amount)?\s*[:#]?\s*([0-9][0-9\s.,]+)\b",
        r"\bAmount\s*Due\s*[:#]?\s*([0-9][0-9\s.,]+)\b",
        r"\bNet\s*(?:to\s*pay|à\s*payer)\s*[:#]?\s*([0-9][0-9\s.,]+)\b",
        r"\bTotal\s*TTC\s*[:#]?\s*([0-9][0-9\s.,]+)\b",
        r"\bMontant\s*(?:TTC|Total)\s*[:#]?\s*([0-9][0-9\s.,]+)\b",
    ]

    po = _find_first(po_patterns, text)
    inv = _find_first(inv_patterns, text)
    amt_raw = _find_first(amount_patterns, text)
    amt = _parse_amount(amt_raw) if amt_raw else None
    return po, inv, amt


def extract_invoice_fields(pdf_path: str | Path) -> Dict[str, object]:
    pdf_path = Path(pdf_path)

    logging.warning("DEBUG_PDF_PATH: %s", pdf_path)

    try:
        raw_text = extract_text(str(pdf_path)) or ""
    except Exception as e:
        logging.exception("DEBUG_PDFMINER_ERROR: %s", e)
        return {"po_number": None, "invoice_number": None, "invoice_amount": None}

    text = _clean_text(raw_text)

    logging.warning("DEBUG_TEXT_LEN: %s", len(text))
    logging.warning("DEBUG_PDF_TEXT_PREVIEW: %s", text[:300])

    if not text:
        return {"po_number": None, "invoice_number": None, "invoice_amount": None}

    po, inv, amt = _extract_fields_from_text(text)

    logging.warning("DEBUG_EXTRACTED: po=%s inv=%s amt=%s", po, inv, amt)

    return {"po_number": po, "invoice_number": inv, "invoice_amount": amt}