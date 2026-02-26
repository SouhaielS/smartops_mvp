from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

from pdfminer.high_level import extract_text


# --------------------------------------------------
# Logging setup
# --------------------------------------------------
logging.getLogger().setLevel(logging.INFO)
logging.info("DEBUG_EXTRACTOR_FILE: %s", __file__)


# --------------------------------------------------
# Text cleaning
# --------------------------------------------------
def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n|\r", "\n", text)
    return text.strip()


# --------------------------------------------------
# Generic pattern finder
# --------------------------------------------------
def _find_first(patterns, text: str) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    return None


# --------------------------------------------------
# Normalize invoice ID
# --------------------------------------------------
def _normalize_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None

    s = raw.strip()
    s = s.strip(" .,:;|-")
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^A-Z0-9\-\/_.]+", "", s.upper())

    return s or None


# --------------------------------------------------
# Amount parser
# --------------------------------------------------
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


# --------------------------------------------------
# Invoice extraction via label
# --------------------------------------------------
def _extract_invoice_by_label(text: str) -> Optional[str]:
    label_patterns = [
        r"(?:Invoice\s*(?:No\.?|Number|N°|Nº|Num(?:ber)?)"
        r"|Facture\s*(?:N°|Nº|No\.?|Num(?:éro)?)"
        r"|Bill\s*(?:No\.?|Number|N°|Nº)"
        r"|Document\s*(?:No\.?|Number|N°|Nº)"
        r"|Ref(?:erence)?)\s*[:#]?\s*([A-Z0-9][A-Z0-9 \-\/_.]{2,60})"
    ]

    for pat in label_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            candidate = m.group(1)
            candidate = candidate.split("\n", 1)[0]
            candidate = candidate.strip()

            chunks = re.split(r"\s+", candidate)
            candidate = " ".join(chunks[:3])

            return _normalize_id(candidate)

    return None


# --------------------------------------------------
# Heuristic invoice detection
# --------------------------------------------------
def _extract_invoice_heuristic(text: str) -> Optional[str]:
    token_patterns = [
        r"\bINV(?:OICE)?[ \-\/_.]?[A-Z0-9][A-Z0-9 \-\/_.]{2,30}\b",
        r"\bFCT[ \-\/_.]?[A-Z0-9][A-Z0-9 \-\/_.]{2,30}\b",
        r"\bFA[ \-\/_.]?[A-Z0-9][A-Z0-9 \-\/_.]{2,30}\b",
        r"\bBILL[ \-\/_.]?[A-Z0-9][A-Z0-9 \-\/_.]{2,30}\b",
    ]

    candidates = []

    for pat in token_patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            candidates.append(m.group(0))

    candidates = [_normalize_id(c) for c in candidates]
    candidates = [c for c in candidates if c and len(c) >= 6]

    if not candidates:
        return None

    candidates.sort(key=len, reverse=True)
    return candidates[0]


# --------------------------------------------------
# Field extraction
# --------------------------------------------------
def _extract_fields_from_text(
    text: str
) -> Tuple[Optional[str], Optional[str], Optional[float]]:

    po_patterns = [
        r"\bPO\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bP\.?O\.?\s*Number\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bBon\s+de\s+commande\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
    ]

    inv_patterns = [
        r"\b(INV[\-\/_.]?[0-9A-Z][0-9A-Z\-\/_.]{2,})\b",
        r"\b(INVOICE[\-\/_.]?[0-9A-Z][0-9A-Z\-\/_.]{2,})\b",
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
    inv = _normalize_id(inv)

    if inv is None:
        inv = _extract_invoice_by_label(text)

    if inv is None:
        inv = _extract_invoice_heuristic(text)

    amt_raw = _find_first(amount_patterns, text)
    amt = _parse_amount(amt_raw) if amt_raw else None

    return po, inv, amt


# --------------------------------------------------
# Main public function
# --------------------------------------------------
def extract_invoice_fields(pdf_path: str | Path) -> Dict[str, object]:

    pdf_path = Path(pdf_path)
    logging.info("DEBUG_PDF_PATH: %s", pdf_path)

    try:
        raw_text = extract_text(str(pdf_path)) or ""
    except Exception as e:
        logging.exception("DEBUG_PDFMINER_ERROR: %s", e)
        return {
            "po_number": None,
            "invoice_number": None,
            "invoice_amount": None,
        }

    text = _clean_text(raw_text)

    logging.info("DEBUG_TEXT_LEN: %s", len(text))
    logging.info("DEBUG_PDF_TEXT_PREVIEW: %s", text[:800])

    if not text:
        return {
            "po_number": None,
            "invoice_number": None,
            "invoice_amount": None,
        }

    po, inv, amt = _extract_fields_from_text(text)

    logging.info("DEBUG_EXTRACTED: po=%s inv=%s amt=%s", po, inv, amt)

    return {
        "po_number": po,
        "invoice_number": inv,
        "invoice_amount": amt,
    }