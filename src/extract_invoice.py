from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

from pdfminer.high_level import extract_text


logging.getLogger().setLevel(logging.INFO)
logging.info("DEBUG_EXTRACTOR_FILE: %s", __file__)


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n|\r", "\n", text)
    return text.strip()


def _normalize_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^A-Z0-9\-\/_.]+", "", s.upper())
    return s or None


def _parse_amount(raw: str) -> Optional[float]:
    if not raw:
        return None

    s = re.sub(r"[^\d,.\-]", "", raw)

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
# Field extraction
# --------------------------------------------------

def _extract_fields_from_text(
    text: str,
) -> Tuple[Optional[str], Optional[str], Optional[float]]:

    # -------- PO extraction --------
    po_match = re.search(
        r"\bPO[_\s-]*(?:Number|No\.?|N°|#)?\s*[:#]?\s*([0-9]{3,20})\b",
        text,
        flags=re.IGNORECASE,
    )
    po = po_match.group(1) if po_match else None

    # -------- Invoice extraction --------
    inv_match = re.search(
        r"\bFacture\s*(?:N°|Nº|No\.?|Num(?:éro)?)\s*[:#]?\s*([A-Z0-9\-\/_.]{3,60})",
        text,
        flags=re.IGNORECASE,
    )

    if not inv_match:
        inv_match = re.search(
            r"\b(INV[\-\/_.]?[0-9A-Z][0-9A-Z\-\/_.]{2,})\b",
            text,
            flags=re.IGNORECASE,
        )

    inv = _normalize_id(inv_match.group(1)) if inv_match else None

    # -------- Amount extraction --------
    amt = None

    # Find Total TTC block
    m_ttc = re.search(r"\bTotal\s*TTC\b", text, flags=re.IGNORECASE)
    if m_ttc:
        window = text[m_ttc.end(): m_ttc.end() + 300]

        # Take LAST amount in window (TTC usually last)
        amounts = re.findall(
            r"([0-9][0-9\s.,]+)\s*(?:DT|TND|Dinars?)\b",
            window,
            flags=re.IGNORECASE,
        )
        if amounts:
            amt = _parse_amount(amounts[-1])

    # Fallback
    if amt is None:
        m_amt = re.search(
            r"([0-9][0-9\s.,]+)\s*(?:DT|TND|Dinars?)\b",
            text,
            flags=re.IGNORECASE,
        )
        if m_amt:
            amt = _parse_amount(m_amt.group(1))

    return po, inv, amt


# --------------------------------------------------
# Public function
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
            "_debug_text_preview": "",
        }

    text = _clean_text(raw_text)
    preview = text[:800] if text else ""

    logging.info("DEBUG_TEXT_LEN: %s", len(text))
    logging.info("DEBUG_PDF_TEXT_PREVIEW: %s", preview)

    if not text:
        return {
            "po_number": None,
            "invoice_number": None,
            "invoice_amount": None,
            "_debug_text_preview": "",
        }

    po, inv, amt = _extract_fields_from_text(text)

    logging.info("DEBUG_EXTRACTED: po=%s inv=%s amt=%s", po, inv, amt)

    return {
        "po_number": po,
        "invoice_number": inv,
        "invoice_amount": amt,
        "_debug_text_preview": preview,
    }