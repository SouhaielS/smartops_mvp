from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Optional, Tuple

from pdfminer.high_level import extract_text


# -----------------------------
# Helpers
# -----------------------------
def _clean_text(text: str) -> str:
    # Normalize whitespace + common PDF oddities
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n|\r", "\n", text)
    return text.strip()


def _find_first(patterns, text: str) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            # Return first captured group if exists, else full match
            if m.lastindex:
                return (m.group(1) or "").strip()
            return (m.group(0) or "").strip()
    return None


def _parse_amount(raw: str) -> Optional[float]:
    """
    Accept formats:
    - 1 234,56
    - 1,234.56
    - 1234.56
    - 1234,56
    """
    if not raw:
        return None
    s = raw.strip()
    # Remove currency symbols and letters
    s = re.sub(r"[^\d,.\- ]", "", s)
    s = s.replace(" ", "")

    # If both comma and dot exist, decide decimal by last separator
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            # comma is decimal, remove dots as thousand separators
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            # dot is decimal, remove commas as thousand separators
            s = s.replace(",", "")
    else:
        # If only comma, treat as decimal
        if "," in s and "." not in s:
            s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def _extract_fields_from_text(text: str) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Try to extract:
    - PO number
    - Invoice number
    - Invoice amount
    """

    # PO number patterns (adapt as needed)
    po_patterns = [
        r"\bPO\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bP\.?O\.?\s*Number\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bBon\s+de\s+commande\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
    ]

    # Invoice number patterns
    inv_patterns = [
        r"\bInvoice\s*(?:No\.?|Number)?\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        r"\bFacture\s*(?:N°|No\.?|Num(?:éro)?)\s*[:#]?\s*([A-Z0-9\-\/]+)\b",
        # Sometimes appears as "INV-12345"
        r"\b(INV[\-\/]?[0-9A-Z]+)\b",
    ]

    # Amount patterns (Total / Amount Due / Net to pay)
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


# -----------------------------
# Main function used by run_batch
# -----------------------------
def extract_invoice_fields(pdf_path: str | Path) -> Dict[str, object]:
    """
    Returns a dict with keys expected by your batch:
    - po_number
    - invoice_number
    - invoice_amount

    If something is missing, values will be None (batch can mark NEEDS_REVIEW).
    """
    pdf_path = Path(pdf_path)

    # Extract text using pdfminer
    try:
        text = extract_text(str(pdf_path))
    except Exception as e:
        print(f"DEBUG_PDFMINER_ERROR: {pdf_path.name} -> {e}")
        return {
            "po_number": None,
            "invoice_number": None,
            "invoice_amount": None,
        }

    text = _clean_text(text or "")

    # DEBUG: show first characters to understand what pdfminer reads
    import logging
logging.warning("DEBUG_PDF_TEXT_PREVIEW: %s", (text or "")[:300])

    # If pdf has no text, it's likely scanned -> needs OCR
    if not text:
        return {
            "po_number": None,
            "invoice_number": None,
            "invoice_amount": None,
        }

    po, inv, amt = _extract_fields_from_text(text)

    return {
        "po_number": po,
        "invoice_number": inv,
        "invoice_amount": amt,
    }