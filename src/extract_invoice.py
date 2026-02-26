# src/extract_invoice.py

from pathlib import Path
import json
import re
from pdfminer.high_level import extract_text


MODEL_NAME = "llama3.2:3b-instruct-q4_0"


# =========================
# PDF TEXT EXTRACTION
# =========================
def extract_text_from_pdf(pdf_path: Path) -> str:
    try:
        return extract_text(str(pdf_path))
    except Exception as e:
        print(f"❌ PDF extraction error: {e}")
        return ""


# =========================
# BACKUP REGEX AMOUNT DETECTOR
# =========================
def detect_amount_with_regex(text: str):
    """
    Backup detection if LLM fails.
    Looks for final payable keywords.
    """
    patterns = [
        r"(TOTAL\s*TTC.*?([\d\s,\.]+))",
        r"(Net\s*à\s*payer.*?([\d\s,\.]+))",
        r"(Total\s*à\s*payer.*?([\d\s,\.]+))",
        r"(Grand\s*Total.*?([\d\s,\.]+))",
        r"(Amount\s*Due.*?([\d\s,\.]+))",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            amount = match.group(2)
            return amount.replace(" ", "").replace(",", ".")

    return None


def _safe_result(error_msg: str = "") -> dict:
    """Return a safe structured dict so the pipeline never crashes."""
    return {
        "Invoice_Number": None,
        "Invoice_Date": None,
        "PO_Number": None,
        "Milestone_Reference": None,
        "Invoice_Amount": None,
        "error": error_msg or None,
    }


# =========================
# LLM EXTRACTION (SAFE)
# =========================
def extract_invoice_fields(pdf_path: Path) -> dict:
    text = extract_text_from_pdf(pdf_path)

    if not text.strip():
        print("⚠️ Empty PDF text")
        return _safe_result("Empty PDF text")

    # Import Ollama lazily (so Streamlit doesn't crash on import)
    try:
        from ollama import chat  # type: ignore
    except Exception as e:
        print(f"⚠️ Ollama python package not available: {e}")
        # Still try regex backup amount
        backup_amount = detect_amount_with_regex(text)
        res = _safe_result("Ollama python package not installed or not importable")
        if backup_amount:
            res["Invoice_Amount"] = backup_amount
        return res

    prompt = f"""
You are a telecom invoice extraction assistant.

Return ONLY valid JSON with EXACT keys:

Invoice_Number
Invoice_Date
PO_Number
Milestone_Reference
Invoice_Amount

Rules:
- PO_Number: extract if you see PO, P.O., Purchase Order, Bon de commande, BC
- Invoice_Amount MUST be FINAL TOTAL TO PAY
- Look for:
    TOTAL TTC
    Net à payer
    Total à payer
    Amount Due
    Grand Total
- If multiple totals exist, choose the largest final payable amount
- Return numeric value only
- If missing, use null
- JSON only

Invoice text:
{text}
"""

    # Call Ollama safely
    try:
        response = chat(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        print(f"⚠️ Ollama call failed: {type(e).__name__}: {e}")
        backup_amount = detect_amount_with_regex(text)
        res = _safe_result(f"Ollama call failed: {type(e).__name__}")
        if backup_amount:
            res["Invoice_Amount"] = backup_amount
        return res

    # Parse model output safely
    try:
        content = response["message"]["content"]
    except Exception as e:
        print(f"⚠️ Unexpected Ollama response format: {e}")
        backup_amount = detect_amount_with_regex(text)
        res = _safe_result("Unexpected Ollama response format")
        if backup_amount:
            res["Invoice_Amount"] = backup_amount
        return res

    print(f"\n🔍 RAW MODEL OUTPUT for {pdf_path.name}:\n{content}\n")

    content = str(content).replace("```json", "").replace("```", "").strip()
    start = content.find("{")
    end = content.rfind("}")

    if start == -1 or end == -1:
        print("⚠️ JSON not found in model output")
        backup_amount = detect_amount_with_regex(text)
        res = _safe_result("JSON not found in model output")
        if backup_amount:
            res["Invoice_Amount"] = backup_amount
        return res

    json_text = content[start : end + 1]

    try:
        result = json.loads(json_text)

        # Ensure required keys exist
        base = _safe_result(None)
        for k in ["Invoice_Number", "Invoice_Date", "PO_Number", "Milestone_Reference", "Invoice_Amount"]:
            if k in result:
                base[k] = result.get(k)

        # Normalize Invoice_Amount
        if base["Invoice_Amount"]:
            val = str(base["Invoice_Amount"])
            base["Invoice_Amount"] = val.replace(" ", "").replace(",", ".")
        else:
            # Backup regex if LLM missed amount
            backup_amount = detect_amount_with_regex(text)
            if backup_amount:
                base["Invoice_Amount"] = backup_amount

        return base

    except Exception as e:
        print("⚠️ JSON parsing error:", e)
        print("JSON candidate:", json_text)
        backup_amount = detect_amount_with_regex(text)
        res = _safe_result("JSON parsing error")
        if backup_amount:
            res["Invoice_Amount"] = backup_amount
        return res