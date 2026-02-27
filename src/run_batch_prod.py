from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.extract_invoice import extract_invoice_fields


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
    force=True,
)

logger = logging.getLogger("run_batch")


def run_batch_prod(
    invoice_dir: str | Path,
    po_register_path: str | Path,
    output_workbook_path: str | Path,
) -> None:
    batch_id = uuid.uuid4().hex[:10]
    processed_at = datetime.utcnow().isoformat(timespec="seconds")

    invoice_dir = Path(invoice_dir)
    po_register_path = Path(po_register_path)
    output_workbook_path = Path(output_workbook_path)

    logger.info("RUN_BATCH_PROD_FILE: %s", __file__)
    logger.info("Batch ID: %s | Processed at: %s", batch_id, processed_at)
    logger.info("Invoice directory: %s", invoice_dir)
    logger.info("PO register: %s", po_register_path)
    logger.info("Output workbook: %s", output_workbook_path)

    # Load PO register (for future checks)
    try:
        _po_df = pd.read_excel(po_register_path)
    except Exception as e:
        logger.exception("Failed to load PO register: %s", e)
        raise

    results: List[Dict] = []

    for pdf_path in invoice_dir.glob("*.pdf"):
        logger.info("Processing invoice: %s", pdf_path.name)

        try:
            fields = extract_invoice_fields(pdf_path)
        except Exception as e:
            logger.exception("Extraction failed for %s: %s", pdf_path.name, e)
            results.append(
                {
                    "file_name": pdf_path.name,
                    "po_number": None,
                    "invoice_number": None,
                    "invoice_amount": None,
                    "status": "ERROR",
                    "reason": "Extraction error",
                    "batch_id": batch_id,
                    "processed_at": processed_at,
                }
            )
            continue

        po_number = fields.get("po_number")
        invoice_number = fields.get("invoice_number")
        invoice_amount = fields.get("invoice_amount")

        status = "VALID"
        reason = ""

        if not invoice_number:
            status = "NEEDS_REVIEW"
            reason = "Invoice number missing"
        elif not po_number:
            status = "NEEDS_REVIEW"
            reason = "PO number missing"
        elif invoice_amount is None:
            status = "NEEDS_REVIEW"
            reason = "Invoice amount missing"

        results.append(
            {
                "file_name": pdf_path.name,
                "po_number": po_number,
                "invoice_number": invoice_number,
                "invoice_amount": invoice_amount,
                "status": status,
                "reason": reason,
                "batch_id": batch_id,
                "processed_at": processed_at,
            }
        )

        logger.info("Result: %s | %s", status, reason or "OK")

    # ---- Duplicate detection in the SAME batch ----
    result_df = pd.DataFrame(results)

    if "invoice_number" in result_df.columns:
        inv_series = result_df["invoice_number"].fillna("").astype(str).str.strip()
        dup_mask = inv_series.ne("") & inv_series.duplicated(keep=False)
        result_df.loc[dup_mask, "status"] = "DUPLICATE"
        result_df.loc[dup_mask, "reason"] = "Duplicate invoice_number in this batch"

        if dup_mask.any():
            logger.info("Duplicates detected: %s", int(dup_mask.sum()))

    output_workbook_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_excel(output_workbook_path, index=False)

    logger.info("Batch completed successfully.")