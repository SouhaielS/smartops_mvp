from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd

from src.extract_invoice import extract_invoice_fields


# --------------------------------------------------
# FORCE LOGGING CONFIG (important for hosted env)
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
    force=True
)

logger = logging.getLogger("run_batch")


# --------------------------------------------------
# MAIN BATCH FUNCTION
# --------------------------------------------------
def run_batch(
    invoice_dir: str | Path,
    po_register_path: str | Path,
    output_workbook_path: str | Path,
) -> None:

    batch_id = uuid.uuid4().hex[:10]
    processed_at = datetime.utcnow().isoformat()

    invoice_dir = Path(invoice_dir)
    po_register_path = Path(po_register_path)
    output_workbook_path = Path(output_workbook_path)

    logger.info("Batch ID: %s | Processed at: %s", batch_id, processed_at)
    logger.info("Invoice dir: %s", invoice_dir)
    logger.info("PO register: %s", po_register_path)
    logger.info("Output workbook: %s", output_workbook_path)

    # --------------------------------------------------
    # Load PO register
    # --------------------------------------------------
    po_df = pd.read_excel(po_register_path)

    results: List[Dict] = []

    # --------------------------------------------------
    # Process each invoice
    # --------------------------------------------------
    for pdf_path in invoice_dir.glob("*.pdf"):

        logger.info("Processing: %s", pdf_path.name)

        # 🔍 DEBUG CALL
        logger.info("DEBUG_CALL_EXTRACTOR: %s", pdf_path)

        fields = extract_invoice_fields(pdf_path)

        # 🔍 DEBUG RESULT
        logger.info("DEBUG_EXTRACT_RESULT: %s", fields)

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

        logger.info("Status: %s | Reason: %s", status, reason)

    # --------------------------------------------------
    # Save batch output
    # --------------------------------------------------
    result_df = pd.DataFrame(results)
    result_df.to_excel(output_workbook_path, index=False)

    logger.info("Batch completed successfully.")