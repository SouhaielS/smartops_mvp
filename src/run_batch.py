from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.extract_invoice import extract_invoice_fields

# Force logging in hosted env (Streamlit can override logging)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
    force=True,
)

logger = logging.getLogger("run_batch")


def run_batch(
    invoice_dir: str | Path,
    po_register_path: str | Path,
    output_workbook_path: str | Path,
) -> None:
    batch_id = uuid.uuid4().hex[:10]
    processed_at = datetime.utcnow().isoformat(timespec="seconds")

    invoice_dir = Path(invoice_dir)
    po_register_path = Path(po_register_path)
    output_workbook_path = Path(output_workbook_path)

    # Put file path info inside always-visible logs
    logger.info("RUN_BATCH_FILE: %s", __file__)

    logger.info("Batch ID: %s | Processed at: %s", batch_id, processed_at)
    logger.info("Invoice dir: %s", invoice_dir)
    logger.info("PO register: %s", po_register_path)
    logger.info("Output workbook: %s", output_workbook_path)

    # Load PO register (kept for later controls)
    _po_df = pd.read_excel(po_register_path)

    results: List[Dict] = []

    for pdf_path in invoice_dir.glob("*.pdf"):
        logger.info("Processing: %s", pdf_path.name)

        fields = extract_invoice_fields(pdf_path)

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

        # ✅ GUARANTEED diagnosis: fields printed inside Status line
        logger.info("Status: %s | Reason: %s | Fields: %s", status, reason, fields)

    output_workbook_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).to_excel(output_workbook_path, index=False)
    logger.info("Batch completed successfully.")