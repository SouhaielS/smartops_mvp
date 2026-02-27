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


def _normalize_str_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _load_history(history_path: Path) -> pd.DataFrame:
    if history_path.exists():
        try:
            df = pd.read_excel(history_path)
            if "invoice_number" in df.columns:
                df["invoice_number"] = _normalize_str_series(df["invoice_number"])
            if "po_number" in df.columns:
                df["po_number"] = _normalize_str_series(df["po_number"])
            return df
        except Exception as e:
            logger.exception("Failed to read history file %s: %s", history_path, e)

    return pd.DataFrame(
        columns=[
            "invoice_number",
            "po_number",
            "invoice_amount",
            "file_name",
            "batch_id",
            "processed_at",
        ]
    )


def _append_to_history(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    # Avoid pandas FutureWarning when existing is empty
    if existing.empty:
        combined = new_rows.copy()
    else:
        combined = pd.concat([existing, new_rows], ignore_index=True)

    if "invoice_number" in combined.columns:
        inv = _normalize_str_series(combined["invoice_number"])
        combined = combined.loc[inv.ne("")].copy()
        combined["invoice_number"] = inv.loc[inv.ne("")]
        combined = combined.drop_duplicates(subset=["invoice_number"], keep="first")

    return combined


def _ensure_po_columns(po_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure minimum columns exist and are correctly typed.
    We only need these for real budget reduction.
    """
    # normalize column names (keep original but trim)
    po_df.columns = [str(c).strip() for c in po_df.columns]

    if "PO_Number" not in po_df.columns:
        raise ValueError("PO register is missing required column: PO_Number")

    po_df["PO_Number"] = po_df["PO_Number"].astype(str).str.strip()

    # Create columns if they don't exist
    for col in ["Total_PO_Value", "Amount_Already_Invoiced", "Remaining_Budget"]:
        if col not in po_df.columns:
            po_df[col] = 0.0

    # Numeric safety
    for col in ["Total_PO_Value", "Amount_Already_Invoiced", "Remaining_Budget"]:
        po_df[col] = pd.to_numeric(po_df[col], errors="coerce").fillna(0.0).astype(float)

    # If Remaining_Budget is zero but Total_PO_Value exists, derive it
    # (does not overwrite non-zero Remaining_Budget)
    mask_zero_remaining = (po_df["Remaining_Budget"] == 0) & (po_df["Total_PO_Value"] > 0)
    if mask_zero_remaining.any():
        po_df.loc[mask_zero_remaining, "Remaining_Budget"] = (
            po_df.loc[mask_zero_remaining, "Total_PO_Value"]
            - po_df.loc[mask_zero_remaining, "Amount_Already_Invoiced"]
        )

    return po_df


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

    output_workbook_path.parent.mkdir(parents=True, exist_ok=True)

    # Persistent history file inside repo folder (server-side)
    history_path = Path("data") / "invoice_history.xlsx"

    logger.info("RUN_BATCH_PROD_FILE: %s", __file__)
    logger.info("Batch ID: %s | Processed at: %s", batch_id, processed_at)
    logger.info("Invoice directory: %s", invoice_dir)
    logger.info("PO register: %s", po_register_path)
    logger.info("Output workbook: %s", output_workbook_path)
    logger.info("History file: %s", history_path)

    # -------------------------------
    # Load PO register (safe typing)
    # -------------------------------
    po_df = pd.read_excel(po_register_path)
    po_df = _ensure_po_columns(po_df)

    # -------------------------------
    # Load history
    # -------------------------------
    history_df = _load_history(history_path)
    history_inv_set = set(
        _normalize_str_series(history_df.get("invoice_number", pd.Series(dtype=str))).tolist()
    )

    results: List[Dict] = []

    # -------------------------------
    # Extract invoices
    # -------------------------------
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

    # -------------------------------
    # Duplicate detection (batch + history)
    # -------------------------------
    batch_df = pd.DataFrame(results)

    if "invoice_number" in batch_df.columns:
        inv_series = _normalize_str_series(batch_df["invoice_number"])

        dup_batch_mask = inv_series.ne("") & inv_series.duplicated(keep=False)
        batch_df.loc[dup_batch_mask, "status"] = "DUPLICATE"
        batch_df.loc[dup_batch_mask, "reason"] = "Duplicate invoice_number in this batch"

        dup_hist_mask = inv_series.ne("") & inv_series.isin(history_inv_set) & (~dup_batch_mask)
        batch_df.loc[dup_hist_mask, "status"] = "DUPLICATE_HISTORY"
        batch_df.loc[dup_hist_mask, "reason"] = "Invoice already processed in previous batch"

        if dup_batch_mask.any():
            logger.info("Duplicates in batch: %s", int(dup_batch_mask.sum()))
        if dup_hist_mask.any():
            logger.info("Duplicates in history: %s", int(dup_hist_mask.sum()))

    # -------------------------------
    # REAL PO Budget Update (VALID only)
    # - Decrease Remaining_Budget
    # - Increase Amount_Already_Invoiced
    # -------------------------------
    # add tracking columns (nice for audit)
    for col in ["remaining_before", "remaining_after", "po_row_index"]:
        if col not in batch_df.columns:
            batch_df[col] = None

    # normalize po_number column for matching
    if "po_number" in batch_df.columns:
        batch_df["po_number"] = _normalize_str_series(batch_df["po_number"])

    for idx, row in batch_df.iterrows():
        if row.get("status") != "VALID":
            continue

        po_number = str(row.get("po_number") or "").strip()
        invoice_amount = row.get("invoice_amount")

        try:
            inv_amt = float(invoice_amount) if invoice_amount is not None else 0.0
        except Exception:
            inv_amt = 0.0

        if not po_number or inv_amt <= 0:
            batch_df.at[idx, "status"] = "NEEDS_REVIEW"
            batch_df.at[idx, "reason"] = "Invalid PO number or invoice amount"
            continue

        matches = po_df[po_df["PO_Number"] == po_number]

        if matches.empty:
            batch_df.at[idx, "status"] = "PO_NOT_FOUND"
            batch_df.at[idx, "reason"] = f"PO {po_number} not found in register"
            continue

        po_index = int(matches.index[0])

        remaining_before = float(po_df.at[po_index, "Remaining_Budget"])
        already_before = float(po_df.at[po_index, "Amount_Already_Invoiced"])

        # Overbudget protection
        if inv_amt > remaining_before:
            batch_df.at[idx, "status"] = "OVERBUDGET"
            batch_df.at[idx, "reason"] = (
                f"Invoice {inv_amt} exceeds Remaining_Budget {remaining_before}"
            )
            batch_df.at[idx, "remaining_before"] = remaining_before
            batch_df.at[idx, "remaining_after"] = remaining_before
            batch_df.at[idx, "po_row_index"] = po_index
            continue

        # Apply real update
        po_df.at[po_index, "Amount_Already_Invoiced"] = already_before + inv_amt
        po_df.at[po_index, "Remaining_Budget"] = remaining_before - inv_amt

        batch_df.at[idx, "remaining_before"] = remaining_before
        batch_df.at[idx, "remaining_after"] = remaining_before - inv_amt
        batch_df.at[idx, "po_row_index"] = po_index

    # -------------------------------
    # Update persistent history (VALID only)
    # -------------------------------
    valid_df = batch_df[batch_df["status"] == "VALID"].copy()
    if not valid_df.empty:
        hist_rows = valid_df[
            ["invoice_number", "po_number", "invoice_amount", "file_name", "batch_id", "processed_at"]
        ].copy()
        hist_rows["invoice_number"] = _normalize_str_series(hist_rows["invoice_number"])
        history_updated_df = _append_to_history(history_df, hist_rows)
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_updated_df.to_excel(history_path, index=False)
        logger.info("History updated with %s VALID invoices.", len(hist_rows))
    else:
        history_updated_df = history_df
        logger.info("No VALID invoices to append to history.")

    # -------------------------------
    # Write ONE workbook with 3 sheets
    # -------------------------------
    with pd.ExcelWriter(output_workbook_path, engine="openpyxl") as writer:
        batch_df.to_excel(writer, sheet_name="Batch_Output", index=False)
        history_updated_df.to_excel(writer, sheet_name="Invoice_History", index=False)
        po_df.to_excel(writer, sheet_name="PO_Register_Updated", index=False)

    logger.info("Batch workbook created successfully.")