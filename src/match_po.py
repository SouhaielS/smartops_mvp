import pandas as pd
from pathlib import Path

PO_PATH = Path("data/PO_Register.xlsx")
OUT_PATH = Path("data/Invoice_Control_Output.xlsx")

REQUIRED_PO_COLS = [
    "PO_Number",
    "Client_Name",
    "Project_Name",
    "Project_Type",
    "Milestone_Name",
    "Milestone_Value",
    "Total_PO_Value",
    "Amount_Already_Invoiced",
    "Remaining_Budget",
]


def load_po_register(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"PO file not found: {path.resolve()}")

    df = pd.read_excel(path, sheet_name="POs")
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in REQUIRED_PO_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in POs sheet: {missing}")

    df["PO_Number"] = df["PO_Number"].astype(str).str.strip()

    float_cols = ["Milestone_Value", "Total_PO_Value", "Amount_Already_Invoiced", "Remaining_Budget"]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    return df


def normalize_amount(raw_amount):
    if isinstance(raw_amount, dict):
        for k in ["Total_Payer", "TTC", "Total", "Net", "value", "amount"]:
            if k in raw_amount and raw_amount[k] is not None:
                raw_amount = raw_amount[k]
                break
        else:
            for v in raw_amount.values():
                try:
                    raw_amount = float(v)
                    break
                except:
                    continue
            else:
                raw_amount = 0

    if isinstance(raw_amount, str):
        raw_amount = raw_amount.replace(" ", "").replace(",", ".")

    try:
        return float(raw_amount or 0)
    except Exception:
        return 0.0


def match_invoice_to_po(po_df: pd.DataFrame, invoice: dict) -> dict:
    po_number = str(invoice.get("PO_Number") or "").strip()
    inv_amount = normalize_amount(invoice.get("Invoice_Amount"))

    if not po_number:
        return {**invoice, "Match_Status": "PO_MISSING", "Reason": "No PO_Number on invoice"}

    matches = po_df[po_df["PO_Number"] == po_number]
    if matches.empty:
        return {**invoice, "Match_Status": "PO_MISSING", "Reason": f"PO {po_number} not found"}

    # ✅ If caller forced a milestone row, use it
    forced_idx = invoice.get("__forced_row_index__")
    if forced_idx is not None:
        forced_idx = int(forced_idx)
        if forced_idx not in po_df.index:
            return {**invoice, "Match_Status": "INVALID", "Reason": "Forced milestone row not found"}
        chosen = po_df.loc[forced_idx]
        row_idx = forced_idx
    else:
        # fallback: first row for the PO (stable)
        chosen = matches.iloc[0]
        row_idx = int(chosen.name)

    remaining_before = float(chosen["Remaining_Budget"])
    already = float(chosen["Amount_Already_Invoiced"])
    milestone_value = float(chosen["Milestone_Value"])

    if inv_amount <= 0:
        return {**invoice, "Match_Status": "INVALID", "Reason": "Invoice_Amount missing/zero"}

    if inv_amount > remaining_before:
        return {
            **invoice,
            "Match_Status": "OVERBUDGET",
            "Reason": f"Invoice {inv_amount} exceeds Remaining_Budget {remaining_before}",
            "Matched_Row_Index": row_idx,
            "Matched_Client": chosen["Client_Name"],
            "Matched_Project": chosen["Project_Name"],
            "Matched_Milestone": chosen["Milestone_Name"],
        }

    if milestone_value > 0 and (already + inv_amount) > milestone_value:
        return {
            **invoice,
            "Match_Status": "MILESTONE_EXCEEDED",
            "Reason": f"Already {already} + invoice {inv_amount} exceeds Milestone_Value {milestone_value}",
            "Matched_Row_Index": row_idx,
            "Matched_Client": chosen["Client_Name"],
            "Matched_Project": chosen["Project_Name"],
            "Matched_Milestone": chosen["Milestone_Name"],
        }

    return {
        **invoice,
        "Invoice_Amount_Numeric": inv_amount,
        "Match_Status": "VALID",
        "Reason": "OK",
        "Matched_Row_Index": row_idx,
        "Matched_Client": chosen["Client_Name"],
        "Matched_Project": chosen["Project_Name"],
        "Matched_Milestone": chosen["Milestone_Name"],
    }


def write_output(invoices_df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        invoices_df.to_excel(writer, sheet_name="Invoice_Control", index=False)