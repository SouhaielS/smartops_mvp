from __future__ import annotations

import inspect
import logging
import shutil
import tempfile
from pathlib import Path

import streamlit as st

# ------------------------------------------------------------
# IMPORTANT: import run_batch from src
# ------------------------------------------------------------
from src.run_batch import run_batch  # <-- keep this import

# ------------------------------------------------------------
# LOG: show which file run_batch is coming from (diagnosis)
# This should appear in hosted logs as INFO:run_batch:...
# ------------------------------------------------------------
logging.getLogger("run_batch").info(
    "DEBUG_IMPORT_run_batch_FROM: %s", inspect.getfile(run_batch)
)

st.set_page_config(page_title="SmartOps MVP - Batch", layout="wide")
st.title("SmartOps MVP — Invoice Batch Processor")

st.markdown(
    """
Upload:
- **Invoices**: multiple PDF files  
- **PO Register**: Excel file (`PO_Register.xlsx`)  

Then click **Run Batch** to generate `Batch_Output.xlsx`.
"""
)

# -----------------------------
# Upload inputs
# -----------------------------
uploaded_invoices = st.file_uploader(
    "Upload invoice PDFs",
    type=["pdf"],
    accept_multiple_files=True,
)

uploaded_po = st.file_uploader(
    "Upload PO Register (Excel)",
    type=["xlsx"],
    accept_multiple_files=False,
)

run_btn = st.button("▶ Run Batch", type="primary")

# -----------------------------
# Run
# -----------------------------
if run_btn:
    if not uploaded_invoices:
        st.error("Please upload at least one invoice PDF.")
        st.stop()

    if not uploaded_po:
        st.error("Please upload the PO Register Excel file.")
        st.stop()

    with st.spinner("Running batch..."):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            invoices_dir = tmpdir / "invoices"
            data_dir = tmpdir / "data"
            invoices_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            # Save invoices
            for f in uploaded_invoices:
                (invoices_dir / f.name).write_bytes(f.getbuffer())

            # Save PO register
            po_path = data_dir / "PO_Register.xlsx"
            po_path.write_bytes(uploaded_po.getbuffer())

            # Output path
            out_path = data_dir / "Batch_Output.xlsx"

            # Run batch
            run_batch(
                invoice_dir=invoices_dir,
                po_register_path=po_path,
                output_workbook_path=out_path,
            )

            if out_path.exists():
                st.success("Batch completed ✅")
                st.download_button(
                    "⬇ Download Batch_Output.xlsx",
                    data=out_path.read_bytes(),
                    file_name="Batch_Output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            else:
                st.error("Batch finished but output file was not created. Check logs.")