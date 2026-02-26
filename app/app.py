from __future__ import annotations

import inspect
import logging
import tempfile
from pathlib import Path

import streamlit as st

from src.run_batch import run_batch

# Log where run_batch is imported from (must appear in hosted logs)
logging.getLogger("run_batch").info(
    "DEBUG_IMPORT_run_batch_FROM: %s", inspect.getfile(run_batch)
)

st.set_page_config(page_title="SmartOps MVP - Batch", layout="wide")
st.title("SmartOps MVP — Invoice Batch Processor")

st.write(
    "Upload invoices (PDF) + PO Register (XLSX), then run batch to generate Batch_Output.xlsx."
)

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

if st.button("▶ Run Batch", type="primary"):
    if not uploaded_invoices:
        st.error("Please upload at least one invoice PDF.")
        st.stop()
    if not uploaded_po:
        st.error("Please upload the PO Register Excel file.")
        st.stop()

    with st.spinner("Running batch..."):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            invoices_dir = tmp / "invoices"
            data_dir = tmp / "data"
            invoices_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            for f in uploaded_invoices:
                (invoices_dir / f.name).write_bytes(f.getbuffer())

            po_path = data_dir / "PO_Register.xlsx"
            po_path.write_bytes(uploaded_po.getbuffer())

            out_path = data_dir / "Batch_Output.xlsx"

            # Use positional args to avoid any keyword mismatch surprises
            run_batch(invoices_dir, po_path, out_path)

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