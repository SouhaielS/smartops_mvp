import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]   # project root
sys.path.append(str(ROOT))
import streamlit as st
from pathlib import Path
import shutil
import tempfile

# Import your batch runner
from src.run_batch import run_batch


st.set_page_config(page_title="Invoice Control Batch", layout="wide")

st.title("📄 Invoice Control + PO Budget Batch")
st.write("Upload invoices + PO register, run the batch, and download the formatted Excel workbook.")

# --- Upload area
col1, col2 = st.columns(2)

with col1:
    po_file = st.file_uploader("Upload PO_Register.xlsx", type=["xlsx"])

with col2:
    invoice_files = st.file_uploader("Upload invoice PDFs", type=["pdf"], accept_multiple_files=True)

st.divider()

# --- Run button
run = st.button("▶ Run Batch", type="primary", disabled=(po_file is None or len(invoice_files) == 0))

if run:
    with st.spinner("Running batch..."):
        # Create a temp workspace
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            invoice_dir = tmpdir / "invoices"
            data_dir = tmpdir / "data"
            invoice_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            # Save uploaded PO register
            po_path = data_dir / "PO_Register.xlsx"
            po_path.write_bytes(po_file.getbuffer())

            # Save uploaded PDFs
            for f in invoice_files:
                (invoice_dir / f.name).write_bytes(f.getbuffer())

            # Output file
            out_path = data_dir / "Batch_Output.xlsx"

            # Run the batch
            out_workbook = run_batch(invoice_dir=invoice_dir, po_register_path=po_path, out_workbook=out_path)

            # Read bytes for download
            out_bytes = out_workbook.read_bytes()

    st.success("✅ Batch completed!")

    st.download_button(
        label="⬇ Download Batch_Output.xlsx",
        data=out_bytes,
        file_name="Batch_Output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )