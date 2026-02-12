import streamlit as st
import pandas as pd
import json

st.set_page_config(page_title="Design Data Pre-Verification", layout="wide")
st.title("🔋 Design Data Pre-Verification")

# ============================================================
# SESSION STATE INIT
# ============================================================
if "df" not in st.session_state:
    st.session_state.df = None

if "edit_mode" not in st.session_state:
    st.session_state.edit_mode = False

# ============================================================
# SAFE NUMBER CONVERTER
# ============================================================
def to_number(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except:
        return None

# ============================================================
# LOAD EXCEL SAFELY (Matrix layout)
# ============================================================
def load_excel(uploaded_file):
    df = pd.read_excel(uploaded_file, header=None)
    df = df.dropna(axis=1, how="all")  # remove empty columns

    # Assign safe column names
    cols = []
    for i in range(df.shape[1]):
        if i == 0: cols.append("S.No")
        elif i == 1: cols.append("Parameter")
        elif i == 2: cols.append("Unit")
        else: cols.append(f"Build_{i-2}")
    df.columns = cols
    return df

# ============================================================
# EXTRACT BUILD DATA (Matrix → Per Build Dict)
# ============================================================
def extract_builds(df):
    param_df = df.iloc[1:].copy()  # remove title row
    build_cols = df.columns[3:]

    builds = {}

    for col in build_cols:
        build_dict = {}
        for _, row in param_df.iterrows():
            param = str(row["Parameter"]).strip()
            value = row[col]
            build_dict[param] = to_number(value) if to_number(value) is not None else value
        builds[col] = build_dict

    return builds

# ============================================================
# VALIDATION RULES
# ============================================================
def run_validation(builds):
    errors = []

    for build_name, data in builds.items():

        # Temperature rule
        temp = data.get("Temperature of Discharge")
        if temp is not None and not (-40 <= temp <= 70):
            errors.append(f"{build_name}: Temperature must be between -40 and 70")

        # Activation time rule
        act = data.get("Std. Max Activation Time")
        if act is not None and act > 2000:
            errors.append(f"{build_name}: Activation time > 2000 ms")

        # Cell formula
        series = data.get("Cells in Series")
        parallel = data.get("Stacks in Parallel")
        cells = data.get("Number of Cells")

        if None not in (series, parallel, cells):
            if series * parallel != cells:
                errors.append(f"{build_name}: Cells ≠ Series × Parallel")

        # Material ratio rule (only if fields exist)
        mat_fields = [
            "Electrolyte Weight per Electrode (grams)",
            "Anode Weight per Electrode (grams)",
            "Cathode Weight per Electrode (grams)",
            "Heat Pellet-1 Weight (grams)"
        ]

        if all(f in data for f in mat_fields):
            total = sum([to_number(data[f]) or 0 for f in mat_fields])
            if total and abs(total - 100) > 0.5:
                errors.append(f"{build_name}: Material ratio must equal 100%")

    return errors

# ============================================================
# FILE UPLOAD
# ============================================================
uploaded_file = st.file_uploader("Upload Design Data Excel", type=["xlsx"])

if uploaded_file:
    if st.session_state.df is None:
        st.session_state.df = load_excel(uploaded_file)

# ============================================================
# SHOW TABLE (VIEW / EDIT MODE)
# ============================================================
if st.session_state.df is not None:

    st.subheader("📄 Excel Preview")

    if st.session_state.edit_mode:
        edited_df = st.data_editor(
            st.session_state.df,
            use_container_width=True,
            num_rows="dynamic"
        )
        st.session_state.df = edited_df
        st.success("Editing enabled — changes saved automatically.")
    else:
        st.dataframe(st.session_state.df, use_container_width=True)

    st.divider()

    # ========================================================
    # BUTTONS
    # ========================================================
    col1, col2, col3, col4 = st.columns(4)

    if col1.button("🧪 Verification"):
        builds = extract_builds(st.session_state.df)
        errors = run_validation(builds)

        st.subheader("Verification Results")

        if errors:
            for e in errors:
                st.error(e)
        else:
            st.success("✅ All builds passed verification")

    if col2.button("⚪ No Verification"):
        st.info("No-Verification workflow started")

    if col3.button("✏️ Edit"):
        st.session_state.edit_mode = True
        st.rerun()

    if col4.button("📦 Generate JSON"):
        builds = extract_builds(st.session_state.df)

        json_data = {}
        for b, data in builds.items():
            json_data[b] = data

        st.subheader("Generated JSON")
        st.json(json_data)

        st.download_button(
            "⬇ Download JSON",
            json.dumps(json_data, indent=4),
            file_name="design_data.json",
            mime="application/json"
        )
