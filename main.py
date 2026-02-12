import streamlit as st
import pandas as pd
import re
import json

# ---------------- LOGIN ----------------
CREDENTIALS = {
    "resindia_eng": "Engineer",
    "resindia_mgr": "Manager"
}

def init_session():
    if 'user_role' not in st.session_state:
        st.session_state.user_role = None
    if 'pre_verify_df' not in st.session_state:
        st.session_state.pre_verify_df = None

def check_login():
    if st.session_state.user_role:
        return True

    st.title("🔐 Login")
    pwd = st.text_input("Enter Access Password", type="password")

    if pwd:
        if pwd in CREDENTIALS:
            st.session_state.user_role = CREDENTIALS[pwd]
            st.success(f"Logged in as {CREDENTIALS[pwd]}")
            st.rerun()
        else:
            st.error("Invalid Password")
    return False

# ---------------- EXCEL HELPERS ----------------
def excel_column_name(n):
    name = ""
    while n >= 0:
        name = chr(n % 26 + 65) + name
        n = n // 26 - 1
    return name

def remove_top_empty_rows(df):
    for i in range(len(df)):
        if not df.iloc[i].isna().all():
            return df.iloc[i:].reset_index(drop=True)
    return df

# ==========================================================
# 🔥🔥 SMART EXCEL → JSON ENGINE 🔥🔥
# ==========================================================
def convert_excel_to_json(df):

    df = df.replace("", pd.NA)

    # ------------------------------------------------------
    # 1️⃣ TEMPERATURE DATA DETECTION (Time,T1,T2,T3)
    # ------------------------------------------------------
    header_row = None
    for i in range(10):
        row = " ".join(df.iloc[i].astype(str)).lower()
        if "time" in row and "t1" in row:
            header_row = i
            break

    if header_row is not None:
        df_temp = df.iloc[header_row+1:].copy()
        df_temp.columns = ["Time","T1","T2","T3"]

        result = []
        for _, row in df_temp.iterrows():
            if pd.notna(row["Time"]):
                result.append({
                    "Time": float(row["Time"]),
                    "T1": float(row["T1"]),
                    "T2": float(row["T2"]),
                    "T3": float(row["T3"])
                })
        return result

    # ------------------------------------------------------
    # 2️⃣ DISCHARGE DATA DETECTION (Duration,Current,Voltage)
    # ------------------------------------------------------
    for r in range(len(df)-1):
        for c in range(len(df.columns)-2):
            h1 = str(df.iloc[r,c]).lower()
            h2 = str(df.iloc[r,c+1]).lower()
            h3 = str(df.iloc[r,c+2]).lower()

            if "duration" in h1 and "current" in h2 and "voltage" in h3:
                return [{
                    str(df.iloc[r,c]).strip(): float(df.iloc[r+1,c]),
                    str(df.iloc[r,c+1]).strip(): float(df.iloc[r+1,c+1]),
                    str(df.iloc[r,c+2]).strip(): str(df.iloc[r+1,c+2])
                }]

    # ------------------------------------------------------
    # 3️⃣ DESIGN / CUSTOMER SPEC (Parameter → Value)
    # ------------------------------------------------------
    param_col = None
    for col in df.columns:
        if df[col].astype(str).str.contains("Battery", case=False).any():
            param_col = col
            break

    if param_col is not None:
        value_col = df.columns[df.columns.get_loc(param_col)+2]
        result = {}

        for i in range(len(df)):
            key = str(df.loc[i,param_col]).strip()
            val = df.loc[i,value_col]

            if key != "" and key.lower() != "nan" and pd.notna(val):
                try:
                    val = float(val)
                except:
                    val = str(val)
                result[key] = val

        return [result]

    return None

# ==========================================================
# STREAMLIT APP
# ==========================================================
def run_app():
    st.set_page_config(layout="wide")
    init_session()

    if not check_login():
        return

    st.title("Thermal Battery Excel Upload")

    uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx','xls'])

    if uploaded_file:

        df = pd.read_excel(uploaded_file, header=None)
        df = remove_top_empty_rows(df)
        df.columns = [excel_column_name(i) for i in range(len(df.columns))]
        st.session_state.pre_verify_df = df

        st.subheader("Excel Preview")
        st.dataframe(df, use_container_width=True, height=500)

        if st.button("💾 SAVE JSON"):
            json_data = convert_excel_to_json(df)

            if json_data is None:
                st.error("Could not detect Excel format")
                return

            json_string = json.dumps(json_data, indent=4)

            st.success("JSON Generated")
            st.code(json_string, language="json")

            st.download_button(
                "Download JSON",
                json_string,
                "battery_data.json",
                "application/json"
            )

run_app()
