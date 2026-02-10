import streamlit as st
import pandas as pd
import json
from google.cloud import storage
from datetime import datetime
import os
import io

# --- Configuration ---
PROJECT_ID = "thermal-battery-agent-ds1" 
BUCKET_NAME = "thermal-battery-archive"

# --- Credentials (Hardcoded for Demo) ---
# In production, use Secrets Manager
CREDENTIALS = {
    "resindia_eng": "Engineer",
    "resindia_mgr": "Manager"
}

# Folder Mapping
SUBFOLDER_MAP = {
    "DesignData": "design_data",
    "CustomerSpecs": "customer_spec",
    "DischargeProfileSpec": "discharge_profile_spec", 
    "TempData": "temp",
    "DischargeData": "discharge_data"
}

# --- Authentication & Session State ---
def init_session():
    if 'user_role' not in st.session_state: st.session_state.user_role = None
    if 'parsed_data' not in st.session_state: st.session_state.parsed_data = None
    if 'data_type_tag' not in st.session_state: st.session_state.data_type_tag = None
    if 'edit_df' not in st.session_state: st.session_state.edit_df = None
    if 'current_blob_name' not in st.session_state: st.session_state.current_blob_name = None

def check_login():
    if st.session_state.user_role:
        return True

    st.markdown("## 🔐 Login Required")
    pwd = st.text_input("Enter Access Password:", type="password")
    
    if pwd:
        if pwd in CREDENTIALS:
            st.session_state.user_role = CREDENTIALS[pwd]
            st.success(f"Logged in as: **{CREDENTIALS[pwd]}**")
            st.rerun()
        else:
            st.error("Invalid Password")
    return False

# --- GCS Helper Functions ---

def get_storage_client():
    return storage.Client(project=PROJECT_ID)

def list_battery_codes(bucket):
    blobs = bucket.list_blobs(delimiter="/")
    list(blobs) 
    folders = [p.replace("/", "") for p in blobs.prefixes]
    return sorted(folders)

def list_files_in_folder(bucket, battery_code, data_type):
    subfolder = SUBFOLDER_MAP.get(data_type)
    if not subfolder: return []
    prefix = f"{battery_code}/{subfolder}/"
    blobs = bucket.list_blobs(prefix=prefix)
    
    file_list = []
    for blob in blobs:
        # Exclude the pending folder itself from the main list
        if blob.name.endswith(".json") and "_pending_approvals" not in blob.name:
            file_list.append(blob.name)
    return file_list

def list_pending_files(bucket, battery_code):
    """Scans all subfolders for _pending_approvals items."""
    pending_items = []
    
    # We iterate through known subfolders to check for pending items
    for tag, subfolder in SUBFOLDER_MAP.items():
        prefix = f"{battery_code}/{subfolder}/_pending_approvals/"
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if blob.name.endswith(".json"):
                pending_items.append({
                    "blob_name": blob.name,
                    "type": tag,
                    "filename": blob.name.split("/")[-1]
                })
    return pending_items

def check_build_exists(bucket, folder_name, subfolder, unique_key_part):
    prefix = f"{folder_name}/{subfolder}/"
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        if unique_key_part in blob.name and "_pending_approvals" not in blob.name:
            return True
    return False

# --- Parsing Functions ---

def parse_design_data_split(file):
    df_raw = pd.read_excel(file, header=None, engine='openpyxl')
    param_names = df_raw.iloc[4:, 1].values
    units = df_raw.iloc[4:, 2].fillna('').astype(str).values
    clean_params = []
    for name, unit in zip(param_names, units):
        clean_name = str(name).strip()
        clean_unit = str(unit).strip()
        clean_params.append(f"{clean_name} ({clean_unit})" if clean_unit and clean_unit.lower() != 'nan' else clean_name)

    data_dict = {}
    total_cols = df_raw.shape[1]
    current_col = 3
    while current_col < total_cols:
        bat_val = df_raw.iloc[4, current_col]
        build_val = df_raw.iloc[5, current_col]
        if pd.isna(bat_val) or pd.isna(build_val):
            current_col += 1
            continue
        bat_code = str(bat_val).strip()
        build_no = str(build_val).strip()
        unique_key = f"Battery-{bat_code}_Build-{build_no}"
        col_values = df_raw.iloc[4:, current_col].values
        build_data = pd.DataFrame([col_values], columns=clean_params)
        data_dict[unique_key] = build_data
        current_col += 1
    return data_dict, "DesignData"

def parse_customer_specs_and_profile(file):
    df_raw = pd.read_excel(file, header=None, engine='openpyxl')
    bat_val = df_raw.iloc[5, 3]
    bat_code = str(bat_val).strip() if pd.notna(bat_val) else "Unknown"
    results = {}
    
    # Specs
    spec_block = df_raw.iloc[6:20, [1, 3]]
    spec_block.columns = ["Parameter", "Value"]
    df_spec = spec_block.set_index("Parameter").transpose()
    df_spec.reset_index(drop=True, inplace=True)
    results[f"CustomerSpecs:Battery-{bat_code}_Spec"] = df_spec
    
    # Profile
    header_check = str(df_raw.iloc[4, 5]).strip()
    if "Duration" in header_check:
        profile_block = df_raw.iloc[5:, [5, 6, 7]]
        profile_block.columns = ["Duration (sec)", "Current (A)", "Voltage (V)"]
        profile_block = profile_block.dropna(how='all')
        profile_block = profile_block[profile_block["Duration (sec)"] != "Duration (sec)"]
        results[f"DischargeProfileSpec:Battery-{bat_code}_Profile"] = profile_block

    return results, "MixedSpecs"

def parse_temp_data_multi(file):
    df_raw = pd.read_excel(file, header=None, engine='openpyxl')
    shared_time = df_raw.iloc[6:, 1].reset_index(drop=True)
    shared_time.name = "Time"
    data_dict = {}
    total_cols = df_raw.shape[1]
    current_col = 2
    while current_col < total_cols:
        header_val = str(df_raw.iloc[5, current_col]).strip()
        if header_val != 'T1': 
             current_col += 1
             continue
        bat_code = str(df_raw.iloc[2, current_col + 1]).strip()
        build_no = str(df_raw.iloc[3, current_col + 1]).strip()
        if bat_code == 'nan': bat_code = "UnknownBatt"
        if build_no == 'nan': build_no = "UnknownBuild"
        unique_key = f"Battery-{bat_code}_Build-{build_no}"
        block_data = df_raw.iloc[6:, current_col : current_col+3]
        block_data.columns = ["T1", "T2", "T3"]
        block_data = block_data.reset_index(drop=True)
        combined = pd.concat([shared_time, block_data], axis=1)
        combined = combined.dropna(subset=["Time"])
        data_dict[unique_key] = combined
        current_col += 3 
    return data_dict, "TempData"

def parse_discharge_data_multi(file):
    df_raw = pd.read_excel(file, header=None, engine='openpyxl')
    data_dict = {}
    total_cols = df_raw.shape[1]
    current_col = 1
    while current_col < total_cols:
        header_val = str(df_raw.iloc[7, current_col]).strip()
        if header_val != 'Time':
            current_col += 1
            continue
        bat_code = str(df_raw.iloc[4, current_col + 1]).strip()
        build_no = str(df_raw.iloc[5, current_col + 1]).strip()
        if bat_code == 'nan': bat_code = "UnknownBatt"
        if build_no == 'nan': build_no = "UnknownBuild"
        unique_key = f"Battery-{bat_code}_Build-{build_no}"
        block_data = df_raw.iloc[8:, current_col : current_col+3]
        block_data.columns = ["Time", "Current", "Voltage"]
        block_data = block_data.dropna(how='all')
        data_dict[unique_key] = block_data
        current_col += 3
    return data_dict, "DischargeData"

# --- Main Application ---
def run_app():
    st.set_page_config(page_title="Thermal Battery Data", layout="wide")
    init_session()

    if not check_login():
        return

    # Header with Role Info
    role_color = "red" if st.session_state.user_role == "Manager" else "blue"
    st.markdown(f"**Logged in as:** :{role_color}[{st.session_state.user_role}]")
    
    # Conditional Tabs
    tabs_list = ["📤 Upload New Data", "✏️ Update Existing Data"]
    if st.session_state.user_role == "Manager":
        tabs_list.append("🔔 Pending Approvals")
    
    tabs = st.tabs(tabs_list)

    # -------------------------------------------------------------------------
    # TAB 1: UPLOAD NEW DATA (INCREMENTAL)
    # -------------------------------------------------------------------------
    with tabs[0]:
        st.markdown("### Upload New Excel Files")
        st.info("ℹ️ System checks for duplicates. Only new builds are uploaded.")
        
        file_type = st.selectbox(
            "Select Data Type",
            ("Design Data (Matrix)", "Customer Specifications", "Temp Data (Shared Time)", "Discharge Data"),
            key="up_type"
        )
        uploaded_file = st.file_uploader(f"Upload {file_type} File", type=['xlsx', 'xls'], key="up_file")

        if uploaded_file:
            try:
                if file_type == "Design Data (Matrix)":
                    data, tag = parse_design_data_split(uploaded_file)
                elif file_type == "Customer Specifications":
                    data, tag = parse_customer_specs_and_profile(uploaded_file)
                elif file_type == "Temp Data (Shared Time)":
                    data, tag = parse_temp_data_multi(uploaded_file)
                elif file_type == "Discharge Data":
                    data, tag = parse_discharge_data_multi(uploaded_file)
                
                if data:
                    st.session_state.parsed_data = data
                    st.session_state.data_type_tag = tag
                    st.success(f"Parsed {len(data)} Data Blocks.")
            except Exception as e:
                st.error(f"Parsing Error: {e}")

        if st.button("☁️ PROCESS UPLOAD", key="btn_proc_up"):
            if st.session_state.parsed_data:
                with st.spinner("Processing..."):
                    try:
                        client = get_storage_client()
                        bucket = client.bucket(BUCKET_NAME)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        
                        new_c, skip_c = 0, 0
                        for key, df_item in st.session_state.parsed_data.items():
                            # Resolve Tag/Folder
                            if ":" in key: spec_tag, real_key = key.split(":")
                            else: spec_tag, real_key = st.session_state.data_type_tag, key

                            subfolder = SUBFOLDER_MAP.get(spec_tag, "misc")
                            
                            # Resolve Battery Code
                            folder = "Unknown"
                            if "Battery-" in real_key:
                                parts = real_key.split('_')
                                for p in parts:
                                    if "Battery-" in p: folder = p.replace("Battery-", "")
                            
                            unique_id = real_key.split('_')[-1]
                            
                            if check_build_exists(bucket, folder, subfolder, unique_id):
                                skip_c += 1
                            else:
                                blob_name = f"{folder}/{subfolder}/{spec_tag}_{timestamp}_{real_key}.json"
                                bucket.blob(blob_name).upload_from_string(df_item.to_json(orient='records', indent=4), content_type='application/json')
                                new_c += 1
                        
                        st.success(f"✅ Uploaded {new_c} new files.")
                        if skip_c > 0: st.warning(f"Skipped {skip_c} duplicates.")
                    except Exception as e:
                        st.error(f"Error: {e}")

    # -------------------------------------------------------------------------
    # TAB 2: UPDATE EXISTING DATA (SUBMIT FOR REVIEW)
    # -------------------------------------------------------------------------
    with tabs[1]:
        st.markdown("### Request Updates to Stored Data")
        st.info("ℹ️ Edits made here are sent to the Manager for approval. They do not overwrite data immediately.")
        
        try:
            client = get_storage_client()
            bucket = client.bucket(BUCKET_NAME)
            
            bat_codes = list_battery_codes(bucket)
            if not bat_codes: st.warning("Archive empty.")
            else:
                c1, c2, c3 = st.columns(3)
                with c1: sel_bat = st.selectbox("Battery Code", bat_codes, key="ed_bat")
                with c2: sel_type = st.selectbox("Data Type", list(SUBFOLDER_MAP.keys()), key="ed_type")
                with c3:
                    files = list_files_in_folder(bucket, sel_bat, sel_type)
                    if files:
                        f_map = {f: f.split('/')[-1] for f in files}
                        sel_file = st.selectbox("Select File", files, format_func=lambda x: f_map[x], key="ed_file")
                    else: sel_file = None

                if sel_file and st.button("📥 Load", key="ed_load"):
                    blob = bucket.blob(sel_file)
                    st.session_state.edit_df = pd.read_json(io.BytesIO(blob.download_as_string()))
                    st.session_state.current_blob_name = sel_file
                    st.rerun()

                if st.session_state.edit_df is not None:
                    st.divider()
                    st.markdown(f"**Editing:** `{st.session_state.current_blob_name}`")
                    edited_df = st.data_editor(st.session_state.edit_df, num_rows="dynamic", width="stretch", height=400)
                    
                    if st.button("📤 SUBMIT FOR APPROVAL", type="primary"):
                        with st.spinner("Sending to Pending Folder..."):
                            try:
                                # Logic: Save to _pending_approvals subfolder
                                # Original: 48/temp/File.json
                                # Pending:  48/temp/_pending_approvals/File.json
                                original_path = st.session_state.current_blob_name
                                path_parts = original_path.split('/')
                                # Insert _pending_approvals before filename
                                pending_path = "/".join(path_parts[:-1]) + "/_pending_approvals/" + path_parts[-1]
                                
                                bucket.blob(pending_path).upload_from_string(edited_df.to_json(orient='records', indent=4), content_type='application/json')
                                st.success("✅ Change Request Submitted! Manager will review.")
                                st.session_state.edit_df = None # Clear
                            except Exception as e:
                                st.error(f"Error: {e}")

        except Exception as e:
            st.error(f"Connection Error: {e}")

    # -------------------------------------------------------------------------
    # TAB 3: MANAGER APPROVALS
    # -------------------------------------------------------------------------
    if st.session_state.user_role == "Manager":
        with tabs[2]:
            st.markdown("### 🔔 Pending Approvals")
            
            client = get_storage_client()
            bucket = client.bucket(BUCKET_NAME)
            
            # Filter by Battery to avoid scanning massive bucket
            bat_codes = list_battery_codes(bucket)
            sel_bat_mgr = st.selectbox("Filter by Battery Code:", bat_codes, key="mgr_bat")
            
            if sel_bat_mgr:
                pending_list = list_pending_files(bucket, sel_bat_mgr)
                
                if not pending_list:
                    st.success("No pending approvals for this battery.")
                else:
                    st.write(f"Found **{len(pending_list)}** pending changes.")
                    
                    # Selector
                    p_map = {item["blob_name"]: item["filename"] for item in pending_list}
                    sel_pending = st.selectbox("Select Change Request:", [p["blob_name"] for p in pending_list], format_func=lambda x: p_map[x])
                    
                    if sel_pending:
                        # Load Pending Data
                        blob_pending = bucket.blob(sel_pending)
                        df_pending = pd.read_json(io.BytesIO(blob_pending.download_as_string()))
                        
                        # Reconstruct Original Path to Load Live Data
                        # Pending: 48/temp/_pending_approvals/File.json
                        # Live:    48/temp/File.json
                        live_path = sel_pending.replace("_pending_approvals/", "")
                        blob_live = bucket.blob(live_path)
                        
                        col_l, col_r = st.columns(2)
                        
                        with col_l:
                            st.subheader("Current (Live)")
                            if blob_live.exists():
                                df_live = pd.read_json(io.BytesIO(blob_live.download_as_string()))
                                st.dataframe(df_live, width="stretch", height=300)
                            else:
                                st.warning("Original file missing (New Creation?)")

                        with col_r:
                            st.subheader("Proposed (Draft)")
                            st.dataframe(df_pending, width="stretch", height=300)
                            
                        st.divider()
                        c_app, c_rej = st.columns([1, 1])
                        
                        with c_app:
                            if st.button("✅ APPROVE & PUBLISH", type="primary"):
                                # Overwrite Live with Pending
                                bucket.blob(live_path).upload_from_string(df_pending.to_json(orient='records', indent=4), content_type='application/json')
                                # Delete Pending
                                blob_pending.delete()
                                st.success("Approved! File updated.")
                                st.rerun()
                                
                        with c_rej:
                            if st.button("❌ REJECT & DELETE"):
                                blob_pending.delete()
                                st.error("Rejected. Draft deleted.")
                                st.rerun()

run_app()