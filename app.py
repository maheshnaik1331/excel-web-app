import os
import io
import uuid
import re
import logging
import pandas as pd
from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

processed_data_cache = {}

HEADER_MAPPING = {
    'Tank No.': ['tankno', 'tanknumber', 'tanknos', 'tankiso', 'tank', 'tanketcc', 'tank_no','Container No.', 'tank#'],
    'Tank Prefix': ['tankprefix', 'tank pref', 'tankpre', 'tank prefx', 'tankpreffix', 'tank_prefix'],
    'Tank Number Part': ['tanknum', 'tanknumberpart', 'tanknumber part', 'tank_number', 'tank num', 'tanknumpart'],
    'Depot-In Date': ['depotindate', 'dateintodepot', 'depotin', 'indate','In Date','Date in', 'movein'],
    'Depot-Out Date': ['depotoutdate', 'dateoutofdepot', 'depotout', 'moveout'],
    'Available Date': ['availabledate', 'available', 'avdate'],
    'Status': ['status', 'currentstatus'],
    'Depot': ['depot', 'depotname', 'depot_no']
}

# Columns explicitly retained in final sheet and display order
FINAL_COLUMNS_ORDER = ['Depot', 'Tank No.', 'Depot-In Date', 'Depot-Out Date', 'Status', 'Available Date', 'Country', 'Location', 'Region']

def find_header_row(sheet_df):
    header_keywords = {term for terms in HEADER_MAPPING.values() for term in terms}
    best_idx = -1
    max_matches = 0
    for i, row in sheet_df.head(20).iterrows():
        row_str = ' '.join(str(s).lower() for s in row.dropna())
        row_norm = re.sub(r'[\s\-]+', '', row_str)
        matches = sum(k in row_norm for k in header_keywords)
        if matches > max_matches:
            max_matches = matches
            best_idx = i
    return best_idx if max_matches > 1 else 0

def normalize_headers(df):
    normalized_col_map = {re.sub(r'[\s\-]+', '', str(col).lower()): col for col in df.columns}
    rename_map = {}
    for norm_col, original_col in normalized_col_map.items():
        for std_name, variants in HEADER_MAPPING.items():
            normalized_variants = [v.lower() for v in variants]
            if norm_col in normalized_variants:
                rename_map[original_col] = std_name
                break
    df = df.rename(columns=rename_map)
    for std_name in HEADER_MAPPING.keys():
        if std_name not in df.columns:
            df[std_name] = None
    logger.debug(f"Normalized columns: {list(df.columns)}")
    return df

def clean_tank_part(value):
    if pd.isna(value):
        return ""
    val_str = str(value).upper()
    val_str = re.sub(r'[\s\-]+', '', val_str)
    val_str = re.sub(r'[^A-Z0-9]', '', val_str)
    return val_str

def clean_date_columns(df):
    for col in ['Depot-In Date', 'Depot-Out Date', 'Available Date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    return df

def build_standard_tank_number(df):
    prefix_exists = 'Tank Prefix' in df.columns and df['Tank Prefix'].notna().any()
    number_exists = 'Tank Number Part' in df.columns and df['Tank Number Part'].notna().any()

    if prefix_exists and number_exists:
        prefix_cleaned = df['Tank Prefix'].apply(clean_tank_part).fillna("")
        number_cleaned = df['Tank Number Part'].apply(clean_tank_part).fillna("")
        df['Tank No.'] = prefix_cleaned.str.cat(number_cleaned, sep='')
        logger.debug(f"Built tank numbers combining prefix and number part: {df['Tank No.'].head(5).tolist()}")
    elif 'Tank No.' in df.columns:
        df['Tank No.'] = df['Tank No.'].apply(clean_tank_part)
        logger.debug(f"Cleaned single tank number column: {df['Tank No.'].head(5).tolist()}")
    else:
        df['Tank No.'] = ""
        logger.warning("No tank number columns found; tank numbers set empty")
    return df

def build_depot_details(df):
    # Add a "Depot Details" field combining all columns except those explicitly excluded
    exclude_cols = set(FINAL_COLUMNS_ORDER)  # Exclude these from details
    detail_cols = [col for col in df.columns if col not in exclude_cols]

    def row_to_detail(row):
        details = []
        for col in detail_cols:
            val = row.get(col, "")
            if pd.isna(val) or val == "":
                continue
            details.append(f"{col}: {val}")
        return "; ".join(details)

    df['Depot Details'] = df.apply(row_to_detail, axis=1)
    return df

def process_source_file(file_storage):
    filename = file_storage.filename
    depot_name_default = os.path.splitext(filename)[0].strip()
    engine = 'openpyxl' if filename.endswith('.xlsx') else 'xlrd'
    xls = pd.ExcelFile(file_storage, engine=engine)

    all_data_frames = []
    for sheet_name in xls.sheet_names:
        temp_df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        if temp_df.empty:
            continue
        header_idx = find_header_row(temp_df)
        df = pd.read_excel(xls, sheet_name=sheet_name, header=header_idx)
        if df.empty:
            continue
        df = normalize_headers(df)

        # Add/fill Depot field
        if 'Depot' not in df.columns or df['Depot'].isnull().all():
            df['Depot'] = depot_name_default
        else:
            df['Depot'].fillna(depot_name_default, inplace=True)

        df = clean_date_columns(df)
        df = build_standard_tank_number(df)

        # Exclude rows with empty Tank No.
        df = df[df['Tank No.'].str.strip() != ""]

        # Build Depot Details (optional: remove if not needed in final output)
        df = build_depot_details(df)

        all_data_frames.append(df)

    if not all_data_frames:
        return pd.DataFrame()

    return pd.concat(all_data_frames, ignore_index=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/process-files', methods=['POST'])
def process_files_route():
    if 'source_files' not in request.files or 'mapping_file' not in request.files:
        return jsonify({'error': 'Source files and mapping file are required.'}), 400

    source_files = request.files.getlist('source_files')
    mapping_file = request.files['mapping_file']

    try:
        all_data = [process_source_file(f) for f in source_files if f]
        if not any(not df.empty for df in all_data):
            return jsonify({'error': 'No data with valid tank numbers extracted from source files.'}), 400

        merged_df = pd.concat(all_data, ignore_index=True)

        engine = 'openpyxl' if mapping_file.filename.endswith('.xlsx') else 'xlrd'
        mapping_df = pd.read_excel(mapping_file, engine=engine)

        required_cols = ['Depot', 'Country', 'Location', 'Region']
        missing_cols = [col for col in required_cols if col not in mapping_df.columns]
        if missing_cols:
            return jsonify({'error': f'Mapping file missing columns: {", ".join(missing_cols)}'}), 400

        merged_df['join_key'] = merged_df['Depot'].astype(str).str.strip().str.lower()
        mapping_df['join_key'] = mapping_df['Depot'].astype(str).str.strip().str.lower()

        mapping_cols = mapping_df.drop(columns=['Depot'])

        final_df = pd.merge(merged_df, mapping_cols, on='join_key', how='left')
        final_df.drop(columns=['join_key'], inplace=True)

        for col in FINAL_COLUMNS_ORDER:
            if col not in final_df.columns:
                final_df[col] = ""

        final_df = final_df[FINAL_COLUMNS_ORDER]

        final_df = final_df.astype(str).replace("nan", "null")

        task_id = str(uuid.uuid4())
        processed_data_cache[task_id] = final_df

        # Return columns with Depot Details appended for UI preview
        columns = FINAL_COLUMNS_ORDER + ['Depot Details'] if 'Depot Details' in final_df.columns else FINAL_COLUMNS_ORDER

        return jsonify({'taskId': task_id, 'columns': columns, 'message': 'Files processed successfully.'})
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': f'Error: {str(e)}'}), 500

@app.route('/api/generate-report', methods=['POST'])
def generate_report_route():
    data = request.get_json()
    task_id = data.get('taskId')
    columns = data.get('columns')
    if not task_id or not columns:
        return jsonify({'error': 'Missing task ID or columns.'}), 400
    df = processed_data_cache.get(task_id)
    if df is None:
        return jsonify({'error': 'Session expired or data not found. Please re-upload files.'}), 404
    # Make sure Depot Details included if selected
    columns_filtered = [col for col in columns if col in df.columns]
    report_df = df[columns_filtered]
    return jsonify({'reportData': report_df.to_dict(orient='records')})

@app.route('/api/download-report', methods=['POST'])
def download_report_route():
    data = request.get_json()
    report_data = data.get('reportData')
    if not report_data:
        return jsonify({'error': 'No data provided for download.'}), 400
    try:
        df_to_save = pd.DataFrame(report_data)
        for col in FINAL_COLUMNS_ORDER:
            if col not in df_to_save.columns:
                df_to_save[col] = ""
        # Always place Depot Details column last if exists
        columns_ordered = FINAL_COLUMNS_ORDER[:]
        if 'Depot Details' in df_to_save.columns:
            columns_ordered.append('Depot Details')
        df_to_save = df_to_save[columns_ordered]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_to_save.to_excel(writer, index=False, sheet_name='Final_Report')
        output.seek(0)
        return send_file(output,
                         download_name='final_report.xlsx',
                         as_attachment=True,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({'error': f'Failed to generate download: {str(e)}'}), 500

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

