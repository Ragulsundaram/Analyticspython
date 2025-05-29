from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import pandas as pd
import sqlite3
import re
import requests

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = 'uploads'
DATABASE = 'data/analytics.db'
OLLAMA_API_URL = 'http://10.71.99.51:6042/api/generate' # Configure your Ollama API endpoint

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
if not os.path.exists(os.path.dirname(DATABASE)):
    os.makedirs(os.path.dirname(DATABASE))

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DATABASE'] = DATABASE
app.config['OLLAMA_API_URL'] = OLLAMA_API_URL

def init_db():
    conn = sqlite3.connect(app.config['DATABASE'])
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            original_filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            headers TEXT NOT NULL, -- Stored as comma-separated string
            uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS relationships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table_id INTEGER NOT NULL,
            source_column TEXT NOT NULL,
            target_table_id INTEGER NOT NULL,
            target_column TEXT NOT NULL,
            FOREIGN KEY (source_table_id) REFERENCES tables (id),
            FOREIGN KEY (target_table_id) REFERENCES tables (id),
            UNIQUE (source_table_id, source_column, target_table_id, target_column)
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS thresholds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            column_name TEXT NOT NULL,
            function TEXT NOT NULL, -- NEW: e.g., 'AVG', 'MAX', 'MIN', 'SUM', 'COUNT'
            operator TEXT NOT NULL, -- e.g., '>', '<', '=', '>=', '<='
            value REAL NOT NULL,    -- Threshold value
            FOREIGN KEY (table_id) REFERENCES tables (id),
            UNIQUE (table_id, column_name, function) -- Only one threshold for a given function on a column per table
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            threshold_id INTEGER, -- Which threshold triggered this
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            function_name TEXT NOT NULL, -- NEW: Store which function was applied
            current_value REAL,
            threshold_value REAL,
            recommendation_text TEXT NOT NULL,
            FOREIGN KEY (threshold_id) REFERENCES thresholds (id)
        );
    ''')
    conn.commit()
    conn.close()

with app.app_context():
    init_db()

def get_db_connection():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def sanitize_sql_name(name):
    return re.sub(r'\W+', '_', name).replace(' ', '_')

def get_ollama_recommendation(prompt):
    try:
        headers = {'Content-Type': 'application/json'}
        data = {
            "model": "devstral:24b", # <<< This was corrected in the last step
            "prompt": prompt,
            "stream": False
        }
        response = requests.post(app.config['OLLAMA_API_URL'], headers=headers, json=data)
        response.raise_for_status()
        
        result = response.json()
        return result.get('response', 'No recommendation generated.')
    except requests.exceptions.ConnectionError:
        app.logger.error("Ollama server not running or unreachable.")
        return "Error: Ollama server not running or unreachable. Please start Olllama and download a model."
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Error calling Ollama API: {e}")
        return f"Error: Failed to get recommendation from Ollama: {e}"

# --- Existing /upload route (MODIFIED to trigger threshold check & Ollama) ---
@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    files = request.files.getlist('files[]')
    if not files:
        return jsonify({"error": "No selected file"}), 400

    uploaded_info = []
    conn = get_db_connection()
    cursor = conn.cursor()

    for file in files:
        if file.filename == '':
            continue
        if file and file.filename.endswith('.csv'):
            original_filename = file.filename
            filename_without_ext = os.path.splitext(original_filename)[0]
            sanitized_table_name = sanitize_sql_name(filename_without_ext)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)

            try:
                file.save(filepath)

                # Read headers and save metadata to 'tables' table
                df_headers = pd.read_csv(filepath, nrows=0)
                headers = df_headers.columns.tolist()
                headers_str = ','.join(headers)

                cursor.execute("SELECT id FROM tables WHERE original_filename = ?", (original_filename,))
                existing_table = cursor.fetchone()
                
                table_id = None
                if existing_table:
                    table_id = existing_table['id']
                    cursor.execute("""
                        UPDATE tables
                        SET name = ?, filepath = ?, headers = ?, uploaded_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (sanitized_table_name, filepath, headers_str, table_id))
                    message = f"File '{original_filename}' updated successfully."
                else:
                    cursor.execute("""
                        INSERT INTO tables (name, original_filename, filepath, headers)
                        VALUES (?, ?, ?, ?)
                    """, (sanitized_table_name, original_filename, filepath, headers_str))
                    table_id = cursor.lastrowid
                    message = f"File '{original_filename}' uploaded successfully."
                
                conn.commit() # Commit table metadata change first
                
                uploaded_info.append({
                    "filename": original_filename,
                    "table_name": sanitized_table_name,
                    "headers": headers,
                    "message": message
                })

                # --- Load CSV data into SQLite for querying and monitoring ---
                full_df = pd.read_csv(filepath)
                full_df.columns = [sanitize_sql_name(col) for col in full_df.columns]
                full_df.to_sql(sanitized_table_name, conn, if_exists='replace', index=False)
                app.logger.info(f"Loaded '{original_filename}' into SQLite table '{sanitized_table_name}'")

                # --- Check thresholds after successful upload and data load ---
                if table_id:
                    check_and_trigger_recommendations(table_id, sanitized_table_name, full_df, conn)


            except Exception as e:
                if os.path.exists(filepath):
                    os.remove(filepath)
                conn.rollback()
                return jsonify({"error": f"Failed to process file {original_filename}: {str(e)}"}), 500
        else:
            return jsonify({"error": f"File {file.filename} is not a CSV"}), 400

    conn.close()
    
    if not uploaded_info:
        return jsonify({"error": "No valid CSV files uploaded or processed."}), 400

    return jsonify({
        "message": "Files processed and saved successfully!",
        "uploaded_tables": uploaded_info
    }), 200

# --- NEW: Function to check thresholds and trigger recommendations ---
def check_and_trigger_recommendations(table_id, table_name, df_data, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id, column_name, function, operator, value FROM thresholds WHERE table_id = ?", (table_id,))
    thresholds = cursor.fetchall()
    
    triggered_recommendations = []

    for threshold in thresholds:
        threshold_id = threshold['id']
        original_column_name = threshold['column_name'] # Keep original for prompt
        sanitized_column_name = sanitize_sql_name(original_column_name) # Use sanitized for DataFrame access
        function = threshold['function']
        operator = threshold['operator']
        value = threshold['value']

        if sanitized_column_name not in df_data.columns:
            app.logger.warning(f"Threshold column '{sanitized_column_name}' (original: {original_column_name}) not found in uploaded data for table '{table_name}'. Skipping.")
            continue

        current_value = None
        # Apply the chosen aggregation function
        try:
            if pd.api.types.is_numeric_dtype(df_data[sanitized_column_name]):
                if function == 'AVG':
                    current_value = df_data[sanitized_column_name].mean()
                elif function == 'MAX':
                    current_value = df_data[sanitized_column_name].max()
                elif function == 'MIN':
                    current_value = df_data[sanitized_column_name].min()
                elif function == 'SUM':
                    current_value = df_data[sanitized_column_name].sum()
                elif function == 'COUNT': # Count non-null values
                    current_value = df_data[sanitized_column_name].count()
                else:
                    app.logger.warning(f"Unknown function '{function}' for threshold on {table_name}.{original_column_name}. Skipping.")
                    continue
            else:
                app.logger.warning(f"Column '{original_column_name}' in table '{table_name}' is not numeric, but a numeric function '{function}' was selected. Skipping threshold check.")
                continue

            # Evaluate the condition
            condition_met = False
            if current_value is not None:
                if operator == '>':
                    condition_met = current_value > value
                elif operator == '<':
                    condition_met = current_value < value
                elif operator == '=':
                    condition_met = current_value == value
                elif operator == '>=':
                    condition_met = current_value >= value
                elif operator == '<=':
                    condition_met = current_value <= value
            
            if condition_met:
                app.logger.info(f"Threshold breached for {table_name}.{original_column_name} ({function}): {current_value} {operator} {value}")
                
                # Prepare prompt for Ollama
                schema_info = ", ".join(df_data.columns.tolist())
                
                # Dynamic context based on common monitoring metrics
                # You can expand this 'if/elif' block for more specific scenarios
                additional_guidance = "" # This was the fix from before
                if "Cpu usage" in original_column_name or "CPU_Usage" in original_column_name:
                    additional_guidance = (
                        f"- If CPU usage is high, common recommendations include 'Identify and terminate resource-intensive applications or processes.', "
                        f"'Review recent code deployments or system updates.', or 'Consider scaling up CPU resources.'\n"
                        f"- If active processes are unusually high, suggest 'Investigate unusual process spikes or potential malware.'\n"
                        f"- If memory usage is high, suggest 'Optimize memory-hungry services or add more RAM.'\n"
                        f"- If disk space is low, suggest 'Clean up temporary files and old logs, or unnecessary data.'\n"
                    )
                elif "Memory usage" in original_column_name or "Memory_Usage" in original_column_name:
                     additional_guidance = (
                        f"- If memory usage is high, common recommendations include 'Identify memory-leaking applications and restart them.', "
                        f"'Optimize memory-hungry services or configurations.', or 'Consider increasing server RAM.'\n"
                    )
                elif "Disk space" in original_column_name or "Disk_Space" in original_column_name:
                    additional_guidance = (
                        f"- If disk space is low, common recommendations include 'Clear temporary files, old logs, and caches.', "
                        f"'Review large files and relocate or delete unnecessary data.', or 'Consider expanding storage capacity.'\n"
                    )
                elif "Processes" in original_column_name or "Active_Processes" in original_column_name:
                    additional_guidance = (
                        f"- If the number of processes is high, common recommendations include 'Identify and stop unnecessary background processes.', "
                        f"'Check for runaway processes or misconfigured services.', or 'Scan for malware or rootkits.'\n"
                    )
                # You can add more 'elif' blocks for other common metrics you monitor (e.g., Network_Latency, Error_Rate)


                prompt = (
                    f"You are an intelligent system monitoring assistant. Your task is to provide concise and actionable "
                    f"recommendations when a system metric breaches a predefined threshold.\n\n"
                    f"The current system data is from a table named '{table_name}' with the following columns: [{schema_info}].\n"
                    f"A critical threshold has been breached: The {function} of the column '{original_column_name}' "
                    f"is currently {current_value}, which is {operator} the defined threshold of {value}.\n\n"
                    f"Based on this, provide a **single, direct, and actionable recommendation** for a system administrator or user. "
                    f"Avoid asking questions or providing lengthy explanations. Focus only on the solution.\n"
                    f"Here are some examples of the type of recommendations expected:\n"
                    f"{additional_guidance}" # Dynamically added guidance
                    f"Provide the recommendation clearly and concisely."
                )
                
                ollama_recommendation_text = get_ollama_recommendation(prompt)
                
                cursor.execute("""
                    INSERT INTO recommendations (threshold_id, table_name, column_name, function_name, current_value, threshold_value, recommendation_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (threshold_id, table_name, original_column_name, function, current_value, value, ollama_recommendation_text))
                conn.commit()
                triggered_recommendations.append({
                    "table": table_name,
                    "column": original_column_name,
                    "function": function,
                    "current_value": current_value,
                    "threshold_value": value,
                    "operator": operator,
                    "recommendation": ollama_recommendation_text
                })
        except Exception as e:
            app.logger.error(f"Error processing threshold for {table_name}.{original_column_name} ({function}): {e}")
            continue # Continue to next threshold even if one fails
    
    return triggered_recommendations

# --- Existing /tables route (no changes) ---
@app.route('/tables', methods=['GET'])
def get_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, original_filename, headers FROM tables ORDER BY uploaded_at DESC")
    tables_data = cursor.fetchall()
    conn.close()

    result = []
    for row in tables_data:
        headers_list = row['headers'].split(',') if row['headers'] else []
        result.append({
            "id": row['id'],
            "name": row['name'], # This is the sanitized name for internal use
            "original_filename": row['original_filename'], # Keep original filename for display
            "headers": headers_list
        })
    return jsonify({"tables": result}), 200

# --- Existing /tables/<int:table_id>/data route (uses SQL data now) ---
@app.route('/tables/<int:table_id>/data', methods=['GET'])
def get_table_data(table_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, original_filename FROM tables WHERE id = ?", (table_id,))
    table_info = cursor.fetchone()
    conn.close()

    if not table_info:
        return jsonify({"error": "Table not found"}), 404

    sanitized_table_name = table_info['name']
    original_filename = table_info['original_filename']

    try:
        conn = get_db_connection()
        df = pd.read_sql_query(f"SELECT * FROM {sanitized_table_name} LIMIT 50", conn)
        conn.close()

        data = df.to_dict(orient='records')
        columns = df.columns.tolist() # These will be sanitized column names
        return jsonify({"data": data, "columns": columns, "table_name": original_filename}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to read data from SQL table {sanitized_table_name}: {str(e)}"}), 500

# --- NEW: Get current calculated value for a column/function ---
@app.route('/tables/<int:table_id>/columns/<string:column_name>/<string:function_name>/current_value', methods=['GET'])
def get_column_current_value(table_id, column_name, function_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, headers FROM tables WHERE id = ?", (table_id,))
    table_info = cursor.fetchone()
    conn.close()

    if not table_info:
        return jsonify({"error": "Table not found"}), 404

    sanitized_table_name = table_info['name']
    original_headers = table_info['headers'].split(',')
    
    # Map original column name to sanitized column name
    # This is crucial because frontend uses original names, but SQL table uses sanitized ones
    sanitized_column_name_in_db = None
    for hdr in original_headers:
        if hdr == column_name:
            sanitized_column_name_in_db = sanitize_sql_name(hdr)
            break
    
    if not sanitized_column_name_in_db:
        return jsonify({"error": f"Column '{column_name}' not found in table '{table_info['original_filename']}'"}), 404


    # Ensure the function name is valid
    allowed_functions = ['AVG', 'MAX', 'MIN', 'SUM', 'COUNT']
    if function_name.upper() not in allowed_functions:
        return jsonify({"error": "Invalid function name. Allowed: AVG, MAX, MIN, SUM, COUNT"}), 400
    
    try:
        conn = get_db_connection()
        # Fetch only the relevant column to check its type and calculate value
        df_column = pd.read_sql_query(f"SELECT \"{sanitized_column_name_in_db}\" FROM {sanitized_table_name}", conn) # Quote column name for safety
        conn.close()

        calculated_value = None
        if not df_column.empty and pd.api.types.is_numeric_dtype(df_column[sanitized_column_name_in_db]):
            if function_name.upper() == 'AVG':
                calculated_value = df_column[sanitized_column_name_in_db].mean()
            elif function_name.upper() == 'MAX':
                calculated_value = df_column[sanitized_column_name_in_db].max()
            elif function_name.upper() == 'MIN':
                calculated_value = df_column[sanitized_column_name_in_db].min()
            elif function_name.upper() == 'SUM':
                calculated_value = df_column[sanitized_column_name_in_db].sum()
            elif function_name.upper() == 'COUNT':
                calculated_value = df_column[sanitized_column_name_in_db].count()
            
            if calculated_value is not None:
                # Round numeric values for cleaner display
                if isinstance(calculated_value, (float, int)):
                    calculated_value = round(calculated_value, 2)
                return jsonify({"current_value": calculated_value, "function": function_name, "column": column_name}), 200
            else:
                return jsonify({"error": "Could not calculate value (e.g., column is empty after filtering).", "current_value": None}), 404
        else:
             return jsonify({"error": f"Column '{column_name}' is not numeric or has no data, cannot apply '{function_name}' function.", "current_value": None}), 400

    except Exception as e:
        return jsonify({"error": f"Error calculating current value: {str(e)}"}), 500

# --- Existing /relationships POST & GET (no changes) ---
@app.route('/relationships', methods=['POST'])
def create_relationship():
    data = request.get_json()
    source_table_id = data.get('source_table_id')
    source_column = data.get('source_column')
    target_table_id = data.get('target_table_id')
    target_column = data.get('target_column')

    if not all([source_table_id, source_column, target_table_id, target_column]):
        return jsonify({"error": "Missing relationship data"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO relationships (source_table_id, source_column, target_table_id, target_column)
            VALUES (?, ?, ?, ?)
        """, (source_table_id, source_column, target_table_id, target_column))
        conn.commit()
        return jsonify({"message": "Relationship created successfully!", "id": cursor.lastrowid}), 201
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"error": "Relationship already exists."}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": f"Failed to create relationship: {str(e)}"}), 500
    finally:
        conn.close()

@app.route('/relationships', methods=['GET'])
def get_relationships():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT
            r.id,
            st.original_filename AS source_table_name, -- Use original filename for display
            r.source_column,
            tt.original_filename AS target_table_name, -- Use original filename for display
            r.target_column
        FROM relationships r
        JOIN tables st ON r.source_table_id = st.id
        JOIN tables tt ON r.target_table_id = tt.id
        ORDER BY r.id DESC;
    """)
    relationships_data = cursor.fetchall()
    conn.close()

    result = []
    for row in relationships_data:
        result.append({
            "id": row['id'],
            "source_table_name": row['source_table_name'],
            "source_column": row['source_column'],
            "target_table_name": row['target_table_name'],
            "target_column": row['target_column']
        })
    return jsonify({"relationships": result}), 200

# --- Existing Threshold Routes (MODIFIED) ---
@app.route('/thresholds', methods=['POST'])
def create_threshold():
    data = request.get_json()
    table_id = data.get('table_id')
    column_name = data.get('column_name')
    function = data.get('function') # NEW
    operator = data.get('operator')
    value = data.get('value')

    if not all([table_id, column_name, function, operator, value is not None]): # NEW: include function
        return jsonify({"error": "Missing threshold data"}), 400

    try:
        value = float(value) # Ensure value is numeric
    except ValueError:
        return jsonify({"error": "Threshold value must be a number."}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO thresholds (table_id, column_name, function, operator, value)
            VALUES (?, ?, ?, ?, ?)
        """, (table_id, column_name, function, operator, value)) # NEW: include function
        conn.commit()
        return jsonify({"message": "Threshold created successfully!", "id": cursor.lastrowid}), 201
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"error": "A threshold already exists for this column with this function in this table."}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": f"Failed to create threshold: {str(e)}"}), 500
    finally:
        conn.close()

@app.route('/thresholds', methods=['GET'])
def get_thresholds():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            t.id,
            tab.original_filename AS table_name, -- Use original filename for display
            t.column_name,
            t.function, -- NEW
            t.operator,
            t.value
        FROM thresholds t
        JOIN tables tab ON t.table_id = tab.id
        ORDER BY tab.name, t.column_name;
    """)
    thresholds_data = cursor.fetchall()
    conn.close()

    result = []
    for row in thresholds_data:
        result.append({
            "id": row['id'],
            "table_name": row['table_name'],
            "column_name": row['column_name'],
            "function": row['function'], # NEW
            "operator": row['operator'],
            "value": row['value']
        })
    return jsonify({"thresholds": result}), 200

# --- Existing Recommendations Routes (MODIFIED) ---
@app.route('/recommendations', methods=['GET'])
def get_past_recommendations():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            r.id,
            r.timestamp,
            r.table_name,
            r.column_name,
            r.function_name,
            r.current_value,
            r.threshold_value,
            thr.operator AS threshold_operator,
            r.recommendation_text -- Ensure this column is selected
        FROM recommendations r
        LEFT JOIN thresholds thr ON r.threshold_id = thr.id
        ORDER BY r.timestamp DESC;
    """)
    recs_data = cursor.fetchall()
    conn.close()

    result = []
    for row in recs_data:
        # --- REVERTED AND SLIGHTLY MODIFIED LINE HERE ---
        # Access directly by column name. If the column is NULL in DB,
        # it will be None in Python, which is fine.
        # The previous IndexError implied the key 'recommendation_text' was sometimes truly missing from the row object
        # which is problematic if the query explicitly selects it.
        # Let's assume the query is correct and the issue was historical data or a very specific timing.
        recommendation_text_val = row['recommendation_text'] if 'recommendation_text' in row.keys() else 'N/A: Recommendation not available or failed to generate.'

        # Alternatively, if you are certain 'recommendation_text' will always be selected
        # (which it should be based on your SQL query), then simply:
        # recommendation_text_val = row['recommendation_text']
        # If the value itself is NULL in the DB, it will be None in Python.
        # You can then display "N/A" if it's None.

        # Let's go with this, it's safer given the persistent issue:
        # Check if the key exists AND if the value isn't None
        if 'recommendation_text' in row.keys() and row['recommendation_text'] is not None:
            recommendation_text_val = row['recommendation_text']
        else:
            recommendation_text_val = 'N/A: Recommendation not available or failed to generate.'
        
        result.append({
            "id": row['id'],
            "timestamp": row['timestamp'],
            "table_name": row['table_name'],
            "column_name": row['column_name'],
            "function_name": row['function_name'],
            "current_value": row['current_value'],
            "threshold_value": row['threshold_value'],
            "threshold_operator": row['threshold_operator'],
            "recommendation_text": recommendation_text_val
        })
    return jsonify({"recommendations": result}), 200


if __name__ == '__main__':
    app.run(debug=True, port=5000)