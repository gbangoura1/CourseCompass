from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import json
import re
import snowflake.connector
from datetime import datetime
import os
import csv

app = Flask(__name__)


# Function to safely convert values to float
def safe_float(value, default=float('nan')):
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


# Snowflake connection parameters
SNOWFLAKE_USER = "BISON"
SNOWFLAKE_PASSWORD = "voshim-bopcyt-3virBe"
SNOWFLAKE_ACCOUNT = "SFEDU02-IOB55870"
SNOWFLAKE_WAREHOUSE = "BISON_WH"
SNOWFLAKE_DATABASE = "BISON_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"

# Function to connect to Snowflake
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        return None

# Function to load data from Snowflake
def load_data_from_snowflake():
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM V_WASHU_COURSES")
            column_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=column_names)
            cursor.close()
            conn.close()

            numeric_cols = ['STANDARDIZED_RATING', 'STANDARDIZED_DIFFICULTY', 'STANDARDIZED_WOULD_TAKE_AGAIN']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except Exception as e:
            print(f"Error loading data from Snowflake: {str(e)}")
            return load_data_from_csv()
    else:
        return load_data_from_csv()

# Fallback CSV loader
def load_data_from_csv():
    try:
        df = pd.read_csv("DataBase-2.csv")
        numeric_cols = ['STANDARDIZED_RATING', 'STANDARDIZED_DIFFICULTY', 'STANDARDIZED_WOULD_TAKE_AGAIN']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception as e:
        print(f"Error loading data from CSV: {str(e)}")
        return pd.DataFrame()

df = load_data_from_snowflake()

@app.route('/search', methods=['GET'])
def search():
    try:
        # Get search parameters
        course_code = request.args.get('course_code', '')
        course_name = request.args.get('course_name', '')
        professor_name = request.args.get('professor_name', '')
        min_rating = request.args.get('min_rating', '')
        max_difficulty = request.args.get('max_difficulty', '')
        tags = request.args.get('tags', '')
        
        # Filter data based on search parameters
        filtered_df = df.copy()
        
        # Apply filters to the dataframe
        if course_code:
            filtered_df = filtered_df[filtered_df['COURSE_CODE'].str.contains(course_code, case=False, na=False)]
        
        if course_name:
            filtered_df = filtered_df[filtered_df['COURSE_NAME'].str.contains(course_name, case=False, na=False)]
        
        if professor_name:
            filtered_df = filtered_df[filtered_df['PROFESSOR_NAME'].str.contains(professor_name, case=False, na=False)]
        
        if min_rating:
            try:
                min_rating_float = safe_float(min_rating)
                # Only filter by min_rating if the value is valid
                if not np.isnan(min_rating_float):
                    filtered_df = filtered_df[filtered_df['STANDARDIZED_RATING'] >= min_rating_float]
            except (ValueError, TypeError):
                pass  # Skip filtering if min_rating can't be converted
        
        if max_difficulty:
            try:
                max_difficulty_float = safe_float(max_difficulty)
                # Only filter by max_difficulty if the value is valid
                if not np.isnan(max_difficulty_float):
                    filtered_df = filtered_df[filtered_df['STANDARDIZED_DIFFICULTY'] <= max_difficulty_float]
            except (ValueError, TypeError):
                pass  # Skip filtering if max_difficulty can't be converted
        
        if tags:
            tag_list = tags.split(',')
            for tag in tag_list:
                filtered_df = filtered_df[filtered_df['TAGS'].str.contains(tag.strip(), case=False, na=False)]
        
        # Convert DataFrame to list of dictionaries
        results = filtered_df.fillna('').to_dict(orient='records')
        
        # Return results
        return jsonify({
            "status": "success",
            "results": results
        })
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        })


@app.route('/submit_review', methods=['POST'])
def submit_review():
    try:
        review_data = request.json

        if not review_data:
            return jsonify({"status": "error", "error": "No review data provided."})

        required_fields = ['course_code', 'professor_name']
        for field in required_fields:
            if field not in review_data or not review_data[field]:
                return jsonify({"status": "error", "error": f"{field} is required."})

        try:
            if 'rating' in review_data:
                try:
                    review_data['rating'] = safe_float(review_data['rating'])
                except ValueError:
                    review_data['rating'] = None  # or set to 0, or skip it entirely

            if 'difficulty' in review_data:
                review_data['difficulty'] = safe_float(review_data['difficulty'])
            if 'would_take_again' in review_data:
                review_data['would_take_again'] = safe_float(review_data['would_take_again'])
        except Exception as e:
            return jsonify({"status": "error", "error": f"Invalid numeric value: {str(e)}"})

        review_data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        reviews_file = "reviews.csv"
        headers = ['course_code', 'course_name', 'professor_name', 'semester', 'section',
                   'rating', 'difficulty', 'would_take_again', 'tags', 'comments', 'timestamp']

        file_exists = os.path.exists(reviews_file)

        with open(reviews_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            if not file_exists:
                writer.writeheader()

            sanitized_data = {
                key: str(review_data.get(key, '')).replace(',', ';') for key in headers
            }

            writer.writerow(sanitized_data)

        return jsonify({"status": "success", "message": "Review submitted successfully."})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})

if __name__ == '__main__':
    app.run(debug=True)
