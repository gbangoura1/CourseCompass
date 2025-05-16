import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
import plotly.graph_objects as go
from datetime import datetime

# API endpoint


# Function to safely convert values to float
def safe_float(value, default=float('nan')):
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

API_ENDPOINT = "http://127.0.0.1:5000"

st.set_page_config(
    page_title="Course Recommendation System",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown('''
<style>
    .main-header {
        font-size: 2.5rem;
        color: #A51417;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #A51417;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .card {
        border-radius: 5px;
        background-color: #f9f9f9;
        padding: 1.5rem;
        margin-bottom: 1rem;
        border-left: 5px solid #A51417;
    }
    .sentiment-tag {
        display: inline-block;
        background-color: #E5E7EB;
        color: #1F2937;
        padding: 0.25rem 0.5rem;
        border-radius: 9999px;
        margin-right: 0.5rem;
        margin-bottom: 0.5rem;
        font-size: 0.875rem;
    }
    .rating-container {
        display: flex;
        align-items: center;
        margin-bottom: 0.5rem;
    }
    .rating-label {
        min-width: 150px;
        font-weight: bold;
    }
    .rating-value {
        margin-left: 1rem;
    }
</style>
''', unsafe_allow_html=True)

st.markdown('<h1 class="main-header">Washington University in St. Louis Course Recommendation System</h1>', unsafe_allow_html=True)

# Sidebar
st.sidebar.image("https://marcomm.washu.edu/app/uploads/2024/02/WashU-SHIELD-Red_RGB.jpg", width=150)
st.sidebar.title("Filters & Preferences")

major = st.sidebar.text_input("Your Major", "Computer Science")
year = st.sidebar.selectbox("Year", [1, 2, 3, 4])

st.sidebar.markdown("### What matters to you?")
st.sidebar.markdown("Select preferences that are important to you:")

importance_rating = st.sidebar.slider("Overall Rating Importance", 0, 10, 8)
importance_difficulty = st.sidebar.slider("Low Difficulty Importance", 0, 10, 5)
importance_would_take_again = st.sidebar.slider("Would Take Again Importance", 0, 10, 7)

sentiment_options = [
    "accessible outside class", "amazing lectures", "caring",
    "clear grading criteria", "extra credit", "get ready to read",
    "gives good feedback", "graded by few things", "group projects",
    "helpful", "inspirational", "lecture heavy", "lots of homework",
    "participation matters", "respected", "skip class? you won't pass",
    "so many papers", "test heavy", "tough grader"
]

tab1, tab2 = st.tabs(["Find Courses", "Submit Review"])

with tab1:
    st.markdown('<h2 class="sub-header">Find Your Ideal Courses</h2>', unsafe_allow_html=True)

    search_col1, search_col2 = st.columns(2)

    with search_col1:
        search_by = st.radio("Search by:", ["Course", "Professor", "Rating", "Advanced Search"])

    with search_col2:
        if search_by == "Course":
            search_term = st.text_input("Enter Course Code or Name:")
        elif search_by == "Professor":
            search_term = st.text_input("Enter Professor Name:")
        elif search_by == "Rating":
            min_rating = st.slider("Minimum Rating", 1.0, 7.0, 4.0, 0.1)
            search_term = str(min_rating)
        else:
            st.write("Use the filters below for advanced search")
            search_term = ""

    if search_by == "Advanced Search":
        adv_col1, adv_col2, adv_col3 = st.columns(3)

        with adv_col1:
            course_code = st.text_input("Course Code:")
            course_name = st.text_input("Course Name:")

        with adv_col2:
            professor_name = st.text_input("Professor Name:")
            min_rating = st.slider("Minimum Rating", 1.0, 7.0, 3.0, 0.1)

        with adv_col3:
            max_difficulty = st.slider("Maximum Difficulty", 1.0, 7.0, 5.0, 0.1)
            selected_tags = st.multiselect("Course Tags:", sentiment_options)

    if st.button("Search Courses"):
        try:
            if search_by == "Course":
                if search_term.isdigit() or (len(search_term) <= 5 and any(c.isdigit() for c in search_term)):
                    response = requests.get(f"{API_ENDPOINT}/search", params={"course_code": search_term})
                else:
                    response = requests.get(f"{API_ENDPOINT}/search", params={"course_name": search_term})

            elif search_by == "Professor":
                response = requests.get(f"{API_ENDPOINT}/search", params={"professor_name": search_term})

            elif search_by == "Rating":
                response = requests.get(f"{API_ENDPOINT}/search", params={"min_rating": search_term})

            else:
                params = {}
                if course_code:
                    params["course_code"] = course_code
                if course_name:
                    params["course_name"] = course_name
                if professor_name:
                    params["professor_name"] = professor_name
                if min_rating:
                    params["min_rating"] = str(min_rating)
                if max_difficulty:
                    params["max_difficulty"] = str(max_difficulty)
                if selected_tags:
                    params["tags"] = ",".join(selected_tags)

                response = requests.get(f"{API_ENDPOINT}/search", params=params)

            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "success":
                    results = result.get("results", [])
                    if not results:
                        st.warning("No courses found matching your criteria.")
                    else:
                        st.success(f"Found {len(results)} courses matching your criteria.")
                        for course in results:
                            # Clean and safely convert values with defaults
                            rating_raw = course.get("STANDARDIZED_RATING", '0')
                            difficulty_raw = course.get("STANDARDIZED_DIFFICULTY", '3.5')
                            would_take_again_raw = course.get("STANDARDIZED_WOULD_TAKE_AGAIN", '0.5')

                            # Replace empty strings or None with default values
                            rating = safe_float(rating_raw) if rating_raw not in [None, ''] else 0.0
                            difficulty = safe_float(difficulty_raw) if difficulty_raw not in [None, ''] else 3.5
                            would_take_again = safe_float(would_take_again_raw) if would_take_again_raw not in [None, ''] else 0.5

                            # Normalize scores
                            rating_score = rating * importance_rating / 10
                            difficulty_score = (7 - difficulty) * importance_difficulty / 10
                            would_take_again_score = would_take_again * 7 * importance_would_take_again / 10

                            course["RECOMMENDATION_SCORE"] = rating_score + difficulty_score + would_take_again_score


                        results.sort(key=lambda x: x.get("RECOMMENDATION_SCORE", 0), reverse=True)

                        for i, course in enumerate(results):
                            with st.container():
                                st.markdown(f'''
                                    <div class="card">
                                        <h3>{course.get('COURSE_CODE', 'N/A')} - {course.get('COURSE_NAME', 'N/A')}</h3>
                                        <p><strong>Professor:</strong> {course.get('PROFESSOR_NAME', 'N/A')}</p>
                                        <p><strong>Rating:</strong> {safe_float(course.get('STANDARDIZED_RATING', ''), 0):.1f}/7</p>
                                        <p><strong>Difficulty:</strong> {safe_float(course.get('STANDARDIZED_DIFFICULTY', ''), 0):.1f}/7</p>
                                        <p><strong>Would Take Again:</strong> {safe_float(course.get('STANDARDIZED_WOULD_TAKE_AGAIN', ''), 0.0) * 100:.0f}%</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                else:
                    st.error("Failed to fetch data from server.")
            else:
                st.error("Server error. Please check backend.")
        except Exception as e:
            st.error(f"Error during search: {e}")