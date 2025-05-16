import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import random
import pandas as pd

def load_courses_csv(filename):
    """
    Load course information from CSV file
    
    Args:
        filename (str): Path to the CSV file
    
    Returns:
        pandas.DataFrame: DataFrame containing course information
    """
    try:
        df = pd.read_csv(filename)
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

def scrape_professor_rating(rmp_id):
    """
    Scrape professor rating information from RateMyProfessor
    
    Args:
        rmp_id (str): RateMyProfessor ID of the professor
    
    Returns:
        dict: Dictionary containing rating information
    """
    if not rmp_id or pd.isna(rmp_id):
        return None
    print(rmp_id)
    url = f"https://www.ratemyprofessors.com/professor/{rmp_id}"
    
    # Set headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.ratemyprofessors.com/'
    }
    
    try:
        # Send GET request to the URL
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve page for professor ID {rmp_id}: {response.status_code}")
            return None
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Initialize dictionary for rating information
        rating_info = {
            'rmp_id': rmp_id,
            'overall_rating': None,
            'would_take_again': None,
            'difficulty': None,
            'tags': []
        }
        
        # Extract overall rating (RatingValue__Numerator-qw8sqy-2 duhvlP)
        rating_element = soup.find('div', class_=re.compile(r'RatingValue__Numerator-.*'))
        if rating_element:
            rating_info['overall_rating'] = rating_element.text.strip()
        
        # Extract "would take again" percentage and "level of difficulty"
        feedback_numbers = soup.find_all('div', class_=re.compile(r'FeedbackItem__FeedbackNumber-.*'))
        if len(feedback_numbers) >= 2:
            would_take_again = feedback_numbers[0].text.strip()
            difficulty = feedback_numbers[1].text.strip()
            
            # Convert percentage to numerical value if it includes '%'
            if '%' in would_take_again:
                would_take_again = would_take_again.replace('%', '')
            
            rating_info['would_take_again'] = would_take_again
            rating_info['difficulty'] = difficulty
        
        # Extract tags (Tag-bs9vf4-0 bmtbjB)
        tag_elements = soup.find_all('span', class_=re.compile(r'Tag-.*'))
        if tag_elements:
            tags = set([tag.text.lower().strip() for tag in tag_elements])
            rating_info['tags'] = tags
        print(rating_info)
        return rating_info
    
    except Exception as e:
        print(f"Error scraping professor ID {rmp_id}: {e}")
        return None

def update_csv_with_ratings(input_csv, output_csv):
    """
    Update CSV file with professor rating information
    
    Args:
        input_csv (str): Path to input CSV file
        output_csv (str): Path to output CSV file
    """
    # Load courses CSV
    df = load_courses_csv(input_csv)
    if df is None:
        return
    
    # Add new columns for ratings
    df['overall_rating'] = None
    df['would_take_again'] = None
    df['difficulty'] = None
    df['tags'] = None
    
    # Only get non-NaN RMP IDs - use dropna() to remove NaN values
    unique_ids = df['rmp_id'].dropna().unique()
    
    print(f"Found {len(unique_ids)} unique professors with RMP IDs")
    
    # Track progress
    processed = 0
    
    # Dictionary to store ratings by rmp_id
    ratings_by_id = {}
    
    for rmp_id in unique_ids:
        # Convert to string and remove decimal if present
        rmp_id_clean = str(int(float(rmp_id))).strip()
        
        # Scrape rating information
        rating_info = scrape_professor_rating(rmp_id_clean)
        
        if rating_info:
            # Store with the clean ID format
            ratings_by_id[rmp_id_clean] = rating_info
        
        # Update progress
        processed += 1
        print(f"Processed {processed}/{len(unique_ids)} professors with RMP IDs")
        
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(1, 3))
    
    # Update DataFrame with scraped information
    for idx, row in df.iterrows():
        rmp_id = row['rmp_id']
        if pd.notna(rmp_id):
            # Convert to the same format used in the dictionary
            rmp_id_clean = str(int(float(rmp_id))).strip()
            
            if rmp_id_clean in ratings_by_id:
                rating_info = ratings_by_id[rmp_id_clean]
                df.at[idx, 'overall_rating'] = rating_info['overall_rating']
                df.at[idx, 'would_take_again'] = rating_info['would_take_again']
                df.at[idx, 'difficulty'] = rating_info['difficulty']
                df.at[idx, 'tags'] = ','.join(rating_info['tags']) if rating_info['tags'] else None
    
    # Convert rmp_id to string and remove .0 for output
    df['rmp_id'] = df['rmp_id'].apply(lambda x: str(int(float(x))) if pd.notna(x) else x)
    
    # Save updated DataFrame to CSV
    df.to_csv(output_csv, index=False)
    print(f"Data saved to {output_csv}")

def main():
    # Input and output file paths
    input_csv = "washu_data_science_courses.csv"
    output_csv = "washu_courses_with_ratings.csv"
    
    # Update CSV with ratings
    update_csv_with_ratings(input_csv, output_csv)

if __name__ == "__main__":
    main()