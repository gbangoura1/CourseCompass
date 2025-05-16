import requests
from bs4 import BeautifulSoup
import csv
import re

def scrape_courses(url):
    """
    Scrape course information from the given URL
    
    Args:
        url (str): URL of the course listing page
    
    Returns:
        list: List of dictionaries containing course information
    """
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        print(f"Failed to retrieve page: {response.status_code}")
        return []
    
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all course articles
    course_articles = soup.find_all('article', class_='course')
    
    courses = []
    
    for article in course_articles:
        # Extract course code
        coursenum_span = article.find('span', class_='coursenum')
        if coursenum_span:
            # Extract the last set of numbers from the course code
            course_code_text = coursenum_span.text.strip()
            # Use regex to find all number sequences and take the last one
            code_matches = re.findall(r'\d+', course_code_text)
            if code_matches:
                course_code = code_matches[-1]  # Get the last match
            else:
                course_code = "N/A"
        else:
            course_code = "N/A"
        
        # Extract course name
        course_name = "N/A"
        h3_tag = article.find('h3')
        if h3_tag:
            a_tag = h3_tag.find('a')
            if a_tag:
                course_name = a_tag.text.strip()
        
        instructors = "N/A"
        time_tag = article.find('time', class_='time')
        if time_tag:
            instructor_text = time_tag.text.strip()
            # Check if there's an "Instructors:" label
            if "Instructors:" in instructor_text:
                instructors = instructor_text.split("Instructors:")[1].strip()
        
        # Alternative method for instructor extraction
        if instructors == "N/A":
            details_div = article.find('div', class_='details')
            if details_div:
                instructor_info = details_div.find(text=re.compile('INSTRUCTORS:', re.IGNORECASE))
                if instructor_info:
                    instructor_parent = instructor_info.parent
                    if instructor_parent:
                        instructors = instructor_parent.text.replace('INSTRUCTORS:', '').strip()
        
        # Split instructors if there are commas and create separate entries
        if instructors != "N/A" and "," in instructors:
            instructor_list = [instr.strip() for instr in instructors.split(",")]
            for instructor in instructor_list:
                courses.append({
                    'course_code': course_code,
                    'course_name': course_name,
                    'instructors': instructor
                })
        else:
            courses.append({
                'course_code': course_code,
                'course_name': course_name,
                'instructors': instructors
            })
    
    return courses

def save_to_csv(courses, filename):
    """
    Save course information to a CSV file
    
    Args:
        courses (list): List of dictionaries containing course information
        filename (str): Name of the CSV file
    """
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['course_code', 'course_name', 'instructors']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for course in courses:
            writer.writerow(course)
    
    print(f"Data saved to {filename}")

def main():
    # URL of the course listing page
    url = "https://sds.wustl.edu/course_listing"  # Replace with actual URL
    
    # Scrape course information
    courses = scrape_courses(url)
    
    # Save to CSV
    save_to_csv(courses, "washu_data_science_courses.csv")
    
    print(f"Scraped {len(courses)} courses")

if __name__ == "__main__":
    main()
