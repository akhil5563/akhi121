import os
import re
import time
from mysql.connector import connect, Error
from concurrent.futures import ThreadPoolExecutor
from linkedin_api import Linkedin
import google.generativeai as genai
from functools import lru_cache
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch sensitive data from .env file
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
LINKEDIN_USERNAME = os.getenv("LINKEDIN_USERNAME")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
GENAI_API_KEY = os.getenv("GENAI_API_KEY")
DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")

# Connect to database
def connect_to_database(host, user, password, database):
    try:
        connection = connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Successfully connected to the database")
            return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

# Batch insert jobs into the database
def batch_insert_jobs(connection, job_data_batch):
    insert_query = f"""
        INSERT INTO {DB_TABLE_NAME} (Id, Url, Role, company, description, Experience, JobType)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, job_data_batch)
            connection.commit()
    except Error as e:
        print(f"Error executing batch insert: {e}")

# Filter experience using regex
def filter_numbers(text):
    pattern = re.findall(r'\b\d+\+?|\b\d+\s*-\s*\d+\b', text)
    if not pattern:
        return 0
    match = pattern[0]
    return int(match.split('-')[0].strip() if '-' in match else match.replace('+', '').strip())

# Initialize generative AI model
def configure_genai(api_key):
    genai.configure(api_key=api_key)
    return genai.GenerativeModel('gemini-1.5-pro')

# Use generative AI to estimate the required experience from the job description
def estimate_experience(model, description):
    prompt = f"""
    You are an intelligent job experience provider. Provide the minimum experience required for the job described below in years (only the number):
    Job Description: {description}
    """
    response = model.generate_content(prompt)
    try:
        return int(response.text.strip())
    except ValueError:
        return filter_numbers(response.text)

# Fetch job details using LinkedIn API (with caching)
@lru_cache(maxsize=1000)
def get_job_details(api, job_id):
    return api.get_job(job_id)

# Fetch job data in parallel using multithreading
def fetch_job_data(api, job):
    tracking_urn = job['trackingUrn'].split(':')[-1]
    return get_job_details(api, tracking_urn)

def parallel_job_search(api, jobs):
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda job: fetch_job_data(api, job), jobs))
    return results

# Perform the job search, fetch details, and save them in batches
def job_search(api, jobs, model, connection, job_type):
    job_data_batch = []
    
    # Fetch job details in parallel
    job_details = parallel_job_search(api, jobs)
    
    for job in job_details:
        try:
            company_name = job["companyDetails"]['com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany']['companyResolutionResult']['name']
            url = job["applyMethod"]["com.linkedin.voyager.jobs.OffsiteApply"]["companyApplyUrl"]
            description = job["description"]["text"]
            title = job['title']

            experience = estimate_experience(model, description)
            
            # Only consider jobs with <= 1 year experience required
            if experience <= 1:
                data_to_insert = (int(job['trackingUrn'].split(':')[-1]), url, title, company_name, description, experience, job_type)
                job_data_batch.append(data_to_insert)

        except Exception as e:
            print(f"Error processing job: {e}")
    
    # Batch insert jobs into the database
    if job_data_batch:
        batch_insert_jobs(connection, job_data_batch)

# Main function to handle database connection, job search, and API calls
def main():
    # Connect to the database
    connection = connect_to_database(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
    if connection is None:
        print("Database connection failed. Exiting.")
        return

    # Configure LinkedIn API and Generative AI
    api = Linkedin(LINKEDIN_USERNAME, LINKEDIN_PASSWORD)
    model = configure_genai(api_key=GENAI_API_KEY)

    # Define job search parameters
    location = "United States"
    keywords = ["Software Engineer","Backend", "Cloud Engineer"]

    # Search jobs and process for each keyword
    for keyword in keywords:
        print(f"Searching jobs for: {keyword}")
        jobs = api.search_jobs(keywords=keyword, location=location, job_type="F", listed_at=86400)
        job_search(api, jobs, model, connection, job_type=1)

    # Close the database connection
    connection.close()
    print("Job search completed and data inserted.")

# Execute the main function
if __name__ == "__main__":
    main()
