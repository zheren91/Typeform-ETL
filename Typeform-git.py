#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import psycopg2
import requests
from datetime import datetime
import json


# In[2]:


conn = psycopg2.connect(
    dbname="X",  # Replace with your database name
    user="X",  # Replace with your username
    password="X.",  # Replace with your password
    host="X",  # Replace with your host if not localhost
    port="X"
)

cur = conn.cursor()


# In[3]:


create_schema_sql = """
CREATE SCHEMA IF NOT EXISTS typeform;

DROP TABLE IF EXISTS typeform.events CASCADE;
CREATE TABLE IF NOT EXISTS typeform.events (
    event_id VARCHAR(50) PRIMARY KEY,
    form_id VARCHAR(50),
    submitted_at TIMESTAMP,
    event_type VARCHAR(255),
    payload JSONB
);

DROP TABLE IF EXISTS typeform.forms CASCADE;
CREATE TABLE IF NOT EXISTS typeform.forms (
    form_id VARCHAR(50) PRIMARY KEY,
    title TEXT
);

"""

# Execute the SQL script
cur.execute(create_schema_sql)

# Commit the changes
conn.commit()

print("Table 'events' and 'forms' created successfully in schema 'typeform'.")


# In[4]:


# Constants
TYPEFORM_API_TOKEN = "API token"
BASE_URL = "https://api.typeform.com/forms"

def list_forms():
    url = BASE_URL
    headers = {"Authorization": f"Bearer {TYPEFORM_API_TOKEN}"}
    params = {"page_size": 200}  # Adjust as needed (max 200 per page)
    all_forms = []

    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error listing forms: {response.status_code}, {response.text}")
            break

        data = response.json()
        #print("API Response:", data)  # Debugging: Print the full API response
        items = data.get("items", [])
        all_forms.extend(items)

        # Pagination
        url = data.get("next_page_url")  # URL for the next page of results

    return all_forms

# Fetch all forms
forms = list_forms()

# Display all forms
if forms:
    print(f"Total forms fetched: {len(forms)}")
    for form in forms:
        print(f"Form ID: {form['id']}, Title: {form['title']}")
else:
    print("No forms found.")


# In[5]:


# Insert forms into the database
insert_form_sql = """
INSERT INTO typeform.forms (form_id, title)
VALUES (%s, %s)
ON CONFLICT (form_id) DO NOTHING;
"""

conn.autocommit = True

for form in forms:
    form_id = form['id']
    title = form['title']
    try:
        cur.execute(insert_form_sql, (form_id, title))
    except Exception as e:
        print(f"Error inserting form {form_id}: {e}")
# Commit the changes
conn.commit()
print("Forms inserted successfully into the 'forms' table.")


# In[ ]:





# In[6]:


def fetch_responses(form_id, since=None):
    url = f"https://api.typeform.com/forms/{form_id}/responses"
    headers = {"Authorization": f"Bearer {TYPEFORM_API_TOKEN}"}
    params = {"page_size": 1000}  # Adjust as needed
    if since:
        params["since"] = since.isoformat() + "Z"  # ISO 8601 format

    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error fetching responses for form {form_id}: {response.status_code}, {response.text}")
            break

        data = response.json()
        items = data.get("items", [])
        for item in items:
            event_id = item.get("landing_id")
            submitted_at = item.get("submitted_at")
            event_type = "form_response"  # Always "form_response" for this endpoint
            payload = item  # Store the entire JSON payload

            # Debugging: Print extracted values
            DEBUG = True
            if DEBUG:
                print(f"Inserting - Event ID: {event_id}, Form ID: {form_id}, Submitted At: {submitted_at}")

            # Insert into PostgreSQL
            try:
                cur.execute("""
                    INSERT INTO typeform.events (event_id, form_id, submitted_at, event_type, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                """, (event_id, form_id, submitted_at, event_type, json.dumps(payload)))
                conn.commit()  # Commit after each response
            except Exception as e:
                print(f"Database error: {e}")

        # Pagination
        url = data.get("next_page_url")

# Fetch responses for all forms
since_date = datetime(2023, 1, 1)  # Replace with your desired date
for form in forms:
    form_id = form["id"]
    form_title = form["title"]
    print(f"Fetching responses for form: {form_title} ({form_id})")
    fetch_responses(form_id, since=since_date)


# In[7]:


# Close the connection
cur.close()
conn.close()


# In[ ]:





# In[ ]:




