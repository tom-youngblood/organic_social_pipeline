import requests
import pandas as pd
import numpy as np
from io import BytesIO
from google.cloud import bigquery
import gspread
from gspread_dataframe import get_as_dataframe

def pb_fetch(dl_link):
    return pd.read_csv(BytesIO(requests.get(dl_link).content))

def process_gspred(gspread_credentials, raw):
    # Get LinkedIn postIDs and names
    credentials_file = '../config/sheets_key.json'
    gc = gspread.service_account(filename=credentials_file)
    links = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])

    # Merge raw and links to get postId and postName variable
    li_raw = raw.merge(links, left_on='postUrl', right_on='link', how='left').rename(columns={'post':'postName', 'id':'postId'}).drop(columns=['link'])

    # Add platform column
    li_raw["platform"] = "LinkedIn"

    return li_raw

def process_li_companies(li_raw):
    # Create a companyId for each company
    li_raw["companyId"] = li_raw["companyName"].str.strip().str.replace(r"[ ,.]", "", regex=True).str.lower()

    # Companies table
    li_companies = li_raw.loc[:,["companyId", "companyName", "companyUrl", "followersCount"]].dropna().drop_duplicates(subset=["companyUrl"])

    return li_raw, li_companies

def process_li_contacts(li_raw, li_companies):
    # Construct LinkedIn Contacts table
    li_contacts = li_raw.loc[:,["sourceUserId", "name", "occupation", "profileLink", "degree", "companyUrl", "postId", "reactionType", "platform"]]

    # Add comapny ID to LI Contacts Table
    li_contacts["companyId"] = li_companies["companyId"]

    # Drop duplicates
    li_contacts = li_contacts.drop_duplicates(subset=["profileLink"])

    # Drop companies
    li_contacts = li_contacts[li_contacts["companyId"].isna()]

    return li_contacts

def process_li_posts(li_raw):
    # Construct LinkedIn Post table and drop duplicates
    li_posts = li_raw.loc[:, ["postUrl", "platform", "postId", "postName"]].drop_duplicates(subset=["postUrl"])

    return li_posts

def bq_query_table(api_key_path, query):
    # Connect to client
    client = bigquery.Client.from_service_account_json(api_key_path)

    # Make Query
    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()

def subset_data(li_companies, li_contacts, li_posts):
    # Query bigquery for current tables
    bq_tables = {}
    for t in ["contacts", "companies", "posts"]:
        bq_tables[t] = bq_query_table("../config/skilled-tangent-448417-n8-3c729616b7f2.json", f"SELECT * FROM `skilled-tangent-448417-n8.pb_dataset.{t}`;")

    # Stack dataframes across platforms
    contacts = li_contacts
    companies = li_companies
    posts = li_posts

    # Subset the new leads to upload to BigQuery
    new_contacts = contacts.loc[~contacts["profileLink"].isin(bq_tables["contacts"]["profileLink"])]
    new_companies = companies.loc[~companies["companyId"].isin(bq_tables["companies"]["companyId"])]
    new_posts = posts.loc[~posts["postUrl"].isin(bq_tables["posts"]["postUrl"])]

    return new_contacts, new_companies, new_posts

def bq_push_tables(api_key_path, dataset_id, **kwargs):
    # Initialize BigQuery client
    client = bigquery.Client.from_service_account_json(api_key_path)

    # Define dataset and table
    dataset_id = dataset_id

    # Upload each table
    for table_name, df in kwargs.items():
        table_id = f"{dataset_id}.{table_name}"

        # Load the DataFrame into BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        # Wait for the load job to complete
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        # Confirm the load
        print(f"Loaded {job.output_rows} rows into {table_name}.")

    return None

def pb_bq_trigger(request):
    main()
    return "pb_bq.py execution triggered.", 200

def main():
    # Fetch leads in Phantombuster database
    raw = pb_fetch(open("../config/pb_link.txt", "r").read().strip())

    # Pull current post information from Google Sheets
    li_raw = process_gspred(open("../config/sheets_key.json", "r").read().strip(), raw)

    # Process LinkedIn companies; create LinkedIn companies table
    li_raw, li_companies = process_li_companies(li_raw)

    # Process LinkedIn contacts; create LinkedIn contacts (fact) table
    li_contacts = process_li_contacts(li_raw, li_companies)

    # Process LinkedIn posts; create LinkedIn posts table
    li_posts = process_li_posts(li_raw)

    # Subset data; Get tables with only new leads (leads that are not already in BigQuery)
    new_contacts, new_companies, new_posts = subset_data(li_companies, li_contacts, li_posts)

    # Print new contacts to push
    print("-NEW ROWS TO PUSH-")
    print(new_contacts)
    print(new_companies)
    print(new_posts)
    print("\n")

    # Push data to BigQuery Tables: companies, contacts, posts
    bq_push_tables(
        "../config/skilled-tangent-448417-n8-3c729616b7f2.json",
        "skilled-tangent-448417-n8.pb_dataset",
        contacts=new_contacts,
        companies=new_companies,
        posts=new_posts)

main()
