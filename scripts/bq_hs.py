from google.cloud import bigquery
import requests
import pandas as pd

def bq_query_table(api_key_path, query):
    client = bigquery.Client.from_service_account_json(api_key_path)
    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

def hs_prepare_request(url, api_key_path):
    # HubSpot Private App Token
    api_key = open(api_key_path, 'r').read().strip()

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    return api_key, headers, url

def hs_fetch_list_contacts(api_key, headers, url, list_id):
    contacts = []
    params = {
        "count": 100
    }

    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        data = response.json()

        # Extract contacts and add to the list
        if "contacts" in data:
            contacts.extend(data["contacts"])

        # Check for pagination (if there are more contacts to fetch)
        if "vid-offset" in data and data.get("has-more", False):
            params["vidOffset"] = data["vid-offset"]  # Update pagination parameter
        else:
            break

    return contacts

def hs_push_contacts_to_list(api_key, new_leads):
    """Pushes all contacts from a DataFrame to HubSpot."""

    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Check if there are leads to push
    if len(new_leads) > 0:

        for _, row in new_leads.iterrows():  # Iterate over DataFrame rows
            hubspot_record = {
                "properties": {
                    "post_id": row.get("postId", ""),
                    "reaction_type": row.get("reactionType", ""),
                    "platform": row.get("platform", ""),
                    "company_id": str(row.get("companyId", "")),  # Convert to string if necessary
                    "post_name": row.get("postName", ""),
                    "firstname": row.get("name").split(" ")[0] if isinstance(row.get("name"), str) else "",
                    "lastname": " ".join(row.get("name").split(" ")[1:]) if isinstance(row.get("name"), str) else "",
                    "jobtitle": row.get("occupation", ""),
                    "linkedin_profile_url_organic_social_pipeline": row.get("profileLink", ""),
                    "hs_linkedin_url": row.get("profileLink", ""),
                    "pb_linkedin_profile_url": row.get("profileLink", ""),
                    "phantombuster_source_user_id": str(row.get("sourceUserId", ""))
                }
            }

            response = requests.post(url, headers=headers, json=hubspot_record)

            if response.status_code == 201:
                print(f"Successfully pushed: {row.get('name')}")
            else:
                print(f"Failed to push: {row.get('name')}, Error: {response.text}")

    else:
        print("No new leads pushed")

def main():
    # Query BigQuery for contact information and Post Names to send to Hubspot
    query = """
            SELECT
                c.*,
                p.postName
            FROM
                `skilled-tangent-448417-n8.pb_dataset.contacts` AS c
            LEFT JOIN
                `skilled-tangent-448417-n8.pb_dataset.posts` AS p
            ON
                c.postId = p.postId;
            """

    # Save BigQuery Leads, dropping duplicates if exist
    bq_leads = bq_query_table("../config/skilled-tangent-448417-n8-3c729616b7f2.json", query).drop_duplicates(subset="profileLink")

    # Prepare HubSpot Contact List request
    # HubSpot API URL for fetching all contacts in a list
    list_id = 246
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all?property=hs_linkedin_url"
    api_key, headers, url = hs_prepare_request(url, "../config/hs_key.txt")

    # Fetch all contacts from the HubSpot list
    all_contacts = hs_fetch_list_contacts(api_key, headers, url, list_id)
    print(f"Total Contacts Retrieved: {len(all_contacts)}")

    # Subset of contacts that are in BQ and not HS (new leads)
    hs_li_links = [all_contacts[i].get("properties").get("hs_linkedin_url").get("value").strip() for i in range(0,len(all_contacts))]
    new_leads = bq_leads.loc[(~bq_leads["profileLink"].isin(hs_li_links))]
    print(f"Number of leads to push: {len(new_leads)}")

    # Push new leads to hubspot
    hs_push_contacts_to_list(api_key, new_leads)

if __name__ == "__main__":
    main()
