import pandas as pd
import requests
import os
import funcs
import json

def main():
    # Parameters for HubSpot API Pull
    HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
    print(HUBSPOT_API_KEY)
    list_id = 246

    # Define the list of properties
    properties = [
        "firstname",
        "lastname",
        "email",
        "company",
        "createdate",
        "organic_social_stage",
        "organic_social_outreached",
        "linkedin_profile_url_organic_social_pipeline",
        "latest_funding_date",
        "latest_funding_stage",
        "total_funding",
        "post_id",
        "post"
    ]

    # Join properties correctly
    property_str = ",".join(properties)

    # Necessary variables
    list_id = 246
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all?property=hs_linkedin_url"

    # Fetch data
    api_key, headers, url = funcs.HS.hs_prepare_request(url, HUBSPOT_API_KEY)
    contacts = funcs.HS.hs_fetch_list_contacts(api_key, headers, url, list_id)

    # Assemble DataFrame
    df = funcs.HS.parse_hubspot_contacts(contacts)
    print(df)

    # Save temp data
    df.to_csv("../temp_data/temp_data.csv")


if __name__ == "__main__":
    main()
