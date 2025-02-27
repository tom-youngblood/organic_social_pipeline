import requests
import pandas as pd

class HS:
    def hs_prepare_request(url, api_key):
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

    def parse_hubspot_contacts(response):
        """
        Extracts vid and all properties from a list of HubSpot contacts (v1 API).
        """
        contacts_list = []

        for contact in response:  # v1 API returns a list, not a dictionary
            parsed_data = {"vid": contact.get("vid")}  # Extract vid

            # Extract properties dynamically
            if "properties" in contact:
                for key, value in contact["properties"].items():
                    parsed_data[key] = value.get("value")  # Extract property values

            contacts_list.append(parsed_data)  # Add to list

        return pd.DataFrame(contacts_list)
