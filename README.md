# LinkedIn to HubSpot Lead Generation Pipeline

## 🚀 Overview
This project is a **fully automated data pipeline** that scrapes LinkedIn post engagers, formats the data into a relational format, stores it in **BigQuery**, and pushes qualified leads into **HubSpot** for outreach. The pipeline has been modularized into separate Python scripts for better maintainability and scalability.

## 📊 Pipeline Architecture
- **Phantombuster API** → Scrapes LinkedIn post engagers.
- **Python Script: pb_bq.py** → Pulls and preprocesses PhantomBuster data into realtional DB, sends to BigQuery.
- **Google BigQuery** → Stores and processes raw lead data
- **Python Script: bq_hs.py** →Pulls BigQuery leads, formats them, and pushes the newest leads to Hubspo.
- **HubSpot API** → Pushes formatted lead data for sales outreach.
- **Sales Team** → Engages with leads and updates stages manually.

## 🛠️ Technologies Used
- **Python**
- **Phantombuster API**
- **Google BigQuery**
- **HubSpot API**
- **Numpy**
- **Pandas**
- **Requests**
- **Gspread**

## 📂 Project Structure
```
📁 linkedin-hubspot-pipeline/
│── 📁 scripts/               # Modular Python scripts
│    ├── pb_bq.py             # Formats scraped data and sends it to BigQuery
│    ├── bq_hs.py             # Formats BigQuery data and sends it to HubSpot
│
│── 📁 config/              # co Configs and API keys (excluded from Git)
│    ├── apollo_key.txt
│    ├── hs_key.txt
│    ├── pb_link.txt
│    ├── phantombuster_key
│    ├── sheets_key.json
│    ├── skilled-tangent.json
│
│── 📁 data/                   # Sample data (if applicable)
│
│── 📁 docs/                   # Documentation
│    ├── hs_bq_example_output.txt
│    ├── pb_bq_example_output.txt
│
│─.gitignore                 # Ignore sensitive files
│─requirements.txt           # Python dependencies
```

## ⚡ Quickstart
### **1️⃣ Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2️⃣ Set Up API Keys**
Ensure that all required API keys are present in the `config/` directory.

### **3️⃣ Run the Full Pipeline**
```bash
python scripts/pb_bq.py
python scripts/bq_hs.py
```
