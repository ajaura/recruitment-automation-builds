"""Candidate Enrichment Solution with People Data Labs for Q4 SE Recruitment""" 

# This solution is powered by PDL Query Builder for tooling on
# Company Data and Person Search APIs for simplified queries and precision 
# targeted results to ensure optimal criteria match and faster process
# For hiring to champion 2021 company growth initiatives.

import requests, json, time, csv, sys
csv.field_size_limit(sys.maxsize)

# Your API key 
API_KEY = "e81a32d92019ad9594192b68db082bb9d3f9b9988f0adb607b5b8bec994ac5e9"

# Company Search API for 1000-5000 employees in the internet industry

# Limit the number of records to pull (to prevent accidentally using up 
# more credits than expected when testing out this code).
MAX_NUM_RECORDS_LIMIT = 10 # The maximum number of records to retrieve
USE_MAX_NUM_RECORDS_LIMIT = True # Set to False to pull all available records

# Pass Endpoint for accessing PDL Company Search API

PDL_URL = "https://api.peopledatalabs.com/v5/company/search"

# Pass content-type & access key for header fields on API request
H = {
  'Content-Type': "application/json",
  'X-api-key': API_KEY
}

# Query based on industry and size 

ES_QUERY = {
  "query": {
    "bool": {
      "must": [
        {"term": {"industry": "internet"}},
        {"term": {"size": "1001-5000"}}
                ]
    }
        }    
    }
  


P = {
  'query': json.dumps(ES_QUERY), #python obj --> json 
  'size': 5, #max num of returned records
}

# Pull all results in multiple batches
#batch = 1
#all_records = []
#start_time = time.time()
#found_all_records = False
#continue_scrolling = True

#while continue_scrolling and not found_all_records: 

  # Check if we reached the maximum number of records we wanted to pull
  #if USE_MAX_NUM_RECORDS_LIMIT:
   # num_records_to_request = MAX_NUM_RECORDS_LIMIT - len(all_records)
    #P['size'] = max(0, min(100, num_records_to_request))
    #if num_records_to_request == 0:
    #  print(f"Stopping - reached maximum number of records to pull "
     #       f"[MAX_NUM_RECORDS_LIMIT = {MAX_NUM_RECORDS_LIMIT}]")
      #break

  # Send Response

response = requests.get(
  PDL_URL,
  headers=H,
  params=P
).json()


if response["status"] == 200:
  data = response['data']
  with open("company_pdl_search.json", "w") as out:
    for record in data:
      out.write(json.dumps(record) + "\n")
  print(f"Successfully selected {len(data)} company records from PDL")
  
  print(f"{response['total']} Total PDL company records exist matching this query. {len(data)} have been written to company_pdl_search.json with details on name, size, and other info to be used for recruitment process.")
else:
  print("Please check your response error code and try your request again.")
  print("Error:", response)


  

# Save Profiles to CSV using UTIL function

#def save_profiles_to_csv(profiles, filename, fields=[], delim=','):
 # # Define header fields
  ##if fields == [] and len(profiles) > 0:
    #  fields = profiles[0].keys()
  # Write csv file
  #with open(filename, 'w') as csvfile:
    #writer = csv.writer(csvfile, delimiter=delim)
    # Write Header:
    #writer.writerow(fields)
    # Write Body:
    #count = 0
    #for profile in profiles:
     # writer.writerow([ profile[field] for field in fields ])
     # count += 1
  #print(f"Wrote {count} lines to: '{filename}'")



# UTIL for CSV header company name field and filename

#csv_header_fields = ['company_name']
#csv_filename = "all_company_profiles.csv"
#save_profiles_to_csv(all_records, csv_filename, csv_header_fields)
