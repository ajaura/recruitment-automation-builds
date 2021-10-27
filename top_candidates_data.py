"""Candidate Enrichment Solution with People Data Labs for Q4 SE Recruitment""" 

# The solution is powered by PDL Query Builder for tooling on
# Company Data and Person Search APIs for simplified queries and precision 
# targeted results to ensure optimal criteria match and faster process
# For hiring to champion 2021 company growth initiatives.

import requests, json, time, csv, sys
csv.field_size_limit(sys.maxsize)

# Your API key 
API_KEY = "e81a32d92019ad9594192b68db082bb9d3f9b9988f0adb607b5b8bec994ac5e9"

# People Search API for 1000-5000 employees in the internet industry


# Limit the number of records to pull to stay within credit limits

MAX_NUM_RECORDS_LIMIT = 50       # Max number of records to return 
USE_MAX_NUM_RECORDS_LIMIT = True # Toggle T/F (max_records/return all records)

PDL_URL = "https://api.peopledatalabs.com/v5/person/search"

header_fields = {
'Content-Type': "application/json",
'X-api-key': API_KEY
}

for i in range (5):
    with open('company_pdl_search.json') as f:
        data = [json.loads(line) for line in f]
        print (data[i])
        company = data[i]
        ES_QUERY = {
        "query": {
        "bool": {
        "must": [
            {
                "term": {
                    "job_company_name": company}
            },

            {
                "term": {
                    "job_title_role": "sales"
                }
            },

            {
                    "term": {
                        "job_title_sub_role": "software"
                }
                
                }

                    ]
                }
            }
    }
    num_records_to_request = 10

    all_records = []
    batch = 1

    parameter_fields = {   
    "query": json.dumps(ES_QUERY),
    "size": num_records_to_request, 
    "pretty": True
    }
    # Still within for loop for company[i]

    # Send Request to pull data for 10 candidates from each company 
    for j in range (10):
        response = requests.get(
        PDL_URL,
        headers=header_fields,
        params=parameter_fields
        ).json()
        j += 1

# Check response status code:
if response['status'] == 200:
        all_records.extend(response['data'])
        print(f"Retrieved {len(response['data'])} records in batch {batch} "
        f"- {response['total'] - len(all_records)} records remaining")
else:
    print(f"Error retrieving some records:\n\t"
        f"[{response['status']} - {response['error']['type']}] "
        f"{response['error']['message']}")

# Get scroll_token from response
if 'scroll_token' in response:
    parameter_fields['scroll_token'] = response['scroll_token']
# else:
    # continue_scrolling = False
    # all_records.extend(response['data'])
    # print(f"Retrieved {len(response['data'])} records in batch {batch} "
    # f"- {response['total'] - len(all_records)} records remaining")
    

# Batch process data from filtered records criteria
batch += 1
## found_all_records = (len(all_records) == response['total'])
time.sleep(6) # buffer to avoid exceeding rate limit 

end_time = time.time()
runtime = end_time - start_time
    
print(f"Successfully recovered {len(all_records)} profiles in "
     f"{batch} batches [{round(runtime, 2)} seconds]")

# Save Profiles to CSV (UTIL function)
def save_profiles_to_csv(profiles, filename, fields=[], delim=','):

    # Define header fields
    if fields == [] and len(profiles) > 0:
        fields = profiles[0].keys()

    # Write CSV file
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=delim)
        # Write Header:
        writer.writerow(fields)
        # Write Body:
        count = 0
        for profile in profiles:
            writer.writerow([ profile[field] for field in fields ])
        count += 1
        print(f"Wrote {count} lines to: '{filename}'")

# Use UTIL function to Save Profiles to CSV    
csv_header_fields = ['job_company_name', 'full_name', "work_email",
                    'job_title', 'linkedin_url']
csv_filename = "Prospective_SE_Candidates_Q4.csv"
save_profiles_to_csv(all_records, csv_filename, csv_header_fields)