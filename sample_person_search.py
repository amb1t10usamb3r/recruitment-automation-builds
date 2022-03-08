
import requests, json, time, csv

API_KEY = "e81a32d92019ad9594192b68db082bb9d3f9b9988f0adb607b5b8bec994ac5e9"

# Limit the number of records to pull (to prevent accidentally using up 
# more credits than expected when testing out this code).
MAX_NUM_RECORDS_LIMIT = 150 # The maximum number of records to retrieve
USE_MAX_NUM_RECORDS_LIMIT = True # Set to False to pull all available records

PDL_URL = "https://api.peopledatalabs.com/v5/person/search"

H = {
  'Content-Type': "application/json",
  'X-api-key': API_KEY
}

ES_QUERY = {
  "query": {
    "bool": {
        "must": [
            {"term": {"job_company_id": "amazon"}}
      ]
    }
  }
}

P = {
  'query': json.dumps(ES_QUERY),
  'size': 100, 
  'pretty': True
}

# Pull all results in multiple batches
batch = 1
all_records = []
start_time = time.time()
found_all_records = False
continue_scrolling = True

while continue_scrolling and not found_all_records: 

  # Check if we reached the maximum number of records we wanted to pull
  if USE_MAX_NUM_RECORDS_LIMIT:
    num_records_to_request = MAX_NUM_RECORDS_LIMIT - len(all_records)
    P['size'] = max(0, min(100, num_records_to_request))
    if num_records_to_request == 0:
      print(f"Stopping - reached maximum number of records to pull "
            f"[MAX_NUM_RECORDS_LIMIT = {MAX_NUM_RECORDS_LIMIT}]")
      break

  # Send Response
  response = requests.get(
    PDL_URL,
    headers=H,
    params=P
  ).json()

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
    P['scroll_token'] = response['scroll_token']
  else:
    continue_scrolling = False
    print(f"Unable to continue scrolling")

  batch += 1
  found_all_records = (len(all_records) == response['total'])
  time.sleep(6) # avoid hitting rate limit thresholds
 
end_time = time.time()
runtime = end_time - start_time
        
print(f"Successfully recovered {len(all_records)} profiles in "
      f"{batch} batches [{round(runtime, 2)} seconds]")

# Save profiles to csv (utility function)
def save_profiles_to_csv(profiles, filename, fields=[], delim=','):
  # Define header fields
  if fields == [] and len(profiles) > 0:
      fields = profiles[0].keys()
  # Write csv file
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

# Use utility function to save profiles to csv    
csv_header_fields = ['work_email', 'full_name', "linkedin_url",
                     'job_title', 'job_company_name']
csv_filename = "all_employee_profiles.csv"
save_profiles_to_csv(all_records, csv_filename, csv_header_fields)