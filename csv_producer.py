from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import json
import csv
import os
import time

# If a service account key (JSON file) is in the current directory, use it.
# Otherwise (for example in Cloud Shell) the default credentials of the logged-in account are used.
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "dev-methods-and-tools"
topic_name = "smartmeter_raw_ahmad"     # the input topic of the Dataflow job
csv_file = "Labels.csv"

NUMERIC_FIELDS = ['time', 'temperature', 'humidity', 'pressure']

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing records from {csv_file} to {topic_path}.\n")

count = 0
with open(csv_file, newline='') as f:
    reader = csv.DictReader(f)   # each row becomes a dictionary, keyed by the header row

    for row in reader:
        # Missing values (empty strings) become None, so they are sent as JSON null.
        # The measurements are converted from strings to numbers.
        record = {}
        for k, v in row.items():
            if v == '':
                record[k] = None
            elif k in NUMERIC_FIELDS:
                record[k] = float(v)
            else:
                record[k] = v

        # Serialize the dictionary to JSON and encode it to bytes
        message = json.dumps(record).encode('utf-8')
        future = publisher.publish(topic_path, message)

        # ensure that the publishing has been completed successfully
        future.result()
        count += 1
        print(f"[{count}] Produced: {record}")

        time.sleep(0.2)   # small delay so the stream can be followed

print(f"\nAll {count} records have been published.")
