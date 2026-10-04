from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import json
import os
import threading

# If a service account key (JSON file) is in the current directory, use it.
# Otherwise (for example in Cloud Shell) the default credentials of the logged-in account are used.
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "dev-methods-and-tools"
subscription_id = "smartmeter_converted_ahmad-sub"   # subscription of the output topic

# create a subscriber to the subscription
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}..\n")

count = 0
lock = threading.Lock()   # the callback runs in several threads, so the counter is protected by a lock

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global count
    # convert from bytes to dictionary (deserialization)
    record = json.loads(message.data.decode('utf-8'))
    with lock:
        count += 1
        print(f"[{count}] Consumed: profile={record['profileName']}, "
              f"temperature={record['temperature']:.2f} F, "
              f"humidity={record['humidity']:.2f} %, "
              f"pressure={record['pressure']:.4f} psi")

    # Report to Pub/Sub that the message was processed successfully
    message.ack()

with subscriber:
    # The callback function will be called for each message received from the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
