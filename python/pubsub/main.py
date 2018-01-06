# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import base64
import json
import logging
import os
import time

from google.cloud import pubsub



# Global list to storage messages received by this instance.
MESSAGES = []

if __name__ == '__main__':
    config = {"test":"1"}
    print(config["test"])
    print(config.setdefault("2", "default value returned"))
    topic_name = "sample"
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    print("Publishing to pubsub")
    # credentials = service_account.Credentials.from_service_account_file('/path/to/key.json')
    ps = pubsub.Client()
    # for topic in ps.list_topics():
    #     print(topic.name)
    topic = ps.topic(topic_name)

    batch = topic.batch(max_messages=10)
    for i in xrange(1, 100):
        batch.publish("""message {} from Python code""".format(i))
        time.sleep(1)

# [END app]
