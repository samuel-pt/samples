"""Backend process that consumes from a message queue and writes to Google PubSub."""
import itertools
import logging
import logging.config
import os
import time
import json
import uuid

from multiprocessing import Process
from google.cloud import pubsub
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import GoogleCloudError
from google.gax.errors import GaxError
from memory_profiler import profile

_PROJECT = 'v2-pipeline-prototype'
_TOPIC = 'sample'
_RETRY_DELAY_SECS = 1
_PROCESS_COUNT = 20


@profile(precision=4)
def process_queue(message_str, ps_batch):
    """ Take messages off a queue and send to Google PubSub topic."""
    while True:
        message = message_str.replace('UUID_TO_BE_REPLACED', str(uuid.uuid4()))
        while True:
            try :
                ps_batch.publish(message)
            except (GaxError, GoogleCloudError) as exc:
                print("failed to send message={0} due to error={1}", message, exc)
                # In the event of a Google PubSub error in send attempt,
                #   retry sending after a delay
                time.sleep(_RETRY_DELAY_SECS)
            else:
                break

@profile(precision=4)
def main():
    """Run a consumer.

    Two environment variables are expected:

    * CONFIG_URI: A PasteDeploy URI pointing at the configuration for the
      application.
    * QUEUE: The name of the queue to consume (currently one of "events" or
      "errors").

    """
    json_message = {'id':'UUID_TO_BE_REPLACED','request_domain':'www.reddit.com','base_platform':'reddit.com','user_preferences_in_beta':False,'listing_size':150,'listing_links':['t3_61ghbo','t3_61gh9k','t3_61gh6m','t3_61ggxb','t3_6124hq','t3_611zm5','t3_611zfc','t3_611zcp','t3_611ytt','t3_611yot','t3_60v3uq','t3_60uke2','t3_60ukb5','t3_60uk5w','t3_60ujkz','t3_60ujfo','t3_60nnu9','t3_60nnet','t3_60nnbh','t3_60nn78','t3_60nn38','t3_60gmxl','t3_60gmuq','t3_60dzqs','t3_60bbge'],'link_fullname':'t2_d7q7i','listing_sort_time_filter':'all','request_base_url':'/user/rubenesque_am_i/submitted/?count=150&after=t3_61m24j','user_preferences_dnt':False,'user_name':'shaved_biscuit','screen_width':1344,'timers_dom_content_loaded':0.10199999809265137,'listing_sort':'new','timers_request':0,'referrer_domain':'www.reddit.com','listing_name':'frontpage','timers_dom_loading':0.45399999618530273,'user_preferences_language':'en','referrer_url':'https://www.reddit.com/','request_user_agent':'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36','screen_height':840,'user_id':'t2_ebxx0','user_logged_in':True,'base_event_topic':'screenview_events','request_client_ip':'73.14.193.227','base_endpoint_timestamp':1496179046878,'base_event_timestamp':1496178926956,'base_uuid':'07074bfd-c513-47b5-b4af-c08c78662bef','base_event_type':'cs.screenview','app_version':'2.9.0.200353','user_loid_created':'1460128047826','link_nsfw':False,'link_url':'https://m.imgur.com/0I17dcK','screen_is_compact_view':'true','link_title':'What is the next Blockbuster style industry collapse?','link_spoiler':False,'user_preferences_nightmode':'true'}
    message_str = json.dumps(json_message)
    # print message_str

    topic_name = 'projects/{0}/topics/{1}'.format(_PROJECT, _TOPIC)
    """
    PubSub batch.size read from configuraton
    If not present, 100 is used as default batch size
    """
    batch_size = 100

    while True:
        try:
            ps = pubsub.Client()
            ps_topic = ps.topic(_TOPIC)
            """
            Google PubSub api will automatically publish the messages in batch
            if no.of messages present in batch equals batch_size
            """
            ps_batch = ps_topic.batch(max_messages = batch_size)
        except NotFound as exc:
            print("Topic {0} not found. Please create it", topic_name)
            time.sleep(_RETRY_DELAY_SECS)
            continue

        print("Pushing messages to {0}".format(_TOPIC))

        processes = []
        for i in range(1, _PROCESS_COUNT):
            p = Process(target=process_queue, args=(message_str, ps_batch))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()


if __name__ == "__main__":
    main()
