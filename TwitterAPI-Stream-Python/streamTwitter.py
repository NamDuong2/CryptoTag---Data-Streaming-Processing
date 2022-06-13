# -*- coding: utf-8 -*-
"""
Created on Thu Apr 7 8:43 PM 2022
this program fetch crypto tags tweet from Twitter API
perform diff with previously fetched data and write
the difference in Kafka
@author: Duong Hoai Nam
"""

import settings
from tweepy import Stream
from kafka import KafkaProducer
import json
from kafka.admin import KafkaAdminClient, NewTopic


class MyStreamListener(Stream):

    def on_status(self, status):
        # Tweepy by default doesn't include retweets in user_timeline
        # therefore tweet.retweeted will always be false.
        if status.retweeted:
            return  # exits the current function
        # https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet
        if not status.truncated:
            data = {
                'id': status.id_str,
                'user_id': status.user.id_str,
                'username': status.user.screen_name,
                'loc': status.user.location,
                'followers': status.user.followers_count,
                'description': status.user.description,
                'text': status.text,
                'retweeted': status.retweeted,
                'retweet_count': status.retweet_count,
                'created_at': str(status.created_at)
            }
        else:
            data = {
                'id': status.id_str,
                'user_id': status.user.id_str,
                'username': status.user.screen_name,
                'loc': status.user.location,
                'followers': status.user.followers_count,
                'description': status.user.description,
                'text': status.extended_tweet['full_text'],
                'retweeted': status.retweeted,
                'retweet_count': status.retweet_count,
                'created_at': str(status.created_at)
            }

        try:
            send_data = producer.send(settings.KAFKA_TOPIC_IN, data)
            print(send_data)
        except:
            print("Sending data to Kafka has failed!!!")

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False


"""
def create_topic(self, topic_name=None, num_partitions=1, replication_factor=1):
    try:
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        self.show_topic_on_console()
        return True
    except TopicAlreadyExistsError as err:
        print(f"Request for topic creation is failed as {topic_name} is already created due to {err}")
        return False
    except Exception as err:
        print(f"Request for topic creation is failing due to {err}")
        return False
"""
# Create topic in Kafka with Python-kafka admin client
try:
    admin_client = KafkaAdminClient(bootstrap_servers=settings.BOOTSTRAP_SERVERS)
    topic_list = []
    topic_list.append(NewTopic(name=settings.KAFKA_TOPIC_IN, num_partitions=1, replication_factor=1))
    topic_list.append(NewTopic(name=settings.KAFKA_TOPIC_OUT, num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topic {}, {} created".format(settings.KAFKA_TOPIC_IN, settings.KAFKA_TOPIC_OUT))
except Exception as err:
    print(f"Request for topic creation is failing due to {err}")


# Set up KafkaProducer & Twitter API
producer = KafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
stream = MyStreamListener(settings.API_KEY, settings.API_KEY_SECRET, settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
stream.filter(track=settings.TRACK_TERMS, languages=['en'], threaded=True)