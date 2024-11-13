#!/usr/bin/env python
import datetime
import json

import time
from random import choice
from confluent_kafka import Producer
#from confluent_kafka.schema_registry import record_subject_name_strategy
#from kafka import KafkaProducer
from confluent_kafka.serialization import StringSerializer
#from kafka import RoundRobinPartitioner

#def create_kafka_producer(bootstrap_servers):
from sseclient import SSEClient as EventSource




def init_namespaces():
    # create a dictionary for the various known namespaces
    # more info https://en.wikipedia.org/wiki/Wikipedia:Namespace#Programming
    namespace_dict = {-2: 'Media',
                      -1: 'Special',
                      0: 'main namespace',
                      1: 'Talk',
                      2: 'User', 3: 'User Talk',
                      4: 'Wikipedia', 5: 'Wikipedia Talk',
                      6: 'File', 7: 'File Talk',
                      8: 'MediaWiki', 9: 'MediaWiki Talk',
                      10: 'Template', 11: 'Template Talk',
                      12: 'Help', 13: 'Help Talk',
                      14: 'Category', 15: 'Category Talk',
                      100: 'Portal', 101: 'Portal Talk',
                      108: 'Book', 109: 'Book Talk',
                      118: 'Draft', 119: 'Draft Talk',
                      446: 'Education Program', 447: 'Education Program Talk',
                      710: 'TimedText', 711: 'TimedText Talk',
                      828: 'Module', 829: 'Module Talk',
                      2300: 'Gadget', 2301: 'Gadget Talk',
                      2302: 'Gadget definition', 2303: 'Gadget definition Talk'}

    return namespace_dict


def construct_event(event_data):
    # use dictionary to change assign namespace value and catch any unknown namespaces (like ns 104)
    try:
        event_data['namespace'] = namespace_dict[event_data['namespace']]
    except KeyError:
        event_data['namespace'] = 'unknown'

    # assign user type value to either bot or human
    #user_type = user_types[event_data['bot']]

    # define the structure of the json event that will be published to kafka topic
    event = json.dumps({"id": event_data['id'],
             "domain": event_data['meta']['domain'],
             "namespace": event_data['namespace'],
             "title": event_data['title'],
             #"comment": event_data['comment'],
             "timestamp": event_data['meta']['dt'],#event_data['timestamp'],
             "user_name": event_data['user'],
             #"user_type": user_type,
             #"minor": event_data['minor'],
             "old_length": event_data['length']['old'],
             "new_length": event_data['length']['new']}).encode('utf-8')

    return event

if __name__ == '__main__':

    namespace_dict = init_namespaces()

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': "localhost:9092" ,
        # 'localhost:45627',

        # Fixed properties
        'acks': 'all',
        #'batch.size': 1 * 1024, #default 16Kb
        #'linger.ms': 500,  # Wait up to 500ms for the batch to fill before sending

    }

#not working
    #partitioner = RoundRobinPartitioner(partitions=3)  # Assume we have 3 partitions in the topic

    # Create Producer instance
    producer = Producer(config)

    # where do we get our messages
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        #executed when a record is succ sent or exception is thrown.
        if err: # if error exists
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: partition {partition}: key = {key:12} value = {value:12} at time = {time}".format(
                topic=msg.topic(), partition=msg.partition() ,  value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = "wikimedia_events"
   # user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther', 'jason', 'cassandra' , 'george' , 'melina' , 'maria' , 'ioanna']
   # products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0

    #producer.produce(topic, "hello world", callback=delivery_callback)
    # for _ in range(50):
    #     #user_id = choice(user_ids)
    #    # product = choice(products)
    #     # Manually assign partition using round-robin logic
    #     #partition = partitioner.partitions()  # We don't need key, just round-robin partitioning
    #
    #     producer.produce(topic = topic, value = product, key = user_id,  timestamp = int(time.time() * 1000) , callback=delivery_callback)
    #     count += 1
    messages_count = 0

    for event in EventSource(url):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
            except ValueError:
                pass
            else:
                #print(event_data)
                # filter out events, keep only article edits (mediawiki.recentchange stream)
                if event_data['type'] == 'edit':
                    # construct valid json event
                    event_to_send = construct_event(event_data)

                    producer.produce('wikipedia-events', value=event_to_send , callback=delivery_callback)

                    messages_count += 1


    # producer.poll(10)
    # print('first for loop')
    # #time.sleep(5)
    #
    # for _ in range(50):
    #     user_id = choice(user_ids)
    #     product = choice(products)
    #     producer.produce(topic = topic, value = product, key = user_id, timestamp = int(time.time() * 1000) , callback=delivery_callback)
    #     count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()