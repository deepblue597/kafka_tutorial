#!/usr/bin/env python
import datetime
import time
from random import choice
from confluent_kafka import Producer
#from confluent_kafka.schema_registry import record_subject_name_strategy
#from kafka import KafkaProducer
from confluent_kafka.serialization import StringSerializer
#from kafka import RoundRobinPartitioner

if __name__ == '__main__':

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

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        #executed when a record is succ sent or exception is thrown.
        if err: # if error exists
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: partition {partition}: key = {key:12} value = {value:12} at time = {time}".format(
                topic=msg.topic(), partition=msg.partition() , key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8'), time=datetime.datetime.fromtimestamp(msg.timestamp()[1]/1000)))

    # Produce data by selecting random values from these lists.
    topic = "tester_topic"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther', 'jason', 'cassandra' , 'george' , 'melina' , 'maria' , 'ioanna']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0

    #producer.produce(topic, "hello world", callback=delivery_callback)
    for _ in range(50):
        user_id = choice(user_ids)
        product = choice(products)
        # Manually assign partition using round-robin logic
        #partition = partitioner.partitions()  # We don't need key, just round-robin partitioning
        producer.produce(topic = topic, value = product, key = user_id,  timestamp = int(time.time() * 1000) , callback=delivery_callback)
        count += 1


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