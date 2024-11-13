#!/usr/bin/env python
import datetime

from confluent_kafka import Consumer

if __name__ == '__main__':

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'localhost:9092',

        # Fixed properties
        'group.id':          'kafka-python-getting-started',
        'auto.offset.reset': 'earliest'
    }
    # none means if we don't have existing consumer group we fail. we must set consumer group
    # earliest read from the beginning of my topic
    # latest i want to read from just now and only the new messages.

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "tester_topic"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: partition {partition} offset {offset} key = {key:12} value = {value:12} at time = {time}".format(
                    topic=msg.topic(), partition = msg.partition(), offset= msg.offset() ,key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8'), time=datetime.datetime.fromtimestamp(msg.timestamp()[1]/1000)))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        print('consumer closed')