import datetime
import json
from logging import Logger

from opensearchpy import OpenSearch, helpers
from confluent_kafka import Consumer


def create_kafka_consumer(server , offset , groupId , auto_commit = True):
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': server,

        # Fixed properties
        'group.id':          groupId,
        'auto.offset.reset': offset,
        'enable.auto.commit': auto_commit # commits what has read from kafka. updates the lag. if we dont commit it will re-read the msgs when we re run it


        # cannot make it work
        #'partition.assignment.strategy': 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor'
    }
    # none means if we don't have existing consumer group we fail. we must set consumer group
    # earliest read from the beginning of my topic
    # latest i want to read from just now and only the new messages.

    consumer = Consumer(config)
    return consumer

# Generator for bulk actions


if __name__ == '__main__':
    host = 'localhost'
    port = 9200



    index_name = 'wikimedia'
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 4
            }
        }
    }

    # Create the client with SSL/TLS and hostname verification disabled.
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = False,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name)



# document = {
    #     'title': 'Moneyball',
    #     'director': 'Bennett Miller',
    #     'year': '2011'
    # }


    # response = client.index(
    #     index = 'python-test-index',
    #     body = document,
    #     id = '1',
    #     refresh = True
    # )

    #print(response)

    movies = '{ "index" : { "_index" : "my-dsl-index", "_id" : "2" } } \n { "title" : "Interstellar", "director" : "Christopher Nolan", "year" : "2014"} \n { "create" : { "_index" : "my-dsl-index", "_id" : "3" } } \n { "title" : "Star Trek Beyond", "director" : "Justin Lin", "year" : "2015"} \n { "update" : {"_id" : "3", "_index" : "my-dsl-index" } } \n { "doc" : {"year" : "2016"} }'

    #client.bulk(movies)


    q = 'miller'
    query = {
        'size': 5,
        'query': {
            'multi_match': {
                'query': q,
                'fields': ['title^2', 'director']
            }
        }
    }

    # response = client.search(
    #     body = query,
    #     index = 'python-test-index'
    # )

    # print(response)

  #  logger = Logger(name='OpenSearchLogger')


    consumer = create_kafka_consumer('localhost:9092' , 'earliest' ,'openSearchDemo' )
    topic = "wikipedia-events"
    consumer.subscribe([topic])
    # ONE AT A TIME INDEX
    # try:
    #
    #     while True:
    #         msg = consumer.poll(1.0)
    #         if msg is None:
    #             # Initial message consumption may take up to
    #             # `session.timeout.ms` for the consumer group to
    #             # rebalance and start consuming
    #             print("Waiting...")
    #         elif msg.error():
    #             print("ERROR: %s".format(msg.error()))
    #         else:
    #             # Extract the (optional) key and value, and print.
    #             print("Consumed event from topic {topic}: partition {partition} offset {offset}  value = {value:12} at time = {time}".format(
    #                 topic=msg.topic(), partition = msg.partition(), offset= msg.offset() , value=msg.value().decode('utf-8'), time=datetime.datetime.fromtimestamp(msg.timestamp()[1]/1000)))
    #
    #             # create a unique id
    #             message_value = msg.value().decode("utf-8")  # Decode the message from bytes to string
    #             message_data = json.loads(message_value)     # Parse the JSON string into a dictionary
    #
    #             # Access the 'id' field
    #             message_id = message_data.get("id")
    #             #send the msg to openSearch
    #             response = client.index(
    #                 index=index_name,
    #                 body= message_data, # Is json needed here ?
    #                 id=str(message_id)  # Optional: use id of wikipedia msg as document ID
    #             )
    #             print(f"Indexed message to OpenSearch: {response}")
    #
    # except Exception as e:
    #     print(f"Failed to index message: {e}")
    # except KeyboardInterrupt:
    #     print("Stopping consumer...")
    # finally:
    #     # Leave group and commit final offsets
    #     #consumer.commit() #if we have autocommit false.
    #     consumer.close()
    #     print('consumer closed')

 # BULK INDEXING
    bulk_actions = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting for messages...")
                continue
            elif msg.error():
                print(f"ERROR: {msg.error()}")
                continue
            else:

                print("Consumed event from topic {topic}: partition {partition} offset {offset}  value = {value:12} at time = {time}".format(
                    topic=msg.topic(), partition = msg.partition(), offset= msg.offset() , value=msg.value().decode('utf-8'), time=datetime.datetime.fromtimestamp(msg.timestamp()[1]/1000)))

                # Deserialize the message value (assuming JSON)
                data = json.loads(msg.value().decode('utf-8'))



                # Access the 'id' field
                messageId = data.get("id")
                #send the msg to openSearch
                bulk_actions.append({
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": str(messageId),
                    "_source": data
                })
                if len(bulk_actions) >= 100:
                    #send_to_opensearch(bulk_actions)
                    success, failed = helpers.bulk(client, bulk_actions)
                    print(f"Successfully indexed {success} documents, {failed} failed.")
                    bulk_actions.clear()

    except KeyboardInterrupt:
        print("Interruption detected, sending remaining data.")
        #send_to_opensearch(bulk_actions)
        success, failed = helpers.bulk(client, bulk_actions)
        print(f"Successfully indexed {success} documents, {failed} failed.")
        bulk_actions.clear()
    finally:
        consumer.close()
        print("Consumer closed.")

        # Prepare bulk action


    # except:
    #     print("Something else went wrong")

    #logger.info("OpenSearch client initialized successfully.")
    #logger.info(f"Connected to OpenSearch on {host}:{port}")