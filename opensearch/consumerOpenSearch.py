from logging import Logger

from opensearchpy import OpenSearch





if __name__ == '__main__':
    host = 'localhost'
    port = 9200

    # Create the client with SSL/TLS and hostname verification disabled.
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = False,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )

    index_name = 'python-test-index'
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 4
            }
        }
    }

    #response = client.indices.create(index_name, body=index_body)


    document = {
        'title': 'Moneyball',
        'director': 'Bennett Miller',
        'year': '2011'
    }


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

    response = client.search(
        body = query,
        index = 'python-test-index'
    )

    print(response)

    logger = Logger(name='OpenSearchLogger')

    logger.info("OpenSearch client initialized successfully.")
    #logger.info(f"Connected to OpenSearch on {host}:{port}")