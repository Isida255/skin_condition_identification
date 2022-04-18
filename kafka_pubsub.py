#consumer program to get the json from new_user topic from cloud kafka confluent
#program to read decode the image from json message and save it the folder from where the program runs
#program refers to update results function from spark_clouddb program.
from confluent_kafka import Consumer, OFFSET_BEGINNING
import base64
import json
import sys

# adding other folder to the system path pls replace with right folder or
# remove if the other program is in same folder as this file
#sys.path.insert(0, '/Users/giridharangovindan/PycharmProjects/finalproject')
from spark_clouddb import update_results

if __name__ == '__main__':
    config = {'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
              'security.protocol': 'SASL_SSL',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': '75JYP7BH76CYFKC7',
              'sasl.password': 'rRErWkw8ZmMR/A00IuODq0CXvCk/NYGB5rXaO9oc93162oWfCZ4f0kD8fE+Q9SJC',
              'group.id': 'python_example_group_1',
              'auto.offset.reset': 'earliest'}
    # Create Consumer instance
    consumer = Consumer(config)
    # Subscribe to topic
    topic = "new_user"
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
                # print("Consumed event from topic {topic}: key = {key} value = {value}".format(
                #     topic=msg.topic(), key=msg.key().decode('utf-8'), value=json.loads(msg.value().decode('utf-8'))))
                #messageread variable will have the values in dict form
                messageread = json.loads(msg.value())
                print(messageread["_id"])
                print(type(messageread))
                encoded_image = messageread["image"]
                image_name=messageread["image_name"]
                decodeit = open(image_name, 'wb')
                decodeit.write(base64.b64decode((encoded_image)))
                decodeit.close()
                # add the call for result set here and load the string into result in json format#
                #defaulted to test result 1 for testing.
                result = 'test result  1'
                result = {"results": result}
                messageread.update(result)
                print(messageread["results"])
                update_results(messageread)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()