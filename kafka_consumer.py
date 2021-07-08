from kafka import KafkaConsumer
import json

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('oms_delivery_bulk_topic_2',
                         group_id='console_consumer_test',
                         bootstrap_servers=['alizew04.infoshop.com.tr:6667','alizew05.infoshop.com.tr:6667','alizew06.infoshop.com.tr:6667'])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    with open("data.json", "w") as f:
        json.dump(json.loads(message.value.decode('utf-8')), f)
    f.close()

    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode('utf-8')))



# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))
