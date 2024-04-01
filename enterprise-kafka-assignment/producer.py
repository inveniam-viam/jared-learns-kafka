#!/usr/bin/env python

import sys
import json
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    # topic = "purchases"
    # user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    # products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    # count = 0
    # for _ in range(10):

    #     user_id = choice(user_ids)
    #     product = choice(products)
    #     producer.produce(topic, product, user_id, callback=delivery_callback)
    #     count += 1
    

    # Producing data that would eventually be streamed to the roster consumer
        
    topic = "raptors-roster"

    playerIDs = [23, 24, 25, 26, 27]
    firstNames = ["Norman", "Greivis", "Otto", "Delon", "Marc"]
    lastNames = ["Powell", "Vasquez", "Porter Jr.", "Wright", "Gasol"]
    ages = [30, 37, 30, 31, 39]
    countries = ["United States", "Venezuela", "United States", "United States", "Spain"]
    colleges = ["UCLA", "Maryland", "Georgetown", "Utah", "FC Barcelona"]


    for i in range(5):

        player_info = {
            "playerID": playerIDs[i],
            "firstName": firstNames[i],
            "lastName": lastNames[i],
            "age": ages[i],
            "country": countries[i],
            "college": colleges[i]
        }
        player_json = json.dumps(player_info)
        producer.produce(topic, key = str(playerIDs[i]) , value=player_json, on_delivery=delivery_callback)



    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
