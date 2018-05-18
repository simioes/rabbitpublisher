#!/bin/python

import threading
from time import sleep
import pika
import time
import argparse
import math
import sys
import json

'''
This helper script is intended to be used to publish messages in RabbitMQ using threads.
To show the manual type: python rabbitmq_publisher.py -h 

Ex: python rabbitmq_publisher.py
'''
payload = { "key": "value"}


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--payload', type=str, default=payload, help="the payload of the message to send")
    parser.add_argument('--username', type=str, default='guest', help="the username to use (default: guest)")
    parser.add_argument('--password', type=str, default='guest', help="the password to use (default: guest)")
    parser.add_argument('--host', type=str, default='localhost', help="the hostname of the server (default: localhost)")
    parser.add_argument('--port', type=int, default='5672', help="the server port (default: 5672)")
    parser.add_argument('--threads', type=int, default='1', help="number of producer threads to start (default: 2)")
    parser.add_argument('--messages', type=int, default='2', help="number of messages each thread should sender (default: 6)")
    parser.add_argument('--vhost', type=str, default='/', help="the virtual host (default: /)")
    parser.add_argument('--exchange', type=str, default='default', help="the exchange (default: default)")
    parser.add_argument('--key', type=str, default='default.*.events.create', help="the routing key (default is blank key)")
    return parser.parse_args()


def create_connection_params(args):
    credentials = pika.PlainCredentials(args.username, args.password)
    return pika.ConnectionParameters(args.host, args.port, args.vhost, credentials)


def tprint(msg):
    name = threading.current_thread().name
    spaces = ' '*(11-len(name))
    print >>sys.stdout, "[" + threading.current_thread().name + "]" + spaces + msg + "\n",


def publisher(args, connection_params):
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    progress = 0
    for i in range(args.messages):
        progress = math.floor(((i+1)*1.0 / args.messages)*100)
        if progress % 20 == 0:
            tprint(str(progress) + "% done")
            progress = progress + 1

        channel.basic_publish(
            exchange=args.exchange, 
            routing_key=args.key, 
            body=args.payload
        )

    channel.close()
    connection.close()
    tprint("Finished, terminating thread")

def main(args):
    connection_params = create_connection_params(args)
    print "Starting", args.threads, "threads sending", args.messages, "messages each, totalling", \
        (args.threads*args.messages), "messages"

    start_time = time.time()
    threads = []

    for i in range(args.threads):
        thread = threading.Thread(target = publisher, args = (args, connection_params, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print "--- %s seconds ---" % (time.time() - start_time)
    print "All threads finished... exiting"


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
