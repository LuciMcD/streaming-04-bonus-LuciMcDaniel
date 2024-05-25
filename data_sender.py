"""
    This is my version v3 of the tasks emitter. Based and mostly copied from v2_emitter_tasks.py. Authored by Dr. Case.
    This program pulls a message from tasks.csv and sends to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Luci McDaniel
    Date: May 24, 2024

"""

import pika
import sys
import webbrowser
import csv
import time

from util_logger import setup_logger
logger, logname = setup_logger(__file__)
        

def offer_rabbitmq_admin_site(show_offer=True):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)

        with open('tasks.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                task = ''.join(row)
                # use the channel to publish a message to the queue
                 # every message passes through an exchange
                ch.basic_publish(exchange="", routing_key=queue_name, body=task)
                # print a message to the console for the user
                logger.info(f" [x] Sent {task}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site(show_offer=False)

    # send the message to the queue
    send_message("localhost","task_queue3")
    time.sleep(3) 