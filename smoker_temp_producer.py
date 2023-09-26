"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Ashley Mersman
    Date: September 16th, 2023
"""

import pika
import sys
import webbrowser
import csv
import time
from util_logger import setup_logger

#configure logging
logger, logname = setup_logger(__file__)


#configure variable for opeing the RabbitMQ admin site
SHOW_OFFER = False

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    logger.info("---***---")
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info(f'Answer is {ans}')
        
def send_message(
        host: str, 
        queue_name_a: str, 
        queue_name_b: str, 
        queue_name_c, 
        input_file: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        #delete 3 queues to start fresh
        ch.queue_delete(queue = queue_name_a)
        ch.queue_delete(queue = queue_name_b)
        ch.queue_delete(queue = queue_name_c)

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name_a, durable=True)
        ch.queue_declare(queue=queue_name_b, durable=True)
        ch.queue_declare(queue=queue_name_c, durable=True)
       
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        with open(input_file, 'r') as file:
            reader = csv.reader(file)
            #skip headers
            header=next(reader)
            logger.info('Header skipped')
            for row in reader:
                #name variables from csv
                time_stamp, smoker_temp, food_temp_a, food_temp_b = row
                
                #define messages
                message_a = time_stamp, smoker_temp
                message_b = time_stamp, food_temp_a
                message_c = time_stamp, food_temp_b

                #encoding of messages
                message_a_encode = ",".join(message_a).encode()
                message_b_encode = ",".join(message_b).encode()
                message_c_encode = ",".join(message_c).encode()

                # use the channel to publish first message to first queue
                # every message passes through an exchange
                ch.basic_publish(
                    exchange="", 
                    routing_key=queue_name_a, 
                    body=message_a_encode)
                # print a message to the console for the user
                logger.info(f" [x] Sent {message_a} to {queue_name_a}")
                
                # publish second message to second queue
                ch.basic_publish(
                    exchange="", 
                    routing_key=queue_name_b, 
                    body=message_b_encode)
                # print a message to the console for the user
                logger.info(f"[x] Sent {message_b} to {queue_name_b}")
            
                ch.basic_publish(
                    exchange="", 
                    routing_key=queue_name_c, 
                    body=message_c_encode)
                #print message to the console using logger
                logger.info(f'[x] Sent {message_c} to {queue_name_c}')
                #wait 15 seconds before sending next message
                time.sleep(30)

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
    #determine if offer_rabbitmq_admin_site() is True/False
    if SHOW_OFFER == True:
        # ask the user if they'd like to open the RabbitMQ Admin site
        offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    message = " ".join(sys.argv[1:]) or "Second task....."
    # send the message to the queue
    send_message("localhost","01-smoker", "02-food-A", "03-food-B", "smoker-temps.csv")