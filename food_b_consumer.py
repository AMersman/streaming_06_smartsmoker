"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import time
import sys
from collections import deque
import csv
from util_logger import setup_logger

logger, logname = setup_logger(__file__)
#set variable min to length of time for the alert
min = 10

#initialze deque of maxlen assuming 30second intervals
queue = deque(maxlen=int(min * 60/30))

#establish divider for ease of viewing
divider="   " * 10

# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    logger.info(divider)
    logger.info(f" [x] Received {body.decode()}")
    logger.info(divider)
    try:
        message = body.decode().split(",")
        #check to see if a temperature is present and append the temperatures to the deque
        if message[1] != "":
            timestamp = message[0]
            temp = float(message[1])
            queue.append(temp)
            #start checking the temperature difference between the first and last readings in intervals of 10 minutes. Alert if th change is less than 1 degree
            if len(queue) >= (min * 60/30):
                temp_change = queue[-1] - queue[0]
                if abs(temp_change)<1:
                    logger.info(divider)
                    logger.info(f'Warning! You hit a food stall at {timestamp}. The temperature has not changed in the past 10 minutes!')
                    logger.info(divider)
        logger.info(divider)
        logger.info(" [x] Done.")
        logger.info(divider)
        # acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as error:
        logger.info(divider)
        logger.error('An error has occured.')
        logger.error(f'Error: {error}')
        logger.info(divider)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "03-food-B"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.info(divider)
        logger.info("---***---")
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.info(f"The error says: {e}")
        logger.info("---***---")
        logger.info(divider)
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=callback)

        # print a message to the console for the user
        logger.info(divider)
        logger.info(" [*] Ready for work. To exit press CTRL+C")
        logger.info(divider)

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info(divider)
        logger.info("---***---")
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        logger.info(divider)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("---***---")
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info(divider)
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "03-food-B")