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
min = 2.5
#initialze deque of maxlen assuming 30second intervals)
queue = deque(maxlen=int(min * 60/30))
#establish divider for ease of viewing
divider="   " * 10

# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    logger.info(divider)
    logger.info(f" [x] Received {body.decode()} for smoker.")
    logger.info(divider)
    #split the message
    try:
        message = body.decode().split(",")
        #check to see if a temperature is present and append the temperatures to the deque
        if message[1] != "":
            timestamp = message[0]
            temp = float(message[1])
            queue.append(temp)
            #start checking the temperature difference between the first and subsequent readings. 
            #Anything larger than 0 indicates a drop in temperature
            if len(queue) >=2:
                temp_change = [(queue[i]-queue[-1]) 
                               for i in range(0,(len(queue)-1))]
                #if there is a drop in temp larger than 15degrees send an alert for the smoker temp.
                if any(tc>15 for tc in temp_change):
                    logger.info(divider)
                    logger.info(f'Smoker Alert at {timestamp}')
                    logger.info(f"The smoker temp has fallen by more than 15 degrees!")
                    logger.info(divider)
        
        # when done with task, tell the user
        logger.info(divider)
        logger.info(" [x] Done.")
        logger.info(divider)
        # acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as error:
        logger.info(divider)
        logger.error('An error as occured with the alert system.')
        logger.error(f'Error: {error}')
        logger.info(divider)
        
# define a main function to run the program
def main(hn: str = "localhost", qn: str = "01-smoker"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.info("---***---")
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.info(f"The error says: {e}")
        logger.info("---***---")
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
        logger.info("---***---")
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        logger.info(divider)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("---***---")
        logger.info(" User interrupted continuous listening process.")
        logger.info(divider)
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        logger.info(divider)
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")