# streaming_06_smartsmoker
# Ashley Mersman
### 09/20/2023
This is module 6 from the Streaming Class at NWMSU. 


# Overview 
This project creates a producer to send messages representing 3 temperatures (smoker, FoodA, FoodB) on three channels respectively. Each is sent to a different consumer queue using RabbitMQ. The consumer then uses deque to create a rolling window and send alerts to the terminal under specific conditions.

# Data
The temperature measurements are rows from the smoker-temp.csv that can be found in this repository. 

# Requirements
Git
Python 3.11
RabbitMQ Server
Virtual Environment 
    .venv
    source .venv/bin/activate
Pika
    pip install pika

# Run the Code
Navigate to the repo containg the code and activate a virutal environment. Run the producer file using: python3 smoker_temo_producer.py and repeat for the consumer files.
 
Enter y to open the RabbitMQ admin page in order to monitor the queues (guest is the username and password). n can be entered if you do not wish to open the RabbitMQ admin page. The data will stream messages from each row every 30 seconds and a confirmation message will show in the terminal. The stream will end when the entire file has been streamed or when interrupted using CTRL+c. 

# Screenshots
<img width="1440" alt="Screen Shot 2023-09-28 at 7 41 39 PM" src="https://github.com/AMersman/streaming_06_smartsmoker/assets/91644580/7fdb20f2-665f-486b-b941-2706e37b2808">
<img width="1440" alt="Screen Shot 2023-09-28 at 7 41 32 PM" src="https://github.com/AMersman/streaming_06_smartsmoker/assets/91644580/8ff0fe76-699f-4003-a144-776839e32dea">
<img width="1440" alt="Screen Shot 2023-09-28 at 7 41 44 PM" src="https://github.com/AMersman/streaming_06_smartsmoker/assets/91644580/f917908b-1059-4b2e-98f6-877a61d8f918">



