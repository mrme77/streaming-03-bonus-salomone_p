import pika
import csv
import signal
import sys

def process_message(message):
    """
    Processes a received message and writes it to an output CSV file.

    Parameters:
        message (str): The received message to be processed and written to the CSV file.

    """
    with open('output.csv', 'a') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(message.split(','))  # Split message by comma and write as CSV row

def callback(ch, method, properties, body):
    """
    Callback function to handle received messages.

    Parameters:
        ch: The channel associated with the callback.
        method: The method frame.
        properties: Message properties.
        body (bytes): The body of the received message.

    """
    message = body.decode()
    print(f"Received message: {message}")
    process_message(message)

def main_consumer(queue_name):
    """
    Listens for and processes messages from a queue on the RabbitMQ server.

    This function establishes a connection to a RabbitMQ server, creates a communication
    channel, and consumes messages from a specific queue. It defines a callback function
    to process received messages and starts consuming messages.

    To exit the message consumption, press CTRL+C.

    Parameters:
        queue_name (str): The name of the queue to consume messages from.

    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    
    # Set up the Ctrl+C handler
    def handle_interrupt(signal, frame):
        """
        Handle interrupt signal (Ctrl+C) by gracefully exiting the program.

        This function is invoked when an interrupt signal (such as Ctrl+C) is received.
        It stops the message consumption, closes the RabbitMQ connection, and exits
        the program with a status code of 0 to indicate successful termination.

        Parameters:
        signal: The interrupt signal received (not used in the function).
        frame: The current stack frame (not used in the function).

        """
        print('\n Peacefully exiting the program...')
        channel.stop_consuming()
        connection.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_interrupt)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        handle_interrupt(None, None)

if __name__ == '__main__':
    queue_name = 'testing_csv_queue'  
    main_consumer(queue_name)
