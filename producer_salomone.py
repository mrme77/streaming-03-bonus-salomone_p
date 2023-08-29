import pika
import csv
import random
import time
import signal
import sys

csv_name = 'Airports2.csv'
host = 'localhost'

connection = None  # To store the RabbitMQ connection

def send_message_to_queue(queue_name, message):
    global connection  # Access the global connection variable
    if connection is None or connection.is_closed:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f" [x] Sent: {message}")

def main_producer(csv_filename, queue_name):
    with open(csv_filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip header row
        for row in reader:
            message = ','.join(row)  # Convert row to a comma-separated string
            send_message_to_queue(queue_name, message)
            sleep_time = random.uniform(1, 3)  # Random sleep between 1 and 3 seconds
            try:
                time.sleep(sleep_time)
            except KeyboardInterrupt:
                handle_interrupt(None, None)  # Handle the interrupt gracefully

def handle_interrupt(signal, frame):
    global connection  # Access the global connection variable
    if connection is not None and connection.is_open:
        connection.close()
        print("\nClosed RabbitMQ connection.")
    print("\n Peacefully exiting the program...")
    sys.exit(0)

if __name__ == '__main__':
    csv_filename = csv_name
    queue_name = 'testing_csv_queue'

    signal.signal(signal.SIGINT, handle_interrupt)

    main_producer(csv_filename, queue_name)
