import pika
import csv
import signal
import sys

def process_message(message):
    with open('output.csv', 'a') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(message.split(','))  # Split message by comma and write as CSV row

def callback(ch, method, properties, body):
    message = body.decode()
    print(f"Received message: {message}")
    process_message(message)

def main_consumer(queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    
    # Set up the Ctrl+C handler
    def handle_interrupt(signal, frame):
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
    queue_name = 'testing_csv_queue'  # Replace with the same queue name as used in the producer
    main_consumer(queue_name)
