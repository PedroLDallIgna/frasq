import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

QUEUE_NAME: str = 'tasks_queue'

def main() -> None:
    connection: BlockingConnection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel: BlockingChannel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME)

    message: str = 'Hello! This is a message from the producer'

    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=message,
    )

    print(f'\t[X] Message sent: {message}')

    connection.close()


if __name__ == '__main__':
    main()
