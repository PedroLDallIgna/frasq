import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

QUEUE_NAME: str = 'tasks_queue'
RABBITMQ_HOST: str = 'localhost'
RABBITMQ_PORT: int = 5672
RABBITMQ_VHOST: str = 'my_vhost'

def main() -> None:
    connection: BlockingConnection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST
        )
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
