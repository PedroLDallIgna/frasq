import pika

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials
from config import settings

def main() -> None:
    credentials: PlainCredentials = pika.PlainCredentials(
        username=settings.user,
        password=settings.password,
    )
    
    connection_parameters: ConnectionParameters = pika.ConnectionParameters(
        host=settings.host,
        port=settings.port,
        virtual_host=settings.vhost,
        credentials=credentials,
    )
    connection: BlockingConnection = pika.BlockingConnection(connection_parameters)
    channel: BlockingChannel = connection.channel()

    channel.queue_declare(queue=settings.queue_name)

    message: str = 'Hello! This is a message from the producer'

    channel.basic_publish(
        exchange='',
        routing_key=settings.queue_name,
        body=message,
    )

    print(f'\t[X] Message sent: {message}')

    connection.close()


if __name__ == '__main__':
    main()
