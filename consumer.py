import pika
import sys
from config import settings

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties
from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials

def callback(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
) -> None:
    """Callback function always called when a message arrives.

    Args:
        ch (BlockingChannel): The communcation channel where the message was received.
        method (Basic.Deliver): Metadata about the message delivery.
        properties (BasicProperties): Properties of the message.
        body (bytes): Message content in bytes format.
    """
    decoded_message: str = body.decode('utf-8')
    print(f'\t[X] Message received: {decoded_message}')
    

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

    channel.basic_consume(
        queue=settings.queue_name,
        on_message_callback=callback,
        auto_ack=True,
    )
    
    print('\t[*] Waiting for messages. To exit press CTRL+C')
    
    channel.start_consuming()
    
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n\t[*] Exiting...')
        sys.exit(0)
