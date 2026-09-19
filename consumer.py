import pika
import sys

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties

QUEUE_NAME: str = 'tasks_queue'
RABBITMQ_HOST: str = 'localhost'
RABBITMQ_PORT: int = 5672
RABBITMQ_VHOST: str = 'my_vhost'

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
    connection: BlockingConnection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST
        )
    )
    channel: BlockingChannel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_consume(
        queue=QUEUE_NAME,
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
