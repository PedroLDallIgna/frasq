import sys
from config import settings

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties
from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials

from rabbitmq_client import RabbitMQClient

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
    delivery_tag: int = method.delivery_tag
    
    try:
        decoded_message: str = body.decode('utf-8')
        print(f'\t[X] Message received: {decoded_message}')
        ch.basic_ack(delivery_tag=delivery_tag)
    except Exception as e:
        print(f'\t[!] Error processing message: {e}')
        ch.basic_nack(delivery_tag=delivery_tag, requeue=True)

def main() -> None:
    client = RabbitMQClient()
    
    try:
        client.connect()
        client.consume(
            callback_function=callback,
            queue_name=settings.queue_name,
            auto_ack=False
        )
    except KeyboardInterrupt:
        print('\n\t[*] Exiting...')
    except Exception as e:
        print(f'\t[!] Error occurred: {e}')
    finally:
        client.close()
        sys.exit(0) 
    
    
if __name__ == '__main__':
    main()
