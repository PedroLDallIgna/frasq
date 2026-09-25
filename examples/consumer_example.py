import sys
from config import settings

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from frasq import FrasQApp

app = FrasQApp(
    username=settings.user,
    password=settings.password,
    host=settings.host,
    port=settings.port,
    vhost=settings.vhost
)
    
@app.subscriber(
    exchange_name=settings.exchange_name,
    queue_name=settings.queue_name,
    routing_key=settings.routing_key,
    auto_ack=False
)
def handle_message(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes
) -> bool | None:
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
        print(f'\t[X] Message [{delivery_tag}] received via \'{method.exchange}\' [RK: \'{method.routing_key}\']: {decoded_message}')
        return True
    except Exception as e:
        print(f'\t[!] Error processing message: {e}')
        return False
    
@app.subscriber(
    exchange_name="new_exchange",
    queue_name="new_queue",
    routing_key="new_routing_key",
    auto_ack=False
)
def handle_other_message(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes
) -> bool | None:
    delivery_tag: int = method.delivery_tag
    
    try:
        decoded_message: str = body.decode('utf-8')
        print(f'\t[X] Message [{delivery_tag}] received via \'{method.exchange}\' [RK: \'{method.routing_key}\']: {decoded_message}')
        return True
    except Exception as e:
        print(f'\t[!] Error processing message: {e}')
        return False

def main() -> None:    
    try:
        app.run()
    except KeyboardInterrupt:
        print('\n\t[*] Exiting...')
    except Exception as e:
        print(f'\t[!] Error occurred: {e}')
    finally:
        app.close()
        sys.exit(0) 
    
if __name__ == '__main__':
    main()
