import sys
from config import settings
from gen.message_pb2 import BasicMessage
from models.message import MessageModel

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
    routing_key=settings.routing_key_string,
    schema=str,
    auto_ack=False
)
def handle_string_message(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: str
) -> bool | None:
    """Callback function always called when a message arrives.

    Args:
        ch (BlockingChannel): The communcation channel where the message was received.
        method (Basic.Deliver): Metadata about the message delivery.
        properties (BasicProperties): Properties of the message.
        body (str): Message content in string format.
    """
    delivery_tag: int = method.delivery_tag
    
    try:
        print(f'\t[X] Message [{delivery_tag}] received via \'{method.exchange}\' [RK: \'{method.routing_key}\']: {body}')
        return True
    except Exception as e:
        print(f'\t[!] Error processing message: {e}')
        return False
    
@app.subscriber(
    exchange_name=settings.exchange_name,
    queue_name=settings.queue_name,
    routing_key=settings.routing_key_pydantic,
    schema=MessageModel,
    auto_ack=False
)
def handle_pydantic_message(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: MessageModel
) -> bool | None:
    """Callback function always called when a Pydantic message arrives.

    Args:
        ch (BlockingChannel): The communcation channel where the message was received.
        method (Basic.Deliver): Metadata about the message delivery.
        properties (BasicProperties): Properties of the message.
        body (MessageModel): Message content in Pydantic model format.
    """
    delivery_tag: int = method.delivery_tag
    
    try:
        print(f'\t[X] Message [{delivery_tag}] received via \'{method.exchange}\' [RK: \'{method.routing_key}\']: {body.message}')
        return True
    except Exception as e:
        print(f'\t[!] Error processing message: {e}')
        return False

@app.subscriber(
    exchange_name=settings.exchange_name,
    queue_name=settings.queue_name,
    routing_key=settings.routing_key_json,
    schema=dict,
    auto_ack=False
)
def handle_json_message(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: dict
) -> bool | None:
    """Callback function always called when a JSON message arrives.

    Args:
        ch (BlockingChannel): The communcation channel where the message was received.
        method (Basic.Deliver): Metadata about the message delivery.
        properties (BasicProperties): Properties of the message.
        body (dict): Message content in dictionary format.
    """
    delivery_tag: int = method.delivery_tag

    try:
        print(f'\t[X] Message [{delivery_tag}] received via \'{method.exchange}\' [RK: \'{method.routing_key}\']: {body['message']}')
        return True
    except Exception as e:
        print(f'\t[!] Error processing message: {e}')
        return False

@app.subscriber(
    exchange_name=settings.exchange_name,
    queue_name=settings.queue_name,
    routing_key=settings.routing_key_protobuf,
    schema=BasicMessage,
    auto_ack=False
)
def handle_protobuf_message(
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: BasicMessage
) -> bool | None:
    """Callback function always called when a Protobuf message arrives.

    Args:
        ch (BlockingChannel): The communcation channel where the message was received.
        method (Basic.Deliver): Metadata about the message delivery.
        properties (BasicProperties): Properties of the message.
        body (BasicMessage): Message content in Protobuf format.
    """
    delivery_tag: int = method.delivery_tag

    try:
        print(f'\t[X] Message [{delivery_tag}] received via \'{method.exchange}\' [RK: \'{method.routing_key}\']: {body.message}')
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
