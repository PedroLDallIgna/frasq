from config import settings
from gen.message_pb2 import BasicMessage
from models.message import MessageModel
from pika.delivery_mode import DeliveryMode

from frasq import FrasQApp

app = FrasQApp(
    username=settings.user,
    password=settings.password,
    host=settings.host,
    port=settings.port,
    vhost=settings.vhost
)

@app.publisher(
    exchange_name=settings.exchange_name,
    routing_key=settings.routing_key_pydantic,
    max_retries=settings.max_retries,
    retry_delay=settings.retry_delay,
    delivery_mode=DeliveryMode.Transient,
)
def send_pydantic_message() -> MessageModel:
    return MessageModel(message='Hello! This is a message from the producer')

@app.publisher(
    exchange_name=settings.exchange_name,
    routing_key=settings.routing_key_string,
    max_retries=settings.max_retries,
    retry_delay=settings.retry_delay,
    delivery_mode=DeliveryMode.Transient,
)
def send_string_message() -> str:
    """Function to send a message to the specified exchange and routing key.

    Returns:
        str: The message to be sent.
    """
    return 'Hello! This is a message from the producer'

@app.publisher(
    exchange_name=settings.exchange_name,
    routing_key=settings.routing_key_json,
    max_retries=settings.max_retries,
    retry_delay=settings.retry_delay,
    delivery_mode=DeliveryMode.Transient,
)
def send_json_message() -> dict:
    """Function to send a JSON message to the specified exchange and routing key.

    Returns:
        dict: The JSON message to be sent.
    """
    return {'message': 'Hello! This is a JSON message from the producer'}

@app.publisher(
    exchange_name=settings.exchange_name,
    routing_key=settings.routing_key_protobuf,
    max_retries=settings.max_retries,
    retry_delay=settings.retry_delay,
    delivery_mode=DeliveryMode.Transient,
)
def send_protobuf_message() -> BasicMessage:
    """Function to send a protobuf message to the specified exchange and routing key.

    Returns:
        BasicMessage: The protobuf message to be sent.
    """
    msg = BasicMessage()
    msg.message = 'Hello! This is a Protobuf message from the producer'
    return msg

def main() -> None:
    try:
        send_string_message()
        send_pydantic_message()
        send_json_message()
        send_protobuf_message()
    except KeyboardInterrupt:
        print('\n\t[*] Exiting...')
    except Exception as e:
        print(f'\t[!] Error occurred: {e}')
    finally:
        app.close()

if __name__ == '__main__':
    main()
