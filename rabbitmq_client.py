import pika
from config import settings
from typing import Callable

from pika.credentials import PlainCredentials
from pika.connection import ConnectionParameters
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties

class RabbitMQClient:
    """
    Utility class to abstract the management of RabbitMQ connection, channels,
    publishing and consuming messages. It provides a simple interface to interact with RabbitMQ. 
    """
    _credentials: PlainCredentials
    _parameters: ConnectionParameters
    _connection: BlockingConnection | None
    _channel: BlockingChannel | None
    
    def __init__(self) -> None:
        self._credentials = pika.PlainCredentials(
            username=settings.user,
            password=settings.password
        )
        self._parameters = pika.ConnectionParameters(
            host=settings.host,
            port=settings.port,
            virtual_host=settings.vhost,
            credentials=self._credentials
        )
        self._connection = None
        self._channel = None
        
    def connect(self) -> None:
        """Establishes a connection to RabbitMQ and opens a channel."""
        if not self._connection or self._connection.is_closed:
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
        
    
    def declare_queue(self, queue_name: str) -> None:
        """Declares a queue in RabbitMQ."""
        if not self._channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        assert self._channel is not None  # For type checking
        self._channel.queue_declare(queue=queue_name)
    
    def publish(self, message: str, queue_name: str) -> None:
        """Publishes a message to the specified queue."""
        self.declare_queue(queue_name)
        assert self._channel is not None  # For type checking
        
        self._channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message
        )
        print(f'\t[X] Message sent to \'{queue_name}\': \'{message}\'')
        
    def consume(
        self,
        callback_function: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None],
        queue_name: str,
    ) -> None:
        """Starts consuming messages from the specified queue using the provided callback function."""
        self.declare_queue(queue_name)
        assert self._channel is not None  # For type checking
        
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback_function,
            auto_ack=True,
        )
        print(f'\t[*] Waiting for messages from \'{queue_name}\'. To exit press CTRL+C')
        self._channel.start_consuming()
        
    def close(self) -> None:
        """Closes the channel and connection to RabbitMQ."""
        if self._channel and not self._channel.is_closed:
            self._channel.close()
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        print('\t[i] Connection closed.')