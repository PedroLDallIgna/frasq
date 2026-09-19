import time

import pika
from config import settings
from typing import Callable

from pika.credentials import PlainCredentials
from pika.connection import ConnectionParameters
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties
from pika.exceptions import AMQPConnectionError, StreamLostError, AMQPChannelError

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
        if self._connection and self._connection.is_open and \
            self._channel and self._channel.is_open:
            return

        attempts: int = 0
        while attempts < settings.max_retries:
            try:
                attempts += 1
                print(f'\t[i] Attempting to connect to RabbitMQ (Attempt {attempts}/{settings.max_retries})...')
                
                self._connection = pika.BlockingConnection(self._parameters)
                self._channel = self._connection.channel()
                
                print('\t[i] Connection established successfully.')
                return
                
            except (AMQPConnectionError, StreamLostError) as e:
                print(f'\t[!] Connection attempt {attempts} failed: {e}')
                if attempts < settings.max_retries:
                    print(f'\t[i] Retrying in {settings.retry_delay} seconds...')
                    time.sleep(settings.retry_delay)
                else:
                    print('\t[!] Maximum connection attempts reached. Could not connect to RabbitMQ.')
                    raise RuntimeError("Failed to connect to RabbitMQ after multiple attempts.") from e
    
    def declare_queue(self, queue_name: str) -> None:
        """Declares a queue in RabbitMQ."""
        if not self._channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        assert self._channel is not None  # For type checking
        
        try:
            self._channel.queue_declare(queue=queue_name)
        except AMQPChannelError as e:
            print(f'\t[!] Failed to declare queue \'{queue_name}\': {e}')
            raise RuntimeError(f"Failed to declare queue '{queue_name}'.") from e
    
    def publish(self, message: str, queue_name: str) -> None:
        """Publishes a message to the specified queue."""
        
        try: 
            self.declare_queue(queue_name)
            assert self._channel is not None  # For type checking
            
            self._channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message
            )
            print(f'\t[X] Message sent to \'{queue_name}\': \'{message}\'')
        except (AMQPConnectionError, AMQPChannelError, StreamLostError, RuntimeError) as e:
            print(f'\t[!] Failed to publish message: {e}. Retrying...')
            self._connection = None
            self._channel = None
            self.connect()  # Attempt to reconnect
            assert self._channel is not None  # For type checking
            self.declare_queue(queue_name)
            
            self._channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message
            )
            print(f'\t[X] Message sent to \'{queue_name}\': \'{message}\' after retrying.')
        
    def consume(
        self,
        callback_function: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None],
        queue_name: str,
        auto_ack: bool = False
    ) -> None:
        """Starts consuming messages from the specified queue using the provided callback function."""
        while True:
            try: 
                self.declare_queue(queue_name)
                assert self._channel is not None  # For type checking
                
                self._channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=callback_function,
                    auto_ack=auto_ack,
                )
                print(f'\t[*] Waiting for messages from \'{queue_name}\'. To exit press CTRL+C')
                self._channel.start_consuming()
            except (AMQPConnectionError, AMQPChannelError, StreamLostError, RuntimeError) as e:
                print(f'\t[!] Error during consuming messages: {e}')
                print('\t[i] Attempting to reconnect and resume consuming...')
                self._connection = None
                self._channel = None
                self.connect()  # Attempt to reconnect
                time.sleep(settings.retry_delay)
            except KeyboardInterrupt:
                print('\n\t[*] Exiting...')
                break
                        
    def close(self) -> None:
        """Closes the channel and connection to RabbitMQ."""
        if self._channel and not self._channel.is_closed:
            self._channel.close()
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        print('\t[i] Connection closed.')