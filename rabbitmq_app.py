import time
import pika
import functools
import json

from typing import Any, Callable, TypeVar

from pika.credentials import PlainCredentials
from pika.connection import ConnectionParameters
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.exceptions import AMQPConnectionError, StreamLostError
from pika.spec import Basic, BasicProperties

F = TypeVar('F', bound=Callable[..., Any])
MessageHandler = Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]

class RabbitMQApp:
    """
    A structure like Flask/FastAPI to manager publishers and consumers of RabbitMQ,
    making use of Python decorators.
    """
    _credentials: PlainCredentials
    _parameters: ConnectionParameters
    _connection: BlockingConnection | None
    _channel: BlockingChannel | None
    _max_retries: int = 5
    
    def __init__(
        self,
        username: str = 'guest',
        password: str = 'guest',
        host: str = 'localhost',
        port: int = 5672,
        vhost: str = '/',
    ) -> None:
        self._credentials = pika.PlainCredentials(
            username=username,
            password=password
        )
        self._parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=self._credentials
        )
        self._connection: BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        
        self._subscribers: list[dict[str, Any]] = []
        
    def _connect(self, max_retries: int, retry_delay: float) -> None:
        """
        Establishes a connection to RabbitMQ and opens a channel.
        
        Parameters:
            max_retries (int): Maximum number of connection attempts before giving up.
            retry_delay (float): Delay in seconds between connection attempts.
        """
        if self._connection and self._connection.is_open and \
            self._channel and self._channel.is_open:
            return
        
        attempts: int = 0
        while attempts < max_retries:
            try:
                attempts += 1
                print(f'\t[i] Attempting to connect to RabbitMQ (Attempt {attempts}/{max_retries})...')
                
                self._connection = pika.BlockingConnection(self._parameters)
                self._channel = self._connection.channel()
                
                print('\t[i] Connection established successfully.')
                return
                
            except (AMQPConnectionError, StreamLostError) as e:
                print(f'\t[!] Connection attempt {attempts} failed: {e}')
                if attempts < max_retries:
                    print(f'\t[i] Retrying in {retry_delay} seconds...')
                    time.sleep(retry_delay)
                else:
                    raise
        
        
    def subscriber(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
        auto_ack: bool = False,
    ) -> Callable[[MessageHandler], MessageHandler]:
        """
        Decorator to register a function as a subscriber to a specific queue and exchange.
        
        Parameters:
            queue_name (str): The name of the queue to subscribe to.
            exchange_name (str): The name of the exchange to bind the queue to.
            routing_key (str): The routing key for binding the queue to the exchange.
            auto_ack (bool): Whether to automatically acknowledge messages. Defaults to False.
        """
        def decorator(func: MessageHandler) -> MessageHandler:
            self._subscribers.append({
                'func': func,
                'queue_name': queue_name,
                'exchange_name': exchange_name,
                'routing_key': routing_key,
                'auto_ack': auto_ack
            })
            
            @functools.wraps(func)
            def wrapper(
                ch: BlockingChannel,
                method: Basic.Deliver,
                properties: BasicProperties,
                body: bytes
            ) -> None:
                return func(ch, method, properties, body)
            
            return wrapper
            
        return decorator
    
    def _publish(
        self,
        message: str,
        exchange_name: str,
        routing_key: str,
        max_retries: int = 5,
        retry_delay: float = 3.0
    ) -> None:
        """
        Publishes a message to the specified exchange with the given routing key.
        
        Parameters:
            message (str): The message to be published.
            exchange_name (str): The name of the exchange to publish to.
            routing_key (str): The routing key for the message.
            max_retries (int): The maximum number of times to retry the connection. Defaults to 5.
            retry_delay (float): The delay in seconds between retry attempts. Defaults to 3.0.
        """
        self._connect(max_retries=max_retries, retry_delay=retry_delay)
        assert self._channel is not None  # For type checking
        
        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        print(f'\t[X] Message sent to \'{exchange_name}\' exchange: \'{message}\'')
        self.close()
        
    def publisher(
        self,
        exchange_name: str,
        routing_key: str,
        max_retries: int = 5,
        retry_delay: float = 3.0
    ) -> Callable[[F], F]:
        """
        Decorator to register a function as a publisher to a specific exchange and routing key.
        
        Parameters:
            exchange_name (str): The name of the exchange to publish to.
            routing_key (str): The routing key for the message.
            max_retries (int): The maximum number of times to retry the connection. Defaults to 5.
            retry_delay (float): The delay in seconds between retry attempts. Defaults to 3.0.
        """
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = func(*args, **kwargs)
                
                if result is not None:
                    if isinstance(result, (dict, list)):
                        message_body = json.dumps(result)
                    else:
                        message_body = str(result)
                    
                    self._publish(
                        message=message_body,
                        exchange_name=exchange_name,
                        routing_key=routing_key,
                        max_retries=max_retries,
                        retry_delay=retry_delay
                    )
                
                return result
            
            return wrapper
        
        return decorator
    
    def run(self, max_retries: int = 5, retry_delay: float = 3.0) -> None:
        """
        Starts consuming messages for all registered subscribers.
        """
        if not self._subscribers:
            print('\t[!] No subscribers registered. Exiting...')
            return
        
        print('\t[i] Starting RabbitMQApp...')
        
        while True:
            try:
                self._connect(max_retries=max_retries, retry_delay=retry_delay)
                assert self._channel is not None
                
                for sub in self._subscribers:
                    self._channel.queue_declare(queue=sub['queue_name'], durable=True)
                    self._channel.exchange_declare(exchange=sub['exchange_name'], exchange_type='direct', durable=True)
                    self._channel.queue_bind(
                        exchange=sub['exchange_name'],
                        queue=sub['queue_name'],
                        routing_key=sub['routing_key']
                    )
                    self._channel.basic_consume(
                        queue=sub['queue_name'],
                        on_message_callback=sub['func'],
                        auto_ack=sub['auto_ack']
                    )
                    print(f'\t[i] Subscribed to queue \'{sub["queue_name"]}\' on exchange \'{sub["exchange_name"]}\' with routing key \'{sub["routing_key"]}\'.')
                
                print('\t[i] Waiting for messages. To exit press CTRL+C')
                self._channel.start_consuming()
                
            except (AMQPConnectionError, StreamLostError) as e:
                print(f'\t[!] Connection lost: {e}. Retrying in {retry_delay} seconds...')
                self._connection = None
                self._channel = None
                time.sleep(retry_delay)
            except KeyboardInterrupt:
                print('\n\t[*] Exiting...')
                break
            
    def close(self) -> None:
        """
        Closes the channel and connection to RabbitMQ.
        """
        if not self._channel and not self._connection:
            print('\t[i] No active connection or channel to close.')
            return
        
        if self._channel and self._channel.is_open:
            self._channel.stop_consuming()
            self._channel.close()
            self._channel = None
            print('\t[i] Channel closed.')
        
        if self._connection and self._connection.is_open:
            self._connection.close()
            self._connection = None
            print('\t[i] Connection closed.')
                
    