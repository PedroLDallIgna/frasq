import os
from typing import NamedTuple
from dotenv import load_dotenv

load_dotenv()

class RabbitMQSettings(NamedTuple):
    """Typed data structure to store RabbitMQ settings."""
    host: str
    port: int
    user: str
    password: str
    vhost: str
    queue_name: str
    exchange_name: str
    exchange_type: str
    routing_key: str
    max_retries: int
    retry_delay: int
    
settings = RabbitMQSettings(
    host=os.getenv('RABBITMQ_HOST', 'localhost'),
    port=int(os.getenv('RABBITMQ_PORT', 5672)),
    user=os.getenv('RABBITMQ_USER', 'guest'),
    password=os.getenv('RABBITMQ_PASSWORD', 'guest'),
    vhost=os.getenv('RABBITMQ_VHOST', '/'),
    queue_name=os.getenv('RABBITMQ_QUEUE_NAME', 'tasks_queue'),
    exchange_name=os.getenv('RABBITMQ_EXCHANGE_NAME', 'tasks_exchange'),
    exchange_type=os.getenv('RABBITMQ_EXCHANGE_TYPE', 'direct'),
    routing_key=os.getenv('RABBITMQ_ROUTING_KEY', 'task_routing_key'),
    max_retries=int(os.getenv('RABBITMQ_MAX_RETRIES', 5)),
    retry_delay=int(os.getenv('RABBITMQ_RETRY_DELAY', 3))
)
