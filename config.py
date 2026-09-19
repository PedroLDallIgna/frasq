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
    
settings = RabbitMQSettings(
    host=os.getenv('RABBITMQ_HOST', 'localhost'),
    port=int(os.getenv('RABBITMQ_PORT', 5672)),
    user=os.getenv('RABBITMQ_USER', 'guest'),
    password=os.getenv('RABBITMQ_PASSWORD', 'guest'),
    vhost=os.getenv('RABBITMQ_VHOST', '/'),
    queue_name=os.getenv('RABBITMQ_QUEUE_NAME', 'tasks_queue'),
)
