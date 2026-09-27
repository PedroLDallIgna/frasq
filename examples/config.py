import os
from typing import Any, Callable, TypeVar
from dotenv import load_dotenv
import functools

load_dotenv()

R = TypeVar('R')

def env_property(key: str, *, default: str, env_type: type = str):
    def decorator(func: Callable[[Any], R]) -> property:
        @property
        @functools.wraps(func)
        def getter(self: Any) -> R:
            raw_val = os.getenv(key)

            if raw_val is None:
                return env_type(default)            
            
            try:
                if env_type == bool:
                    return raw_val.lower() in ('true', '1', 'yes')
                return env_type(raw_val)
            except (ValueError, TypeError):
                return raw_val
            
        return getter
    
    return decorator

class Settings:
    @env_property('RABBITMQ_HOST', default='localhost', env_type=str)
    def host(self) -> str:
        ...
    
    @env_property('RABBITMQ_PORT', default='5672', env_type=int)
    def port(self) -> int:
        ...
        
    @env_property('RABBITMQ_USER', default='guest', env_type=str)
    def user(self) -> str:
        ...
        
    @env_property('RABBITMQ_PASSWORD', default='guest', env_type=str)
    def password(self) -> str:
        ...
        
    @env_property('RABBITMQ_VHOST', default='/', env_type=str)
    def vhost(self) -> str:
        ...
        
    @env_property('RABBITMQ_QUEUE_NAME', default='tasks_queue', env_type=str)
    def queue_name(self) -> str:
        ...
        
    @env_property('RABBITMQ_EXCHANGE_NAME', default='tasks_exchange', env_type=str)
    def exchange_name(self) -> str:
        ...
        
    @env_property('RABBITMQ_EXCHANGE_TYPE', default='direct', env_type=str)
    def exchange_type(self) -> str:
        ...
        
    @env_property('RABBITMQ_ROUTING_KEY_STRING', default='rk.string', env_type=str)
    def routing_key_string(self) -> str:
        ...
        
    @env_property('RABBITMQ_ROUTING_KEY_JSON', default='rk.json', env_type=str)
    def routing_key_json(self) -> str:
        ...
        
    @env_property('RABBITMQ_ROUTING_KEY_PYDANTIC', default='rk.pydantic', env_type=str)
    def routing_key_pydantic(self) -> str:
        ...
        
    @env_property('RABBITMQ_ROUTING_KEY_PROTOBUF', default='rk.protobuf', env_type=str)
    def routing_key_protobuf(self) -> str:
        ...
        
    @env_property('RABBITMQ_MAX_RETRIES', default='5', env_type=int)
    def max_retries(self) -> int:
        ...
        
    @env_property('RABBITMQ_RETRY_DELAY', default='3', env_type=int)
    def retry_delay(self) -> int:
        ...

settings = Settings()
