from rabbitmq_client import RabbitMQClient
from config import settings

def main() -> None:
    client = RabbitMQClient()
    
    try:
        message: str = 'Hello! This is a message from the producer'
        client.connect()
        client.publish(
            message=message,
            exchange_name=settings.exchange_name,
            exchange_type=settings.exchange_type,
            queue_name=settings.queue_name,
            routing_key=settings.routing_key
        )
    except Exception as e:
        print(f'\t[!] Error occurred: {e}')
    finally:
        client.close()


if __name__ == '__main__':
    main()
