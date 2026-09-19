from rabbitmq_client import RabbitMQClient
from config import settings

def main() -> None:
    client = RabbitMQClient()
    
    try:
        message: str = 'Hello! This is a message from the producer'
        client.connect()
        client.publish(message=message, queue_name=settings.queue_name)
    except Exception as e:
        print(f'\t[!] Error occurred: {e}')
    finally:
        client.close()


if __name__ == '__main__':
    main()
