from config import settings

from rabbitmq_app import RabbitMQApp

app = RabbitMQApp(
    username=settings.user,
    password=settings.password,
    host=settings.host,
    port=settings.port,
    vhost=settings.vhost
)

@app.publisher(
    exchange_name=settings.exchange_name,
    routing_key=settings.routing_key,
    max_retries=settings.max_retries,
    retry_delay=settings.retry_delay
)
def send_message() -> str:
    """Function to send a message to the specified exchange and routing key.

    Returns:
        str: The message to be sent.
    """
    return 'Hello! This is a message from the producer'

def main() -> None:
    try:
        send_message()
    except KeyboardInterrupt:
        print('\n\t[*] Exiting...')
    except Exception as e:
        print(f'\t[!] Error occurred: {e}')
    finally:
        app.close()

if __name__ == '__main__':
    main()
