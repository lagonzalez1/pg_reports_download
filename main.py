import os
from Config.RabbitMQ import RabbitMQ
from Config.PostgresClient import PostgresClient
from Config.Client import Client
from Parser.TutorParser import TutorParser
from Parser.StudentParser import StudentParser
from S3.main import S3Instance
from dotenv import load_dotenv
import time
import json
import logging

load_dotenv()


# --- Python logger ---
logging.basicConfig(
    level=logging.INFO, # Adjust to logging.DEBUG for more verbose logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

EXCHANGE     = os.getenv("EXCHANGE")
QUEUE        = os.getenv("QUEUE")
ROUTING_KEY  = os.getenv("ROUTING_KEY")
RABBIT_LOCAL  = os.getenv("RABBIT_LOCAL")
PREFETCH_COUNT = 1
EXCHANGE_TYPE = "direct"
TUTOR = "tutor"
STUDENT = "student"
DONE = "DONE"
ZERO = 0


def create_callback(db):
    def on_message_test(channel, method, properties, body):
        print(body)
        client = Client(body)        
        s3 = S3Instance("tracker-client-storage")
        entity = client.get_entity()
        if entity == TUTOR:
            data = db.get_tutor_file_data(client.get_body())
            if data is None:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)      
                return
            tutor_parser = TutorParser(data, client.get_sort_key())
            file = tutor_parser.get_file()
            if file is None:
                db.update_organization_report((DONE, ZERO, client.get_s3_output_key()))
                channel.basic_ack(delivery_tag=method.delivery_tag)      
                return 
            s3.put_object(client.get_s3_output_key(), file)
            db.update_organization_report((DONE, ZERO, client.get_s3_output_key()))
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  
        elif entity == STUDENT:
            student_sessions = db.get_student_sessions(client.get_body())
            student_assesments = db.get_student_assessments(client.get_body())
            if student_sessions is None:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            student_parser = StudentParser(student_sessions, student_assesments, client.get_sort_key(), client.get_data_type())
            file = student_parser.get_file()
            if file is None:
                db.update_organization_report((DONE, ZERO, client.get_s3_output_key()))
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return 
            s3.put_object(client.get_s3_output_key(), file)
            db.update_organization_report((DONE, ZERO, client.get_s3_output_key()))
            channel.basic_ack(delivery_tag=method.delivery_tag)

    return on_message_test


def main():
    mq = RabbitMQ(PREFETCH_COUNT, EXCHANGE, QUEUE, ROUTING_KEY, EXCHANGE_TYPE)
    db = PostgresClient()
    callback = create_callback(db)
    mq.set_callback(callback)
    channel = mq.get_channel()
    connection = mq.get_connection()
    try:
        logging.info(f"RabbitMQ consuming on {QUEUE} with routing key {ROUTING_KEY}")
        channel.start_consuming()
    except KeyboardInterrupt as e:
        logging.error("Error occured unable to start consuming from RabbitMQ")
    finally:
        channel.close()
        connection.close()
        db.close()


if __name__ == "__main__":
    main()