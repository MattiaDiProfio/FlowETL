import sys
import json
import logging
from kafka import KafkaProducer # https://github.com/wbarnha/kafka-python-ng
from kafka.errors import NoBrokersAvailable
from observer_utils import BaseObserver
from threading import Thread
from datetime import datetime


if __name__ == "__main__":

    try:    
        kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), key_serializer=lambda k: k.encode('utf-8'))

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(name)s %(levelname)s - %(asctime)s : %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

        # Create file handler for logging to a file
        # get current timestamp, and use it to name the file in the logs folder
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        file_handler = logging.FileHandler(f"logs\\{timestamp}.logs")
        file_handler.setFormatter(formatter)

        # Create console handler for logging to the console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        # Add both handlers to the root logger
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        root_logger.info("Connected to broker")

        source_logger = logging.getLogger("SourceObserver")
        target_logger = logging.getLogger("TargetObserver")

        # this assumes the script is invoked within the observers directory!!!
        source_directory = "..\\input\\source"
        target_directory = "..\\input\\target"

        source_observer = BaseObserver("s", source_directory, kafka_producer, source_logger)
        target_observer = BaseObserver("t", target_directory, kafka_producer, target_logger)

        source_logger.info(f"Observer stopped on folder {source_directory}")
        target_logger.info(f"Observer started on folder {target_directory}")

        t1 = Thread(source_observer.run())
        t2 = Thread(target_observer.run())

        source_logger.info(f"Observer stopped")
        target_logger.info(f"Observer stopped")

    except NoBrokersAvailable:
        root_logger.error("NoBrokersAvailable : no broker could be detected")

