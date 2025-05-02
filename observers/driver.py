import json
import logging
import sys
from datetime import datetime
from threading import Thread
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from observer_utils import BaseObserver


if __name__ == "__main__":

    # define root logger object
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    try:    
        # create a producer connection between the observers and the Kafka broker
        kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), key_serializer=lambda k: k.encode('utf-8'))

        # create a file system handler to dump the log messages in a file timestamped with the execution time
        formatter = logging.Formatter('%(name)s %(levelname)s - %(asctime)s : %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_handler = logging.FileHandler(f"logs\\{timestamp}.logs")
        file_handler.setFormatter(formatter)

        # create a console handler to print logger's output to console 
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        # add file system handler and console handler to the logger object
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        root_logger.info("Connected to broker")
        source_logger = logging.getLogger("SourceObserver")
        target_logger = logging.getLogger("TargetObserver")

        # NOTE : this path assumes that the driver.py script is invoked from within the \observers directory
        source_directory = "..\\input\\source"
        target_directory = "..\\input\\target"

        # initialise the source and target observers
        source_observer = BaseObserver("s", source_directory, kafka_producer, source_logger)
        target_observer = BaseObserver("t", target_directory, kafka_producer, target_logger)

        source_logger.info(f"Observer started on folder {source_directory}")
        target_logger.info(f"Observer started on folder {target_directory}")

        # start both observers each on a dedicated process
        t1 = Thread(source_observer.run())
        t2 = Thread(target_observer.run())

        source_logger.info(f"Observer stopped")
        target_logger.info(f"Observer stopped")

        # close connection to Kafka broker 
        kafka_producer.close()
        
    except NoBrokersAvailable:
        root_logger.error("NoBrokersAvailable : no broker could be detected")

