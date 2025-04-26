import csv
import json
import logging
import sys
from datetime import datetime
from docker_services.dags.planning_utils import *
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, UnrecognizedBrokerVersion


def load(path, reconstruction_key, ir, logger):
    """
    Method  converts the transformed internal representation back to its original file type and writes its contents to the output folder

    :param1 path: the source's file name and type
    :param1 reconstruction_key: the reconstruction key required if the original file type is of type JSON
    :param1 ir: the transformed internal representation
    :param1 logger: logger object required to display messages about the method's progress
    :return: None
    """
    # extract the source file's name and type from the path
    filename = path.split("\\")[-1]
    file_ext = filename.split(".")[-1]

    if file_ext == "csv":

        # if the original file is structured, simply dumpt the the internal representation to a csv file
        with open("output\\" + filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(ir) 
        logger.info(f"Successfully loaded the transformed file to '\\output'")
        
    elif file_ext == "json":

        # initialise empty list of transformed json objects, i.e., the contents of the internal represenation
        reconstructed_objs = [] 

        keys = ir[0] # extract the internal representation's column headers, corresponding to the union of keys of the json objects 

        # iterate the internal representation's values and convert each row into a json object, discarding extended key-value pairs
        for row in ir[1:]:
            obj = {}
            for key, value in zip(keys, row):
                if value != '_ext_':
                    obj[key] = value
            reconstructed_objs.append(obj)

        with open(path, 'r') as file:
            dictionary = json.load(file)

        # read over the json file, making sure to keep non-transformed attributes
        if reconstruction_key and reconstruction_key in dictionary:
            dictionary[reconstruction_key] = reconstructed_objs

            with open("output\\" + filename, "w") as file:
                json.dump(dictionary, file, indent=4)

        else:
            # edge case occurs when the source json is just a list of objects
            with open("output\\" + filename, "w") as file:
                json.dump(reconstructed_objs, file, indent=4)

        logger.info(f"Successfully loaded the transformed file to '\\output'")

    else:
        logger.error(f"File type '{file_ext}' not supported")
        

if __name__ == "__main__":

    try:
        # instantiate a consumer and a producer to interact with the Kafka broker
        consumer = KafkaConsumer('optimalPlans', bootstrap_servers=['localhost:9092'])
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), key_serializer=lambda k: k.encode('utf-8'))

        # define the root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(name)s %(levelname)s - %(asctime)s : %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_handler = logging.FileHandler(f"logs\\{timestamp}.logs")
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        root_logger.info("Connected to broker")

        # poll the Kafka broker until a transformation is published
        message = consumer.poll()
        while not message:
            message = consumer.poll()   

        for partition, consumer_records in message.items():
            for record in consumer_records:
                if record.key == b'plan':
                    
                    # read the transformation plan consumed from the broker
                    plan = json.loads(record.value.decode('utf-8'))['plan']
                    root_logger.info(f"Received optimal plan -> {plan}")

                    # read the entire source file into an internal representation and infer its schema
                    ir = to_internal("input\\source\\" + plan[0])[1]
                    schema = infer_schema(ir)

                    # compute the pre-ETL metrics  
                    pre_etl_metrics = {
                        "from" : "pre_etl_pipeline", 
                        "contents": compute_etl_metrics(ir, schema) 
                    }
                    
                    # publish the pre-ETL metrics to the Kafka broker
                    producer.send("etlMetrics", key="metrics", value=pre_etl_metrics)
                    root_logger.info(f"Published the following metrics to Reporter : {pre_etl_metrics}")

                    try:
                        # apply the transformation plan to the source file specified within the plan
                        filepath, reconstruction_key, ir = apply_etl_plan(plan)
                        schema = infer_schema(ir)

                        if not ir: 
                            # occurs when the apply_etl_plan method fails
                            print("No IR detected!")
                            raise BaseException()
                        
                        else:
                            root_logger.info(f"Applied plan to file '{filepath}'")

                            # load the transformed internal representation to the output folder
                            print("loading...")
                            load(filepath, reconstruction_key, ir, root_logger)
                            print("loading done")

                            # compute post-ETL metrics
                            post_etl_metrics = {
                                "from" : "post_etl_pipeline", 
                                "contents": compute_etl_metrics(ir, schema) # returns a dictionary of metrics {}
                            }

                            # publish post-ETL metrics
                            producer.send("etlMetrics", key="metrics", value=post_etl_metrics)
                            root_logger.info(f"Published the following metrics to Reporter : {post_etl_metrics}")

                    except BaseException as e:
                        root_logger.error(f"Error occured during the transform or load phase : {e}")
                        
                        # compute default metrics in case of ETL failure while trying to apply the transformation plan
                        post_etl_metrics = {
                            "from" : "post_etl_pipeline", 
                            "contents" : {
                                'missing': {'missing_cells_percent': 0.0},
                                'outliers' : {'numerical_outliers_percent' : 0.0},
                                'duplicates': {'duplicate_rows_percent' : 0.0},
                                'dq': 0.0
                            }
                        }
                    
                        # publish the default ETL metrics to the Kafka broker
                        producer.send("etlMetrics", key="metrics", value=post_etl_metrics)
                        root_logger.info(f"Published the following metrics to Reporter : {post_etl_metrics}")

        # close connections between the ETL worker and the Kafka broker
        producer.close()
        consumer.close()

    except (NoBrokersAvailable, UnrecognizedBrokerVersion) as exc:
        root_logger.error("NoBrokersAvailable : no broker could be detected")
        root_logger.error(exc)