import sys
import csv
import json
import logging
from kafka.errors import NoBrokersAvailable, UnrecognizedBrokerVersion
from kafka import KafkaConsumer, KafkaProducer
from docker_services.dags.planning_utils import *
from datetime import datetime

def load(path, reconstruction_key, ir, logger):

    print("This is the IR fed into the load method ")
    print(ir)


    # extract the filename from path
    filename = path.split("\\")[-1]
    file_ext = filename.split(".")[-1]

    if file_ext == "csv":
        with open("output\\" + filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(ir) 
        logger.info(f"Successfully loaded the transformed file to '\\output'")
        
    elif file_ext == "json":

        reconstructed_objs = [] # the list of transformed objects
        keys = ir[0]
        for row in ir[1:]:
            obj = {}
            for key, value in zip(keys, row):
                if value != '_ext_':
                    obj[key] = value
            reconstructed_objs.append(obj)

        # read over json file, making sure to keep non-transformed attributes
        with open(path, 'r') as file:
            dictionary = json.load(file)

        if reconstruction_key and reconstruction_key in dictionary:
            dictionary[reconstruction_key] = reconstructed_objs

        with open("output\\" + filename, "w") as file:
            json.dump(dictionary, file, indent=4)

        logger.info(f"Successfully loaded the transformed file to '\\output'")

    else:
        logger.error(f"File type '{file_ext}' not supported")
        


if __name__ == "__main__":

    try:
        consumer = KafkaConsumer('optimalPlans', bootstrap_servers=['localhost:9092'])
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), key_serializer=lambda k: k.encode('utf-8'))

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

        message = consumer.poll()
        while not message:
            message = consumer.poll()   

        for partition, consumer_records in message.items():
            for record in consumer_records:
                if record.key == b'plan':
                    
                    plan = json.loads(record.value.decode('utf-8'))['plan']
                    root_logger.info(f"Received optimal plan -> {plan}")

                    # compute the internal representation and schema of the raw source file, NOTE that this is repeated work and can be recycled
                    ir = to_internal("input\\source\\" + plan[0])[1]
                    schema = infer_schema(ir)
                    
                    print()
                    print()
                    print(ir)
                    print()
                    print()
                    print(schema)
                    print()

                    # extract the pre_etl_metrics 
                    pre_etl_metrics = {
                        "from" : "pre_etl_pipeline", 
                        "contents": compute_etl_metrics(ir, schema) # returns a dictionary of metrics {}
                    }
                    
                    producer.send("etlMetrics", key="metrics", value=pre_etl_metrics)
                    root_logger.info(f"Published the following metrics to Reporter : {pre_etl_metrics}")

                    try:
                        filepath, reconstruction_key, ir = apply_etl_plan(plan)
                        schema = infer_schema(ir)
                        if not ir: 
                            print("No IR detected!")
                            raise BaseException()
                        
                        else:
                            root_logger.info(f"Applied plan to file '{filepath}'")

                            print("loading...")
                            load(filepath, reconstruction_key, ir, root_logger)
                            print("loading done")

                            post_etl_metrics = {
                                "from" : "post_etl_pipeline", 
                                "contents": compute_etl_metrics(ir, schema) # returns a dictionary of metrics {}
                            }
                        
                            producer.send("etlMetrics", key="metrics", value=post_etl_metrics)
                            root_logger.info(f"Published the following metrics to Reporter : {post_etl_metrics}")

                    except BaseException as e:
                        root_logger.error(f"Error occured during the transform or load phase : {e}")
                        post_etl_metrics = {
                            "from" : "post_etl_pipeline", 
                            "contents" : {
                                'missing': {'missing_cells_percent': 0.0},
                                'outliers' : {'numerical_outliers_percent' : 0.0},
                                'duplicates': {'duplicate_rows_percent' : 0.0},
                                'dq': 0.0
                            }
                        }
                    
                        producer.send("etlMetrics", key="metrics", value=post_etl_metrics)
                        root_logger.info(f"Published the following metrics to Reporter : {post_etl_metrics}")

        producer.close()
        consumer.close()

    except (NoBrokersAvailable, UnrecognizedBrokerVersion) as exc:
        root_logger.error("NoBrokersAvailable : no broker could be detected")
        root_logger.error(exc)