import os
import csv
import json
import random
import logging
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class BaseObserver:
 
    def __init__(self, role, directory, kafka_producer, logger):
        self.observer = Observer()
        self.logger = logger
        self.role = role
        self.directory = directory
        self.kafka_producer = kafka_producer
        self.upload_detected = False

    def run(self):
        event_handler = EventHandler(self.kafka_producer, self.logger, self.role, self)
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()
        try:
            while not self.upload_detected:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
        finally:
            self.observer.stop()
            self.observer.join()

    def stop(self):
        self.observer.stop()
        self.observer.join()


class EventHandler(FileSystemEventHandler):
 
    def __init__(self, kafka_producer, logger, role, observer_instance):
        self.kafka_producer = kafka_producer
        self.logger = logger
        self.role = role
        self.observer_instance = observer_instance

    def on_any_event(self, event):
        
        if not event.is_directory and event.event_type == 'created':

            filepath = event.src_path
            filename = filepath.split("\\")[-1]
            filesize = round(os.path.getsize(filepath) / (1024 * 2), 2)

            self.logger.info(f"detected file '{filename}'")

            # compute the internal representation of the file's contents
            associated_key, internal_representation = to_internal(filepath)

            if not internal_representation:
                self.logger.warning("No data could be detected in the uploaded file")
                return 
            
            # extract a sample of the internal representation
            sampled_IR = extract_sample(internal_representation)
            serialised_IR =  json.dumps(sampled_IR, indent=4)
            objects_count = len(internal_representation)-1

            # observer metrics
            observer_metrics = {
                "from" : "source_observer", 
                "contents" : {'filename' : filename, 'objects_count': objects_count, 'filesize_mbs' : filesize}
            }

            if self.role == 's':
                self.logger.info(f"Extracted {objects_count} objects from the uploaded file")
                self.kafka_producer.send("etlMetrics", key="metrics", value=observer_metrics)
                self.logger.info(f"Published the following metrics to Reporter : {observer_metrics}")

                self.kafka_producer.send("inputSamples", key="data", value={"name" : filename, "associatedKey" : associated_key , "contents" : serialised_IR})
                self.logger.info(f"Published the following information to broker - filename : '{filename}', associatedKey : '{associated_key}', contents (hidden) : {objects_count}")

            if self.role == 't':

                observer_metrics['contents']['objects_count'] = len(internal_representation)-1
                self.logger.info(f"Extracted {len(internal_representation)-1} objects from the uploaded file")
                observer_metrics['from'] = "target_observer"
                self.kafka_producer.send("etlMetrics", key="metrics", value=observer_metrics)

                self.logger.info(f"Published the following metrics to Reporter : {observer_metrics}")
                self.kafka_producer.send("targetSamples", key="data", value={"name" : filename, "contents" : json.dumps(internal_representation, indent=4)})
                self.logger.info(f"Published the following information to broker - filename : '{filename}', contents (hidden) : {len(internal_representation)-1}")

            self.observer_instance.upload_detected = True



def extract_list(collection, key):
    if isinstance(collection, list): 
        return (key, collection)
    if isinstance(collection, dict):
        for k, value in collection.items():
            result = extract_list(value, k)
            if result: 
                return result

def to_internal(filepath):

    internal_representation = [] 

    # extract the file type
    filetype = filepath.split(".")[-1]
    filename = filepath.split("\\")[-1]

    if filetype == 'csv':
        with open(filepath, 'r', encoding='utf=8-sig') as file:
            internal_representation = [ row for row in csv.reader(file)]
            return (None, internal_representation)
            
    elif filetype == 'json':

        try:
            with open(filepath, 'r') as file:
                dictionary = json.load(file)
        except json.decoder.JSONDecodeError as err:
            logging.warning(f"json.decoder.JSONDecodeError : file '{filename}' does not contain a collection of objects")
            return (None, None)

        # extract the first list of objects
        l = extract_list(dictionary, key=None) 
        associated_key, objects = l if l else (None, None)

        if not objects:
            logging.warning(f"file '{filename}' does not contain a collection of objects")
            return (None, None)

        # construct a union of all attributes across objects
        attributes_union = list(set(attribute for object in objects for attribute in object.keys()))

        # set the attributes_union as the column headers of the internal representation
        internal_representation.append(attributes_union)

        for object in objects:

            # extend each object's attribute set to match the attributes_union
            extended_object_values = []
            for attribute in attributes_union:
                extended_object_values.append(object[attribute] if attribute in object else '_ext_')
            internal_representation.append(extended_object_values)

        return (associated_key, internal_representation)
    
    else:
        logging.error(f"file type '{filetype}' not supported")
        return (None, None)
    

def extract_sample(internal_representation, p=0.05):

    logging.info(f"Extraction sample percentage set to {p * 100}%")

    sampled_internal_representation = []
    # add the headers, since we do not want to sample them
    headers = internal_representation[0]
    sampled_internal_representation.append(headers)

    # choose sample of rows

    cap = 50 # cap the IR sample to 50 entries
    
    sampled_row_indices = random.sample(range(1, len(internal_representation)), int( min(cap, (len(internal_representation)-1) * p) ))
    for row_index in sampled_row_indices:
        sampled_internal_representation.append(internal_representation[row_index])

    return sampled_internal_representation
