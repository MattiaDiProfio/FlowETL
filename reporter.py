
"""
this reported class listens for information coming from a specific topic of the kafka broker
as the messages come in, the reporter "populates" itself with the information within the payload
once an internal check for "all attributes filled?" returned true, the report "compiles" and the script terminates
"""

import json
import logging
import textwrap
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, UnrecognizedBrokerVersion

class ETLReport:

    def __init__(self, broker):
        # NOTE - if the report is set to verbose, then it will include all "[]" below, otherwise we omit them to keep the report concise

        self.broker = broker

        self.start_datetime = datetime.now() # start datetime of the etl run, i.e. when the first file is uploaded to the input 

        self.source_info = { # holds information about the source file (not the sample, the actual file)
            'filename' : '',
            'objects_count': 0,
            'filesize_mbs' : 0,
        }

        self.target_info = { # holds information about the target_file
            'filename' : '',
            'objects_count': 0,
            'filesize_mbs' : 0,
        }

        self.planning_engine_info = {
            'plans_computed_count': 0,
            'plans_failed_count': 0,
            'max_dq_achieved': 0,
            'best_plan': []
        }

        # this is the pre-etl run info
        self.pre_etl_run_info = {
            'missing': {'missing_cells_percent': 0},
            'outliers' : {'numerical_outliers_percent' : 0},
            'duplicates': {'duplicate_rows_percent' : 0},
            'dq': 0
        }

        # this is the post-etl run info
        self.post_etl_run_info = {
            'missing': {'missing_cells_percent': 0},
            'outliers' : {'numerical_outliers_percent' : 0},
            'duplicates': {'duplicate_rows_percent' : 0},
            'dq': 0
        }

        self.source_info_populated = False
        self.target_info_populated = False
        self.planning_info = False
        self.pre_etl_info = False
        self.post_etl_info = False

    # returns true if all report properties are complete, false otherwise
    def is_complete(self):
        return all([self.source_info_populated,self.target_info_populated,self.planning_info,self.pre_etl_info,self.post_etl_info])
    
    # listens to kafka broker until it dies or until the report is fully populated
    def run(self):
        try:
            message = self.broker.poll()
            while not message: 
                message = self.broker.poll()   

            for partition, records in message.items():
                for record in records:

                    if record.key == b'metrics':
                        
                        # read the metrics received
                        
                        data = json.loads(record.value.decode('utf-8'))
                        
                        # update the report if possible
                        data_from = data['from'] # this tells us which component published the metrics
                        contents = data['contents']

                        if data_from == 'source_observer': 
                            self.source_info = contents
                            self.source_info_populated = True
                        if data_from == 'target_observer': 
                            self.target_info = contents
                            self.target_info_populated = True
                        if data_from == 'planning_engine': 
                            self.planning_engine_info = contents
                            self.planning_info = True
                        if data_from == 'pre_etl_pipeline': 
                            self.pre_etl_run_info = contents
                            self.pre_etl_info = True
                        if data_from == 'post_etl_pipeline': 
                            self.post_etl_run_info = contents
                            self.post_etl_info = True
        
            # if the report is full, call its save method 
            # connection, otherwise recursively call populate to keep on listening
            if self.is_complete(): 

                self.save()
                self.broker.close()

            else: self.run() # keep on listening for more metrics, until the report is fully populated

        except (NoBrokersAvailable, UnrecognizedBrokerVersion, ValueError):
            logging.error("NoBrokersAvailable : no broker could be detected")


    # compile instance contents into a report string
    def compile(self):
        if self.is_complete():
            # Calculate the width of the box (including borders)
            box_width = 100 # Based on the header line in your latest example
            
            # Helper function to create a padded line with proper right border alignment
            def format_line(label, value):
                content = f"| {label}: {value}"
                padding = " " * (box_width - len(content) - 1)  # -1 for the end pipe
                return f"{content}{padding}|"
        
            # Helper function to create section headers
            def format_header(title):
                dashes_each_side = (box_width - len(title) - 6) // 2
                return f"+ {'-' * (dashes_each_side+1)}{title}{'-' * (dashes_each_side+1)} +"
            
            # Create the final header line
            def format_final_line():
                return f"+ {'-' * (box_width - 4)} +"
        
            report = textwrap.dedent(f"""\
            {format_header("-------ETL Report-------")}
            {format_line("time-elapsed", f"{(datetime.now() - self.start_datetime).total_seconds()} s")}
            {format_header("---Source File Info---")}
            {format_line("filename", self.source_info['filename'])}
            {format_line("objects-count", self.source_info['objects_count'])}
            {format_line("filesize", f"{self.source_info['filesize_mbs']} MBs")}
            {format_header("---Target File Info---")}
            {format_line("filename", self.target_info['filename'])}
            {format_line("objects-count", self.target_info['objects_count'])}
            {format_line("filesize", f"{self.target_info['filesize_mbs']} MBs")}
            {format_header("-Planning Engine Stats")}
            {format_line("plans-computed", self.planning_engine_info['plans_computed_count'])}
            {format_line("plans-failed", self.planning_engine_info['plans_failed_count'])}
            {format_line("max-data-quality-score", self.planning_engine_info['max_dq_achieved'])}
            {format_line("best-plan", self.planning_engine_info['best_plan'])}
            {format_header("-Pre-ETL Source Stats-")}
            {format_line("missing-cells-percent", f"{self.pre_etl_run_info['missing']['missing_cells_percent']} %")}
            {format_line("numerical-outliers-percent", f"{self.pre_etl_run_info['outliers']['numerical_outliers_percent']} %")}
            {format_line("duplicate-rows-percent", f"{self.pre_etl_run_info['duplicates']['duplicate_rows_percent']} %")}
            {format_line("data-quality-score", self.pre_etl_run_info['dq'])}
            {format_header("-Post-ETL Source Stats")}
            {format_line("missing-cells-percent", f"{self.post_etl_run_info['missing']['missing_cells_percent']} %")}
            {format_line("numerical-outliers-percent", f"{self.post_etl_run_info['outliers']['numerical_outliers_percent']} %")}
            {format_line("duplicate-rows-percent", f"{self.post_etl_run_info['duplicates']['duplicate_rows_percent']} %")}
            {format_line("data-quality-score", self.post_etl_run_info['dq'])}
            {format_final_line()}""")
            return report
        else:
            logging.warning("Cannot compile an incomplete report")
            return ""
        
    # "save" the report by writing its contents to the logs folder
    def save(self):
        report_name = f"etl_{self.start_datetime.strftime("%Y_%m_%d_%H_%M_%S")}.logs"
        write_path = f"etl_logs\\{report_name}"
        compiled_report = self.compile()
        with open(write_path, "w") as file: file.write(compiled_report)
        logging.info(f"ETL report written and saved to '{write_path}'")

if __name__ == "__main__":

    try:
        consumer = KafkaConsumer('etlMetrics', bootstrap_servers=['localhost:9092'])
        logging.basicConfig(level="INFO")
        logging.info("Connected to broker")
        r = ETLReport(consumer)
        r.run()

    except (NoBrokersAvailable, UnrecognizedBrokerVersion, ValueError):
        logging.error("NoBrokersAvailable : no broker could be detected")