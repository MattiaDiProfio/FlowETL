import json
import logging
import textwrap
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, UnrecognizedBrokerVersion

class ETLReport:


    def __init__(self, broker):
        """
        Method initialises the ETLReport object

        :param broker: Kafka broker consumer instance used to self-populate the report object
        """

        self.broker = broker

        # set the ETL's start time to when the report is first started
        self.start_datetime = datetime.now() 

        # define dictionaries to hold the respective metrics for each subsection of the ETL report
        self.source_info = { 'filename' : '','objects_count': 0,'filesize_mbs' : 0 }
        self.target_info = { 'filename' : '','objects_count': 0,'filesize_mbs' : 0 }
        self.planning_engine_info = { 'plans_computed_count' : 0,'plans_failed_count': 0,'max_dq_achieved': 0,'best_plan': [] }
        self.pre_etl_run_info = {'missing': {'missing_cells_percent': 0},'outliers' : {'numerical_outliers_percent' : 0},'duplicates': {'duplicate_rows_percent' : 0},'dq': 0 }
        self.post_etl_run_info = {'missing': {'missing_cells_percent': 0},'outliers' : {'numerical_outliers_percent' : 0},'duplicates': {'duplicate_rows_percent' : 0},'dq': 0}

        # define control variables used to check the completion status of the report
        self.source_info_populated = False
        self.target_info_populated = False
        self.planning_info = False
        self.pre_etl_info = False
        self.post_etl_info = False


    def is_complete(self):
        """
        Method checks whether all subsections of the report have been populated

        :param None:
        :return True if all subsections are populated, false otherwise
        """
        return all([self.source_info_populated,self.target_info_populated,self.planning_info,self.pre_etl_info,self.post_etl_info])
    

    def run(self):
        """
        Method listens to Kafak broker until the ETLReport subsections are all populated or the Kafka broker fails
        """

        try:

            # continue to poll the broker's 'etlMetrics' topic
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

                        # update the subsection of the report for which the metrics were received
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
        
            # once the report is fully completed, write its contents to the required folder
            if self.is_complete(): 
                self.save()
                self.broker.close()

            else: 
                # keep on listening for more metrics, until the report is fully populated
                self.run() 

        except (NoBrokersAvailable, UnrecognizedBrokerVersion, ValueError):
            logging.error("NoBrokersAvailable : no broker could be detected")


    def compile(self):
        """
        Method compiles the ETLReport instance's contents into a string report
        """

        # ensure all subsections of the report have been filled out
        if self.is_complete():
            
            report_width = 100 
        
            def format_line(label, value):
                content = f"| {label}: {value}"
                padding = " " * (report_width - len(content) - 1)  # -1 for the end pipe
                return f"{content}{padding}|"
        
            def format_header(title):
                dashes_each_side = (report_width - len(title) - 6) // 2
                return f"+ {'-' * (dashes_each_side+1)}{title}{'-' * (dashes_each_side+1)} +"
            
            def format_final_line():
                return f"+ {'-' * (report_width - 4)} +"
        
            # build and return the report string
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
        

    def save(self):
        """
        Method writes the contents of a full report to the designated logs folder
        """

        # define time-stamped report name and destination path
        report_name = f"etl_{self.start_datetime.strftime("%Y_%m_%d_%H_%M_%S")}.logs"
        write_path = f"etl_logs\\{report_name}"

        # compile the report's content to a string 
        compiled_report = self.compile()

        # dump the compiled report to the logs destination
        with open(write_path, "w") as file: 
            file.write(compiled_report)

        logging.info(f"ETL report written and saved to '{write_path}'")


if __name__ == "__main__":

    try:
        # create an instance of a consumer to collect metrics from the 'etlMetrics' topic
        consumer = KafkaConsumer('etlMetrics', bootstrap_servers=['localhost:9092'])

        logging.basicConfig(level="INFO")
        logging.info("Connected to broker")

        # instantiate and start the reporter object
        r = ETLReport(consumer)
        r.run()
        
    except (NoBrokersAvailable, UnrecognizedBrokerVersion, ValueError):
        logging.error("NoBrokersAvailable : no broker could be detected")