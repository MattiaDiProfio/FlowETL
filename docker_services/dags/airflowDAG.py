from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
import os
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from planning_utils import (
    apply_airflow_plan,
    compute_dq,
    extract_code,
    generate_plans,
    infer_schema,
    standardise_features,
)


def consume_sample(task_instance):
    """
    Method consumes the sampled internal representation of the source file from the Kafka broker

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """

    try:
        # initialise a consumer on the 'inputSamples' topic 
        consumer = KafkaConsumer('inputSamples', bootstrap_servers=['kafka:9091'])

        # poll Kafka broker until payload is published to topic
        messages = consumer.poll()
        while not messages: 
            messages = consumer.poll()

        for records in messages.values():
            for record in records:

                # extract and deserialise payload contents into a dictionary
                message_contents_json = json.loads(record.value.decode('utf-8'))

                # extract relevant artifacts from payload and push them to Xcoms
                file_name = message_contents_json['name']
                associated_key = message_contents_json['associatedKey']
                sample = message_contents_json['contents']
                task_instance.xcom_push(key="name", value=file_name)
                task_instance.xcom_push(key="sample", value=sample)
                task_instance.xcom_push(key="associatedKey", value=associated_key)

        # close connection to Kafka broker
        consumer.close()

    except KafkaError as e:
        logging.error(f"KafkaError : {e}")
        raise AirflowFailException()


def consume_target(task_instance):
    """
    Method consumes the internal representation of the target file from the Kafka broker

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """

    try:
        # initialise a consumer on the 'targetSamples' topic 
        consumer = KafkaConsumer('targetSamples', bootstrap_servers=['kafka:9091'])

        # poll Kafka broker until payload is published to topic
        messages = consumer.poll()
        while not messages: 
            messages = consumer.poll()

        for records in messages.values():
            for record in records:

                # extract and deserialise payload contents into a dictionary
                message_contents_json = json.loads(record.value.decode('utf-8'))

                # extract payload contents and push to XComs
                target_schema = message_contents_json['contents'] 
                task_instance.xcom_push(key='target', value=target_schema)

        # close connection to Kafka broker
        consumer.close()

    except KafkaError as e:
        logging.error(f"KafkaError : {e}")
        raise AirflowFailException()


def infer_initial_schemas(task_instance):
    """
    Method infers the source and target schemas from the respective internal representations

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """
        
    # pull the internal representation from XComs
    source_IR = task_instance.xcom_pull(key='sample', task_ids='consumeSample')
    target_IR = task_instance.xcom_pull(key='target', task_ids='consumeTarget')
    
    # infer the source and target schemas
    source_schema = infer_schema(json.loads(source_IR))
    target_schema = infer_schema(json.loads(target_IR))

    # push the inferred schemas to XComs
    task_instance.xcom_push(key="source_schema", value=source_schema)
    task_instance.xcom_push(key="target_schema", value=target_schema)


def compute_schema_map(task_instance):
    """
    Method attempts to compute a mapping between the source and target schemas using LLM inference

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """
       
    # pull inferred and target schemas from Xcoms
    source_schema = task_instance.xcom_pull(key='source_schema', task_ids='inferInitialSchemas')
    target_schema = task_instance.xcom_pull(key='target_schema', task_ids='inferInitialSchemas')

    # extract headers from source and target schemas
    source_headers = list(source_schema.keys())
    target_headers = list(target_schema.keys())

    # define LLM prompt to guid inference of schema mapping 
    prompt = f"""
    You are given a list of source column headers and target column headers. Your task is to infer a plausible mapping from source to target columns.

    Rules:
    - At most two source attributes can be merged into one target attribute: represent this as `("s1", "s2"): ("t1")`
    - A source attribute may also be split into two target attributes: represent this as `("s1"): ("t1", "t2")`
    - If a source column is dropped, use an empty tuple as the value: `("s1"): ()`
    - If a target column is created, use an empty tuple as the key: `(): ("t1")`
    - Every source and target column can appear at most once in each mapping.

    Additional constraints:
    - Do not include placeholder values such as `None`
    - Ensure no duplicate keys in the output
    - Return a valid Python dictionary using tuple keys and values
    - Do not add any comments, explanations, or surrounding text

    Input:
    Source columns = {source_headers}
    Target columns = {target_headers}

    Return your answer inside a Python code block in the following format:

    ```python
    {{ ("source1",): ("target1",), ... }}
    ```
    """

    logging.info(f"LLM Prompt : \n{prompt} \n")

    # extract API key from environment variables 
    openrouter_key = os.environ.get('OPENROUTER_API_KEY')

    # send inference request to open-router API and gather response
    response = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={ "Authorization": f"Bearer {openrouter_key}" },
        data=json.dumps({
            "model" : "anthropic/claude-3.7-sonnet",
            "messages" : [{ "role": "user", "content": prompt }]
        })
    )

    if response.status_code == 200:

        # in case of a successfull response, extract its contents and push them to XComs
        response_data = response.json()
        schema_map = response_data['choices'][0]['message']['content']
        logging.info(f"LLM Response : \n{schema_map} \n schema map = {schema_map}, type = {type(schema_map)}")
        task_instance.xcom_push(key="schema_map", value=schema_map)

    else:
        raise AirflowFailException("Error occured during schema mapping inference step")


def apply_schema_map(task_instance):
    """
    Method attempts to standardise the source internal representation's columns using the previously computed schema mapping

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """
    
    # pull the internal representation and schema mapping from XComs 
    IR = json.loads(task_instance.xcom_pull(key='sample', task_ids='consumeSample'))
    mapping = task_instance.xcom_pull(key='schema_map', task_ids='computeSchemaMap')
    mapping = extract_code(mapping)

    # check if a mapping has been sucessfully computed
    if not mapping:
        raise AirflowFailException("Could not parse mapping!")
    
    # standardise the internal representation sample and push it back to XComs
    IR = standardise_features(IR, mapping)
    task_instance.xcom_push(key="sample", value=IR)


def infer_modified_schema(task_instance):
    """
    Method re-infers the internal representation's schema post column standardisation

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """

    # pull the modified internal representation from XComs 
    source_IR = task_instance.xcom_pull(key='sample', task_ids='applySchemaMap')

    # infer the (possibly) changed schema
    source_schema = infer_schema(source_IR)

    # push the newly inferred schema to Xcoms
    task_instance.xcom_push(key="source_schema", value=source_schema)


def infer_transformation_logic(task_instance):
    """
    Method infers a python method which standardises the internal representation's values according to the target provided 

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """
    
    # pull the artifacts required for LLM inference from XComs
    inputTable = task_instance.xcom_pull(key='sample', task_ids='applySchemaMap')
    outputTable = task_instance.xcom_pull(key='target', task_ids='consumeTarget')
    inputSchema = task_instance.xcom_pull(key='source_schema', task_ids='inferModifiedSchema')
    outputSchema = task_instance.xcom_pull(key='target_schema', task_ids='inferInitialSchemas')
    mapping = extract_code(task_instance.xcom_pull(key='schema_map', task_ids='computeSchemaMap'))

    # define the LLM prompt for this inference task
    prompt = f"""
    You are given two tables represented as 2D lists: an input table and an output table. Your task is to write a Python function that transforms the input table into the output table.

    Your function must:
    - Perform the necessary data transformations to match the output table
    - Use the provided column mapping and schemas to guide your logic
    - Leave any cell with the value '_ext_' unchanged
    - Handle numeric operations carefully: convert strings to float before using them
    - If a cell contains `A|B`, you may split on `|` and use both values.
    - Assume column renaming and reordering is already done
    - Be named exactly `transform_table` since there are some other processes which expect this name

    Special mapping rules:
    - `("col1", "col2") -> ("new_col",)` means you should merge these two columns into one
    - `("col",) -> ("new_col",)` means this column was renamed (already done)
    - `("col",) -> ()` means this column was dropped — you don’t need to process it
    - `() -> ("new_col",)` means a new column was created — use other values to populate it
    - `("col1",) -> ("new_col1", "new_col2")` means this column is split into two new columns - use the original column values to populate them

    Important:
    - Return only a valid, executable Python function — no explanations, no comments
    - Your response will be evaluated by `exec()`, so the code must not contain errors
    - Your logic should generalize to similar tables — **do not hardcode, do not provide samples, and do not randomly generate cell values**.
    
    Generate the code for the following:

    Input Table: {inputTable}

    Output Table: {outputTable}

    Input Schema: {inputSchema}

    Output Schema: {outputSchema}

    Column Mapping: {mapping}

    Respond only with Python code — nothing else. """

    logging.info(f"LLM Prompt : \n{prompt} \n")

    # extract the open-router API key from the environment variables
    openrouter_key = os.environ.get('OPENROUTER_API_KEY')

    # send a request to the LLM's API and collect the response object returned
    response = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={ "Authorization": f"Bearer {openrouter_key}" },
        data=json.dumps({
            "system" : """
                Your task is to create Python functions based on the provided natural language requests. "
                "The requests will describe the desired functionality of the function, including the input parameters and expected return value. "
                "Implement the functions according to the given specifications, ensuring that they handle edge cases, perform necessary validations, "
                "and follow best practices for Python programming.
            """,
            "model" : "anthropic/claude-3.7-sonnet",
            "messages" : [{ "role": "user", "content": prompt }]
        })
    )

    if response.status_code == 200:

        # extract the response contents if successfull and publish the inferred python method to XComs
        response_data = response.json()
        response = response_data['choices'][0]['message']['content']
        task_instance.xcom_push(key="llm_response", value=response)

    else:
        logging.error("Error occured during LLM inference")
        raise AirflowFailException()


def compute_optimal_plan(task_instance):
    """
    Method computes a plan composed of Data Task Nodes which maximises the data quality score on the sampled source internal representation 

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """

    # pull the internal representation and its schema from XComs
    inferred_schema = task_instance.xcom_pull(key='source_schema', task_ids='inferModifiedSchema')
    IR = task_instance.xcom_pull(key='sample', task_ids='applySchemaMap')

    # initialise artifacts to be returned
    max_data_quality, best_plan = 0.0, ()
    total_plans, plans_failed = 0, 0

    # generate all possible plans and evaluate them individually
    for plan in generate_plans():
        total_plans += 1

        # create an intermediate internal representation by applying the current plan to the input one
        transformed_IR = apply_airflow_plan(IR, inferred_schema, plan)

        # record plan failure
        if not transformed_IR:
            logging.warning(f"Plan {plan} failed to execute")
            plans_failed += 1

        else:
            try:
                # compute the data quality achieved by the current plan on the internal representation
                data_quality = compute_dq(transformed_IR, inferred_schema)
                logging.info(f"DQ : {round(data_quality, 3)} - {plan} ")

                if data_quality == 1.0:
                    # terminate plan search once a perfect plan is detected 
                    best_plan = plan
                    max_data_quality = data_quality
                    break

                else:
                    # update the best plan and data quality score achieved so far if a better plan is found
                    if data_quality > max_data_quality:
                        best_plan = plan
                        max_data_quality = data_quality

            except BaseException as e:
                pass

    if best_plan == ():
        # no successful plan could be found, therefore default to a non-failing plan 
        logging.warning("No plan managed to run successfully, consider providing more representative examples or increasing sample/examples sizes")
        logging.warning("Returning default plan. ['missingValues/impute', 'duplicates', 'outliers/impute']")
        best_plan = ['missingValues/impute', 'duplicates', 'outliers/impute']


    # compute the plan search metrics object to be shared with the Reporting Engine and publish it to XComs 
    engine_metrics = {
        "from" : "planning_engine", 
        "contents" : {
            'plans_computed_count' : total_plans, 
            'plans_failed_count': plans_failed, 
            'max_dq_achieved' : round(max_data_quality, 3),
            'best_plan': best_plan
        }
    }
    task_instance.xcom_push(key="engine_metrics", value=engine_metrics)

    logging.info(f"Best plan {plan} achieved DQ score of {round(max_data_quality, 3)}")    
    
    # pull required artifacts from XComs
    logic = task_instance.xcom_pull(key='llm_response', task_ids='InferTransformationLogic')
    mapping = task_instance.xcom_pull(key='schema_map', task_ids='computeSchemaMap')
    filename = task_instance.xcom_pull(key='name', task_ids='consumeSample')
    associated_key = task_instance.xcom_pull(key='associatedKey', task_ids='consumeSample')
    schema = task_instance.xcom_pull(key='source_schema', task_ids='inferModifiedSchema')

    # construct the optimal plan computed for the source internal representation and push it to XComs
    best_plan = [filename, {'associated_key' : associated_key}, {'standardiseFeatures' : mapping}] + list(best_plan) + [{'standardiseValues' : logic}]
    task_instance.xcom_push(key="optimal_plan", value={'schema': schema, 'plan' : best_plan})


def publish_optimal_plan(task_instance):
    """
    Method publishes the previously computed transformation plan to the Kafka broker 

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """

    # pull optimal plan from XComs
    plan = task_instance.xcom_pull(key='optimal_plan', task_ids='computeOptimalPlan')

    try:

        # enstablish connection to Kafka broker
        producer = KafkaProducer(bootstrap_servers=['kafka:9091'])  
        logging.info("Connected to broker")

        # serialise and publish the transformation plan to the 'optimalPlans' topic on the Kafka broker
        producer.send('optimalPlans', key=bytes('plan', 'utf-8'), value=bytes(json.dumps(plan).encode('utf-8')))
        logging.info(f"Optimal plan {plan} published to broker")

        # close connection to Kafka broker
        producer.close()

    except KafkaError:
        logging.error("NoBrokersAvailable : no broker could be detected")
        raise AirflowFailException()


def publish_metrics(task_instance):
    """
    Method publishes the execution metrics collected during the current Airflow runtime to the Kafka broker 

    :param task_instance: task instance object required to interact with XComs
    :return: None
    """

    # pull the metrics from XComs
    engine_metrics = task_instance.xcom_pull(key='engine_metrics', task_ids='computeOptimalPlan')

    try:
        # enstablish connection to Kafka broker
        producer = KafkaProducer(bootstrap_servers=['kafka:9091'])  

        # serialise and publish the metrics to the 'etlMetrics' topic on the Kafka broker
        logging.info("Connected to broker")
        producer.send('etlMetrics', key=bytes('metrics', 'utf-8'), value=bytes(json.dumps(engine_metrics).encode('utf-8')))

        # close connection to Kafka broker
        logging.info(f"Metrics {engine_metrics} published to broker")

        # close connection to Kafka broker
        producer.close()

    except KafkaError:
        logging.error("NoBrokersAvailable : no broker could be detected")
        raise AirflowFailException()


# define Airflow directed acyclic graph 
with DAG(
    'PlanningEngine', 
    default_args={'owner': 'airflow','retries': 1,'retry_delay': timedelta(minutes=5)},
    schedule_interval=None,  
    start_date=datetime(2025, 2, 4),  
    catchup=False,
) as dag:
    
    # define task nodes
    consumeSample = PythonOperator(task_id='consumeSample', python_callable=consume_sample)
    consumeTarget = PythonOperator(task_id='consumeTarget', python_callable=consume_target)
    inferInitialSchemas = PythonOperator(task_id='inferInitialSchemas', python_callable=infer_initial_schemas)
    computeSchemaMap = PythonOperator(task_id='computeSchemaMap', python_callable=compute_schema_map)
    applySchemaMap = PythonOperator(task_id='applySchemaMap', python_callable=apply_schema_map)
    inferModifiedSchema = PythonOperator(task_id='inferModifiedSchema', python_callable=infer_modified_schema)
    InferTransformationLogic = PythonOperator(task_id='InferTransformationLogic', python_callable=infer_transformation_logic) # LLM inference
    computeOptimalPlan = PythonOperator(task_id='computeOptimalPlan', python_callable=compute_optimal_plan)
    publishOptimalPlan = PythonOperator(task_id='publishOptimalPlan', python_callable=publish_optimal_plan)
    publishMetrics = PythonOperator(task_id='publishMetrics', python_callable=publish_metrics)

    # define nodes dependencies
    consumeSample
    consumeTarget
    [consumeSample, consumeTarget] >> inferInitialSchemas
    inferInitialSchemas >> computeSchemaMap
    computeSchemaMap >> applySchemaMap
    applySchemaMap >> inferModifiedSchema
    inferModifiedSchema >> InferTransformationLogic 
    InferTransformationLogic>> computeOptimalPlan
    computeOptimalPlan >> publishOptimalPlan
    publishOptimalPlan >> publishMetrics