from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import json
import requests
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from openai import OpenAI
from planning_utils import infer_schema, gale_shapley, generate_plans, compute_dq, apply_airflow_plan, extract_code

def consume_sample(ti):
    try:
        consumer = KafkaConsumer('inputSamples', bootstrap_servers=['kafka:9091'])
        messages = consumer.poll()
        while not messages: messages = consumer.poll()
        for records in messages.values():
            for record in records:
                message_contents_raw = record.value.decode('utf-8')

                # in order to access the payload properties, we need to convert it from raw bytes to json object
                message_contents_json = json.loads(message_contents_raw)

                # extract properties file name and infered schema from json payload, then push them to xcoms
                file_name = message_contents_json['name']
                associated_key = message_contents_json['associatedKey']
                sample = message_contents_json['contents']

                ti.xcom_push(key="name", value=file_name)
                ti.xcom_push(key="sample", value=sample)
                ti.xcom_push(key="associatedKey", value=associated_key)

        consumer.close()

    except KafkaError as e:
        logging.error(f"KafkaError : {e}")
        raise AirflowFailException()

def consume_target(ti):
    try:
        consumer = KafkaConsumer('targetSamples', bootstrap_servers=['kafka:9091'])
        messages = consumer.poll()
        while not messages: messages = consumer.poll()
        for records in messages.values():
            for record in records:

                message_contents_raw = record.value.decode('utf-8')
                message_contents_json = json.loads(message_contents_raw)
                target_schema = message_contents_json['contents'] 

                # NOTE - we do not extract the file name again, since we obtained it from the consume_sample function already
                ti.xcom_push(key='target', value=target_schema)

        consumer.close()

    except KafkaError as e:
        logging.error(f"KafkaError : {e}")
        raise AirflowFailException()


def infer_initial_schemas(ti):
    # extract file name, sampled internal representation of the file, and the target schema for that file from xcoms
    source_IR = ti.xcom_pull(key='sample', task_ids='consumeSample')
    target_IR = ti.xcom_pull(key='target', task_ids='consumeTarget')
    
    # infer the schema from both IRs and publish to Xcoms
    source_schema = infer_schema(json.loads(source_IR))
    target_schema = infer_schema(json.loads(target_IR))

    ti.xcom_push(key="source_schema", value=source_schema)
    ti.xcom_push(key="target_schema", value=target_schema)


def compute_schema_map(ti):
    
    # pull inferred and target schemas from xcoms
    source_schema = ti.xcom_pull(key='source_schema', task_ids='inferInitialSchemas')
    target_schema = ti.xcom_pull(key='target_schema', task_ids='inferInitialSchemas')

    # extract headers from source and target schemas
    source_headers = list(source_schema.keys())
    target_headers = list(target_schema.keys())

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
    openrouter_key = os.environ.get('OPENROUTER_API_KEY')

    response = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={ "Authorization": f"Bearer {openrouter_key}" },
        data=json.dumps({
            "model" : "anthropic/claude-3.7-sonnet",
            "messages" : [{ "role": "user", "content": prompt }]
        })
    )

    if response.status_code == 200:
        response_data = response.json()
        schema_map = response_data['choices'][0]['message']['content']
        logging.info(f"LLM Response : \n{schema_map} \n schema map = {schema_map}, type = {type(schema_map)}")
        ti.xcom_push(key="schema_map", value=schema_map)
    else:
        raise AirflowFailException("Error occured during schema mapping inference step")


def apply_schema_map(ti):

    print("Pulling internal representation (IR)...")
    IR = json.loads(ti.xcom_pull(key='sample', task_ids='consumeSample'))
    print("IR pulled:")
    for row in IR:
        print(row)

    print("\nPulling schema map...")
    mapping = ti.xcom_pull(key='schema_map', task_ids='computeSchemaMap')
    print("Schema map pulled:", mapping)

    mapping = extract_code(mapping)
    print("check", mapping, type(mapping))
    if not mapping:
        raise AirflowFailException("Could not parse mapping!")
    
    for x, y in mapping.items():
        print(f"\nProcessing mapping: {x} -> {y}")

        if len(x) == 2:
            print("Case: Merge two source columns into one target column")

            x1_column_name, x2_column_name = x
            x1_indx, x2_indx = IR[0].index(x1_column_name), IR[0].index(x2_column_name)
            print(f"Found column indices - {x1_column_name}: {x1_indx}, {x2_column_name}: {x2_indx}")

            IR[0][x1_indx] = y[0]
            print(f"Renamed header '{x1_column_name}' to '{y[0]}'")

            for i, row in enumerate(IR[1:], start=1):
                data_string = json.dumps(row[x1_indx]) + "|" + json.dumps(row[x2_indx]) 
                values = data_string.replace('"', '').split('|')
                merged_value = f"{values[0]}|{values[1]}"
                print(f"Row {i} - Merging '{row[x1_indx]}' and '{row[x2_indx]}' -> '{merged_value}'")
                row[x1_indx] = merged_value

            for i, row in enumerate(IR):
                print(f"Row {i} before dropping index {x2_indx}: {row}")
                row.pop(x2_indx)
                print(f"Row {i} after drop: {row}")

        else:
            if x == ():
                print("Case: Create new columns:", y)

                for t in y:
                    IR[0].append(t)
                    print(f"Appended new column to header: {t}")

                for row_indx in range(1, len(IR)):
                    IR[row_indx].append('_ext_')
                    print(f"Row {row_indx} after appending '_ext_': {IR[row_indx]}")

            elif y == ():
                print("Case: Drop columns:", x)

                column_names_todrop_indices = [IR[0].index(s) for s in x]
                print("Column indices to drop:", column_names_todrop_indices)

                for i in range(len(IR)):
                    original_row = IR[i]
                    IR[i] = [IR[i][j] for j in range(len(IR[i])) if j not in column_names_todrop_indices]
                    print(f"Row {i} before: {original_row}")
                    print(f"Row {i} after drop: {IR[i]}")

            else:
                print(f"Case: Rename single column {x} to {y}")
                x, y = x[0], y[0] 
                column_indx = IR[0].index(x)
                print(f"Column '{x}' found at index {column_indx}")
                IR[0][column_indx] = y
                print(f"Header after rename: {IR[0]}")

    print("\nFinal IR after applying schema map:")
    for row in IR:
        print(row)

    ti.xcom_push(key="sample", value=IR)




def infer_modified_schema(ti):
    # extract file name, sampled internal representation of the file, and the target schema for that file from xcoms
    source_IR = ti.xcom_pull(key='sample', task_ids='applySchemaMap')

    # infer the schema from the IR and publish it to xcoms
    source_schema = infer_schema(source_IR)
    ti.xcom_push(key="source_schema", value=source_schema)


def infer_transformation_logic(ti):

    # TODO - extract/compute required artifacts....
    inputTable = ti.xcom_pull(key='sample', task_ids='applySchemaMap')
    outputTable = ti.xcom_pull(key='target', task_ids='consumeTarget')
    inputSchema = ti.xcom_pull(key='source_schema', task_ids='inferModifiedSchema')
    outputSchema = ti.xcom_pull(key='target_schema', task_ids='inferInitialSchemas')
    mapping = extract_code(ti.xcom_pull(key='schema_map', task_ids='computeSchemaMap'))

    # construct the prompt
    prompt = f"""
    You are given two tables represented as 2D lists: an input table and an output table. Your task is to write a Python function that transforms the input table into the output table.

    Your function must:
    - Perform the necessary data transformations to match the output table
    - Use the provided column mapping and schemas to guide your logic
    - Leave any cell with the value '_ext_' unchanged
    - Handle numeric operations carefully: convert strings to float before using them
    - If a value is in the form "A|B", you may use both components for derived values
    - Assume column renaming and reordering is already done

    Special mapping rules:
    - `("col1", "col2") -> ("new_col",)` means you should merge these two columns into one
    - `("col",) -> ("new_col",)` means this column was renamed (already done)
    - `("col",) -> ()` means this column was dropped — you don’t need to process it
    - `() -> ("new_col",)` means a new column was created — use other values to populate it
    - `("col1",) -> ("new_col1", "new_col2")` means this column is split into two new columns - use the original column values to populate them

    Important:
    - Return only a valid, executable Python function — no explanations, no comments
    - Your response will be evaluated by `exec()`, so the code must not contain errors
    - Your logic should generalize to similar tables — **do not hardcode and do not provide samples**. Operate on the input table instead.
    
    Generate the code for the following:

    Input Table: {inputTable}

    Output Table: {outputTable}

    Input Schema: {inputSchema}

    Output Schema: {outputSchema}

    Column Mapping: {mapping}

    Respond only with Python code — nothing else. """


    logging.info(f"LLM Prompt : \n{prompt} \n")

    # do inference on the LLM
    openrouter_key = os.environ.get('OPENROUTER_API_KEY')

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
        response_data = response.json()
        response = response_data['choices'][0]['message']['content']
        print(f"LLM Response : \n{response} \n")
        ti.xcom_push(key="llm_response", value=response)

    else:
        logging.error("Error occured during LLM inference")
        raise AirflowFailException()


def compute_optimal_plan(ti):

    # pull inferred schema from xcoms
    inferred_schema = ti.xcom_pull(key='source_schema', task_ids='inferModifiedSchema')
    internal_representation = ti.xcom_pull(key='sample', task_ids='applySchemaMap')

    max_data_quality, best_plan = 0.0, ()
    total_plans, plans_failed = 0, 0

    for plan in generate_plans():
        total_plans += 1
        transformed_IR = apply_airflow_plan(internal_representation, inferred_schema, plan)
        if not transformed_IR:
            logging.warning(f"Plan {plan} failed to execute")
            plans_failed += 1
        else:
            try:
                data_quality = compute_dq(transformed_IR, inferred_schema)
                logging.info(f"DQ : {round(data_quality, 3)} - {plan} ")
                if data_quality == 1.0:
                    best_plan = plan
                    max_data_quality = data_quality
                    break
                else:
                    if data_quality > max_data_quality:
                        best_plan = plan
                        max_data_quality = data_quality
            except BaseException as e:
                pass

    if best_plan == ():
        logging.warning("No plan managed to run successfully, consider providing more representative examples or increasing sample/examples sizes")
        logging.warning("Returning default plan. ['missingValues/impute', 'duplicates', 'outliers/impute']")
        best_plan = ['missingValues/impute', 'duplicates', 'outliers/impute']

    # construct planning engine metrics and send to kafka etlMetrics topic
    engine_metrics = {
        "from" : "planning_engine", 
        "contents" : {
            'plans_computed_count' : total_plans, 
            'plans_failed_count': plans_failed, 
            'max_dq_achieved' : round(max_data_quality, 3),
            'best_plan': best_plan
        }
    }

    # push metrics to xcom
    ti.xcom_push(key="engine_metrics", value=engine_metrics)

    logging.info(f"Best plan {plan} achieved DQ score of {round(max_data_quality, 3)}")    
    
    # pull from xcoms the python method (transformation logic) returned by the LLM inference task
    logic = ti.xcom_pull(key='llm_response', task_ids='InferTransformationLogic')

    # pull schema mapping from xcoms
    mapping = ti.xcom_pull(key='schema_map', task_ids='computeSchemaMap')

    # pull the file name for which this plan has been computed from xcoms
    filename = ti.xcom_pull(key='name', task_ids='consumeSample')

    # get the associated key with the internal representation, required to reconstruct IR into json objects
    associated_key = ti.xcom_pull(key='associatedKey', task_ids='consumeSample')

    # prepend step to plan
    best_plan = [filename, {'associated_key' : associated_key}, {'standardiseFeatures' : mapping}] + list(best_plan) + [{'standardiseValues' : logic}]

    # pull last computed schema from xcoms
    schema = ti.xcom_pull(key='source_schema', task_ids='inferModifiedSchema')

    # push best plan to xcoms
    ti.xcom_push(key="optimal_plan", value={'schema': schema, 'plan' : best_plan})


def publish_optimal_plan(ti):

    # get optimal plan from xcoms
    plan = ti.xcom_pull(key='optimal_plan', task_ids='computeOptimalPlan')

    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9091'])  
        logging.info("Connected to broker")
        producer.send('optimalPlans', key=bytes('plan', 'utf-8'), value=bytes(json.dumps(plan).encode('utf-8')))
        logging.info(f"Optimal plan {plan} published to broker")

        producer.close()
    except KafkaError:
        logging.error("NoBrokersAvailable : no broker could be detected")
        raise AirflowFailException()


def publish_metrics(ti):
    # get optimal plan from xcoms
    engine_metrics = ti.xcom_pull(key='engine_metrics', task_ids='computeOptimalPlan')

    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9091'])  
        logging.info("Connected to broker")
        producer.send('etlMetrics', key=bytes('metrics', 'utf-8'), value=bytes(json.dumps(engine_metrics).encode('utf-8')))
        logging.info(f"Metrics {engine_metrics} published to broker")

        producer.close()
    except KafkaError:
        logging.error("NoBrokersAvailable : no broker could be detected")
        raise AirflowFailException()


with DAG(
    'computer', 
    default_args={'owner': 'airflow','retries': 1,'retry_delay': timedelta(minutes=5)},
    schedule_interval=None,  
    start_date=datetime(2025, 2, 4),  
    catchup=False,

) as dag:
    
    # define dag nodes
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

    # define dag nodes dependencies
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