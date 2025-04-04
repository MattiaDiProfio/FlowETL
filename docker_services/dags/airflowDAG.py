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
from planning_utils import infer_schema, gale_shapley, generate_plans, compute_dq, apply_airflow_plan

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

    # run gale-shapley to compute a schema map
    schema_map = gale_shapley(source_headers, target_headers)

    if not schema_map:
        raise AirflowFailException("Schema map step failed due to algorithm timeout")
    
    # publish schema map to xcoms
    ti.xcom_push(key="schema_map", value=schema_map)

def apply_schema_map(ti):

    internal_representation = json.loads(ti.xcom_pull(key='sample', task_ids='consumeSample'))
    mapping = ti.xcom_pull(key='schema_map', task_ids='computeSchemaMap')

    for x_attribute, y_attribute in mapping.items():

        source = x_attribute.split(",")

        if len(source) == 2:

            # need to merge multiple columns in the internal_representation and rename the resulting columns
            x1_column_name, x2_column_name = source[0], source[1]
            
            # locate the indices where these columns occur within the headers of the internal representation
            x1_indx, x2_indx = internal_representation[0].index(x1_column_name), internal_representation[0].index(x2_column_name)

            # rename the column x1_column_name to y_attribute
            internal_representation[0][x1_indx] = y_attribute

            # join column x2 onto column x1
            for row in internal_representation[1:]:
                data_string = json.dumps(row[x1_indx]) + "|" + json.dumps(row[x2_indx]) # NOTE that this is a temporary fix until we figure out value normalisation!
                values = data_string.replace('"', '').split('|')
                row[x1_indx] = f"{values[0]}|{values[1]}"

                

            # drop column x2 from the internal representation
            for row in internal_representation:
                row.pop(x2_indx)

        else:
            attribute = source[0]

            # check if the attribute is one of the special keywords
            if attribute == 'DROP':
                
                # extract the column names to be dropped
                column_names_todrop = y_attribute.split(",")

                if column_names_todrop[0] != '': # this occurs when we try to split an empty string, aka there is nothing to drop

                    # get the index of each column to be dropped within the headers of the internal representation
                    column_names_todrop_indices = [ internal_representation[0].index(name) for name in column_names_todrop ]

                    # remove each column index from each row in the internal representation
                    for i in range(len(internal_representation)):
                        internal_representation[i] = [ internal_representation[i][j] for j in range(len(internal_representation[i])) if j not in column_names_todrop_indices ]

            elif attribute == 'CREATE':
                
                # extract the column names to be created
                column_names_tocreate = y_attribute.split(",")

                if column_names_tocreate[0] != '': # this occurs when we try to split an empty string, aka there is nothing to create

                    # create a new column in the internal representation
                    for name in column_names_tocreate:

                        # extend the column headers
                        internal_representation[0].append(name)

                        # populate the new column
                        for row_indx in range(1, len(internal_representation)):
                            internal_representation[row_indx].append('_ext_')

            else:
                # simply rename the column to whatever is the value of y_attribute
                column_indx = internal_representation[0].index(attribute)
                internal_representation[0][column_indx] = y_attribute

    # push the new internal representation to xcoms, this time the IR has been adapted to match the schema map
    ti.xcom_push(key="sample", value=internal_representation)


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
    mapping = ti.xcom_pull(key='schema_map', task_ids='computeSchemaMap')

    # construct the prompt
    prompt = f"""
    Write a python function which takes in a table represented by a 2D list and applies all the necessary steps required to transform the input table into the output one. 
    Use the columns mapping and the table schemas provided to inform your decisions. 
    Return only the python code, without justifying your decisions and without adding comments. 

    Here is an example to help you understand the task:
    Inputs
    Input table = [['first name', 'last_name', 'salary', 'tax'],['John', 'Snow', 45210, 0.25],['Amy', 'Smith', 59440, 0.30] ],
    Output table = [['Name', 'Salary', 'Tax (%)', 'Net Income'],['J. Snow', 45210, 25, 33908],['A. Smith', 59440, 30, 41608] ], 
    Input table schema = {{'first name' : 'string', 'last_name' : 'string', 'salary' : 'number', 'tax' : 'number' }},
    Output table schema = {{'Name' : 'string', 'Salary' : 'number', 'Tax (%)' : 'number', 'Net Income' : 'number'}},
    Input-Output columns mapping = {{ "first name,last_name" : "Name", "salary" : "Salary", "tax" : "Tax (%)", "CREATE" : "Net Income", "DROP" : "" }}

    Process:
    Use the Input-Output columns mapping to notice that the 'first name' and 'last_name' columns from the input table are merged into the 'Name' column in the output.
    The values of these columns are merged by abbreviating the first name and joining it to last name, with a space in between.
    The column 'salary' is simply renamed, while the 'tax' column is renamed to 'Tax %' and the values are multiplied by 100. The mapping contains two special keys, 'CREATE'
    and 'DROP'. The 'CREATE' key tells us that 'Net Income' is a new column, and looking at the other columns we see that it is likely populated by taking the product of 'salary' and 'tax'.

    Output:
    def transform_table(input_table):
        output_table = [["Name", "Salary", "Tax (%)", "Net Income"]] # taken from Input-Output columns mapping
        for row in input_table[1:]:
            first_name, last_name, salary, tax = row
            name = f'{{first_name[0]}}. {{last_name}}'
            tax_percent = str(float(tax) * 100))
            net_income = str(float(salary) - (float(salary) * float(tax)))
            output_table.append([name, salary, tax_percent, net_income])
        return output_table
    
    Your code should leave cells with value of '_ext_' unchanged.
    Tip : before using any values for numerical operations, convert them to float. This is because in the input table they are likely stored as strings.
    Tip : if a column is populated with values separated by '|', use these values to infer the value for that column
    Notice how the method assumes that columns have been re-named already, hence it does not bother with this step. 
    Also notice how the output code assumes that the input table is empty. 
    See if applying mathematical or formatting functions to the values achieves the desired output.
    Include any required imports within the function. 
    Only return valid python code. 
    The response will be turned into an executable using the exec() method, so it should be formatted such that no errors occur. 
    The code you return should also generalise to other inputs. 
    Do not overfit to the example input table.
    
    Now operate on the given artifacts:
    Input table = {inputTable}, Output table = {outputTable}, Input table schema = {inputSchema}, Output table schema = {outputSchema}, Input-Output columns mapping = {mapping}
    """

    logging.info(f"LLM Prompt : \n{prompt} \n")

    def extract_code(text):
        start = text.find("```python") + len("```python")
        end = text.find("```", start)
        return text[start:end].strip() if start > len("```python") - 1 and end != -1 else None

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
        response = extract_code(response_data['choices'][0]['message']['content'])
        logging.info(f"LLM Response : \n{response} \n")
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
    'PlanningEngine', 
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