# FlowETL - An Example-Driven Autonomous ETL Pipeline

Mattia Di Profio

A dissertation submitted in partial fulfilment of the requirements for the degree of BSc in Computing Science at the University of Aberdeen.

## Overview 

FlowETL is a novel example-based autonomous ETL pipeline designed to automatically standardise and prepare input datasets according to a concise, user-defined target dataset. The evaluation results show promising generalisation capabilities Currently, only CSV and JSON files are accepted. 

Refer to the full report `FlowETL.pdf` for an extensive account of the problem specification, design, implementation, evaluation, and future work for this project.

## User Manual

This manual outlines the steps to install and run FlowETL. 

### System Requirements

- Python 3 installed

- Git and Git Bash - required to interact with FlowETL through the command line

- Docker Desktop - required to instantiate the Planning Engine and the Messaging System components within FlowETL

### Setup Instructions 

**Obtain the FlowETL codebase** using one of following approaches:

1. Using the Git Bash terminal, create and travel into a new folder. Run the command `git
clone https://github.com/MattiaDiProfio/FlowETL.git`.

2. At the top of this page, click `Code` > `Download ZIP`.


**Provide the LLM Inference API key**.

Within the base folder for FlowETL, create a `.env` file and add `OPENROUTER_API_KEY="..."`.
You should use the API key provided by the author or set up your own one from [OpenRouter](https://openrouter.ai/).


**Build FlowETL Instance**.

1. Using Git Bash, travel to the directory `docker_services/.`

2. Run the command `docker build -t test:latest .` to create a Docker image using the
Dockerfile.

3. Using Git Bash, travel back to the base directory and run the command `pip install -r requirements.txt` to install all the required dependecies.

### Starting the Pipeline

1. Launch Docker Desktop from your machine

2. Using Git Bash, travel to the directory `docker_services/` and execute the command
`docker compose up -d` to start the Planning Engine and Messaging Broker.

3. Wait for 30 seconds, then open up new browser tab (Google Chrome is recommended). Travel to `localhost:8080` and login using "airflow" for both username and password.

4. Click the DAG name and press the start button. You should see the first two task nodes
turning light green, indicating that the Planning Engine has started.

5. To start the Observers, open a new Git Bash terminal and activate the virtual environment
by running the command `source venv/Scripts/activate`, then travel to `observers/`
and run the command `python driver.py`

6. To start the ETL worker and Reporting Engine, open a new Git Bash terminal for each,
travel to the base directory of your FlowETL codebase, activate the virtual environment.
Run the command `python etl_worker.py` in one terminal and `python reporter.py` in
the other.

### Stopping the Pipeline

1. Stop all processes running on Git Bash by clicking on each terminal and pressing `CTRL + C` on your keyboard.

2. Stop the docker services either via Docker Desktop or by travelling to the directory `docker_services/` and running the command `docker compose down`.

### Using the Pipeline

1. Given a source file to be transformed, define a Target file according to the following rules.
Refer to the `evaluation_datasets` folder for 13 examples of both CSV and JSON files.

    - Column headers/keys can be renamed, merged, dropped, split, or unchanged

    - Columns/keys must not contain missing values or outliers

    - Rows/objects should not be duplicated

    - It should be representative of the transformations to be applied

    - It should be concise (no more than 10 rows/objects)

2. Upload the source file to the `input/source` folder

3. Upload the target file to the `input/target` folder

4. Wait around 2 minutes. A transformed copy of the source file will be found in the output
folder. Meanwhile, you can check the Planning Engine’s progress through the Airflow dashboard.

5. Information about the runtime will be found in the `observers/logs`, `logs`, and `etl_logs`
folders.


### Troubleshooting and Common Errors

- Failure during LLM inference - could be caused by either an invalid API key or insufficient
credits. Check with the author, or check your Open-Router account if you are supplying
your own key.

- ETL worker cannot apply plan due to syntax or logic error. Could be caused by the nondeterministic nature of the LLM. Try to modify the target file if there are some underrepresented transformations.

It is advised to check the execution logs for any errors or anomalies, in particular the Planning
Engine’s individual task nodes. Click on any task node and travel to the Logs or XCom tags for
more information about the execution of the node

## Maintainance Manual

This manual provides additional information to configure, extend, or evaluate the project. 

### Contributing

The supported ways of contributing to this project are listed below. If you wish for any changes
to be integrated within the FlowETL codebase, open a pull request.

1. **Adding/Modifying DTNs & Strategies** : Add more DTNs and strategies within the `generate_plans` method in the `docker_services/dags/planning_utils.py` file. Change the following methods to modify or add strategies :

    - `missing_value_handler` for handling missing values
    - `duplicate_values_handler` for duplicate rows handling
    - `outlier_handler` for numerical outliers handling

2. **Extending the Inward Translation Mechanism** : Modify the to_internal method found in `docker_services/dags/planning_utils.py`, `observers/observer_utils.py`, and
`evaluation/bonobo_etl_experiment/bonoboutils.py` to handle more file types by
extending the conditional statement (i.e., ...`if filetype == ’xml’`...) and defining
the translation logic from source to the internal representation. Additionally the `load` method must be modified to reflect these changes.

### Additional Configurations

This subsection provides guidance on configuring FlowETL’s internal parameters and running
evaluation subroutines.

**Hyper-parameter Tuning**

- Sampling percentage `p` - found in the `extract_sample` method within the file `observers/observer_utils.py`. Note that p should be between 0.01 and 1.0 and be kept low for larger files (5000+ objects).

- Sample size upper bound `cap` - found in the `extract_sample` method within the file `observers/observer_utils.py`. This should be kept below 100 for optimal performance.

- LLM task prompts - found within `docker_services/dags/airflowDAG.py` file in the `infer_transformation_logic` and `compute_schema_map` methods respectively.

**Running Unit Tests** 

Activate the virtual environment venv from within your FlowETL base directory, then run `python unit_tests.py`.

**Running Bonobo Evaluation**

Activate the virtual environment `testvenv`, then travel to the folder `evaluation/bonobo_etl_experiment` and modify the `bonoboworker.py` file to set the source dataset filepath. Finally, run the worker with `python bonoboworker.py`

### Bug Reports

Two minor bugs have been identified during the evaluation stage of this project.

1. LLM Hallucination : In some instances, the model used for both inference tasks (Claude3.7-Sonnet), hallucinates on the transformation logic inference step within the *inferTransformationLogic* node of the Planning Engine. Try modifying the prompt hyper-parameters or validate the target dataset against the requirements listed above

2. Reporting Engine Runtime : In cases of an upstream component failure, such as the ETL
Worker, the Reporting Engine will continue polling the Kafka broker endlessly and requires
to be manually terminated. Implementing a subroutine which checks the uptime of other
components is a possible solution.