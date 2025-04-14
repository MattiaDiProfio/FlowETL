v1 contains the results of running the planning engine on all files. This version of the planning engine uses a modfied gale-shapley as the underlying 
matching engine and it contains an example to guide the LLM during the inference of transformation logic.

v2 contains the results of running the planning engine on all files. This version of the planning engine uses an LLM for both the schema matching and inference of transformation logic. Also, a different style of prompting is used. **these are also the end-to-end results of runnning the latest version of flowETL!!**

each plan is scored using the PlanEval metric
