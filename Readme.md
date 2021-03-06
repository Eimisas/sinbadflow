![Logo](https://raw.githubusercontent.com/Eimisas/sinbadflow/master/img/logo.png)
# Simple pipeline creation and execution tool

![Tests](https://github.com/Eimisas/Sinbadflow/workflows/Tests/badge.svg)

Sinbadflow is a simple pipeline creation and execution tool. It was created having Databricks notebooks workflow in mind, however with flexible implementation options the tool can be customized to fit any task. Named after famous cartoon "Sinbad: Legend of the Seven Seas" the library provides ability to create and run agents with specific triggers and conditional functions in parallel or single mode. With the simple, yet intuitive, code based syntax we can create elaborative pipelines to help with any data engineering, data science or software development task.

## Installation

To install use:

```pip install sinbadflow```

## Usage

Sinbadflow supports single or parallel run with different execution triggers. To build a pipeline use ```>>``` symbol between two agents. Example Databricks notebooks pipeline (one-by-one execution):

```python
from sinbadflow.agents.databricks import DatabricksAgent as dbr
from sinbadflow import Trigger

pipeline = dbr('/path/to/notebook1') >> dbr('path/to/another/notebook')
```
Parallel run pipeline (agents in list are executed in parallel mode):

```python
pipeline = [dbr('/parallel_run_notebook'), dbr('/another_parallel_notebook')]
```

The flow can be controlled by using triggers. Sinbadflow supports these triggers:

* ```Trigger.DEFAULT``` - default trigger, the agent is always executed.
* ```Trigger.OK_PREV``` - agent will be executed if previous agent finished successfully.
* ```Trigger.OK_ALL``` - agent will be executed if so far no fails are recorded in the pipeline.
* ```Trigger.FAIL_PREV``` - agent will be executed if previous agent run failed.
* ```Trigger.FAIL_ALL``` - agent will be executed if all previous runs failed.

An example workflow would look like this:

```python
execution = dbr('/execute')
parallel_handles = [dbr('/handle_ok', Trigger.OK_PREV), dbr('/handle_fail', Trigger.FAIL_PREV)]
save = dbr('/save_all', Trigger.OK_ALL)
fail_handling = dbr('/log_all_failed', Trigger.FAIL_ALL)


pipeline = execution >> parallel_handles >> save >> fail_handling
```
To run the pipeline:

```python
from sinbadflow import Sinbadflow

sf = Sinbadflow()

sf.run(pipeline)
```
The pipeline will be executed and results will be logged with selected method (```print/logging``` supported). Sinbadflow will always run the full pipeline, there is no implementation for early stoppage if the pipeline fails.

## Conditional functions

For more flexible workflow control Sinbadflow also supports conditional functions check. This serves as more elaborative triggers for the agents. 

```python
from sinbadflow.agents.databricks import DatabricksAgent as dbr
from datetime import date

def is_monday():
    return date.today().weekday() == 0

notebook1 = dbr('/notebook1', conditional_func=is_monday)
notebook2 = dbr('/notebook2', conditional_func=is_monday)

pipeline = notebook1 >> notebook2
```
In the example above notebooks will be skipped if today is not Monday because of `conditional_fuc` function. Sinbadflow also provides ability to apply conditional function to the whole pipeline using `apply_conditional_func` method.

```python
from sinbadflow.utils import apply_conditional_func

pipeline = dbr('/notebook1') >> dbr('/notebook2') >> dbr('/notebook3')

pipeline = apply_conditional_func(pipeline, is_monday)
```

## Custom Agents

Sinbadflow provides ability to create your own agents. In order to do that, your agent must inherit from ```BaseAgent``` class, pass the ```data``` and `trigger` parameters to parent class (also `**kwargs` if you are planning to use conditional functions) and implement ```run()``` method. An example ```DummyAgent```:

```python
from sinbadflow.agents import BaseAgent
from sinbadflow import Trigger
from sinbadflow import Sinbadflow


class DummyAgent(BaseAgent):
    def __init__(self, data=None, trigger=Trigger.DEFAULT, **kwargs):
        super(DummyAgent, self).__init__(data, trigger, **kwargs)

    def run(self):
        print(f'        Running my DummyAgent with data: {self.data}')

def condition():
    return False

secret_data = DummyAgent('secret_data')

parallel_data = [DummyAgent('simple_data', conditional_func=condition),
                 DummyAgent('important_data', Trigger.OK_ALL)]

pipeline =  secret_data >> parallel_data

sf = Sinbadflow()
sf.run(pipeline)

```

## DatabricksAgent - cluster modes

Out of the box Sinbadflow comes with `DatabricksAgent` which can be used to run Databricks notebooks on interactive or job clusters. `DatabricksAgent` init arguments:

```python
notebook_path                                    #Notebook location in the workspace
trigger = Trigger.DEFAULT                        #Trigger
timeout=1800                                     #Notebook run timeout
args={}                                          #Notebook arguments
cluster_mode='interactive'                       #Cluster mode (interactive/job)
job_args={)                                      #Job cluster parameters  
conditional_func=default_func()                  #Conditional function
```
Default `job_args` parameters for job cluster creation (more information about job_args <a href='https://docs.databricks.com/dev-tools/api/latest/jobs.html'>see here</a>):

```python
{
    'spark_version': '6.4.x-scala2.11',
    'node_type_id': 'Standard_DS3_v2',
    'driver_node_type_id': 'Standard_DS3_v2',
    'num_workers': 1
}    
```

By default the notebook will be executed on interactive cluster using `dbutils` library. To run notebook on separate job cluster use the following code:

```python
from sinbadflow.agents.databricks import DatabricksAgent as dbr, JobSubmitter
from sinbadflow.executor import Sinbadflow

#set new job_args
new_job_args = {
    'num_workers':10,
    'driver_node_type_id': 'Standard_DS3_v2'
    }

job_notebook = dbr('notebook1', job_args=new_job_args, cluster_mode='job')
interactive_notebook = dbr('notebook2')

pipeline = job_notebook >> interactive_notebook

##Access token is used for job cluster creation and notebook submission
JobSubmitter.set_access_token('<DATABRICKS ACCESS TOKEN>')

sf = Sinbadflow()
sf.run(pipeline)
```

As shown in the example above you can mix and match agent runs on interactive/job clusters to achieve the optimal solution.

## Additional help
Full API docs can be found <a href='https://eimisas.github.io/sinbadflow_api_docs/index.html' target='_blank'>here</a>.

Use built in ```help()``` method for additional information about the classes and methods. 

Do not hesitate to contact me with any question. Pull requests are encouraged!

## Contributors

Special thank you for everyone who contributed to the project:

[Robertas Sys](https://github.com/rob-sys), [Emilija Lamanauskaite](https://github.com/emilijalamanauskaite)

## Upcoming feature list

- [ ] href link to the current job run for DatabricksAgent notebook submission on job cluster.
- [ ] "break all pipeline if one element fails" functionality for executor.
- [ ] status_handler result extraction functionality (not only logging/printing the results).
- [ ] more agents (GCP Dataproc, Mysql, etc.)