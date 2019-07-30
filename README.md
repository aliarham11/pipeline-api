# pipeline-api

This is a configurable pipeline script executor / trigger.  The pipelines in this service are triggered by sending request to the appropriate endpoint that attached to the pipelines.

## Project Structure Description
- `config.yml` file

it contains pipeline configurations such as pipeline name, endpoint url, and handler.  You can also add `options` to specify the runner, job_id, etc.  example : 
```yaml
## Inside config.yml file 
...
pipelines:
      - name: data_lake_sink:                                            # pipeline name
        endpoint: /data_lake_sink                                        # endpoint url that trigger the pipeline
        handler_class: data_lake_sink.handler.DataLakeSinkHandler        # it is the handler class
        options:                                                         # optional variable
            runner: DataFlowRunner
            job_id_prefix: data_lake_sink

      - name: data_mart_sink:
        endpoint: /data_mart_sink
        handler: ${DATA_MART_SINK_HANDLER}   # you can also define your handler in env var
        options:
            runner: LocalRunner
...
```

- `start.py` file

Entry point for this project.  It contains method that read config definition and parse environment variable attached in `config.yml`

- `pipeline_service/core` folder

Contains core function of this project such as api endpoint generator

- `pipeline_service/pipelines` folder

Contains pipeline scripts and handler, can be grouped per folder based on context.  Pipeline handler must implement method `handle` becasuse it will be executed if its endpoint hit. 

example :
```python
## inside 'pipelines/data_lake_sink/handler.py'

class DataLakeSinkHandler:
    
    def __init__self(self, options):
        self.options = options
    
    def handle(options):
        # pipeline execution script definition.
        # you should write your pipeline code in separate files 
        # for easier script packaging purpose before submiting your pipeline to runner.
        # For example, this `handle` method will execute `pipeline.py` script that located in the same 
        # folder with `handler.py`. So, you dont need to worry about registering whole dependencies coming
        # from `pipeline_service` app.
        # for example with apache-beam, read : https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/
        command = 'python {}/pipeline.py'.format(BASE_DIR)
        subprocess.Popen(command)

    
    # of course you can add some methods besides `handle`
    def collect_data(self, source_data):
        # collect source of data before pipeline execution
        
```

- `pipeline_service/test` folder

Contains test case method for pipeline scripts

## Development
To install requirements make sure to run:
```
pip -r requirements.txt
```

If you want to add some packages during development, add the package name in `requirements.txt` and then run:
```
pip install [package_name]
```

To start the service:
```
bash ./run.sh start [host] [port]
```

To run the pytest:
```
bash ./run.sh test
```

nb : Because apache-beam currently only support python2.7, make sure you have python2.7 to submit apache-beam pipeline to runner

