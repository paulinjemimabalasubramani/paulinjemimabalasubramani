# %% Import Libraries

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.hooks.http import HttpHook

from dag_modules.dag_common import default_args, jars, executor_cores, executor_memory, num_executors, src_path, spark_conn_id, spark_conf



# %% Pipeline Parameters

pipelinekey = 'METRICS_MIGRATE_ASSETS_RAA'
python_spark_code = 'migrate_csv_with_date_3'

tags = ['DB:Metrics', 'SC:Assets']

schedule_interval = '0 13 * * *' # https://crontab.guru/


# %%


class MSTeamsWebhookHook(HttpHook):
    """
    This hook allows you to post messages to MS Teams using the Incoming Webhook connector.
    Takes both MS Teams webhook token directly and connection that has MS Teams webhook token.
    If both supplied, the webhook token will be appended to the host in the connection.
    :param http_conn_id: connection that has MS Teams webhook URL
    :type http_conn_id: str
    :param webhook_token: MS Teams webhook token
    :type webhook_token: str
    :param message: The message you want to send on MS Teams
    :type message: str
    :param subtitle: The subtitle of the message to send
    :type subtitle: str
    :param button_text: The text of the action button
    :type button_text: str
    :param button_url: The URL for the action button click
    :type button_url : str
    :param theme_color: Hex code of the card theme, without the #
    :type message: str
    :param proxy: Proxy to use when making the webhook request
    :type proxy: str
    """
    def __init__(self,
                 http_conn_id=None,
                 webhook_token=None,
                 message="",
                 subtitle="",
                 button_text="",
                 button_url="",
                 theme_color="00FF00",
                 proxy=None,
                 *args,
                 **kwargs
                 ):
        super(MSTeamsWebhookHook, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = self.get_token(webhook_token, http_conn_id)
        self.message = message
        self.subtitle = subtitle
        self.button_text = button_text
        self.button_url = button_url
        self.theme_color = theme_color
        self.proxy = proxy

    def get_proxy(self, http_conn_id):
        conn = self.get_connection(http_conn_id)
        extra = conn.extra_dejson
        print(extra)
        return extra.get("proxy", '')

    def get_token(self, token, http_conn_id):
        """
        Given either a manually set token or a conn_id, return the webhook_token to use
        :param token: The manually provided token
        :param conn_id: The conn_id provided
        :return: webhook_token (str) to use
        """
        if token:
            return token
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            return extra.get('webhook_token', '')
        else:
            raise AirflowException('Cannot get URL: No valid MS Teams '
                                   'webhook URL nor conn_id supplied')

    def build_message(self):
        cardjson = """
                {{
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "{3}",
            "summary": "{0}",
            "sections": [{{
                "activityTitle": "{1}",
                "activitySubtitle": "{2}",
                "markdown": true,
                "potentialAction": [
                    {{
                        "@type": "OpenUri",
                        "name": "{4}",
                        "targets": [
                            {{ "os": "default", "uri": "{5}" }}
                        ]
                    }}
                ]
            }}]
            }}
                """
        return cardjson.format(self.message, self.message, self.subtitle, self.theme_color,
                               self.button_text, self.button_url)

    def execute(self):
        """
        Remote Popen (actually execute the webhook call)
        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        proxies = {}
        proxy_url = self.get_proxy(self.http_conn_id)
        print("Proxy is : " + proxy_url)
        if len(proxy_url) > 5:
            proxies = {'https': proxy_url}

        self.run(endpoint=self.webhook_token,
                 data=self.build_message(),
                 headers={'Content-type': 'application/json'},
                 extra_options={'proxies': proxies})






# %% Function to run on task failure

def on_failure(context):
    """
    Function to run on task failure
    """
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id

    logs_url = f"http://10.128.25.83:8282/log?dag_id={dag_id}&task_id={task_id}&execution_date={context['ts']}"

    teams_notification_hook = MSTeamsWebhookHook(
        http_conn_id='msteams_webhook',
        message=f"Failed Pipeline: {dag_id} Task: {task_id}",
        subtitle="Pipeline Failure",
        button_text="Logs",
        button_url=logs_url,
        theme_color="FF0000"
    )
    teams_notification_hook.execute()

    title = f"Title {dag_id} - {task_id}"
    body = title



default_args = {**default_args,
    'on_failure_callback': on_failure,
    'start_date': days_ago(1),
    'catchup': False,
    }



# %% Create DAG

with DAG(
    dag_id = pipelinekey.lower(),
    default_args = default_args,
    description = pipelinekey,
    schedule_interval = schedule_interval,
    tags = tags,
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    copy_files = BashOperator(
        task_id = f'COPY_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/copy_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    migrate_data = SparkSubmitOperator(
        task_id = pipelinekey,
        application = f'{src_path}/{python_spark_code}.py',
        name = pipelinekey.lower(),
        jars = jars,
        conn_id = spark_conn_id,
        num_executors = num_executors,
        executor_cores = executor_cores,
        executor_memory = executor_memory,
        conf = spark_conf,
        application_args = [
            '--pipelinekey1', pipelinekey,
            ],
        dag = dag
        )

    delete_files = BashOperator(
        task_id = f'DELETE_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/delete_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    startpipe >> copy_files >> migrate_data >> delete_files




# %%


