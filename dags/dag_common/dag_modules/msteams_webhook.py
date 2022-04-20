"""
Library for sending messages to MS Teams

"""

# %% Import Libraries

from urllib.parse import quote

from airflow.providers.http.hooks.http import HttpHook



# %% Paramneters

msteams_on_failure_conn_id = 'msteams_on_failure'
msteams_on_success_conn_id = 'msteams_on_success'
msteams_sla_miss_conn_id = msteams_on_failure_conn_id



# %% Build On MS Teams Card

def msteams_card(message:str, subtitle:str, button_url:str, button_text:str='Logs'):
    """
    Build MS Teams Card
    """
    cardjson = f"""
    {{
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "00FF00",
        "summary": "{message}",
        "sections": [{{
            "activityTitle": "{message}",
            "activitySubtitle": "{subtitle}",
            "markdown": true,
            "potentialAction": [
                {{
                    "@type": "OpenUri",
                    "name": "{button_text}",
                    "targets": [
                        {{ "os": "default", "uri": "{button_url}" }}
                    ]
                }}
            ]
        }}]
    }}
    """

    return cardjson



# %% Airflow MS Teams Web Hook

def msteams_httphook(http_conn_id:str, data):
    """
    Airflow MS Teams Web Hook
    """
    httphook = HttpHook(method='POST', http_conn_id=http_conn_id)

    conn = httphook.get_connection(conn_id=http_conn_id)
    webhook_token = conn.extra_dejson.get('webhook_token', '')

    proxy_url = conn.extra_dejson.get("proxy", '')
    proxies = {'https': proxy_url} if len(proxy_url) > 5 else {}

    httphook.run(
        endpoint = webhook_token,
        data = data,
        headers = {'Content-type': 'application/json'},
        extra_options = {
            'proxies': proxies,
            'check_response': False,
            },
        )



# %% Construct Airflow log url

def get_log_url(context, airflow_webserver_link):
    """
    Construct Airflow log url
    """
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = quote(context['ts'], safe='')

    log_url = f'{airflow_webserver_link}/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}'
    return log_url



# %% Construct Airflow dag url

def get_dag_url(context, airflow_webserver_link):
    """
    Construct Airflow dag url
    """
    dag_id = context['dag_run'].dag_id
    dag_url = f'{airflow_webserver_link}/tree?dag_id={dag_id}'
    return dag_url



# %% Function to run on task failure

def on_failure(airflow_webserver_link):
    """
    Function to run on task failure
    """

    def on_failure_context(context):
        """
        Function to run on task failure
        """
        dag_id = context['dag_run'].dag_id
        task_id = context['task_instance'].task_id

        logs_url = get_log_url(context=context, airflow_webserver_link=airflow_webserver_link)

        data = msteams_card(
            message = f'Failed: {dag_id} Task: {task_id}',
            subtitle = airflow_webserver_link,
            button_url = logs_url,
            button_text = 'Logs',
            )

        msteams_httphook(http_conn_id=msteams_on_failure_conn_id, data=data)

    return on_failure_context



# %% Function to run on task success

def on_success(airflow_webserver_link):
    """
    Function to run on task success
    """

    def on_success_context(context):
        """
        Function to run on task success
        """
        dag_id = context['dag_run'].dag_id
        task_id = context['task_instance'].task_id
        if task_id.lower() not in ['end_pipe']: return

        dag_url = get_dag_url(context=context, airflow_webserver_link=airflow_webserver_link)

        data = msteams_card(
            message = f'Success: {dag_id}',
            subtitle = airflow_webserver_link,
            button_url = dag_url,
            button_text = 'Go to Pipeline',
            )

        msteams_httphook(http_conn_id=msteams_on_success_conn_id, data=data)

    return on_success_context



# %% Function to run on SLA miss

def sla_miss(airflow_webserver_link):
    """
    Function to run on SLA miss
    """

    def sla_miss_context(context):
        """
        Function to run on SLA miss
        """
        dag_id = context['dag_run'].dag_id
        task_id = context['task_instance'].task_id

        logs_url = get_log_url(context=context, airflow_webserver_link=airflow_webserver_link)

        data = msteams_card(
            message = f'SLA Miss. Task is taking long time: {dag_id} Task: {task_id}',
            subtitle = airflow_webserver_link,
            button_url = logs_url,
            button_text = 'Logs',
            )

        msteams_httphook(http_conn_id=msteams_sla_miss_conn_id, data=data)

    return sla_miss_context



# %%


