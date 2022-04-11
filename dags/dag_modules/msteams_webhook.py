"""
Library for sending messages to MS Teams

"""

# %% Import Libraries

from urllib.parse import quote

from airflow.providers.http.hooks.http import HttpHook



# %% Paramneters

msteams_http_conn_id = 'msteams_webhook'



# %% Build On Failure Card

def on_failure_card(message:str, subtitle:str, button_url:str, button_text:str='Logs'):
    """
    Build On Failure Card
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

        data = on_failure_card(
            message = f'Failed: {dag_id} Task: {task_id}',
            subtitle = airflow_webserver_link,
            button_url = logs_url,
            )

        msteams_httphook(http_conn_id=msteams_http_conn_id, data=data)

    return on_failure_context



# %%


