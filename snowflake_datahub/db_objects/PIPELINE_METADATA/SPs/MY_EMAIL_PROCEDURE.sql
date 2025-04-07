CREATE OR REPLACE PROCEDURE DATAHUB.PIPELINE_METADATA.MY_EMAIL_PROCEDURE("EMAIL_INTEGRATION_NAME" VARCHAR, "EMAIL_ADDRESS" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','tabulate')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark
import pandas

def main(
  session: snowflake.snowpark.Session,
  email_integration_name: str,
  email_address: str
) -> str:
    table_pandas_df: pandas.DataFrame = session.sql("SELECT CURRENT_DATE").to_pandas()
    table_as_html: str = table_pandas_df.to_markdown(
      tablefmt="html",
      index=False
    )
    email_as_html: str = f"""
        <p>Today''s report of companies</p>
        <p>{table_as_html}</p>
    """
    success: bool = session.call(
      "SYSTEM$SEND_EMAIL",
      email_integration_name,
      email_address,
      ''Example email notification in HTML format'',
      email_as_html,
      ''text/html''
    )

    return "Email sent successfully" if success else "Sending email failed"

';