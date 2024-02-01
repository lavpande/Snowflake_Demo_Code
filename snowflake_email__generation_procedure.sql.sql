CREATE OR REPLACE PROCEDURE MY_EMAIL_PROCEDURE(table_name STRING, email_integration_name STRING, email_address STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'tabulate')
HANDLER = 'main'
AS 
$$

import snowflake.snowpark
import pandas as pd
from tabulate import tabulate

def main(
    session: snowflake.snowpark.Session,
    table_name: str,
    email_integration_name: str,
    email_address: str
) -> str:
    # Fetch data from Snowflake
    table_pandas_df: pd.DataFrame = session.table(table_name).to_pandas()

    # Convert DataFrame to grid-style HTML table
    table_as_html: str = tabulate(table_pandas_df, headers='keys', tablefmt='html', showindex=False)

    # HTML email template
    email_as_html: str = f"""
    <html>
    <body>
        <p>Today's report of companies</p>
        {table_as_html}
    </body>
    </html>
    """
    
    # Send the email using Snowflake's system$send_email function
    success: bool = session.call(
        "system$send_email",
        email_integration_name,
        email_address,
        'Example email notification in HTML format',
        email_as_html,
        'text/html'
    )
    
    return "Email sent successfully" if success else "Sending email failed"

$$;
