CREATE OR REPLACE PROCEDURE email_ouput_formatted(
    email_integration_name STRING,
    query_id STRING,
    send_to STRING,
    subject STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark

def main(session, email_integration_name, query_id, send_to, subject):
    # Fetch data from Snowflake
    result = session.sql("SELECT * FROM table(result_scan(?))", params=[query_id])

    # Convert Snowflake result to a Pandas DataFrame
    result_df = result.to_pandas()

    # Convert DataFrame to HTML table
    html_table = result_df.to_html(classes='table table-striped', index=False)

    # Apply styling to the HTML table
    html_table = html_table.replace('class="dataframe"', 'style="border: solid 2px #DDEEEE; border-collapse: collapse; border-spacing: 0; font: normal 14px Roboto, sans-serif;"')
    html_table = html_table.replace('<th>', '<th style="background-color: #DDEFEF; border: solid 1px #DDEEEE; color: #336B6B; padding: 10px; text-align: left; text-shadow: 1px 1px 1px #fff;">')
    html_table = html_table.replace('<td>', '<td style="border: solid 1px #DDEEEE; color: #333; padding: 10px; text-shadow: 1px 1px 1px #fff;">')

    # Send the email using Snowflake's system$send_email function
    session.call('system$send_email', email_integration_name, send_to, subject, html_table)

    return 'Email sent successfully.'
$$;

  ## Sample Run command :
call email_ouput_formatted('email_integration','01b20c0b-0735-004d-1d070001b02a','<your_name>@gmail.com', 'results from snowflake');
