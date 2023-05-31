# Building-an-Automated-Weather-Data-Pipeline-with-Airflow-From-Ingestion-to-Data-Warehouse
This project focuses on building a robust data pipeline using Apache Airflow to automate the ingestion of weather data from the OpenWeatherAPI and loading it into a data warehouse, specifically AWS Redshift. The pipeline involves multiple steps, including data ingestion, schema creation, data transformation, and data loading. By leveraging Airflow's task scheduling and dependency management capabilities, the pipeline ensures the seamless execution of each step, resulting in a reliable and efficient data flow from the API to the data warehouse.

# data_ingestion.py
*Please ensure that you have the necessary API key, permissions to access the OpenWeatherMap API.(Create account to get that)

A list of city names is created, representing the cities for which you want to fetch weather data.
For each city, an API call is made to the OpenWeatherMap API using the constructed URL with the city name and API key.

API response is obtained as JSON data.
  -The relevant details such as country, city name, temperature, sunrise time, sunset time, and timezone are extracted from the JSON data, and   store in dictionary. The dictionary for each city is appended to a list 'prod_data'.
  -Once all the cities have been processed, the list 'prod_data' contains dictionaries with the weather details for each city.
  The prod_data list is converted into a pandas DataFrame called 'raw_data_frame'.

*The DataFrame organizes the weather details in a tabular format, with columns for country, city name, temperature, sunrise time, sunset time, and timezone.

# schema_creation.py
The necessary credentials such as the host name, port number, database name, username, and password are retrieved from the configuration file (config_file.ini). If getting error, go with normal variable-value things.
Establish Connection with redshift and create schema to store data.

*Make sure to provide the correct credentials and have the necessary permissions to access the data warehouse.

# data_pipeline.py
Import the necessary modules and classes from Airflow and other libraries.
Create instances of PythonOperator for each task in the data pipeline:
  transform_task: Executes the transform_data() function.
  load_task: Executes the load_data_to_redshift() function.
  get_dataFrame: Executes the ingest_data() function from the data_ingestion module.
  schema_creation: Executes the create_schema() function from the schema_creation module.

Define the dependencies between the tasks using the >> operator.
 *get_dataFrame should run before schema_creation.
 *schema_creation should run before transform_task.
 *transform_task should run before load_task.

No need to run any file individually. **To execute the data pipeline, you need to start the Airflow scheduler and trigger the DAG either manually or based on the schedule interval you have specified**.

# Please note that you need to have Airflow and its dependencies properly installed and configured for this code to work.
if any query, feel free to ask on Linkedln. www.linkedin.com/in/nsk7

