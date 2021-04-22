# -*- coding: utf-8 -*-

"""
streaming-file-sink.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to an S3 Bucket
    4. Queries from the Source Table and
       creates a tumbling window over 1 minute to calculate the average price over the window.
    5. These tumbling window results are inserted into the Sink table (S3)
"""

from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Tumble
import os
import json

# 1. Creates a Table Environment
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
statement_set = table_env.create_statement_set()


APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)  # set this env var in your local environment

if is_local:
    # only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///"
        + CURRENT_DIR
        + "/lib/amazon-kinesis-sql-connector-flink-2.0.3.jar;file:///"
        + CURRENT_DIR
        + "/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop-1.11.2.jar",
    )

    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.mode", "EXACTLY_ONCE"
    )
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "1min"
    )


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def create_source_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                ticker VARCHAR(6),
                price DOUBLE,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

              )
              PARTITIONED BY (ticker)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )


def create_sink_table(table_name, bucket_name):
    return """ CREATE TABLE {0} (
                ticker VARCHAR(6),
                price DOUBLE,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

              )
              PARTITIONED BY (ticker)
              WITH (
                  'connector'='filesystem',
                  'path'='s3a://{1}/',
                  'format'='csv',
                  'sink.partition-commit.policy.kind'='success-file',
                  'sink.partition-commit.delay' = '1 min'
              ) """.format(
        table_name, bucket_name)


def count_by_word(input_table_name):
    # use SQL Table in the Table API
    input_table = table_env.from_path(input_table_name)

    tumbling_window_table = (
        input_table.window(
            Tumble.over("1.minute").on("event_time").alias("one_minute_window")
        )
        .group_by("ticker, one_minute_window")
        .select("ticker, price.avg as price, one_minute_window.end as event_time")
    )

    return tumbling_window_table


def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    sink_property_group_key = "sink.config.0"

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"
    input_starting_position_key = "flink.stream.initpos"

    output_sink_key = "output.bucket.name"

    # tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, sink_property_group_key)

    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    output_bucket_name = output_property_map[output_sink_key]

    # 2. Creates a source table from a Kinesis Data Stream
    table_env.execute_sql(
        create_source_table(
            input_table_name, input_stream, input_region, stream_initpos
        )
    )

    # 3. Creates a sink table writing to an S3 Bucket
    create_sink = create_sink_table(
        output_table_name, output_bucket_name
    )
    table_env.execute_sql(create_sink)

    # 4. Queries from the Source Table and creates a tumbling window over 1 minute to calculate the average price
    # over the window.
    tumbling_window_table = count_by_word(input_table_name)

    # 5. These tumbling windows are inserted into the sink table (S3)
    tumbling_window_table.execute_insert(output_table_name).wait()

    statement_set.execute()


if __name__ == "__main__":
    main()
