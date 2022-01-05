ALTER TABLE `hudi_customer` ADD
  PARTITION (`mktsegment` = 'Market Segment 1 MODIFIED') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 1 MODIFIED/'
  PARTITION (`mktsegment` = 'Market Segment 1') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 1/'
  PARTITION (`mktsegment` = 'Market Segment 2') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 2/'
  PARTITION (`mktsegment` = 'Market Segment 3') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 3/'
  PARTITION (`mktsegment` = 'Market Segment 4') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 4/'
  PARTITION (`mktsegment` = 'Market Segment 5') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 5/'
  PARTITION (`mktsegment` = 'Market Segment 6') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 6/'
  PARTITION (`mktsegment` = 'Market Segment 7') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 7/'
  PARTITION (`mktsegment` = 'Market Segment 7 Modified4') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 7 Modified4/'
  PARTITION (`mktsegment` = 'Market Segment 8') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 8/'
  PARTITION (`mktsegment` = 'Market Segment 9') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 9/'
  PARTITION (`mktsegment` = 'Market Segment 10') LOCATION 's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi/Market Segment 10/';
  

  drop table if exists `hudi_customer`;

CREATE EXTERNAL TABLE `hudi_customer`(
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `customer_id` bigint,
  `name` string,
  `ts` bigint)
  PARTITIONED BY ( 
  `mktsegment` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION
  's3://msk-lab-sales-db-bucket/hudi/flink/sqlapi';

  CREATE TABLE IF NOT EXISTS `customer_hudi` (
      ts TIMESTAMP(3),
      customer_id BIGINT,
      name STRING,
      mktsegment STRING,
      PRIMARY KEY (`customer_id`) NOT Enforced
    )
    PARTITIONED BY (`mktsegment`)
    WITH (
      'connector' = 'hudi',
      'write.tasks' = '4',
      'path' = 's3://msk-lab-sales-db-bucket/hudi/flink-emr/sqlapi',
      'read.streaming.enabled' = 'true',  -- this option enable the streaming read
      'read.streaming.check-interval' = '10', -- specifies the check interval for finding new source commits, default 60s.
      'hoodie.datasource.query.type' = 'snapshot',
      'table.type' = 'MERGE_ON_READ' --  MERGE_ON_READ table or, by default is COPY_ON_WRITE
    );


CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 's3a://msk-lab-sales-db-bucket/hudi/t1',
  'hoodie.datasource.query.type' = 'snapshot',
  'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
);

SET execution.checkpointing.interval = 1min;

CREATE TABLE IF NOT EXISTS CustomerKafka (
      `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
      `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
      `record_time` TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
      `CUST_ID` BIGINT,
      `NAME` STRING,
      `MKTSEGMENT` STRING,
       WATERMARK FOR event_time AS event_time
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'salesdb.salesdb.CUSTOMER',
      'properties.bootstrap.servers' = 'b-3.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-2.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-1.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092',
      'properties.group.id' = 'kdaConsumerGroup',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'debezium-json'
    );

CREATE TABLE IF NOT EXISTS CustomerSiteKafka (
      `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
      `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
      `record_time` TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
      `CUST_ID` BIGINT,
      `SITE_ID` BIGINT,
      `STATE` STRING,
      `CITY` STRING,
      `COUNTRY` STRING,
       WATERMARK FOR event_time AS event_time
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'salesdb.salesdb.CUSTOMER_SITE',
      'properties.bootstrap.servers' = 'b-3.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-2.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-1.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092',
      'properties.group.id' = 'kdaConsumerGroup2',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'debezium-json'
    );

CREATE TABLE  SalesOrderAllKafka (
      `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
      `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
      `record_time` TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
      `ORDER_ID` BIGINT,
      `SITE_ID` BIGINT,
      `ORDER_DATE` STRING,
      `SHIP_MODE` STRING,
       WATERMARK FOR event_time AS event_time
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'salesdb.salesdb.SALES_ORDER_ALL',
      'properties.bootstrap.servers' = 'b-3.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-2.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-1.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092',
      'properties.group.id' = 'kdaConsumerGroup3',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'debezium-json'
    );

CREATE TABLE `customer_hudi` (
      `order_count` BIGINT,
      `customer_id` BIGINT,
      `name` STRING,
      `mktsegment` STRING,
      `ts` TIMESTAMP(3),
      PRIMARY KEY (`customer_id`) NOT Enforced
    )
    PARTITIONED BY (`mktsegment`)
    WITH (
      'connector' = 'hudi',
      'path' = 's3://msk-lab-sales-db-bucket/hudi/flink-emr/cust-orders',
      'table.type' = 'MERGE_ON_READ' --  MERGE_ON_READ table or, by default is COPY_ON_WRITE
    );

CREATE TABLE `customer_hudi_read` (
      `_hoodie_commit_time` string,
      `_hoodie_commit_seqno` string,
      `_hoodie_record_key` string,
      `order_count` BIGINT,
      `customer_id` BIGINT,
      `name` STRING,
      `mktsegment` STRING,
      `ts` TIMESTAMP(3),
      PRIMARY KEY (`customer_id`) NOT Enforced
    )
    PARTITIONED BY (`mktsegment`)
    WITH (
      'connector' = 'hudi',
      'hoodie.datasource.query.type' = 'snapshot',
      'path' = 's3://msk-lab-sales-db-bucket/hudi/flink-emr/cust-orders',
      'table.type' = 'MERGE_ON_READ' --  MERGE_ON_READ table or, by default is COPY_ON_WRITE
    );


SET sql-client.execution.result-mode=tableau;

select count(*) from customer_hudi_read where customer_id <= 10;

select * from customer_hudi_read where customer_id <= 10;

select * from customer_hudi where customer_id <= 10;

select * from CustomerTable;


 aws emr add-steps --cluster-id j-1UFA3PF8AOGXT --steps Type=CUSTOM_JAR,Name=HudiFlinkJob,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args="bash","-c","\"flink run -m yarn-cluster /home/hadoop/flink-app/FlinkGettingStarted-1.0-SNAPSHOT.jar --kafka-topic salesdb.salesdb.CUSTOMER --brokers b-3.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-2.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092,b-1.mskcluster-msk-connec.dr9a8p.c13.kafka.us-east-1.amazonaws.com:9092 --s3Path s3a://msk-lab-sales-db-bucket/hudi/flink-emr\""

insert into customer_hudi select count(O.ORDER_ID), C.CUST_ID, C.NAME, C.MKTSEGMENT, PROCTIME() from CustomerKafka C JOIN CustomerSiteKafka CS ON C.CUST_ID = CS.CUST_ID JOIN SalesOrderAllKafka O ON O.SITE_ID = CS.SITE_ID GROUP BY  C.CUST_ID, C.NAME, C.MKTSEGMENT;

insert into SALES_ORDER_ALL values (29001, 1, now(), 'STANDARD');
insert into SALES_ORDER_ALL values (29002, 3203, now(), 'TWO-DAY');
insert into SALES_ORDER_ALL values (29003, 1, now(), 'STANDARD');
insert into SALES_ORDER_ALL values (29004, 3203, now(), 'TWO-DAY');
insert into SALES_ORDER_ALL values (29005, 1, now(), 'STANDARD');
