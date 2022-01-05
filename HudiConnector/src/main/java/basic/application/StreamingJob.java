/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package basic.application;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Map;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameter;
		parameter = ParameterTool.fromArgs(args);

		//read the parameters from the Kinesis Analytics environment
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		Properties flinkProperties = null;

		String kafkaTopic = parameter.get("kafka-topic", "AWSKafkaTutorialTopic");
		String brokers = parameter.get("brokers", "");
		String s3Path = parameter.get("s3Path", "");

		if (applicationProperties != null) {
			flinkProperties = applicationProperties.get("FlinkApplicationProperties");
		}

		if (flinkProperties != null) {
			kafkaTopic = flinkProperties.get("kafka-topic").toString();
			brokers = flinkProperties.get("brokers").toString();
			s3Path = flinkProperties.get("s3Path").toString();
		}

		LOG.info("kafkaTopic is :" + kafkaTopic);
		LOG.info("brokers is :" + brokers);
		LOG.info("s3Path is :" + s3Path);

		//Create Properties object for the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("bootstrap.servers", brokers);

		//Process stream using sql API
		StreamingSQLAPI.process(env, kafkaTopic, s3Path , kafkaProps);
	}


	public static class StreamingSQLAPI {

		public static void process(StreamExecutionEnvironment env, String kafkaTopic, String s3Path, Properties kafkaProperties)  {
			org.apache.flink.table.api.bridge.java.StreamTableEnvironment streamTableEnvironment = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(
					env, EnvironmentSettings.newInstance().useBlinkPlanner().build());

			Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
			configuration.setString("execution.checkpointing.interval", "1 min");
//			configuration.setString("table-exec-source-idle-timeout", "30 sec");

			final String createTableStmt = "CREATE TABLE IF NOT EXISTS CustomerTable (\n" +
					"  `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format\n" +
					"  `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format\n" +
					"  `record_time` TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,\n" +
					"  `CUST_ID` BIGINT,\n" +
					"  `NAME` STRING,\n" +
					"  `MKTSEGMENT` STRING,\n" +
					"   WATERMARK FOR event_time AS event_time\n" +
					") WITH (\n" +
					"  'connector' = 'kafka',\n" +
					"  'topic' = '"+ kafkaTopic +"',\n" +
					"  'properties.bootstrap.servers' = '"+  kafkaProperties.get("bootstrap.servers") +"',\n" +
					"  'properties.group.id' = 'kdaConsumerGroup',\n" +
					"  'scan.startup.mode' = 'earliest-offset',\n" +
					"  'value.format' = 'debezium-json'\n" +
					")";

			final String s3Sink = "CREATE TABLE IF NOT EXISTS `customer_hudi` (\n" +
					"  ts TIMESTAMP(3),\n" +
					"  customer_id BIGINT,\n" +
					"  name STRING,\n" +
					"  mktsegment STRING,\n" +
					"  PRIMARY KEY (`customer_id`) NOT Enforced\n" +
					")\n" +
					"PARTITIONED BY (`mktsegment`)\n" +
					"WITH (\n" +
					"  'connector' = 'hudi',\n" +
					"  'read.streaming.enabled' = 'true',\n" +
					"  'write.tasks' = '4',\n" +
					"  'path' = '" + s3Path + "',\n" +
					"  'hoodie.datasource.query.type' = 'snapshot',\n" +
					"  'table.type' = 'MERGE_ON_READ' --  MERGE_ON_READ table or, by default is COPY_ON_WRITE\n" +
					")";

			streamTableEnvironment.executeSql(createTableStmt);
			streamTableEnvironment.executeSql(s3Sink);

			final String insertSql = "insert into customer_hudi select event_time, CUST_ID,  NAME , MKTSEGMENT from CustomerTable";
			streamTableEnvironment.executeSql(insertSql);
		}
	}

}



