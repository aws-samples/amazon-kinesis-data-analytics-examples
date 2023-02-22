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

package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	private static void configureConnectorPropsWithConfigProviders(KafkaSourceBuilder<String> builder, Properties appProperties) {
		// see https://github.com/aws-samples/msk-config-providers

		// define names of config providers:
		builder.setProperty("config.providers", "secretsmanager,s3import");

		// provide implementation classes for each provider:
		builder.setProperty("config.providers.secretsmanager.class", "com.amazonaws.kafka.config.providers.SecretsManagerConfigProvider");
		builder.setProperty("config.providers.s3import.class", "com.amazonaws.kafka.config.providers.S3ImportConfigProvider");

		String region = appProperties.get(Helpers.S3_BUCKET_REGION_KEY).toString();
		String keystoreS3Bucket = appProperties.get(Helpers.KEYSTORE_S3_BUCKET_KEY).toString();
		String keystoreS3Path = appProperties.get(Helpers.KEYSTORE_S3_PATH_KEY).toString();
		String truststoreS3Bucket = appProperties.get(Helpers.TRUSTSTORE_S3_BUCKET_KEY).toString();
		String truststoreS3Path = appProperties.get(Helpers.TRUSTSTORE_S3_PATH_KEY).toString();
		String keystorePassSecret = appProperties.get(Helpers.KEYSTORE_PASS_SECRET_KEY).toString();
		String keystorePassSecretField = appProperties.get(Helpers.KEYSTORE_PASS_SECRET_FIELD_KEY).toString();

		// region, etc..
		builder.setProperty("config.providers.s3import.param.region", region);

		// properties
		builder.setProperty("ssl.truststore.location", "${s3import:" + region + ":" + truststoreS3Bucket + "/" + truststoreS3Path + "}");
		builder.setProperty("ssl.keystore.type", "PKCS12");
		builder.setProperty("ssl.keystore.location", "${s3import:" + region + ":" + keystoreS3Bucket + "/" + keystoreS3Path + "}");
		builder.setProperty("ssl.keystore.password", "${secretsmanager:" + keystorePassSecret + ":" + keystorePassSecretField + "}");
		builder.setProperty("ssl.key.password", "${secretsmanager:" + keystorePassSecret + ":" + keystorePassSecretField + "}");
	}

	private static KafkaSource<String> getKafkaSource(StreamExecutionEnvironment env,
													 Properties appProperties) {

		KafkaSourceBuilder<String> builder = KafkaSource.builder();

		String brokers = appProperties.get(Helpers.MSKBOOTSTRAP_SERVERS).toString();
		String inputTopic = appProperties.get(Helpers.KAFKA_SOURCE_TOPIC_KEY).toString();
		String consumerGroupId = appProperties.get(Helpers.KAFKA_CONSUMER_GROUP_ID_KEY).toString();

		builder.setProperty("security.protocol", "SSL");

		configureConnectorPropsWithConfigProviders(builder, appProperties);

		KafkaSource<String> source = builder
				.setBootstrapServers(brokers)
				.setTopics(inputTopic)
				.setGroupId(consumerGroupId)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		return source;
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties appProperties = Helpers.getAppProperties();
		if(appProperties == null) {
			LOG.error("Incorrectly specified application properties. Exiting...");
			return;
		}

		KafkaSource<String> kafkaSource = getKafkaSource(env, appProperties);
		DataStream<String> orderStream = env.fromSource(kafkaSource,
				WatermarkStrategy.noWatermarks(),
				"Kafka Source");

		orderStream.print();

		// execute program
		env.execute("KDA Flink from MSK mTLS");
	}
}