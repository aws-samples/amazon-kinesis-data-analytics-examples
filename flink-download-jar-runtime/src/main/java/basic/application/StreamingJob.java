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
 *
 * This file has been extended from the Apache Flink project skeleton.
 */


package basic.application;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
 *
 * <p>Disclaimer: This code is not production ready.</p>
 *
 */
public class StreamingJob {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env, String region, String inputStreamName) {
		WatermarkStrategy watermarkStrategy = WatermarkStrategy
				.forBoundedOutOfOrderness(Duration.ofSeconds(20)).withIdleness(Duration.ofMinutes(1));
		Properties inputProperties = new Properties();

		inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
		inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");
		inputProperties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
		return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties))
				.assignTimestampsAndWatermarks(watermarkStrategy);
	}

	private static StreamingFileSink<String> createS3SinkFromStaticConfig(String s3SinkPath) {

		final StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new org.apache.flink.core.fs.Path(s3SinkPath), new SimpleStringEncoder<String>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
								.withInactivityInterval(TimeUnit.SECONDS.toMillis(15))
								.withMaxPartSize(10 * 1024)
								.build())
				.build();
		return sink;
	}


	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final ParameterTool parameter = ParameterTool.fromArgs(args);
		//read the parameters from the Kinesis Analytics environment
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		Properties flinkProperties = null;


		String s3SinkPath = parameter.get("s3SinkPath", "");
		String region = parameter.get("region", "us-east-1");
		String inputStreamName = parameter.get("inputStreamName", "");
		String userJarBucket = parameter.get("userJarBucket", "{AWS BUCKET NAME}");
		String userJarKey = parameter.get("userJarKey", "{USER JAR KEY PREFIX (i.e. prefix/myfile.jar)}");

		if (applicationProperties != null) {
			flinkProperties = applicationProperties.get("FlinkApplicationProperties");
		}

		if (flinkProperties != null) {
			s3SinkPath = flinkProperties.get("s3SinkPath").toString();
			region = flinkProperties.get("region").toString();
			inputStreamName = flinkProperties.get("inputStreamName").toString();
			userJarBucket = flinkProperties.get("userJarBucket").toString();
			userJarKey = flinkProperties.get("userJarKey").toString();

		}

		final String userJarFileName = userJarKey.substring(userJarKey.lastIndexOf("/") + 1);
		final String userJarFileURI = "/tmp/" + userJarKey.substring(userJarKey.lastIndexOf("/") + 1);

		LOG.info("s3SinkPath is :" + s3SinkPath);
		LOG.info("region is :" + region);
		LOG.info("inputStreamName is :" + inputStreamName);
		LOG.info("userJarBucket is :" + userJarBucket);
		LOG.info("userJarKey is :" + userJarKey);

		downloadUserJars(region, userJarKey, userJarFileURI,userJarBucket);

		LOG.info("Starting the application...");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// If the referenced JAR is needed to run as part of the Flink operators, usually it's a best practice to
		// ... download the jar files and save them in the cache store. This way Task Managers can access them with no delay.
		env.registerCachedFile(userJarFileURI, userJarKey);
		env.getCheckpointConfig().setCheckpointInterval(Time.minutes(1).toMilliseconds());

		DataStream<String> input = createSourceFromStaticConfig(env, region, inputStreamName);
		SingleOutputStreamOperator<Tuple2<String, Integer>> tokenizedStream = input
				.process(new ProcessTokenizer(userJarKey));

		DataStream<String> inputStream = tokenizedStream
				.keyBy(event -> event.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(15)))
				.sum(1)
				.map(value -> value.f0 + " Count: " + value.f1 + "\n");

		inputStream.addSink(createS3SinkFromStaticConfig(s3SinkPath));
		env.execute("Flink Streaming Java API Skeleton");
	}


	/**
	 * @param region specifies the AWS region
	 * @param userJarKey is the prefix of the JAR in Amazon S3
	 * @param userJarFileURI is the location URI, where the JAR is is downloaded
	 * @param userJarBucket is the bucket name, form where the JAR file should be downloaded
	 * @throws IOException
	 */
	private static void downloadUserJars(String region, String userJarKey,String userJarFileURI, String userJarBucket) throws IOException {
		Region aws_region = Region.of(region);

		S3Client s3 = S3Client.builder()
				.region(aws_region)
				.build();

		// Get an object and print its contents.
		LOG.info("Downloading " + userJarKey);

		Path myFile = Paths.get(userJarFileURI);

		GetObjectRequest request = GetObjectRequest.builder()
				.bucket(userJarBucket)
				.key(userJarKey)
				.build();

		s3.getObject(request, myFile);
		s3.close();
	}

	// you don't need this process function if the JAR you're downloading only needs to be referenced in the main method
	// ... and not within the operators.
	public static final class ProcessTokenizer  extends ProcessFunction<String, Tuple2<String, Integer>> {
		private final String userJarFileName;

		public ProcessTokenizer(String userJarFileName) {
			this.userJarFileName = userJarFileName;
		}
		/** The state that is maintained by this process function */
		private URLClassLoader userCodeClassLoader;
		private Class clazz;
		// This method will be called once each time the job starts or restarts.
		//... you can use the URLClassLoader directly in main method if you don't need to reference the
		//... downloaded JAR in the stream processing operators
		@Override
		public void open(Configuration parameters) throws Exception {

			File jar = getRuntimeContext().getDistributedCache().getFile(userJarFileName);
			ClassLoader original = getRuntimeContext().getUserCodeClassLoader();
			userCodeClassLoader = new URLClassLoader(new URL[]{ jar.toURL() }, original);

			// Get the reference to the class
			clazz = userCodeClassLoader.loadClass("com.myapp.core.utils.EventData");
		}

		@Override
		public void processElement(String s, ProcessFunction<String, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {

			String[] tokens = s.toLowerCase().split("\\W+");
			for (String token : tokens) {
				// Create the instance of the loaded class
				Object instance = clazz.getConstructor(String.class, Date.class, int.class, String.class).newInstance(
						s,
						new Date(System.currentTimeMillis()),
						tokens.length,
						token
				);
				// invoke any method from the class.
				Method m = clazz.getMethod("methodName");
				String result = (String)m.invoke(instance);
				if (token.length() > 0) {
					collector.collect(new Tuple2<>(token + result, 1));
				}
			}
		}

		@Override
		public void close() throws Exception {
			userCodeClassLoader.close();
		}
	}

}
