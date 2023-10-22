/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.amazon.embeddedmodelinference;

import ai.djl.ModelException;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.translate.TranslateException;
import com.amazon.embeddedmodelinference.ml.Classifier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.FileSystem;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class EMI {

    private static final Logger logger = LoggerFactory.getLogger(EMI.class);
    private static Classifier classifier = Classifier.getInstance();
    private static String s3SourcePath;
    private static String s3SinkPath;

    // time (in seconds) to buffer before sending to classify
    private static int listOfImagesBufferDuration;


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env;
        String isLocal = System.getenv("IS_LOCAL");

        env = setUpStreamingEnvironmentAndReturnEnv(isLocal);


        final FileSource<StreamedImage> source =
                FileSource.forRecordStreamFormat(new ImageReaderFormat(), new Path(s3SourcePath))
                        .monitorContinuously(Duration.ofSeconds(10))
                        .build();

        DataStream<StreamedImage> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");


        // create a keyed window in order to get an even distribution of data across the subtasks
        DataStream<String> classifications = stream.keyBy(x -> x.getId())

                // the image classifier performs better with batches of images
                // so we will use tumbling window to create these batches
                .window(TumblingProcessingTimeWindows.of(Time.seconds(listOfImagesBufferDuration)))

                // and process those batches in a process window function
                .process(new ProcessWindowFunction<StreamedImage, String, String, TimeWindow>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        // for pytorch
                        System.setProperty("PYTORCH_PRECXX11", "true");
                    }

                    @Override
                    public void process(String s,
                                        ProcessWindowFunction<StreamedImage, String, String, TimeWindow>.Context context,
                                        Iterable<StreamedImage> iterableImages,
                                        Collector<String> out) throws Exception {


                            List<Image> listOfImages = new ArrayList<Image>();
                            iterableImages.forEach(x -> {
                                listOfImages.add(x.getImage());
                            });
                        try
                        {
                            // batch classify images
                            List<Classifications> list = classifier.predict(listOfImages);
                            for (Classifications classifications : list) {
                                Classifications.Classification cl = classifications.best();
                                String ret = cl.getClassName() + ": " + cl.getProbability();
                                out.collect(ret);
                            }
                        } catch (ModelException | IOException | TranslateException e) {
                            logger.error("Failed predict", e);
                        }
                        }
                    });

        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(s3SinkPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(256))
                                .build())
                .build();
        classifications.sinkTo(sink);

        env.execute("Embedded Model Inference");
    }

    private static StreamExecutionEnvironment setUpStreamingEnvironmentAndReturnEnv(String isLocal) throws IOException {
        StreamExecutionEnvironment env;
        // if running locally, set up local flink environment with WebUI
        if (isLocal != null && isLocal.equals("true")) {
            Configuration configs = new Configuration();
            configs.setString("s3.access.key", System.getenv("s3.access.key"));
            configs.setString("s3.secret.key", System.getenv("s3.secret.key"));
            configs.setString("rest.flamegraph.enabled", "true");
            configs.setString(ConfigConstants.ENV_FLINK_PLUGINS_DIR, System.getenv("plugins.dir"));

            PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configs);
            FileSystem.initialize(configs, pluginManager);

            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configs);
            env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
            env.setStateBackend(new HashMapStateBackend());

            String checkpointPath = "file://" + System.getProperty("user.dir") + "/local-checkpoints";
            env.getCheckpointConfig().setCheckpointStorage(checkpointPath);


            // set up your s3 bucket(s) in s3.source.path and s3.sink.path or update these variables here
            String bucket = "my-sample-images";
            String prefix = "";
            String fullPathSource = "s3://" + bucket + "/" + prefix;
            String fullPathSink = "s3://" + bucket + "/" + prefix + "/output";
            s3SourcePath = System.getProperty("s3.source.path", fullPathSource);
            s3SinkPath = System.getProperty("s3.sink.path", fullPathSink);
            listOfImagesBufferDuration = Integer.parseInt(System.getProperty("image.buffer.duration", "60"));

        } else // remote server on KDA
        {

            env = StreamExecutionEnvironment.getExecutionEnvironment();

            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties properties = applicationProperties.get("appProperties");
            s3SourcePath = properties.getProperty("s3.source.path");
            s3SinkPath = properties.getProperty("s3.sink.path");
            listOfImagesBufferDuration = Integer.parseInt(properties.getProperty("image.buffer.duration", "60"));

        }
        return env;
    }


}
