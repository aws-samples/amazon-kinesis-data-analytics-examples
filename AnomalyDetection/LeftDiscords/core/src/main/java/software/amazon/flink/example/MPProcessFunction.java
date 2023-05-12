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

package software.amazon.flink.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MPProcessFunction extends ProcessFunction<String, OutputWithLabel> implements CheckpointedFunction {

    private transient ListState<TimeSeries> checkpointedTimeSeries;
    private transient ListState<Threshold> checkpointedThreshold;

    private TimeSeries timeSeriesData;
    private Threshold threshold;
    private final int sequenceLength;
    private final int initializationPeriods;

    private MPProcessFunction(int sequenceLength) {
        super();
        this.sequenceLength = sequenceLength;
        this.initializationPeriods = 16;
        this.timeSeriesData = new TimeSeries(64, sequenceLength, initializationPeriods);
        this.threshold = new Threshold();
    }

    public static MPProcessFunction withTimeSeriesPeriod(int period) {
        return new MPProcessFunction((int) Math.floor(period * 0.8));
    }

    @Override
    public void processElement(String dataPoint, ProcessFunction<String, OutputWithLabel>.Context context,
                               Collector<OutputWithLabel> collector) {

        double record = Double.parseDouble(dataPoint);

        int currentIndex = timeSeriesData.add(record);

        double minDistance = 0.0;
        String anomalyTag = "INITIALISING";

        if (timeSeriesData.readyToCompute()) {
            minDistance = timeSeriesData.computeNearestNeighbourDistance();
            threshold.update(minDistance);
        }

        /*
         * Algorithm will wait for initializationPeriods * sequenceLength data points until starting
         * to compute the Matrix Profile (MP).
         */
        if (timeSeriesData.readyToInfer()) {
            anomalyTag = minDistance > threshold.getThreshold() ? "IS_ANOMALY" : "IS_NOT_ANOMALY";
        }

        OutputWithLabel output = new OutputWithLabel(currentIndex, record, minDistance, anomalyTag);

        collector.collect(output);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedTimeSeries.clear();
        checkpointedThreshold.clear();

        checkpointedTimeSeries.add(this.timeSeriesData);
        checkpointedThreshold.add(this.threshold);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<TimeSeries> timeSeriesListStateDescriptor =
                new ListStateDescriptor<>(
                        "time-series-data",
                        TypeInformation.of(new TypeHint<>() {}));

        ListStateDescriptor<Threshold> thresholdListStateDescriptor =
                new ListStateDescriptor<>(
                        "threshold",
                        TypeInformation.of(new TypeHint<>() {}));

        checkpointedTimeSeries = context.getOperatorStateStore().getListState(timeSeriesListStateDescriptor);

        checkpointedThreshold = context.getOperatorStateStore().getListState(thresholdListStateDescriptor);

        if (context.isRestored()) {
            for (TimeSeries timeSeriesCheckpointedData : checkpointedTimeSeries.get()) {
                this.timeSeriesData = timeSeriesCheckpointedData;
            }

            for (Threshold threshold : checkpointedThreshold.get()) {
                this.threshold = threshold;
            }
        }
    }
}
