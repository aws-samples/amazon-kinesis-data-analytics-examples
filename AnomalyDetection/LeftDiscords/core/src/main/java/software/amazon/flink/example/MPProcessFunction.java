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

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MPProcessFunction extends ProcessFunction<String, OutputWithLabel> {

    private final TimeSeries timeSeriesData;
    private int currentIndex = 0;
    private final int sequenceLength;
    private final int initializationPeriods;
    private final Threshold threshold;

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

        Double record = Double.parseDouble(dataPoint);

        timeSeriesData.add(record);

        Double minDistance = 0.0;
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

        currentIndex += 1;
    }
}
