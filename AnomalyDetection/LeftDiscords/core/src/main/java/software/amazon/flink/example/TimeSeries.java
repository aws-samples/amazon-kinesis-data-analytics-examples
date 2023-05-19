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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TimeSeries implements Serializable {

    private int size = 0;
    private final int maxSize;
    private final int sequenceLength;
    private final int initializationPeriods;

    private final List<Double> contextWindow;

    private int currentIndex;

    private int timeSeriesIndex = 0;

    public TimeSeries(int contextWindowSize, int sequenceLength, int initializationPeriods) {
        this.sequenceLength = sequenceLength;
        this.maxSize = contextWindowSize*sequenceLength;
        this.initializationPeriods = initializationPeriods;

        this.contextWindow = new ArrayList<>(maxSize);

        for(int i = 0; i < maxSize; i++) {
            contextWindow.add(null);
        }

        this.currentIndex = 0;
    }

    private int getIndex(int currentIndex, int step) {
        return (currentIndex + maxSize + step) % maxSize;
    }

    /**
     * Adds an element to the TimeSeries.
     * If the contextWindow is full, overrides the oldest record.
     *
     * @param element the element to be added
     */
    public int add(Double element) {
        contextWindow.set(currentIndex, element);

        currentIndex = getIndex(currentIndex, 1);

        size = Math.min(size + 1, maxSize);

        timeSeriesIndex += 1;

        return this.timeSeriesIndex;
    }

    private int size() {
        return size;
    }

    public Boolean readyToCompute() {
        return size() > 2 * sequenceLength;
    }

    public Boolean readyToInfer() {
        return size() > initializationPeriods * sequenceLength;
    }

    /**
     * Finds the distance from the closest non-self-matching subsequence
     * in the context window and the current subsequence.
     *
     * @return the distance to the closest neighbour
     */
    public Double computeNearestNeighbourDistance() {
        double minDistance = Double.MAX_VALUE;

        int prevNonSelfMatching = getIndex(currentIndex, -(2*sequenceLength));

        while (prevNonSelfMatching != currentIndex) {
            Double currDistance = computeSquaredDistance(prevNonSelfMatching);
            minDistance = Math.min(currDistance, minDistance);

            prevNonSelfMatching = getIndex(prevNonSelfMatching, -1);
        }

        return minDistance;
    }

    /**
     * Computes the Euclidean distance between the last subsequence of length sequenceLength
     * and the subsequence of length sequenceLength starting in startingIndex
     *
     * @return the distance between the two subsequences
     */
    private Double computeSquaredDistance(int startingIndex) {
        double squaredDistance = 0.0;

        int otherVectorIndex = startingIndex;
        int currentVectorIndex = getIndex(currentIndex, -sequenceLength);

        for (int i = 0; i < sequenceLength && contextWindow.get(otherVectorIndex) != null; i++) {
            Double difference = contextWindow.get(otherVectorIndex) - contextWindow.get(currentVectorIndex);
            squaredDistance += difference * difference;

            otherVectorIndex = getIndex(otherVectorIndex, 1);
            currentVectorIndex = getIndex(currentVectorIndex, 1);
        }

        return squaredDistance;
    }
}