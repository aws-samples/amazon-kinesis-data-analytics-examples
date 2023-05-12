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

public class Threshold implements Serializable {

    private double sum = 0.0;
    private double squaredSum = 0.0;
    private int counter = 0;

    public void update(double record) {
        sum += record;
        squaredSum = squaredSum + record*record;
        counter = counter + 1;
    }

    /**
     * Computes the threshold as two standard deviations away from the mean (p = 0.02)
     *
     * @return an estimated threshold
     */
    public double getThreshold() {
        double mean = sum/counter;

        return mean + 2 * Math.sqrt(squaredSum / counter - mean * mean);
    }
}
