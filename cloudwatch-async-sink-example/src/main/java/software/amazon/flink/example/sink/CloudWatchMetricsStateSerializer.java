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

package software.amazon.flink.example.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CloudWatchMetricsStateSerializer extends AsyncSinkWriterStateSerializer<MetricDatum> {

    @Override
    protected void serializeRequestToStream(MetricDatum metricDatum, DataOutputStream dataOutputStream) throws IOException {
        writeString(metricDatum.metricName(), dataOutputStream);
        dataOutputStream.writeLong(metricDatum.timestamp().toEpochMilli());
        dataOutputStream.writeDouble(metricDatum.value());

        dataOutputStream.writeInt(metricDatum.dimensions().size());
        for (Dimension dimension : metricDatum.dimensions()) {
            writeString(dimension.name(), dataOutputStream);
            writeString(dimension.value(), dataOutputStream);
        }
    }

    @Override
    protected MetricDatum deserializeRequestFromStream(long l, DataInputStream dataInputStream) throws IOException {
        MetricDatum.Builder builder = MetricDatum.builder();

        builder.metricName(readString(dataInputStream));
        builder.timestamp(Instant.ofEpochMilli(dataInputStream.readLong()));
        builder.value(dataInputStream.readDouble());

        int dimensionCount = dataInputStream.readInt();
        List<Dimension> dimensions = new ArrayList<>(dimensionCount);

        for (int i = 0; i < dimensionCount; i++) {
            dimensions.add(Dimension.builder()
                    .name(readString(dataInputStream))
                    .value(readString(dataInputStream))
                    .build());
        }

        builder.dimensions(dimensions);

        return builder.build();
    }

    @Override
    public int getVersion() {
        return 1;
    }

    private void writeString(final String str, final DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(str.length());
        dataOutputStream.writeBytes(str);
    }

    private String readString(final DataInputStream dataInputStream) throws IOException {
        return new String(dataInputStream.readNBytes(dataInputStream.readInt()), UTF_8);
    }
}
