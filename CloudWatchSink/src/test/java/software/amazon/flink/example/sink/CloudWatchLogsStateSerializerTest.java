/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class CloudWatchLogsStateSerializerTest {

    @Test
    void serde() throws Exception {
        LogEvent expected = LogEvent.builder()
                .log("a dummy log event")
                .timestamp(Instant.now())
                .build();

        CloudWatchLogsStateSerializer serializer = new CloudWatchLogsStateSerializer();

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(os);

        serializer.serializeRequestToStream(expected, dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

        LogEvent actual = serializer.deserializeRequestFromStream(0, dis);

        assertThat(actual.getLog())
                .isEqualTo(expected.getLog());
        assertThat(actual.getTimestamp().toEpochMilli())
                .isEqualTo(expected.getTimestamp().toEpochMilli());
    }
}
