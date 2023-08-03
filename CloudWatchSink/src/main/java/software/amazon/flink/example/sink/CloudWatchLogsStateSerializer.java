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

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CloudWatchLogsStateSerializer extends AsyncSinkWriterStateSerializer<LogEvent> {

    @Override
    protected void serializeRequestToStream(LogEvent logEvent, DataOutputStream dataOutputStream) throws IOException {
        writeString(logEvent.getLog(), dataOutputStream);
        dataOutputStream.writeLong(logEvent.getTimestamp().toEpochMilli());
    }

    @Override
    protected LogEvent deserializeRequestFromStream(long l, DataInputStream dataInputStream) throws IOException {
        return LogEvent.builder()
                .log(readString(dataInputStream))
                .timestamp(Instant.ofEpochMilli(dataInputStream.readLong()))
                .build();
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
