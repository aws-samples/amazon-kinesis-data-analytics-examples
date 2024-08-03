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

import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public final class ImageReader implements StreamFormat.Reader<StreamedImage> {

    private static final Logger logger = LoggerFactory.getLogger(ImageReader.class);

    private final FSDataInputStream in;

    ImageReader(FSDataInputStream in) {
        this.in = in;
    }

    @Nullable
    @Override
    public StreamedImage read() throws IOException {
        byte[] allBytes = in.readAllBytes();
        if(allBytes.length == 0)
        {
            return null;
        }

        try
        {
            // read all bytes as needed for image
            Image image = ImageFactory.getInstance().fromInputStream(new ByteArrayInputStream(allBytes));
            return new StreamedImage(image);
        }
        catch(Exception ex)
        {
            logger.error(ex.getLocalizedMessage());
            return null;
        }


    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}

final class ImageReaderFormat extends SimpleStreamFormat<StreamedImage> {
    private static final long serialVersionUID = 1L;

    @Override
    public Reader<StreamedImage> createReader(Configuration config, FSDataInputStream stream)
            throws IOException {
        return new ImageReader(stream);
    }

    @Override
    public TypeInformation<StreamedImage> getProducedType() {
        return TypeInformation.of(StreamedImage.class);
    }
}
