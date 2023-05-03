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
