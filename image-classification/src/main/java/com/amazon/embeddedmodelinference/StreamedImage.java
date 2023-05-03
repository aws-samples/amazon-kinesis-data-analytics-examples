package com.amazon.embeddedmodelinference;

import ai.djl.modality.cv.Image;

import java.util.UUID;

public class StreamedImage {

    private Image image;
    private String id;

    public StreamedImage(Image image) {
        this.image = image;
        this.id = UUID.randomUUID().toString();
    }


    public String getId() {
        return id;
    }

    public Image getImage() {
        return image;
    }

    @Override
    public String toString() {
        return id;
    }
}
