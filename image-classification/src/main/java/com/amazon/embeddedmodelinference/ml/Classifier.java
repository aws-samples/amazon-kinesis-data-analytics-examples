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
package com.amazon.embeddedmodelinference.ml;

import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.metric.Metrics;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class Classifier implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(Classifier.class);

    private static Classifier instance;

    private String modelUrl;

    private transient ZooModel<Image, Classifications> model;
    private transient Predictor<Image, Classifications> predictor;

    private Classifier(String modelUrl) {
        this.modelUrl = modelUrl;
    }

    public static Classifier getInstance() {
        return getInstance(null);
    }

    public static synchronized Classifier getInstance(String modelUrl) {
        if (instance == null) {
            if (modelUrl == null) {
                modelUrl = "djl://ai.djl.pytorch/resnet";
                logger.info("Use resnet18 from DJL model zoo.");
            } else {
                logger.info("Loading model from: {}", modelUrl);
            }
            instance = new Classifier(modelUrl);
        }
        return instance;
    }

    public List<Classifications> predict(List<Image> images) throws ModelException, IOException, TranslateException {
        return getPredictor().batchPredict(images);
    }

    public synchronized void close() {
        if (predictor != null) {
            predictor.close();
            model.close();
            predictor = null;
            model = null;
        }
    }

    private Predictor<Image, Classifications> getPredictor() throws ModelException, IOException {
        if (predictor == null) {
            Criteria<Image, Classifications> criteria = Criteria.builder()
                    .setTypes(Image.class, Classifications.class)
                    .optModelUrls(modelUrl)
                    .build();
            model = criteria.loadModel();
            predictor = model.newPredictor();
            Metrics metrics = new Metrics();
            metrics.setLimit(100); // print metrics every 100 inference
            metrics.setOnLimit((m, k) -> {
                System.out.println(m.percentile(k, 50));
            });
        }
        return predictor;
    }
}
