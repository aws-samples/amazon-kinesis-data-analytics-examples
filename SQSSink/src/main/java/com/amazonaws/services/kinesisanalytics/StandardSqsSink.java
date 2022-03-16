package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardSQSSink implements SinkFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(StandardSQSSink.class);

    private static final AmazonSQS SQS = AmazonSQSClientBuilder.defaultClient();

    private final String queueUrl;

    public StandardSQSSink(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public void invoke(String value, Context context) {
        SendMessageResult sendMessageResult = SQS.sendMessage(this.queueUrl, value);
        LOG.debug("message published to sqs - " + sendMessageResult);
    }
}
