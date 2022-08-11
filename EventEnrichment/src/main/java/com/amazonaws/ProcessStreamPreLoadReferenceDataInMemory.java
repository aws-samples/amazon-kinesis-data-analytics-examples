package com.amazonaws;

import com.amazonaws.operators.PartitionPreLoadEnrichmentData;
import com.amazonaws.operators.PreLoadEnrichmentDataInMemory;
import com.amazonaws.pojo.Customer;
import com.amazonaws.utils.KinesisStreamInitialiser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProcessStreamPreLoadReferenceDataInMemory {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessStreamPreLoadReferenceDataInMemory.class);
    private static final String DATA_STREAM_NAME = "event-data-enrichment";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //read the parameters specified from the command line
        ParameterTool parameter = ParameterTool.fromArgs(args);

        Properties kinesisConsumerConfig = KinesisStreamInitialiser.getKinesisConsumerConfig(parameter);
        System.out.println(kinesisConsumerConfig);
        //create Kinesis source
        DataStream<Customer> customerStream = KinesisStreamInitialiser.getKinesisStream(env, kinesisConsumerConfig, DATA_STREAM_NAME);

        customerStream = customerStream
                //remove all events that aren't CustomerEvent
                .filter(event -> Customer.class.isAssignableFrom(event.getClass()))
                .keyBy(event -> event.getRole())
                .flatMap(new PreLoadEnrichmentDataInMemory());

        //print customerStream to stdout
        customerStream.print();


        LOG.info("Reading events from stream {}", parameter.get("InputStreamName", DATA_STREAM_NAME));

        env.execute();
    }

}
