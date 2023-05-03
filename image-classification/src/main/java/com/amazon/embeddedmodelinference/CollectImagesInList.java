package com.amazon.embeddedmodelinference;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class CollectImagesInList extends ProcessFunction<StreamedImage, List<StreamedImage>> implements CheckpointedFunction {

    private static final Logger logger = LoggerFactory.getLogger(EMI.class);
    /**
     * The ListState with count, a running sum.
     */
    private transient ListState<StreamedImage> checkpointedImages;
    private List<StreamedImage> bufferedImages;
    private transient ValueState<Long> lastTimer;
    private int bufferSize;

    public CollectImagesInList(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    @Override
    public void open(Configuration config) {
        // for pytorch
        System.setProperty("PYTORCH_PRECXX11", "true");

        bufferedImages = new ArrayList<>();

        // setup timer state
        ValueStateDescriptor<Long> lastTimerDesc =
                new ValueStateDescriptor<Long>("lastTimer", Long.class);
        lastTimer = getRuntimeContext().getState(lastTimerDesc);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedImages.clear();
        checkpointedImages.addAll(bufferedImages);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<StreamedImage> descriptor =
                new ListStateDescriptor<StreamedImage>("image-list",
                        TypeInformation.of(new TypeHint<StreamedImage>() {
                        }));

        checkpointedImages = context.getKeyedStateStore().getListState(descriptor);

        if(context.isRestored())
        {
            for(StreamedImage image: checkpointedImages.get())
            {
                bufferedImages.add(image);
            }
        }
    }

    @Override
    public void processElement(StreamedImage streamedImage, ProcessFunction<StreamedImage, List<StreamedImage>>.Context context, Collector<List<StreamedImage>> collector) throws Exception {
        bufferedImages.add(streamedImage);

        // if the count is equal to the buffer size, emit and clear
        if (bufferedImages.size() >= bufferSize) {
            collector.collect(bufferedImages);
            checkpointedImages.clear();
            bufferedImages.clear();
        }
        else {
            // get current time and compute timeout time
            long currentTime = context.timerService().currentProcessingTime();
            long timeoutTime = currentTime + Duration.ofMinutes(5).toMillis();
            // register timer for timeout time
            context.timerService().registerProcessingTimeTimer(timeoutTime);
            // remember timeout time
            lastTimer.update(timeoutTime);
        }
    }

    @Override
    public void onTimer(long timestamp, ProcessFunction<StreamedImage, List<StreamedImage>>.OnTimerContext ctx, Collector<List<StreamedImage>> out) throws Exception {
        // check if this was the last timer we registered
        if (lastTimer.value().compareTo(timestamp) == 0) {
            // it was, so no data was received afterwards.
            // fire an alert.
            if(bufferedImages.size() > 0)
                out.collect(bufferedImages);
            bufferedImages.clear();
            checkpointedImages.clear();
        }
    }
}
