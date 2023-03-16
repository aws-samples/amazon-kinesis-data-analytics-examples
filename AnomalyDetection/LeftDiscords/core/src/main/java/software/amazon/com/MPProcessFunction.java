package software.amazon.com;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONException;
import org.json.JSONObject;

public class MPProcessFunction extends ProcessFunction<String, String> {

    private final TimeSeries timeSeriesData;
    private int currentIndex = 0;
    private final int sequenceLength;
    private final int initializationPeriods;
    private final Threshold threshold;

    private MPProcessFunction(int sequenceLength) {
        super();
        this.sequenceLength = sequenceLength;
        this.initializationPeriods = 16;
        this.timeSeriesData = new TimeSeries(64, sequenceLength, initializationPeriods);
        this.threshold = new Threshold();
    }

    public static MPProcessFunction withTimeSeriesPeriod(int period) {
        return new MPProcessFunction((int) Math.floor(period * 0.8));
    }

    @Override
    public void processElement(String dataPoint, ProcessFunction<String, String>.Context context,
                               Collector<String> collector) throws JSONException {

        Double record = Double.parseDouble(dataPoint);

        timeSeriesData.add(record);

        Double minDistance = 0.0;
        String anomalyTag = "INITIALISING";

        if (timeSeriesData.readyToCompute()) {
            minDistance = timeSeriesData.computeNearestNeighbourDistance();
            threshold.update(minDistance);
        }

        /**
         * Algorithm will wait for initializationPeriods * sequenceLength data points until starting
         * to compute the Matrix Profile (MP).
         */
        if (timeSeriesData.readyToInfer()) {
            anomalyTag = minDistance > threshold.getThreshold() ? "IS_ANOMALY" : "IS_NOT_ANOMALY";
        }

        JSONObject output = new JSONObject();

        output.put("index", currentIndex);
        output.put("input", dataPoint);
        output.put("aMP", minDistance);
        output.put("anomalyTag", anomalyTag);

        collector.collect(output.toString());

        currentIndex += 1;
    }
}
