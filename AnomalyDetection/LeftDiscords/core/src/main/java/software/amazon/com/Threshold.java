package software.amazon.com;

public class Threshold {

    Double sum;
    Double squaredSum;
    int counter;

    public Threshold() {
        this.sum = 0.0;
        this.squaredSum = 0.0;
        this.counter = 0;
    }

    public void update(Double record) {
        sum = sum + record;
        squaredSum = squaredSum + record*record;
        counter = counter + 1;
    }

    /**
     * Computes the threshold as two standard deviations away from the mean (p = 0.02)
     *
     * @return an estimated threshold
     */
    public Double getThreshold() {
        Double mean = sum/counter;

        return mean + 2 * Math.sqrt(squaredSum/counter - mean*mean);
    }
}
