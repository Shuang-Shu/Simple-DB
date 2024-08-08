package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it
     * receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through
     * the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed. For
     * example, you shouldn't
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this
     *                class for histogramming
     * @param max     The maximum integer value that will ever be passed to this
     *                class for histogramming
     */
    private int[] bucket;
    private int min;
    private int max;
    private int totalRange;
    private double range;
    private int ntups;

    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        // max与min都在事先指定好，可视作先验条件
        this.bucket = new int[buckets];
        this.max = max;
        this.min = min;
        this.totalRange = max - min;
        this.range = (double) this.totalRange / buckets;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (int) ((v - this.min) / this.range);
        index = Math.min(this.bucket.length - 1, index);
        this.bucket[index] += 1;
        this.ntups += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int index = (int) ((v - this.min) / this.range);
        index = Math.min(index, this.bucket.length - 1);
        index = Math.max(index, 0);
        double leftBound = this.min + index * this.range;
        double rightBound = this.max;
        if (this.ntups == 0)
            return -1;
        switch (op) {
            case EQUALS:
                if (v > this.max || v < this.min)
                    return 0;
                return (Math.min(((double) this.bucket[index] / (double) this.range), this.bucket[index]))
                        / (double) this.ntups;
            case GREATER_THAN:
                // 比v更大的值估计
                if (v < this.min)
                    return 1;
                else if (v > this.max)
                    return 0;
                int sum = 0;
                if (v < leftBound)
                    return 1;
                for (int i = index + 1; i < this.bucket.length; ++i)
                    sum += this.bucket[i];
                return (double) sum / this.ntups;
            case LESS_THAN:
                if (v > this.max)
                    return 1;
                else if (v < this.min)
                    return 0;
                return Math.max(0, 1 - this.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v)
                        - this.estimateSelectivity(Predicate.Op.EQUALS, v));
            case GREATER_THAN_OR_EQ:
                if (v < this.min)
                    return 1;
                else if (v > this.max)
                    return 0;
                rightBound = this.min + (index + 1) * this.range;
                double temp = (this.bucket[index] / (double) this.ntups) * (rightBound - v) / this.range;
                return this.estimateSelectivity(Predicate.Op.GREATER_THAN, v) + temp;
            case LESS_THAN_OR_EQ:
                return 1 - this.estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case NOT_EQUALS:
                return Math.max(0, 1 - this.estimateSelectivity(Predicate.Op.EQUALS, v));
            default:
                break;
        }
        return -1;
    }

    /**
     * @return
     *         the average selectivity of this histogram.
     * 
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    public int getNtups() {
        return this.ntups;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String result = "";
        double leftBound = 0;
        double rightBound = 0;
        for (int i = 0; i < this.bucket.length - 1; ++i) {
            leftBound = this.min + i * this.range;
            rightBound = this.min + (i + 1) * this.range;
            result += "[";
            result += Double.toString(leftBound) + "," + Double.toString(rightBound) + "):"
                    + Integer.toString(this.bucket[i]);
            result += ", ";
        }
        leftBound = this.min + (this.bucket.length - 1) * this.range;
        rightBound = this.max;
        result += "[";
        result += Double.toString(leftBound) + "," + Double.toString(rightBound) + "):"
                + Integer.toString(this.bucket[this.bucket.length - 1]);
        return result;
    }
}
