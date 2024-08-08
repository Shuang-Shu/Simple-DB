package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IO_COST_PER_PAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    private DbFile dbFile;
    private IntHistogram[] intHistogram;
    private int[] max;
    private int[] min;
    private boolean[] isIntField;
    private int ioCostPerPage;
    private int totalTups;

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IO_COST_PER_PAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *                      The table over which to compute statistics
     * @param ioCostPerPage
     *                      The cost per page of IO. This doesn't differentiate
     *                      between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc tempTd = this.dbFile.getTupleDesc();
        this.min = new int[tempTd.numFields()];
        this.max = new int[tempTd.numFields()];
        this.isIntField = new boolean[tempTd.numFields()];
        for (int i = 0; i < tempTd.numFields(); ++i) {
            this.max[i] = Integer.MIN_VALUE;
            this.min[i] = Integer.MAX_VALUE;
            if (tempTd.getFieldType(i) == (Type.INT_TYPE)) {
                this.isIntField[i] = true;
            }
        }
        this.intHistogram = new IntHistogram[this.dbFile.getTupleDesc().numFields()];
        DbFileIterator tupleIterator = this.dbFile.iterator(null);
        try {
            tupleIterator.open();
            while (tupleIterator.hasNext()) {
                this.totalTups += 1;
                Tuple tuple = tupleIterator.next();
                TupleDesc td = tuple.getTupleDesc();
                for (int i = 0; i < td.numFields(); ++i) {
                    if (this.isIntField[i]) {
                        int intFieldVal = ((IntField) tuple.getField(i)).getValue();
                        this.min[i] = Math.min(this.min[i], intFieldVal);
                        this.max[i] = Math.max(this.max[i], intFieldVal);
                    }
                }
            }
            tupleIterator.rewind();
            for (int i = 0; i < this.dbFile.getTupleDesc().numFields(); ++i) {
                if (this.isIntField[i]) {
                    this.intHistogram[i] = new IntHistogram(NUM_HIST_BINS, this.min[i], this.max[i]);
                }
            }
            while (tupleIterator.hasNext()) {
                Tuple tuple = tupleIterator.next();
                TupleDesc td = tuple.getTupleDesc();
                for (int i = 0; i < td.numFields(); ++i) {
                    if (isIntField[i]) {
                        this.intHistogram[i].addValue(((IntField) tuple.getField(i)).getValue());
                    }
                }
            }
            tupleIterator.close();
        } catch (Exception e) {
            System.out.println("tupleIterator error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        int pageNum = (int) (((HeapFile) this.dbFile).getFile().length() / BufferPool.getPageSize());
        return pageNum * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *                          The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        long ntups = 0;
        for (int i = 0; i < this.isIntField.length; ++i) {
            if (this.isIntField[i]) {
                ntups = this.intHistogram[i].getNtups();
            }
        }
        return (int) (ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * 
     * @param field
     *              the index of the field
     * @param op
     *              the operator in the predicate
     *              The semantic of the method is that, given the table, and then
     *              given a
     *              tuple, of which we do not know the value of the field, return
     *              the
     *              expected selectivity. You may estimate this value from the
     *              histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *                 The field over which the predicate ranges
     * @param op
     *                 The logical operation in the predicate
     * @param constant
     *                 The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        switch (constant.getType()) {
            case STRING_TYPE:
                return 1.0;
            case INT_TYPE:
                return this.intHistogram[field].estimateSelectivity(op, ((IntField) constant).getValue());
            default:
                return 1.0;
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return this.totalTups;
    }

}
