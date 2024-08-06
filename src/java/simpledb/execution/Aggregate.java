package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     *               为本运算提供元组的OpIterator操作符
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    private OpIterator child;

    private int groupField;
    private int aggregateFiled;

    private Aggregator.Op op;
    private Aggregator aggregator;
    
    private TupleDesc tupleDesc;
    private OpIterator aggResultIter;

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.groupField = gfield;
        this.aggregateFiled = afield;
        this.op = aop;
        this.tupleDesc = child.getTupleDesc();
        if (this.groupField != -1) {
            // 若需要进行分组
            if (this.tupleDesc.getFieldType(afield).equals(Type.INT_TYPE)) {
                this.aggregator = new IntegerAggregator(gfield, this.tupleDesc.getFieldType(this.groupField), afield,
                        this.op);
            } else {
                this.aggregator = new StringAggregator(gfield, this.tupleDesc.getFieldType(this.groupField), afield,
                        this.op);
            }
        } else {
            this.aggregator = new IntegerAggregator(this.groupField, null, this.aggregateFiled, this.op);
        }
        try {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.aggResultIter = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.tupleDesc.getFieldName(groupField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aggregateFiled;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return this.tupleDesc.getFieldName(this.aggregateFiled);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        aggResultIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.aggResultIter.hasNext())
            return this.aggResultIter.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.aggResultIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.aggResultIter.getTupleDesc();
    }

    public void close() {
        // some code goes here
        super.close();
        this.aggResultIter.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] result = { this.child };
        return result;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        for (int i = 0; i < children.length; ++i)
            this.child = children[i];
    }

}
