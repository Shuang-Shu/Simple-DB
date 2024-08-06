package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *              The predicate to filter tuples with
     * @param child
     *              The child operator
     */

    private Predicate predicate;
    private OpIterator opIterator;

    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.opIterator = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.opIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.opIterator.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * 可视为hasNext()和next()的聚合方法，同时添加了filter()的功能
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     *         返回下一个通过filter的tuple，意即若未通过，则继续迭代
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple temp;
        while (this.opIterator.hasNext()) {
            temp = this.opIterator.next();
            if (this.predicate.filter(temp))
                return temp;
            else
                continue;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
