package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId t;
    private OpIterator child;
    private boolean isOpen;
    private boolean isInit;

    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t=t;
        this.child=child;
        this.isOpen=false;
        this.isInit=true;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] types={Type.INT_TYPE};
        return new TupleDesc(types);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.isOpen=true;
        this.child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.isOpen=false;
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.isInit=true;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count=0;
        while (this.child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(this.t, this.child.next());
                count++;
            }
            catch (Exception e){
            }
        }
        if(!this.isInit)
            return null;
        Tuple tuple=new Tuple(this.getTupleDesc());
        Field field=new IntField(count);
        tuple.setField(0, field);
        this.isInit=false;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opIterators={this.child};
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child=children[0];
    }

}
