package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int groupField;
    private int aggregateField;
    private Type groupFieldType;
    private Map<Object, Integer> map;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupField=gbfield;
        this.aggregateField=afield;
        this.groupFieldType=gbfieldtype;
        this.map=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup){
        // some code goes here
        Object key=null;
        Object field=null;
        if(this.groupField!=NO_GROUPING) {
            field = (tup.getField(groupField));
            if(field instanceof IntField)
                key=((IntField)field).getValue();
            else
                key=((StringField)field).getValue();
        }else {
            key="_NO_GROUPING";
            //field=Type.INT_TYPE;
        }
        //int value = ((IntField) (tup.getField(aggregateFiled))).getValue();
        if(map.containsKey(key)){
            map.put(key, map.get(key)+1);
        }else
            map.put(key, 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    class StringAggregateorIterator implements OpIterator{
        private boolean isOpen=false;
        private Iterator<Map.Entry<Object, Integer>> iterator;
        public StringAggregateorIterator(){
            iterator=map.entrySet().iterator();
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen=true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, IllegalStateException {
            if(this.isOpen==false)
                throw new IllegalStateException();
            Map.Entry<Object, Integer> entry = this.iterator.next();
            if(groupField!=NO_GROUPING) {
                Type[] temp = {groupFieldType, Type.INT_TYPE};
                TupleDesc td = new TupleDesc(temp);
                Tuple result = new Tuple(td);
                Field field1 = null;
                if (groupFieldType == Type.INT_TYPE)
                    field1 = new IntField((Integer) entry.getKey());
                else {
                    String key = (String) entry.getKey();
                    field1 = new StringField(key, key.length());
                }
                Field field2 = new IntField((Integer) entry.getValue());
                result.setField(0, field1);
                result.setField(1, field2);
                return result;
            }else{
                Type[] temp={Type.INT_TYPE};
                TupleDesc td=new TupleDesc(temp);
                Tuple result=new Tuple(td);
                Field field=new IntField(entry.getValue());
                result.setField(0, field);
                return result;
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if(isOpen==false)
                throw new IllegalStateException();
            this.iterator=map.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            Type[] temp={groupFieldType, Type.INT_TYPE};
            TupleDesc td=new TupleDesc(temp);
            return td;
        }

        @Override
        public void close() {
            this.isOpen=false;
        }
    }

    public OpIterator iterator() {
        // some code goes here
        return new StringAggregateorIterator();
    }

}
