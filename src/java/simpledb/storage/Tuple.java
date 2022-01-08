package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 * Tuple维护着关于一个元组内容的信息。元组有一个由TupleDesc对象指定的模式，并包含有每个字段数据的Field对象。
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *用指定的模式（类型）创建一个新元组。
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     *            这个元组的模式。它必须是一个有效的TupleDesc实例，至少有一个字段。
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc=td;
        this.fields=new ArrayList<>();
        for(int i=0;i<td.numFields();++i)
            this.fields.add(null);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     *
     *
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 暂时做不了
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i>=0&&i<this.fields.size())
            this.fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if(i>=0&&i<this.fields.size())
            return this.fields.get(i);
        else
            return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder result= new StringBuilder();
        Iterator<Field> iterator=this.fields();
        while (iterator.hasNext())
            result.append(iterator.next().toString()).append("\t");
        return result.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return this.fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tupleDesc=td;
        ArrayList<Field> newFields=new ArrayList<>(td.numFields());
        for(int i=0;i<this.fields.size();++i)
            newFields.set(i, this.fields.get(i));
        this.fields=newFields;
    }

    private TupleDesc tupleDesc=null;
    private ArrayList<Field> fields;
    private RecordId recordId;
}