package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * 一个帮助类，便于组织每个域的信息
     * */
    public static class TDItem implements Serializable {
        /*
        * 一个内部类，用于描述某一个Tuple属性的属性（即域名和域类型）
        *
        * */
        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        public String getFieldName(){
            return this.fieldName;
        }
        public Type getFieldType(){
            return this.fieldType;
        }
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        一个返回Iterator<TDItem> 的函数，用以迭代整个TupleDesc
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.TdList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 有typeAr和fieldAr作为参数的构造函数
     *
     * 用typeAr(ray).length字段创建一个新的TupleDesc，其中有指定类型的字段，有相关的命名字段。
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields.
     *
     *            Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here_OK
        this.TdList=new ArrayList<>();
        for(int i=0;i<typeAr.length;++i)
            this.TdList.add(new TDItem(typeAr[i], fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 仅有typeAr的为参数的构造函数
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.TdList=new ArrayList<>();
        for (Type type : typeAr) this.TdList.add(new TDItem(type, null));
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TdList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.TdList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return this.TdList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        Iterator<TDItem> iterator=this.iterator();
        int i=0;
        while (iterator.hasNext()){
            if(Objects.equals(iterator.next().fieldName, name))
                return i;
            i++;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size=0;
        Iterator<TDItem> iter=this.iterator();
        while (iter.hasNext())
            size+=iter.next().fieldType.getLen();
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int size1= td1.numFields();
        int size2= td2.numFields();
        String [] tempName=new String[size1+size2];
        Type[] tempType=new Type[size1+size2];
        for(int i=0;i<size1;++i) {
            tempName[i] = td1.TdList.get(i).fieldName;
            tempType[i]= td1.TdList.get(i).fieldType;
        }
        for(int i=size1;i<size1+size2;++i){
            tempName[i] = td2.TdList.get(i-size1).fieldName;
            tempType[i]= td2.TdList.get(i-size1).fieldType;
        }
        return new TupleDesc(tempType, tempName);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(o==this)
            //若o为这个对象本身的的引用
            return true;
        if(!(o instanceof TupleDesc))
            return false;
        TupleDesc temp=(TupleDesc)o;
        if(this.numFields()!=temp.numFields())
            return false;

        Iterator<TDItem> iterator1=this.iterator();
        Iterator<TDItem> iterator2=temp.iterator();

        while (iterator1.hasNext()){
            TDItem temp1=iterator1.next();
            TDItem temp2=iterator2.next();
            if(temp1.fieldType!=temp2.fieldType|| !Objects.equals(temp1.fieldName, temp2.fieldName))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.TdList.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder result= new StringBuilder();
        for(int i=0;i<this.TdList.size()-1;++i)
            result.append(this.TdList.get(i).toString()).append(",");
        result.append(this.TdList.get(this.TdList.size() - 1).toString());
        return result.toString();
    }
    //Tuple类域，一个TdItem的array
    private final ArrayList<TDItem> TdList;
}
