package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 * 谓词将tuples与一个指定的字段值进行比较。
 * Predicate对象用于filter运算符
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    // 一个名为Op的枚举类
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *          a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private int field;
    private Op op;
    private Field operand;

    /**
     * Constructor.
     * 
     * @param field
     *                field number of passed in tuples to compare against.
     *                传入进行比较的tuple的field标号
     * @param op
     *                operation to use for comparison
     *                用于比较的操作符
     * @param operand (操作数，运算对象)
     *                field value to compare passed in tuples to
     *                要将传入的图元与之比较的字段值
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        // some code goes here
        return this.field;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        // some code goes here
        return this.op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        // some code goes here
        return this.operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *          The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
        return (t.getField(this.field).compare(this.op, this.operand));
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        return "field=" + this.field + " op=" + this.op.toString() + " operand=" + this.operand.toString();
    }
}
