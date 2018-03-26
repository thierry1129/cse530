package hw1;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 *
 * @author Sam Madden modified by Doug Shook
 */
public class Tuple {
    private TupleDesc t;
    private int pid;
    private int tid;
    private Field[] tfield;


    /**
     * Creates a new tuple with the given description
     *
     * @param t the schema for this tuple
     */
    public Tuple(TupleDesc t) {
        this.t = t;
        this.tfield = new Field[t.numFields()];
        this.pid = 0;
        this.tid = 0;
    }

    public TupleDesc getDesc() {
        return t;
    }

    /**
     * retrieves the page id where this tuple is stored
     *
     * @return the page id of this tuple
     */
    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * retrieves the tuple (slot) id of this tuple
     *
     * @return the slot where this tuple is stored
     */
    public int getId() {
        return tid;
    }

    public void setId(int id) {
        tid = id;
    }

    public void setDesc(TupleDesc td) {
        this.t = td;
    }

    /**
     * Stores the given data at the i-th field
     *
     * @param i the field number to store the data
     * @param v the data
     */
    public void setField(int i, Field v) {
        tfield[i] = v;
    }

    public Field[] getFieldArr() {
        return this.tfield;
    }

    public Field getField(int i) {
        return tfield[i];
    }

    /**
     * Creates a string representation of this tuple that displays its contents.
     * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
     * the String columns to readable text).
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("");
        for (Field a : tfield) {
            if (a.getType().equals(Type.STRING)) {
                sb.append(a.toString());
//				String s = new String(a.toByteArray());
//				sb.append(s);			
            } else {
                // handle the printing of int type, could be wrong .
                sb.append(a.toString());
                sb.append(" ");
            }


        }

        //your code here
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        final Tuple other = (Tuple) obj;
        if (other.getDesc() != this.getDesc()) {
            return false;
        }

        if (!Arrays.equals(this.getFieldArr(), other.getFieldArr())) {
            return false;
        }
        return true;


    }
}
