package hw2;

import java.util.ArrayList;

import hw1.Field;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.*;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 *
 * @author Doug Shook
 */
public class Relation {

    private ArrayList<Tuple> tuples;
    private TupleDesc td;

    public Relation(ArrayList<Tuple> l, TupleDesc td) {
        this.tuples = l;
        this.td = td;
    }

    /**
     * This method performs a select operation on a relation
     *
     * @param field   number (refer to TupleDesc) of the field to be compared, left side of comparison
     * @param eq      the comparison operator
     * @param operand a constant to be compared against the given column
     * @return
     */
    public Relation select(int field, RelationalOperator eq, Field operand) {
        ArrayList<Tuple> tll = new ArrayList<Tuple>();
        System.out.println("aaaa" + this.getTuples().size());
        for (Tuple tpl : this.getTuples()) {
            //System.out.println("getfield is "+tpl.getField(field));
            if (tpl.getField(field).compare(eq, operand)) {
                tll.add(tpl);
                System.out.println("in get tuples in select");
            }
        }
        return new Relation(tll, this.getDesc());
    }

    /**
     * This method performs a rename operation on a relation
     *
     * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
     * @param names  a list of new names. The order of these names is the same as the order of field numbers in the field list
     * @return
     */
    public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
        TupleDesc newTd = td;
        String[] oldField = td.getFieldArr();

        for (int i = 0; i < fields.size(); i++) {
            oldField[fields.get(i)] = names.get(i);
        }
        newTd.setFields(oldField);

        return new Relation(this.getTuples(), newTd);
    }

    /**
     * This method performs a project operation on a relation
     *
     * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
     * @return
     */
    public Relation project(ArrayList<Integer> fields) {
        ArrayList<Tuple> tples = new ArrayList<Tuple>();
        int resSz = fields.size();
        String[] fieldName = new String[resSz];
        Type[] typeArr = new Type[resSz];

        for (int a = 0; a < fields.size(); a++) {
            Integer target = fields.get(a);
            fieldName[a] = this.td.getFieldName(target);
            typeArr[a] = this.td.getType(target);
        }
        TupleDesc resDesc = new TupleDesc(typeArr, fieldName);
        //Relation res = new Relation(tples,)
        for (Tuple tp : this.getTuples()) {
            Tuple tar = new Tuple(resDesc);
            for (int a = 0; a < fields.size(); a++) {
                tar.setField(a, tp.getField(fields.get(a)));
            }
            tples.add(tar);
        }

        return new Relation(tples, resDesc);

    }

    /**
     * This method performs a join between this relation and a second relation.
     * The resulting relation will contain all of the columns from both of the given relations,
     * joined using the equality operator (=)
     *
     * @param other  the relation to be joined
     * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
     * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
     * @return
     */
    public Relation join(Relation other, int field1, int field2) {
        ArrayList<Tuple> tples = new ArrayList<Tuple>();
        int thisSz = this.getDesc().numFields();
        int otherSz = other.getDesc().numFields();
        String[] fieldName = new String[thisSz + otherSz];
        Type[] typeArr = new Type[thisSz + otherSz];
        // first we assign the things in this
        for (int k = 0; k < thisSz; k++) {
            fieldName[k] = this.getDesc().getFieldName(k);
            typeArr[k] = this.getDesc().getType(k);
        }
        // then we assign things in other relation
        for (int n = 0; n < otherSz; n++) {
            int actual = n + thisSz;
            System.out.println("this sz is " + actual);
            fieldName[actual] = other.getDesc().getFieldName(n);
            typeArr[actual] = other.getDesc().getType(n);
        }

        TupleDesc newDesc = new TupleDesc(typeArr, fieldName);

        for (int m = 0; m < this.getTuples().size(); m++) {
            for (int j = 0; j < other.getTuples().size(); j++) {
                Field fdThis = this.getTuples().get(m).getField(field1);
                Field fdOther = other.getTuples().get(j).getField(field2);
                if (fdThis.compare(RelationalOperator.EQ, fdOther)) {

                    Tuple newTuple = new Tuple(newDesc);
                    for (int k = 0; k < thisSz; k++) {
                        newTuple.setField(k, this.getTuples().get(m).getField(k));
                    }
                    // then we assign things in other relation
                    for (int n = 0; n < otherSz; n++) {
                        int actual = n + thisSz;
                        newTuple.setField(actual, other.getTuples().get(j).getField(n));
                    }
                    tples.add(newTuple);
                }

            }
        }
        return new Relation(tples, newDesc);
    }

    /**
     * Performs an aggregation operation on a relation. See the lab write up for details.
     *
     * @param op      the aggregation operation to be performed
     * @param groupBy whether or not a grouping should be performed
     * @return
     */
    public Relation aggregate(AggregateOperator op, boolean groupBy) {
        Aggregator ag = new Aggregator(op, groupBy, this.td);
        for (Tuple tp : tuples) {
            ag.merge(tp);
        }
        ArrayList<Tuple> res = ag.getResults();
        Relation returnRes = new Relation(res, this.td);
        return returnRes;
    }

    public TupleDesc getDesc() {
        return this.td;
    }

    public ArrayList<Tuple> getTuples() {
        return this.tuples;
    }

    /**
     * Returns a string representation of this relation. The string representation should
     * first contain the TupleDesc, followed by each of the tuples in this relation
     */
    public String toString() {
        String str = "";
        for (int i = 0; i < tuples.size(); i++) {
            str += tuples.get(i).toString() + "\n";
        }
        return str;
    }
}
