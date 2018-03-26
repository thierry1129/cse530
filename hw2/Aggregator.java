package hw2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hw1.Field;
import hw1.IntField;
import hw1.RelationalOperator;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 *
 * @author Doug Shook
 */
public class Aggregator {
    private AggregateOperator ag;
    private boolean gb;
    private TupleDesc tdc;
    private ArrayList<Tuple> tupleList;
    private ArrayList<Tuple> compareList;

    public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
        this.ag = o;
        this.gb = groupBy;
        this.tdc = td;
        this.tupleList = new ArrayList<Tuple>();
        this.compareList = new ArrayList<Tuple>();
    }

    /**
     * Merges the given tuple into the current aggregation
     *
     * @param t the tuple to be aggregated
     */
    public void merge(Tuple t) {
        // first coloumn, always going to be there
        Type first = t.getDesc().getType(0);
        Field a = t.getField(0);
        Field second = null;
        tupleList.add(t);
        if (gb) {
            // deal with aggregate and group by
            if (first.equals(Type.STRING)) {
                // now the second field is a string field
                second = new StringField(t.getField(0).toByteArray());
                Tuple newTuple = new Tuple(this.tdc);
                newTuple.setField(0, second);
                compareList.add(newTuple);

            } else {
                second = new IntField(t.getField(0).toByteArray());
                Tuple newTuple = new Tuple(this.tdc);
                newTuple.setField(0, second);
                if (!compareList.contains(newTuple)) {
                    compareList.add(newTuple);
                }
            }
        }
    }

    /**
     * Returns the result of the aggregation
     *
     * @return a list containing the tuples after aggregation
     */
    public ArrayList<Tuple> getResults() {
        ArrayList<Tuple> result = new ArrayList<Tuple>();

        switch (this.ag) {
            case COUNT:
                if (!gb) {
                    int res = 0;
                    for (Tuple a : tupleList) {
                        res++;
                    }
                    Tuple newTup = new Tuple(this.tdc);
                    newTup.setField(0, new IntField(res));
                    result.add(newTup);
                } else {
                    for (Tuple ca : compareList) {
                        int res = 0;
                        for (Tuple tl : tupleList) {
                            if (tl.equals(ca))
                                res++;
                        }
                        Tuple newTup = new Tuple(this.tdc);
                        newTup.setField(0, ca.getField(0));
                        newTup.setField(1, new IntField(res));
                        result.add(newTup);
                    }
                }
                break;

            case MAX:
                if (!gb) {
                    int max = Integer.MIN_VALUE;
                    boolean whetherInt = false;
                    String firstVal = "";

                    if (tupleList.get(0).getField(0).getType().equals(Type.INT)) {
                        whetherInt = true;
                    }
                    if (!whetherInt) {
                        StringField first = (StringField) tupleList.get(0).getField(0);
                        firstVal = first.toString();
                    }

                    for (Tuple tp : tupleList) {
                        if (tp.getDesc().getType(0).equals(Type.STRING)) {
                            StringField val = (StringField) tp.getField(0);
                            String actualv = val.toString();

                            if (actualv.compareTo(firstVal) == 1) {    // if the new string is supposed to be lexicographically greater than the cu
                                // max,  set the cur max to be the new
                                firstVal = actualv;
                            }


                        } else {
                            IntField intFl = new IntField(tp.getField(0).toByteArray());
                            int actualVal = intFl.getValue();
                            if (actualVal > max) {
                                max = actualVal;
                            }
                        }
                    }
                    if (whetherInt) {
                        Tuple maxTup = new Tuple(this.tdc);
                        maxTup.setField(0, new IntField(max));
                        result.add(maxTup);
                    } else {
                        Tuple maxTup = new Tuple(this.tdc);
                        maxTup.setField(0, new StringField(firstVal));
                        result.add(maxTup);
                    }

                } else {
                    int max = Integer.MIN_VALUE;
                    boolean whetherInt = false;

                    String firstVal = "";

                    if (compareList.get(0).getField(0).getType().equals(Type.INT)) {
                        whetherInt = true;
                    }
                    if (!whetherInt) {
                        StringField first = (StringField) tupleList.get(0).getField(1);
                        firstVal = first.toString();
                    }

                    for (Tuple a : compareList) {
                        Type aTp = a.getDesc().getType(0);
                     IntField intVal = (IntField) a.getField(0);
                        int actualVal = intVal.getValue();

                        for (Tuple b : tupleList) {
                            if (b.getDesc().getType(0) == Type.INT) {
                                IntField intVal1 = (IntField) b.getField(0);
                                int actualVal1 = intVal1.getValue();
                                if (actualVal1 == actualVal) {
                                    IntField compareVal = (IntField) b.getField(1);
                                    int valueb = compareVal.getValue();
                                    if (valueb > max) {
                                        max = valueb;
                                    }
                                }

                            } else {
                               StringField val = (StringField) b.getField(1);
                                String actualv = val.toString();

                                if (actualv.compareTo(firstVal) == 1) {    // if the new string is supposed to be lexicographically greater than the cu
                                    // max,  set the cur max to be the new
                                    firstVal = actualv;
                                }
                            }
                        }
                        if (whetherInt) {
                            Tuple maxTup = new Tuple(this.tdc);
                            maxTup.setField(0, a.getField(0));
                            maxTup.setField(1, new IntField(max));
                            result.add(maxTup);
                        } else {
                            Tuple maxTup = new Tuple(this.tdc);
                            maxTup.setField(0, a.getField(0));
                            maxTup.setField(1,new StringField(firstVal));
                            result.add(maxTup);
                        }
                    }
                }
                break;
            case MIN:
                if (!gb) {
                    int min = Integer.MAX_VALUE;
                    boolean whetherInt = false;
                    String firstVal = "";
                    if (tupleList.get(0).getField(0).getType().equals(Type.INT)) {
                        whetherInt = true;
                    }
                    if (!whetherInt) {
                        StringField first = (StringField) tupleList.get(0).getField(0);
                        firstVal = first.toString();

                    }
                    for (Tuple tp : tupleList) {
                        if (tp.getDesc().getType(0).equals(Type.STRING)) {
                            StringField val = (StringField) tp.getField(0);
                            String actualv = val.toString();

                            if (actualv.compareTo(firstVal) == -1) {    // if the new string is supposed to be lexicographically SMALLER than the cu
                                // max,  set the cur mIN to be the new
                                firstVal = actualv;
                            }

                        } else {
                            IntField intFl = new IntField(tp.getField(0).toByteArray());
                            int actualVal = intFl.getValue();
                            if (actualVal < min) {
                                min = actualVal;
                            }
                        }
                    }
                    if (whetherInt) {
                        Tuple maxTup = new Tuple(this.tdc);
                        maxTup.setField(0, new IntField(min));
                        result.add(maxTup);
                    } else {
                        Tuple maxTup = new Tuple(this.tdc);
                        maxTup.setField(0, new StringField(firstVal));
                        result.add(maxTup);
                    }
                } else {
                    int min = Integer.MAX_VALUE;
                    boolean whetherInt = false;

                    String firstVal = "";

                    if (compareList.get(0).getField(0).getType().equals(Type.INT)) {
                        whetherInt = true;
                    }
                    if (!whetherInt) {
                        StringField first = (StringField) tupleList.get(0).getField(1);
                        firstVal = first.toString();

                    }
                    for (Tuple a : compareList) {
                        Type aTp = a.getDesc().getType(0);


                            IntField intVal = (IntField) a.getField(0);
                            int actualVal = intVal.getValue();

                            for (Tuple b : tupleList) {
                                if (b.getDesc().getType(0) == Type.INT) {
                                    IntField intVal1 = (IntField) b.getField(0);
                                    int actualVal1 = intVal1.getValue();
                                    if (actualVal1 == actualVal) {
                                        IntField compareVal = (IntField) b.getField(1);
                                        int valueb = compareVal.getValue();
                                        if (valueb > min) {
                                            min = valueb;
                                        }
                                    }
                                } else {
                                    StringField val = (StringField) b.getField(1);
                                    String actualv = val.toString();

                                    if (actualv.compareTo(firstVal) == 1) {    // if the new string is supposed to be lexicographically greater than the cu
                                        // max,  set the cur max to be the new
                                        firstVal = actualv;
                                    }

                                }
                        }
                        if (whetherInt) {
                            Tuple maxTup = new Tuple(this.tdc);
                            maxTup.setField(0, a.getField(0));
                            maxTup.setField(1, new IntField(min));
                            result.add(maxTup);
                        } else {
                            Tuple maxTup = new Tuple(this.tdc);
                            maxTup.setField(0, a.getField(0));
                            maxTup.setField(1,new StringField(firstVal));
                            result.add(maxTup);
                        }
                    }
                }
                break;
            case AVG:
                int sum = 0;

                if (!gb) {
                    for (Tuple tp : tupleList) {
                        if (tp.getDesc().getType(0).equals(Type.STRING)) {
                            System.out.println("cannot call avg on string");
                        } else {
                            int val = ((IntField) tp.getField(0)).getValue();
                            sum += val;

                        }


                    }
                    int avgValue = sum / tupleList.size();
                    // not sure what the new tuple's pid and id should be
                    Tuple avgTup = new Tuple(this.tdc);
                    avgTup.setField(0, new IntField(avgValue));

                } else {
                    // now it is group by avg
                    for (Tuple a : compareList) {
                        Type aTp = a.getDesc().getType(0);
                        int sumt = 0;
                        int ct = 0;
                        if (aTp.equals(Type.STRING)) {
                            System.out.println("cannot call avg on string");

                        } else {
                            IntField intVal = (IntField) a.getField(0);
                            int actualVal = intVal.getValue();

                            for (Tuple b : tupleList) {
                                if (b.getDesc().getType(0) == Type.INT) {
                                    IntField intVal1 = (IntField) b.getField(0);
                                    int actualVal1 = intVal1.getValue();
                                    if (actualVal1 == actualVal) {
                                        IntField compareVal = (IntField) b.getField(1);
                                        int valueb = compareVal.getValue();
                                        sum += valueb;
                                        ct++;
                                    }
                                } else {
                                    System.out.println("cannot call group by avg on string ");
                                }
                            }
                        }
                        int trueAvg = sum / ct;
                        Tuple avgTup = new Tuple(this.tdc);
                        avgTup.setField(0, a.getField(0));
                        avgTup.setField(1, new IntField(trueAvg));
                        result.add(avgTup);
                    }
                }

                break;
            case SUM:
                int sumA = 0;
                if (!gb) {
                    for (Tuple tp : tupleList) {
                        if (tp.getDesc().getType(0).equals(Type.STRING)) {
                            System.out.println("cannot call sUM on string");
                        } else {
                            int val = ((IntField) tp.getField(0)).getValue();
                            sumA += val;
                        }
                    }

                    // not sure what the new tuple's pid and id should be
                    Tuple avgTup = new Tuple(this.tdc);
                    avgTup.setField(0, new IntField(sumA));
                    result.add(avgTup);

                } else {
                    // now it is group by sum
                    int sumt = 0;
                    for (Tuple a : compareList) {
                        sumt = 0;
                        Type aTp = a.getDesc().getType(0);

                        if (aTp.equals(Type.STRING)) {
                            System.out.println("cannot call sum on string");

                        } else {
                            IntField intVal = (IntField) a.getField(0);
                            int actualVal = intVal.getValue();

                            for (Tuple b : tupleList) {
                                if (b.getDesc().getType(0) == Type.INT) {
                                    IntField intVal1 = (IntField) b.getField(0);
                                    int actualVal1 = intVal1.getValue();
                                    if (actualVal1 == actualVal) {
                                        IntField compareVal = (IntField) b.getField(1);
                                        int valueb = compareVal.getValue();
                                        sumt += valueb;
                                    }

                                } else {
                                    System.out.println("cannot call SUM on string ");
                                }

                            }
                        }
                        Tuple avgTup = new Tuple(this.tdc);
                        avgTup.setField(0, a.getField(0));
                        avgTup.setField(1, new IntField(sumt));
                        if (!result.contains(avgTup)) {
                            result.add(avgTup);
                        }
                    }

                }
                break;
        }// this is the closing for switch
        return result;
    }
}
