

package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.WhereExpressionVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

    private String q;

    public Query(String q) {
        this.q = q;
    }

    public Relation execute() {
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(q);
        } catch (JSQLParserException e) {
            System.out.println("Unable to parse query");
            e.printStackTrace();
        }
        Select selectStatement = (Select) statement;
        PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();
        List<Expression> ifGroupList = sb.getGroupByColumnReferences();
        boolean ifAgg = false;
        if (ifGroupList != null) {
            ifAgg = true;
        }
        //sb.select
        TablesNamesFinder tbs = new TablesNamesFinder();
        List<String> tblist = tbs.getTableList(selectStatement);
        Catalog catalog = Database.getCatalog();

        List<Join> joinList = sb.getJoins();


        // variables;
        String name = null;
        int tableId = 0;
        ArrayList<Tuple> tupleList = null;
        TupleDesc tdc = null;
        Relation firstRel = null;

        String name2 = null;
        int tableId2 = 0;
        ArrayList<Tuple> tupleList2 = null;
        TupleDesc tdc2 = null;
        Relation secondRel = null;

        name = tblist.get(0);
        tableId = catalog.getTableId(name);
        tupleList = catalog.getDbFile(tableId).getAllTuples();
        tdc = catalog.getTupleDesc(tableId);
        firstRel = new Relation(tupleList, tdc);

        Relation res = null;


        if (joinList != null) {
            name2 = tblist.get(1);
            tableId2 = catalog.getTableId(name2);
            tupleList2 = catalog.getDbFile(tableId2).getAllTuples();
            tdc2 = catalog.getTupleDesc(tableId2);
            secondRel = new Relation(tupleList2, tdc2);
            for (Join joins : joinList) {
                String[] exp = joins.getOnExpression().toString().split("=");
                String firstCol = exp[0].split("\\.")[1].trim();
                System.out.println(firstCol);
                String secondCol = exp[1].split("\\.")[1].trim();
                System.out.println(secondCol);
                int fieldA = firstRel.getDesc().nameToId(firstCol);
                int fieldB = secondRel.getDesc().nameToId(secondCol);
                res = firstRel.join(secondRel, fieldA, fieldB);
            }
        }
        if (joinList == null) {
            //First table
            // select items
            List<SelectItem> selectList = sb.getSelectItems();
            ColumnVisitor clmn = new ColumnVisitor();
            boolean ifRealAgg = false;
            ArrayList<Integer> resCol = new ArrayList<Integer>();
            for (SelectItem sel : selectList) {
                if (sel instanceof AllColumns) {
                    return firstRel;
                }
                sel.accept(clmn);
                if (clmn.isAggregate()) {
                    AggregateOperator o = clmn.getOp();
                    String nameCol = clmn.getColumn();
                    resCol.add(tdc.nameToId(nameCol));
                    ifRealAgg = true;
                } else {
                    String clmnName = clmn.getColumn();
                    int idClm = tdc.nameToId(clmnName);
                    resCol.add(idClm);
                }
            }

            // now do where
            WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
            if (sb.getWhere() != null) {
                sb.getWhere().accept(whereVisitor);
                Relation rel = firstRel.select(tdc.nameToId(whereVisitor.getLeft()), whereVisitor.getOp(), whereVisitor.getRight());
                Relation preAgg = rel.project(resCol);
                if (clmn.isAggregate()) {
                    System.out.println("aggregating in where");
                    //ifagg reflects if group by
                    Relation agged = firstRel.project(resCol).aggregate(clmn.getOp(), ifAgg);
                    return agged;
                }
                return preAgg;

            } else {
                // no where clause
                if (ifRealAgg) {
                    Relation res2 = firstRel.project(resCol).aggregate(clmn.getOp(), ifAgg);
                    return res2;
                }
                Relation noAggRes = firstRel.project(resCol);
                return noAggRes;
            }
        } else {
            String nameCol = null;
            List<SelectItem> selectList = sb.getSelectItems();
            ColumnVisitor clmn = new ColumnVisitor();
            ArrayList<Integer> resCol = new ArrayList<Integer>();
            int pointer = 0;
            for (SelectItem sel : selectList) {
                if (sel instanceof AllColumns) {
                    return firstRel;
                }
                sel.accept(clmn);
                if (clmn.isAggregate()) {
                    AggregateOperator o = clmn.getOp();
                    nameCol = clmn.getColumn();
                    resCol.add(tdc.nameToId(nameCol));
                } else {
                    String clmnName = clmn.getColumn();
                    resCol.add(pointer);
                    pointer++;
                }
            }
            // now do where
            WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
            if (sb.getWhere() != null) {
                sb.getWhere().accept(whereVisitor);
                Relation rel = res.select(tdc.nameToId(whereVisitor.getLeft()), whereVisitor.getOp(), whereVisitor.getRight());
                Relation preAgg = rel.project(resCol);
                if (clmn.isAggregate()) {
                    System.out.println("aggregating in where");
                    Relation agged = rel.aggregate(clmn.getOp(), ifAgg);
                    return agged;
                }
                return preAgg;

            } else {
                if (ifAgg) {
                    resCol.add(res.getDesc().nameToId(nameCol));
                    Relation res2 = res.project(resCol).aggregate(clmn.getOp(), ifAgg);
                    return res2;
                }
                Relation noAggRes = res.project(resCol);
                return noAggRes;
            }
        }
    }
}
