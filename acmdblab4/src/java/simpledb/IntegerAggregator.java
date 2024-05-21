package simpledb;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbIndex;
    int agIndex;
    TupleDesc originalTd;
    TupleDesc td;
    Type gbFieldType;
    Op aggreOp;
    HashMap<Field, Integer> gval2agval;
    HashMap<Field, Integer[]> gval2count_sum;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbindex, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbIndex = gbIndex;
        this.gbFieldType = gbFieldType;
        this.agIndex = agIndex;
        this.aggreOp = aggreOp;
        this.td=td;
        gval2agval = new HashMap<>();
        gval2count_sum = new HashMap<>();
        
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field aggreField;
        Field gbField = null;
        Integer newVal;
        aggreField = tup.getField(agIndex);
        int toAggregate;
        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("该tuple的指定列不是Type.INT_TYPE类型");
        }
        toAggregate = ((IntField) aggreField).getValue();
        //初始化originalTd，并确保每一次聚合的tuple的td与其相同
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("待聚合tuple的tupleDesc不一致");
        }
        if (gbIndex != Aggregator.NO_GROUPING) {
            //如果gbIdex为NO_GROUPING，那么不用给gbField赋值，即为初始值null即可
            gbField = tup.getField(gbIndex);
        }
        //开始进行聚合操作
        //平均值的操作需要维护gval2count_sum，所以单独处理
        if (aggreOp == Op.AVG) {
            if (gval2count_sum.containsKey(gbField)) {//如果这个map已经处理过这个分组
                Integer[] oldCountAndSum = gval2count_sum.get(gbField);//之前处理该分组的总次数以及所有操作数的和
                int oldCount = oldCountAndSum[0];
                int oldSum = oldCountAndSum[1];
                //更新该分组对应的记录，将次数加1,并将总和加上待聚合的值
                gval2count_sum.put(gbField, new Integer[]{oldCount + 1, oldSum + toAggregate});
            } else {//否则为第一次处理该分组的tuple
                gval2count_sum.put(gbField, new Integer[]{1, toAggregate});
            }
            //直接由gval2count_sum这个map记录的信息得到该分组对应的聚合值并保存在gval2agval中
            Integer[] c2s=gval2count_sum.get(gbField);
            int currentCount = c2s[0];
            int currentSum = c2s[1];
            gval2agval.put(gbField, currentSum / currentCount);
            //在这里结束，此方法剩下的代码是对应除了求平均值其他的操作的
            return;
        }

        //除了求平均值的其他聚合操作
        if (gval2agval.containsKey(gbField)) {
            Integer oldVal = gval2agval.get(gbField);
            newVal = calcuNewValue(oldVal, toAggregate, aggreOp);
        } else if (aggreOp == Op.COUNT) {//如果是对应分组的第一个参加聚合操作的tuple，那么除了count操作，其他操作结果都是待聚合值
            newVal = 1;
        } else {
            newVal = toAggregate;
        }
        gval2agval.put(gbField, newVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : gval2agval.entrySet()) {
            Tuple t = new Tuple(td);//该tuple不必setRecordId，因为RecordId对进行操作后的tuple没有意义
            //分别处理不分组与有分组的情形
            if (gbIndex == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(g2a.getValue()));
            } else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
