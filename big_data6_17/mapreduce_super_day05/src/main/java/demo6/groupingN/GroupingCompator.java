package demo6.groupingN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingCompator extends WritableComparator {

    /**
     * 覆写无参构造 指定我们的框架反射出来的两个比较的是哪个java类
     */
    public GroupingCompator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        //如果比较后 orderId相同 那么相同的数据就会发送到同一个集合中去
        return first.getOrderId().compareTo(second.getOrderId());
        //return super.compare(a, b);
    }
}
