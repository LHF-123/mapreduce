package com.atguigu.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author LHF
 * @date 2020/12/20 22:49
 */
public class OrderComparator extends WritableComparator {

    //Sort类通过继承WritableComparator来比较两个MyKey对象
    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    /**
     * @Description 将相同orderId的放到一组KV值里处理，不然就会每一个OrderBean对象就是一个key
     *              bean里的compara是排序，这个是分组
     * @param
     * @throws
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}
