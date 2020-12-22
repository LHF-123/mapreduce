package com.atguigu.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author LHF
 * @date 2020/12/20 22:49
 */
public class OrderComparator extends WritableComparator {

    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}
