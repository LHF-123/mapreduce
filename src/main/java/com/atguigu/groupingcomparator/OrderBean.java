package com.atguigu.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/20 22:39
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String productId;
    private double price;

    @Override
    public String toString() {
        return orderId + "\t" + productId + "\t" + price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(@NotNull OrderBean o) {
        int compare = this.orderId.compareTo(o.orderId);

        if (compare == 0){
            return Double.compare(o.price, this.price);
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(productId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.price = in.readDouble();
    }
}
