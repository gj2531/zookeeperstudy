package demo5.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    @Override
    public String toString() {
        return orderId+"\t"+price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    /**
     * 判断orderId是否一样 只有相同了 才有可比较性
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0){
            i = this.price.compareTo(o.price);
        }
        return -i;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readUTF();
        this.price=dataInput.readDouble();
    }
}
