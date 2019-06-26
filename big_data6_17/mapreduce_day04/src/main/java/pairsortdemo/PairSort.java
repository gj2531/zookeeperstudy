package pairsortdemo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairSort implements WritableComparable<PairSort> {
    private String first;
    private int second;

    @Override
    public String toString() {
        return first+" "+second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compareTo(PairSort o) {
        int i = this.first.compareTo(o.first);
        if (i != 0){
            return i;
        }else {
            int i1 = Integer.valueOf(this.second).compareTo(Integer.valueOf(o.getSecond()));
            return -i1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }
}
