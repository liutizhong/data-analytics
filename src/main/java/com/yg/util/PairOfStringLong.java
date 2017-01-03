package com.yg.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by liuti on 2017/1/3.
 */
public class PairOfStringLong implements WritableComparable<PairOfStringLong> {
    private String leftElement;
    private long rightElement;

    static {
        WritableComparator.define(PairOfStringLong.class, new PairOfStringLong.Comparator());
    }

    public PairOfStringLong() {
    }

    public PairOfStringLong(String left, long right) {
        this.set(left, right);
    }

    public void readFields(DataInput in) throws IOException {
        this.leftElement = in.readUTF();
        this.rightElement = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.leftElement);
        out.writeLong(this.rightElement);
    }

    public String getLeftElement() {
        return this.leftElement;
    }

    public long getRightElement() {
        return this.rightElement;
    }

    public String getKey() {
        return this.leftElement;
    }

    public long getValue() {
        return this.rightElement;
    }

    public void set(String left, long right) {
        this.leftElement = left;
        this.rightElement = right;
    }

    public boolean equals(Object obj) {
        PairOfStringLong pair = (PairOfStringLong)obj;
        return this.leftElement.equals(pair.getLeftElement()) && this.rightElement == pair.getRightElement();
    }

    public int compareTo(PairOfStringLong pair) {
        String pl = pair.getLeftElement();
        long pr = pair.getRightElement();
        return this.leftElement.equals(pl)?(this.rightElement == pr?0:(this.rightElement < pr?-1:1)):this.leftElement.compareTo(pl);
    }

    public int hashCode() {
        return this.leftElement.hashCode() + (int)(this.rightElement % 2147483647L);
    }

    public String toString() {
        return "(" + this.leftElement + ", " + this.rightElement + ")";
    }

    public PairOfStringLong clone() {
        return new PairOfStringLong(this.leftElement, this.rightElement);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(PairOfStringLong.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String thisLeftValue = WritableComparatorUtils.readUTF(b1, s1);
            String thatLeftValue = WritableComparatorUtils.readUTF(b2, s2);
            if(thisLeftValue.equals(thatLeftValue)) {
                int s1offset = readUnsignedShort(b1, s1);
                int s2offset = readUnsignedShort(b2, s2);
                long thisRightValue = readLong(b1, s1 + 2 + s1offset);
                long thatRightValue = readLong(b2, s2 + 2 + s2offset);
                return thisRightValue < thatRightValue?-1:(thisRightValue == thatRightValue?0:1);
            } else {
                return thisLeftValue.compareTo(thatLeftValue);
            }
        }
    }
}
