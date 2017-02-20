package project1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	private int joinKey;// custmers id
	private int sourceIndex;// 1=Customers; 2=Transactions

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(int joinKey, int sourceIndex) {
		this.joinKey = joinKey;
		this.sourceIndex = sourceIndex;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(joinKey).append("\t").append(sourceIndex)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		joinKey = WritableUtils.readVInt(dataInput);
		sourceIndex = WritableUtils.readVInt(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, joinKey);
		WritableUtils.writeVInt(dataOutput, sourceIndex);
	}

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int returnValue = compare(joinKey, o.getjoinKey());
        if (returnValue != 0) {
            return returnValue;
        }
        return compare(sourceIndex, o.getsourceIndex());
    }

    @Override
	public int hashCode() {
		return joinKey;
	}

    public static int compare(int k1, int k2) {
        return (k1 < k2 ? -1 : (k1 == k2 ? 0 : 1));
    }

	public int getjoinKey() {
		return joinKey;
	}

	public void setjoinKey(int joinKey) {
		this.joinKey = joinKey;
	}

	public int getsourceIndex() {
		return sourceIndex;
	}

	public void setsourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
	}
}
