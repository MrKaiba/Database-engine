import java.util.Hashtable;

public class Tuple {
    Hashtable<String, Object> tuple;
    public Tuple(Hashtable<String, Object> tuple) {
        this.tuple = tuple;
    }
    public boolean isSame(Tuple tuple) {
        return this.tuple.equals(tuple.tuple);
    }
    public boolean compare(String clusteringKey, Tuple otherTuple) {
        Object thisValue = this.tuple.get(clusteringKey);
        Object otherValue = otherTuple.tuple.get(clusteringKey);

        Comparable<Object> comparableThisValue = (Comparable<Object>) thisValue;
        return comparableThisValue.compareTo(otherValue) >= 0;
    }

}
