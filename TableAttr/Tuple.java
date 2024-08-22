package TableAttr;

import java.util.Hashtable;
import java.util.Map;

public class Tuple {
    Hashtable<String, Object> tuple;
    public Tuple(Hashtable<String, Object> tuple) {
        this.tuple = tuple;
    }
    public Hashtable<String, Object> getTuple() {
        return this.tuple;
    }
    public boolean isSame(Tuple tuple) {
        return this.tuple.equals(tuple.tuple);
    }
    public Object getColValue(String colName) {
        return this.tuple.get(colName);
    }
    public boolean compare(String clusteringKey, Tuple otherTuple) {
        Object thisValue = this.tuple.get(clusteringKey);
        Object otherValue = otherTuple.tuple.get(clusteringKey);

        Comparable<Object> comparableThisValue = (Comparable<Object>) thisValue;
        return comparableThisValue.compareTo(otherValue) >= 0;
    }
    public Tuple joinTuples(Tuple otherTuple, String ignoredCol) {
        Hashtable<String, Object> tupleHash = new Hashtable(this.tuple);
        for(Map.Entry<String, Object> entry : otherTuple.getTuple().entrySet()) {
            String key = entry.getKey();
            if(key.equals(ignoredCol)) continue;
            int num = 1;
            while(tupleHash.containsKey(key)) {
                if(num != 1) {
                    key = key.substring(0, key.length() - 1);
                }
                key += num;
                num++;
            }
            tupleHash.put(key, entry.getValue());
        }
        Tuple newTuple = new Tuple(tupleHash);
        return newTuple;
    }
    public String toString() {
        String str = "";
        for (Map.Entry<String, Object> entry : this.tuple.entrySet()) {
            str += entry.getValue() + ",";
        }
        return str.substring(0, str.length() - 1);
    }
}
