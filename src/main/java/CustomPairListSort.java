import javafx.util.Pair;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomPairListSort {

    public static List<Pair<Integer, Text>> sortedListCreation(List<Pair<Integer, Text>> toBeSorted){
        List<Pair<Integer, Text>> customPairList = new ArrayList<>();
        List<Integer> sortedKeys = getAllKeysSorted(toBeSorted);
        for(Integer key : sortedKeys){
            for(Pair<Integer, Text> p : toBeSorted){
                if(toBeSorted.size() == customPairList.size()){
                    return customPairList;
                }

                if(!customPairList.contains(p) && p.getKey().equals(key)){
                        customPairList.add(p);
                }
            }
        }

        return customPairList;
    }

    public static List<Integer> getAllKeysSorted(List<Pair<Integer, Text>> getKeyList){
        List<Integer> keyList  = new ArrayList<>();
        for(Pair<Integer, Text> key : getKeyList){
            keyList.add(key.getKey());
        }
        Collections.sort(keyList);
        Collections.reverse(keyList);
        return keyList;
    }

    public static List<Text> getAllValues(List<Pair<Integer, Text>> getValueList){
        List<Text> valueList = new ArrayList<>();
        for(Pair<Integer, Text> value : getValueList){
            valueList.add(value.getValue());
        }
        return valueList;
    }
}

