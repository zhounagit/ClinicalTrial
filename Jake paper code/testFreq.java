/**
 * Created by Na on 1/11/16.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import java.util.HashMap;
import java.util.TreeMap;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.*;

public class testFreq {
    public static void main(String[] args)
    
    {
        String inputFile = "/Users/HIA/Desktop/paperdata/filter_by_city_facility_malaria.txt";
        SparkConf conf = new SparkConf().setAppName("FP-Growth Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile(inputFile).cache();
        JavaRDD<List<String>> transactions = data.map(
                                                      new Function<String, List<String>>() {
                                                          public List<String> call(String line) {
                                                              String[] parts = line.split(" ");
                                                              return Arrays.asList(parts);
                                                          }
                                                      }
                                                      );
        
        FPGrowth fpg = new FPGrowth()
        .setMinSupport(0.001)
        .setNumPartitions(10);
        
        FPGrowthModel<String> model = fpg.run(transactions);
        System.out.println("Just for a test: " + model.freqItemsets());
        
        Vector<String> zip_item = new Vector<String>();
        Vector<Integer> zip_item_freq = new Vector<Integer>();
        
        
        HashMap<String, Integer> map1 = new HashMap<String, Integer>();
        
        
        try {
            
            PrintWriter one_freq = new PrintWriter(new FileWriter("/Users/HIA/Desktop/paperdata/filter_by_city_facility_malaria_freq_sortedMap.csv", true), true);
            for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect())
            {
                
                String s= itemset.javaItems().toString();
                int f = (int) itemset.freq();
                
                if (itemset.javaItems().size() == 1) {
                    
                    map1.put(s,f);
                    zip_item.add(s);
                    zip_item_freq.add(f);
                    
                }
                
                
            }
            
            
            TreeMap<String, Integer> sortedMap1 = sortMapByValue(map1);
            
            for(Map.Entry<String,Integer> entry: sortedMap1.entrySet()){
                String key = entry.getKey();
                Integer value = entry.getValue();
                one_freq.write(key+ "\t"+value +"\n");
            }
            
            one_freq.close();
            
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static TreeMap<String,Integer> sortMapByValue(HashMap<String,Integer> map){
        Comparator<String> comparator = new ValComparator(map);
        TreeMap<String,Integer> result = new TreeMap<String, Integer>(comparator);
        result.putAll(map);
        return result;
        
    }
    
}



class ValComparator<K,V extends Comparable<V>> implements Comparator<K> {
    
    HashMap<K, V> map = new HashMap<K, V>();
    
    public ValComparator(HashMap<K, V> map) {
        this.map.putAll(map);
    }
    
    public int compare(K a, K b) {
        
        return map.get(b).compareTo(map.get(a));//in ascending order
        
        
    }
}