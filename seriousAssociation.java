

/**
 * Created by Na on 1/11/16.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import java.util.*;

public class SeriousAssociation {
    public static void main(String[] args)
    
    {
        String inputFile="/Users/HIA/Na/serious/serious_subtitle.txt";
        SparkConf conf=new SparkConf().setAppName("FP-Growth Example").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
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
        
        Map<String,Integer> freq1 = new HashMap<String, Integer>();
        Map<String,Integer> freq2 = new HashMap<String, Integer>();
        Map<String,Integer> freq3 = new HashMap<String, Integer>();
        Map<String,Integer> freq4 = new HashMap<String, Integer>();
        
        try{
            
            PrintWriter out_con = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0413serious_con.csv", true), true);
            PrintWriter out_con1 = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0413serious_con1.csv", true), true);
            PrintWriter out_con2 = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0413serious_con2.csv", true), true);
            PrintWriter out_con3 = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0413serious_con3.csv", true), true);
            PrintWriter out_freq = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0413serious_freq.csv", true), true);
            
            out_con.append("degree");
            out_con.append(",");
            out_con.append("source");
            out_con.append(",");
            out_con.append("frequency");
            out_con.append(",");
            out_con.append("target");
            out_con.append(",");
            out_con.append("type");
            out_con.append("\n");
            
            out_con1.append("degree");
            out_con1.append(",");
            out_con1.append("source");
            out_con1.append(",");
            out_con1.append("frequency");
            out_con1.append(",");
            out_con1.append("target");
            out_con1.append(",");
            out_con1.append("type");
            out_con1.append("\n");
            
            out_con2.append("degree");
            out_con2.append(",");
            out_con2.append("source");
            out_con2.append(",");
            out_con2.append("frequency");
            out_con2.append(",");
            out_con2.append("target");
            out_con2.append(",");
            out_con2.append("type");
            out_con2.append("\n");
            
            out_con3.append("degree");
            out_con3.append(",");
            out_con3.append("source");
            out_con3.append(",");
            out_con3.append("frequency");
            out_con3.append(",");
            out_con3.append("target");
            out_con3.append(",");
            out_con3.append("type");
            out_con3.append("\n");
            
            
            out_freq.append("event");
            out_freq.append(",");
            out_freq.append("freq");
            out_freq.append(",");
            out_freq.append("type");
            out_freq.append("\n");
            
            
            for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
                
                out_freq.append(itemset.javaItems().toString().replace(",", " "));
                out_freq.append(",");
                out_freq.append(Double.toString(itemset.freq()));
                out_freq.append(",");
                out_freq.append("undirected");
                out_freq.append("\n");
                
                if(itemset.javaItems().size()==1){
                    freq1.put(itemset.javaItems().toString(), (int) itemset.freq());
                    
                }else if(itemset.javaItems().size()==2){
                    freq2.put(itemset.javaItems().toString(), (int) itemset.freq());
                    
                }else if(itemset.javaItems().size()==3){
                    freq3.put(itemset.javaItems().toString(), (int) itemset.freq());
                    
                }else if(itemset.javaItems().size()==4){
                    freq4.put(itemset.javaItems().toString(), (int) itemset.freq());
                    
                }
                
            }
            
            
            double minConfidence = 0.2;
            
            
            for (AssociationRules.Rule<String> rule
                 : model.generateAssociationRules(minConfidence).toJavaRDD().collect())
            {
                
                if (rule.javaConsequent().contains("Death"))
                {
                    
                    if(rule.javaAntecedent().size() == 1 )
                    {
                        
                        Iterator<Map.Entry<String,Integer>> iterator =freq1.entrySet().iterator();
                        
                        while (iterator.hasNext()) {
                            
                            Map.Entry<String, Integer> entry = iterator.next();
                            String key = entry.getKey().toString().replace("[","").replace("]","");
                            Integer value = entry.getValue();
                            
                            boolean F = rule.javaAntecedent().toString().replace("[","").replace("]","").equals(key);
                            
                            
                            if (F==true) {
                                
                                out_con1.append(Double.toString(rule.confidence()));
                                out_con1.append(",");
                                out_con1.append(rule.javaAntecedent().toString().replace(",", " "));
                                out_con1.append(",");
                                out_con1.append(value.toString());
                                out_con1.append(",");
                                out_con1.append(rule.javaConsequent().toString().replace(",", " "));
                                out_con1.append(",");
                                out_con1.append("undirected");
                                out_con1.append("\n");
                                
                            }
                            
                            
                        }
                        
                        
                        
                        
                    }else if (rule.javaAntecedent().size() == 2) {
                        
                        Iterator<Map.Entry<String,Integer>> iterator =freq2.entrySet().iterator();
                        
                        while (iterator.hasNext()) {
                            
                            Map.Entry<String, Integer> entry = iterator.next();
                            String key = entry.getKey().toString().replace("[","").replace("]","");
                            Integer value = entry.getValue();
                            System.out.println("current key 2: " + key + "\n");
                            boolean F = rule.javaAntecedent().toString().replace("[","").replace("]","").equals(key);
                            System.out.println("F is 2: "+F);
                            
                            if (F==true) {
                                
                                out_con2.append(Double.toString(rule.confidence()));
                                out_con2.append(",");
                                out_con2.append(rule.javaAntecedent().toString().replace(",", " "));
                                out_con2.append(",");
                                out_con2.append(value.toString());
                                out_con2.append(",");
                                out_con2.append(rule.javaConsequent().toString().replace(",", " "));
                                out_con2.append(",");
                                out_con2.append("undirected");
                                out_con2.append("\n");
                                
                                
                            }
                            
                        }
                        
                        
                        
                        
                    } else if (rule.javaAntecedent().size() == 3)
                    {
                        
                        Iterator<Map.Entry<String,Integer>> iterator =freq3.entrySet().iterator();
                        
                        while (iterator.hasNext()) {
                            
                            Map.Entry<String, Integer> entry = iterator.next();
                            String key = entry.getKey().toString().replace("[","").replace("]","");
                            Integer value = entry.getValue();
                            System.out.println("current key 3: " + key + "\n");
                            boolean F = rule.javaAntecedent().toString().replace("[","").replace("]","").equals(key);
                            System.out.println("F is 3: "+F);
                            
                            if (F==true) {
                                
                                out_con3.append(Double.toString(rule.confidence()));
                                out_con3.append(",");
                                out_con3.append(rule.javaAntecedent().toString().replace(",", " "));
                                out_con3.append(",");
                                out_con3.append(value.toString());
                                out_con3.append(",");
                                out_con3.append(rule.javaConsequent().toString().replace(",", " "));
                                out_con3.append(",");
                                out_con3.append("undirected");
                                out_con3.append("\n");
                                
                                
                            }
                            
                        }
                        
                        
                    }
                    
                    
                }
            }
        
            out_freq.close();
            out_con1.close();
            out_con2.close();
            out_con3.close();
            out_con.close();
            
        }catch(IOException e){
            e.printStackTrace();
        }
        
    }
}
