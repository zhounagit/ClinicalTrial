/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// $example on$
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


public class JavaMulticlassClassificationMetricsExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Multi class Classification Metrics Example").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        sc.setLogLevel("OFF");
        String path = "src/main/resources/3040features_full_table.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();
        
        //get cpu usage
        com.sun.management.OperatingSystemMXBean operatingSystemMXBean =
        (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long prevProcessCpuTime = operatingSystemMXBean.getProcessCpuTime();
        
        try{
            
            PrintWriter out = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0615_3040features_statistic.csv", true), true);
            out.write("topNumberedFeatures");
            out.write(",");
            out.write("class label"); //class labe1 = 0.0, without death; class label 1.0, with death. 
            out.write(",");
            out.write("precision");
            out.write(",");
            out.write("recall");
            out.write(",");
            out.write("f1-score");
            out.write(",");
            out.write("accuracy");
            out.write(",");
            out.write("class label");
            out.write(",");
            out.write("precision");
            out.write(",");
            out.write("recall");
            out.write(",");
            out.write("f1-score");
            out.write(",");
            out.write("accuracy");
            out.write("\n");
            
            ChiSqSelector selector;
            
            //for(int i=0;i<=2069;i+=5)
            for(int i=0;i<=608;i++)
            {
                if(i==0){
                    selector = new ChiSqSelector(1);
                }else { //i>=1
                    selector = new ChiSqSelector(i*5);
                }
                
                // Create ChiSqSelector model (selecting features)
                final ChiSqSelectorModel transformer = selector.fit(data.rdd());
                JavaRDD<LabeledPoint> filteredData = data.map(
                                                              lp -> new LabeledPoint(lp.label(), transformer.transform(lp.features()))
                                                              );
                
                // Split initial RDD into two... [80% training data, 20% testing data].
                JavaRDD<LabeledPoint>[] splits = filteredData.randomSplit(new double[]{0.8, 0.2}, 11L);
                JavaRDD<LabeledPoint> training = splits[0].cache();
                JavaRDD<LabeledPoint> test = splits[1];
                
                //System.out.println(splits);
                // Run training algorithm to build the model.
                LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(2)
                .run(training.rdd());
                
                // Compute raw scores on the test set.
                JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
                                                                                 new Tuple2<>(model.predict(p.features()), p.label()));
                
                // Get evaluation metrics.
                MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
                
                // Confusion matrix
                // Matrix confusion = metrics.confusionMatrix();
                //System.out.println("Confusion matrix: \n" + confusion);
                //System.out.println("Accuracy = " + metrics.accuracy());
                System.out.println(metrics.labels().length);
                
                
                if(i==0){
                    out.write(String.valueOf(i+1));
                }else {
                    out.write(String.valueOf(i*5));
                }
                out.write(",");
                
                for (int j = 0; j < metrics.labels().length; j++) {
                    
                    System.out.print("\t class:\t" + metrics.labels()[j] + "\t" + metrics.precision(metrics.labels()[j]) + "\t" + metrics.recall(
                                                                                                                                                 metrics.labels()[j]) + "\t" + metrics.fMeasure(metrics.labels()[j]) + "\t" + metrics.accuracy());
                    
                    out.write(String.valueOf(metrics.labels()[j]));
                    out.write(",");
                    out.write(String.valueOf(metrics.precision(metrics.labels()[j])));
                    out.write(",");
                    out.write(String.valueOf(metrics.recall(metrics.labels()[j])));
                    out.write(",");
                    out.write(String.valueOf(metrics.fMeasure(metrics.labels()[j])));
                    out.write(",");
                    out.write(String.valueOf(metrics.accuracy()));
                    out.write(",");
                    
                    //after head titles, switch to next line
                    if(j==1){
                        out.write("\n");
                    }
                }
                
                //Weighted stats
                //System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
                //System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
                //System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
                //System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());
                // Save and load model
                //model.save(sc, "target/tmp/LogisticRegressionModel "+System.currentTimeMillis());
            }
            
            out.close();
            
        }catch(IOException e){
            e.printStackTrace();
        }
        
        //LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc,"target/tmp/LogisticRegressionModel");
        sc.stop();
        
        //get cpu process time
        long processCpuTime = operatingSystemMXBean.getProcessCpuTime();
        long cpuTime = processCpuTime - prevProcessCpuTime;
        System.out.println("CPU time: "+cpuTime/1000000000.0 +"s"); //nanoseconds convert to seconds
    }
}
