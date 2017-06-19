# ClinicalTrial
- Found top confidence serious adverse events which are strong associated with death event.
  Practiced FP-Growth and AssociationRules. 
  
- Optimize a classification model to predict clinical trial death events from investigated fields
 conditions, interventions, phase, serious adverse events, participants average age, participant#, etc.
 
- JavaConnectMongo.java :
   - This is the basic source file to generate serious adverse event subtitle, its category, clinical trial id, condition, intervention....,definitely not the most simple one. The coding style is following Robomongo trial data structure, you can remove or add more searching criteria. Make sure refer to schema.txt to updata data element in "MongoCursor<Document> cursor" of JavaConnectMongo.java
 
  -Later source codes are based on this version, refer to folder "Generate new features/Features.java" to see updated version to get above data.
  
- Generate new features : 
This folder has source code to generate data of trial id, Number of Participant, Mean Age, Phase, Intervention, Condition and Serious Adverse Events, all these features are imported from a csv file which consists of 3040 features. 

The 3040 features result will be the input to JavaMulticlassClassificationMetricsExample.java, the classification model will generate
class label(0.0 - without death; 1.0 - with death), precision, recall, f1-score, accuracy, or confusion matrix. 

For other classification source code, please refer to spark-rdd-dataframe-dataset-master project in IntelliJ IDEA.
  
- Folder gephi graph (demo) folders have previous graphs made to domenstrate top high confidence/frequency serious adverse events to event "Death". 

- For more source data, please refer to Appendix.doc.

- Analyzing Lethal Adverse Event Association Patterns in Clinical Trials_Draft.docx is the earlier version of paper draft, the classfication model on 3040 selected features can be considered in final paper. 

- Refer to /Users/HIA/Na/serious or ~/other for more test results.

- Startup mongoDB : /Users/HIA/project/tool/mongoDB
  sh start-db.sh 
  use Robomongo viewer to check each trial structure.
  
- Desktop/weiheng/Readme/schema.txt to get each data element.
