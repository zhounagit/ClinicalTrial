# ClinicalTrial
- Found top confidence serious adverse events which are strong associated with death event.
  Practiced FP-Growth and AssociationRules. 
- Optimize a classification model to predict clinical trial death events from investigated fields
 conditions, interventions, phase, serious adverse events, participants average age, participant#, countries, etc.
- JavaConnectMongo.java :
  This is the basic source file to generate serious adverse event subtitle, its category, clinical trial id, condition, intervention.... 
  You can add more searching criteria based on this skeleton file. 
- Generate new features : This folder has source code to generate data of trial id, Number of Participant, Mean Age, Phase, Intervention, Condition and Serious Adverse Events, totally 3040 features will be monitored in JavaMulticlassClassificationMetricsExample.java.
  For other classification source code, please refer to spark-rdd-dataframe-dataset-master project in IntelliJ IDEA.
- Folder gephi graph (demo) folders have previous graphs made to domenstrate top high confidence/frequency serious adverse events to event "Death". 
- For more source data, please refer to Appendix.doc.
