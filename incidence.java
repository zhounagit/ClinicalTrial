/**
 * Created by Na Zhou on 10/26/16.
 */
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.parquet.it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.bson.Document;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Projections.*;
import static java.util.Arrays.asList;


public class impactIncidence {
    
    
    
    public static void inputQuery(MongoCursor<Document> cursor, String event_var)
    {
        
        //connect to mongodb
        MongoClient mclient = new MongoClient("localhost", 27017);
        MongoDatabase db = mclient.getDatabase("clinicaltrial");
        MongoCollection dbc = db.getCollection("trials");
        System.out.println("MongoDB is connected by this test");
        
        //Initialize query
        Document query = new Document("$and", asList(new Document("clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.sub_title", new Document("$exists", true)),
                                                     new Document("clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.sub_title", new Document("$exists", true))));
        
        Document query1 = new Document("$or",asList(new Document("clinical_study.id_info.nct_id","NCT00036270"),new Document("clinical_study.id_info.nct_id","NCT00089791")));
        
        //return all documents#
        
        // System.out.println("Number of documents: " + dbc.count(new Document()));
        
        cursor = dbc.find(query).projection(fields(include(
                                                           "clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.sub_title",
                                                           "clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.sub_title",
                                                           "clinical_study.clinical_results.reported_events.other_events.category_list.category.title", "clinical_study.id_info.nct_id",
                                                           "clinical_study.clinical_results.reported_events.group_list.group.title",
                                                           "clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.counts.subjects_affected",
                                                           "clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.counts.subjects_at_risk",
                                                           "clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.counts.events",
                                                           "clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.counts.subjects_affected",
                                                           "clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.counts.subjects_at_risk",
                                                           "clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.counts.events",
                                                           "clinical_study.clinical_results.reported_events.serious_events.category_list.category.title","clinical_study.phase",
                                                           "clinical_study.intervention.intervention_name", "clinical_study.condition", "clinical_study.intervention.intervention_type", "clinical_study.condition_browse.mesh_term"
                                                           ), excludeId())).iterator();
        
        
        //general table to store all death incidence with current event_var
        List<Double> arr_current_event_with_death = new DoubleArrayList();
        List<Double> arr_no_event_with_death = new DoubleArrayList();
        Map<String, Double> death_incidence_subtraction = new HashMap<String, Double>();
        Double ave_death_inci_with_event =0.0;
        Double ave_death_inci_no_event =0.0;
        
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject clinical_study = json.optJSONObject("clinical_study");
                JSONObject reported_events = json.optJSONObject("clinical_study").optJSONObject("clinical_results").optJSONObject("reported_events");
                
                JSONObject id_info = json.optJSONObject("clinical_study").optJSONObject("id_info");
                String nct_id = id_info.getString("nct_id");
                String nct_id_temp = null;
                Double trial_with_event_death = 0.0;
                Double trial_no_event_death = 0.0;
                
                // add serious or other event to events arraylist
                ArrayList<String> events = new ArrayList<String>();
                if (!reported_events.isNull("serious_events")) {
                    events.add("serious_events");
                }
                
                //if (!reported_events.isNull("other_events")) {
                //  events.add("other_events");
                // }
                
                try {
                    
                    PrintWriter clinical_results_trial = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026clinical_results_trial.csv", true), true);
                    
                    //get subtitle
                    PrintWriter out = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026serious_subtitle_category.txt", true), true);
                    PrintWriter with_event_death_arm = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026death_incidence_from_death_arm_with_event.csv", true), true);
                    PrintWriter no_event_death_arm = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026death_incidence_from_death_arm_non_event.csv", true), true);
                    PrintWriter death_incidence = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026death_with_without_event.csv", true), true);
                    
                    PrintWriter trial_with_event_d = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026trial_with_event_death_incidence.csv", true), true);
                    PrintWriter trial_no_event_d = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026trial_no_event_death_incidence.csv", true), true);
                    
                    if(clinical_study.has("clinical_results")){
                        
                        clinical_results_trial.append(nct_id);
                        clinical_results_trial.append("\n");
                    }
                    
                    //check on each arm
                    int subjects_affected_size = 0;
                    int subjects_at_risk_size = 0;
                    int affected_temp = 0;
                    int at_risk_temp = 0;
                    
                    
                    for (int e = 0; e < events.size(); e++) {
                        
                        JSONObject category_list = reported_events.optJSONObject(events.get(e)).optJSONObject("category_list");
                        
                        //group categories ----- column names in Serious Adverse Events
                        if (!reported_events.isNull("group_list")) {
                            int counts_group = 0;
                            JSONObject group_list = reported_events.optJSONObject("group_list");
                            
                            //if group is JSONArray, below counts will be JSONArray except NCT00401778
                            if (group_list.get("group") instanceof JSONArray) {
                                
                                JSONArray arr_group = group_list.optJSONArray("group");
                                counts_group = arr_group.length();
                                
                            } else {
                                counts_group = 1;
                                
                            }
                            
                            if (category_list.getJSONArray("category") != null)
                            {
                                
                                //arr_category is serious category JSONArray
                                JSONArray arr_category = category_list.getJSONArray("category");
                                
                                //iCount is the column index of serious adverse events table
                                for (int iCount = 0; iCount < counts_group; iCount++)
                                {
                                    //add sub_title to array arr_subtitle
                                    ArrayList<String> arr_subtitle = new ArrayList<String>();
                                    ArrayList<Float> current_arm_incidence = new ArrayList<Float>();
                                    Map<String,Double> event_and_aveIncidence = new HashMap<String, Double>();
                                    Map<String,Double> no_event_and_aveIncidence = new HashMap<String, Double>();
                                    
                                    // j is the row index of serious adverse events table,except j=0 (neglect total result)
                                    for (int j = 1; j < arr_category.length(); j++)
                                    {
                                        
                                        JSONObject event_list = arr_category.getJSONObject(j).getJSONObject("event_list");
                                        
                                        
                                        //if event is JSONObject
                                        if (event_list.get("event") instanceof JSONObject)
                                        {
                                            
                                            //what if counts is not array even group is an array? for NCT00401778
                                            JSONObject event = event_list.getJSONObject("event");
                                            
                                            if (event.get("counts") instanceof JSONArray)
                                            {
                                                if (iCount < event.getJSONArray("counts").length())
                                                {
                                                    
                                                    if (event.getJSONArray("counts").getJSONObject(iCount).has("subjects_affected")) {
                                                        subjects_affected_size = event.getJSONArray("counts").getJSONObject(iCount)
                                                        .getInt("subjects_affected");
                                                    }
                                                    if (event.getJSONArray("counts").getJSONObject(iCount).has("subjects_at_risk")) {
                                                        
                                                        subjects_at_risk_size = event.getJSONArray("counts").getJSONObject(iCount)
                                                        .getInt("subjects_at_risk");
                                                        
                                                    }
                                                }
                                                
                                            } else if (event.get("counts") instanceof JSONObject)
                                                
                                            {
                                                if (event.getJSONObject("counts").has("subjects_affected")) {
                                                    subjects_affected_size = event.getJSONObject("counts").getInt("subjects_affected");
                                                }
                                                
                                                if (event.getJSONObject("counts").has("subjects_at_risk")) {
                                                    
                                                    subjects_at_risk_size = event.getJSONObject("counts").getInt("subjects_at_risk");
                                                }
                                                
                                            }
                                            
                                            if (subjects_affected_size != 0)
                                            {
                                                
                                                
                                                //sub_title is String
                                                if (event.get("sub_title") instanceof String) {
                                                    
                                                    if(arr_category.getJSONObject(j).has("title"))
                                                    {
                                                        
                                                        //subtitle - category
                                                        arr_subtitle.add(event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                         arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                        
                                                    }
                                                    
                                                    String sub_title = event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death");
                                                    
                                                    if (sub_title.contains("Death"))
                                                    {
                                                        
                                                        //add each arm
                                                        affected_temp = subjects_affected_size;
                                                        at_risk_temp = subjects_at_risk_size;
                                                        
                                                        if (affected_temp > 0)
                                                        {
                                                
                                                            float ratio = (float) (affected_temp) / at_risk_temp;
                                                            current_arm_incidence.add(ratio);
                                                            
                                                        }
                                            
                                                        
                                                        
                                                    }
                                                    
                                                    //sub_title is JSONObject
                                                } else if (event.get("sub_title") instanceof JSONObject) {
                                                    
                                                    
                                                    if(arr_category.getJSONObject(j).has("title"))
                                                    {
                                                        
                                                        arr_subtitle.add(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                         replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                         arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                        
                                                    }
                                                    
                                                    
                                                    String sub_title = event.getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_").
                                                    replaceAll("(?i:.*Death.*)", "Death");
                                                    
                                                    if (sub_title.contains("Death"))
                                                    {
                                                        
                                                        //add each arm
                                                        affected_temp = subjects_affected_size;
                                                        at_risk_temp = subjects_at_risk_size;
                                                        
                                                        if (affected_temp > 0)
                                                        {
                                                
                                                            float ratio = (float) (affected_temp) / at_risk_temp;
                                                            current_arm_incidence.add(ratio);
                                                            
                                                        }
                                                        
                                                    }
                                                    
                                                }
                                                
                                            }
                                            
                                            
                                            //else if event is JSONArray
                                        } else if (event_list.get("event") instanceof JSONArray) {
                                            
                                            JSONArray event = event_list.getJSONArray("event");
                                            
                                            //traverse event array size
                                            for (int iEvent = 0; iEvent < event.length(); iEvent++)
                                            {
                                                
                                                //**************** to fix NCT00088881 issue: the third group exists but counts only has two groups content
                                                if (event.getJSONObject(iEvent).get("counts") instanceof JSONArray) {
                                                    
                                                    JSONArray counts = event.getJSONObject(iEvent).getJSONArray("counts");
                                                    
                                                    if (iCount < counts.length())
                                                    {
                                                        if (counts.getJSONObject(iCount).has("subjects_affected")) {
                                                            subjects_affected_size = counts.getJSONObject(iCount).getInt("subjects_affected");
                                                        }
                                                        
                                                        if (counts.getJSONObject(iCount).has("subjects_at_risk")) {
                                                            
                                                            subjects_at_risk_size = counts.getJSONObject(iCount).getInt("subjects_at_risk");
                                                        }
                                                        
                                                        
                                                    }
                                                } else if (event.getJSONObject(iEvent).get("counts") instanceof JSONObject)
                                                {
                                                    
                                                    if (event.getJSONObject(iEvent).getJSONObject("counts").has("subjects_affected")) {
                                                        subjects_affected_size = event.getJSONObject(iEvent).getJSONObject("counts").getInt("subjects_affected");
                                                        
                                                    }
                                                    
                                                    if (event.getJSONObject(iEvent).getJSONObject("counts").has("subjects_at_risk")) {
                                                        
                                                        subjects_at_risk_size = event.getJSONObject(iEvent).getJSONObject("counts").getInt("subjects_at_risk");
                                                        
                                                    }
                                                }
                                                
                                                
                                                if (subjects_affected_size != 0)
                                                {
                                                    
                                                    if (event.getJSONObject(iEvent).has("sub_title")) {
                                                        
                                                        if (event.getJSONObject(iEvent).get("sub_title") instanceof String) {
                                                            
                                                            
                                                            if(arr_category.getJSONObject(j).has("title")) {
                                                                
                                                                arr_subtitle.add(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                                 replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                
                                                            }
                                                            
                                                            
                                                            String sub_title = event.getJSONObject(iEvent).getString("sub_title").trim().replace(",", " ").replace(" ", "_").
                                                            replaceAll("(?i:.*Death.*)", "Death");
                                                            
                                                            if (sub_title.contains("Death"))
                                                            {
                                                                
                                                                //add each arm
                                                                affected_temp = subjects_affected_size;
                                                                at_risk_temp = subjects_at_risk_size;
                                                                
                                                                if (affected_temp > 0)
                                                                {
                                                                    
                                                     
                                                                    float ratio = (float) (affected_temp) / at_risk_temp;
                                                                    current_arm_incidence.add(ratio);
                                                                    
                                                                }
                                                                
                                               
                                                                
                                                                
                                                            }
                                                            
                                                            
                                                        } else if (event.getJSONObject(iEvent).get("sub_title") instanceof JSONObject) {
                                                            
                                                            if(arr_category.getJSONObject(j).has("title"))
                                                            {
                                                                
                                                                arr_subtitle.add(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                                 .replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                
                                                            }
                                                            
                                                            String sub_title = event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_")
                                                            .replaceAll("(?i:.*Death.*)", "Death");
                                                            
                                                            
                                                            if (sub_title.contains("Death"))
                                                            {
                                                                
                                                                //add each arm
                                                                affected_temp = subjects_affected_size;
                                                                at_risk_temp = subjects_at_risk_size;
                                                                
                                                                if (affected_temp > 0)
                                                                {
                                                            
                                                                    float ratio = (float) (affected_temp) / at_risk_temp;
                                                                    current_arm_incidence.add(ratio);
                                                                    
                                                                    
                                                                }
                                                                
                                                    
                                                                
                                                            }
                                                            
                                                        }
                                                    }
                                                    
                                                }
                                                
                                            }
                                            
                                            
                                        }
                                        
                                        
                                    }
                                    
                                    
                                    String s = arr_subtitle.toString();
                                    s = s.substring(1, s.length() - 1).replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                    .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT");
                                    
                                    
                                    if (!s.isEmpty())
                                    {
                                        
                                        if(s.contains(event_var))
                                        {
                                            Double sum_event = 0.0;
                                            if(current_arm_incidence.size()>0)
                                            {
                                                
                                                for(int j=0;j<current_arm_incidence.size();j++){
                                                    // this loop is traverse each death incidence in current arm, add it to sum_event
                                                    with_event_death_arm.append(nct_id);
                                                    with_event_death_arm.append(",");
                                                    with_event_death_arm.append("arm "+String.valueOf(iCount));
                                                    with_event_death_arm.append(",");
                                                    with_event_death_arm.append(current_arm_incidence.get(j).toString() + "\n");
                                                    sum_event += current_arm_incidence.get(j);
                                                    
                                                }
                                                
                                                trial_with_event_death += sum_event;
                                                
                                                //arr_current_event_with_death stores the average death incidence in each arm
                                                arr_current_event_with_death.add(sum_event);
                                                
                                                //All death arms average incidence with current event
                                                event_and_aveIncidence.put(event_var, sum_event / current_arm_incidence.size());
                                                //this is for each death arm, death average incidence with current event_var
                                                death_incidence.append(event_var);
                                                death_incidence.append(",");
                                                death_incidence.append(Double.toString(sum_event));
                                                death_incidence.append("\n");
                                                
                                            }
                                            
                                            
                                            
                                        }else
                                        {
                                            Double sum_no_event=0.0;
                                            
                                            if(current_arm_incidence.size()>0)
                                            {
                                                
                                                for(int j=0;j<current_arm_incidence.size();j++)
                                                {
                                                    
                                                    no_event_death_arm.append(nct_id);
                                                    no_event_death_arm.append(",");
                                                    no_event_death_arm.append("arm " + String.valueOf(iCount));
                                                    no_event_death_arm.append(",");
                                                    no_event_death_arm.append(current_arm_incidence.get(j).toString() + "\n");
                                                    sum_no_event +=current_arm_incidence.get(j);
                                                    
                                                }
                                                
                                                trial_no_event_death += sum_no_event;
                                                
                                                //All death arms average incidence without current event
                                                no_event_and_aveIncidence.put("No " + event_var, sum_no_event / current_arm_incidence.size());
                                                
                                                death_incidence.append("No " + event_var);
                                                death_incidence.append(",");
                                                death_incidence.append(Double.toString(sum_no_event));
                                                death_incidence.append("\n");
                                                
                                                arr_no_event_with_death.add(sum_no_event);
                                                
                                            }
                                            
                                        }
                                        
                                        out.write(s + "\n");
                                        
                                        
                                    }
                                    
                                } //iCount
                                
                            }
                            
                        }
                        
                    }
                    
                    
                    
                    if(trial_with_event_death>0.0){
                        
                        trial_with_event_d.append(nct_id);
                        trial_with_event_d.append(",");
                        trial_with_event_d.append(String.valueOf(trial_with_event_death));
                        trial_with_event_d.append("\n");
                    }
                    
                    
                    if(trial_no_event_death>0.0){
                        
                        trial_no_event_d.append(nct_id);
                        trial_no_event_d.append(",");
                        trial_no_event_d.append(String.valueOf(trial_no_event_death));
                        trial_no_event_d.append("\n");
                        
                    }
                    
                    
                    
                    out.close();
                    clinical_results_trial.close();
                    no_event_death_arm.close();
                    with_event_death_arm.close();
                    death_incidence.close();
                    trial_with_event_d.close();
                    trial_no_event_d.close();
                    
                    
                } catch (IOException exp) {
                    exp.printStackTrace();
                }
                
            }
            
            cursor.close();
            
        } catch (JSONException e) {
            e.printStackTrace();
        }
        
        
        //post processing all death_incidence with or without event_var
        double sum1=0.0;
        double sum2=0.0;
        for(int i=0;i<arr_current_event_with_death.size();i++)
        {
            sum1 +=arr_current_event_with_death.get(i);
        }
        
        
        
        ave_death_inci_with_event = sum1/arr_current_event_with_death.size();
        System.out.println("with this event death ave incidence is: "+ave_death_inci_with_event);
        
        for(int j=0;j<arr_no_event_with_death.size();j++)
        {
            sum2 +=arr_no_event_with_death.get(j);
        }
        
        ave_death_inci_no_event = sum2/arr_no_event_with_death.size();
        System.out.println("without this event death ave incidence is: "+ave_death_inci_no_event);
        
        //subtraction of (death incidence with event) - (death incidence without event)
        double subtraction = ave_death_inci_with_event - ave_death_inci_no_event;
        if(subtraction >0){
            
            System.out.println("we find event can increase death incidence: "+event_var);
        }
        death_incidence_subtraction.put(event_var, subtraction);
        System.out.println("event: " + event_var + "\tsubtraction: " + subtraction);
        double [] valueA = new double[arr_current_event_with_death.size()];
        double [] valueB = new double[arr_no_event_with_death.size()];
        
        for(int i=0;i<arr_current_event_with_death.size();i++){
            
            valueA[i] = arr_current_event_with_death.get(i);
        }
        
        for(int j=0;j<arr_no_event_with_death.size();j++){
            
            valueB[j]=arr_no_event_with_death.get(j);
        }
        
        TTest naTTest = new TTest();
        //            double naResult = naTTest.tTest(valueA,valueB);
        
        try{
            
            PrintWriter pValue_death_incidence = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/1026pValue_death_incidence.csv", true), true);
            
            pValue_death_incidence.append(event_var);
            pValue_death_incidence.append(",");
            pValue_death_incidence.append(String.valueOf(ave_death_inci_with_event));
            pValue_death_incidence.append(",");
            pValue_death_incidence.append(String.valueOf(ave_death_inci_no_event));
            pValue_death_incidence.append(",");
            // pValue_death_incidence.append(String.valueOf(naResult));
            pValue_death_incidence.append(",");
            pValue_death_incidence.append(String.valueOf(subtraction));
            pValue_death_incidence.append(",");
            pValue_death_incidence.append("\n");
            
            pValue_death_incidence.close();
            
        }catch(IOException e){
            
            e.printStackTrace();
        }
        
    }
    
    public static void main(String[] args) {
        
        List <String> incidence_test = new ArrayList<String>();
        
        //read excel file to array List.
        String csvFile = "/Users/HIA/Desktop/Final_Paper/ssae_confidence_findP_2.csv";
        BufferedReader br = null;
        String line = "";
        String csvSplitBy =",";
        try {
            
            br = new BufferedReader(new FileReader(csvFile));
            while((line =br.readLine())!=null){
                String[] serious_conf_event = line.split(csvSplitBy);
                
                //read data from first column
                incidence_test.add(serious_conf_event[0]);
            }
            
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException ee){
            ee.printStackTrace();
        }finally {
            if(br !=null){
                try{
                    br.close();
                }catch (IOException e1){
                    e1.printStackTrace();
                }
            }
        }
        
        MongoCursor<Document> cursor=null;
        System.out.println("event lists: "+incidence_test);
        
        for(int i=0; i<incidence_test.size();i++){
            inputQuery(cursor, incidence_test.get(i));
        }
        
    }
}

























































































































































































































