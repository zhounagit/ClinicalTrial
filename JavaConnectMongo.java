import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import static com.mongodb.client.model.Projections.*;
import static java.util.Arrays.asList;

/**
 * Created by Na on 1/10/16. This is the first version of javaConnectMongo to generate serious adverse event subtitle, trial id,
 condition,intervention, etc. It can be simplified up to your needs, even the coding style is very basic. Please welcome to make it more
 efficient.
 */

public class javaConnectMongo {
    
    public static void inputQuery(MongoCursor<Document> cursor)
    {  
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject reported_events = json.optJSONObject("clinical_study").optJSONObject("clinical_results").optJSONObject("reported_events");
                
                JSONObject id_info = json.optJSONObject("clinical_study").optJSONObject("id_info");
                String nct_id = id_info.getString("nct_id");
                String nct_temp1 =null;
                String nct_temp2 =null;
                
                // add serious or other event to events arraylist
                ArrayList<String> events = new ArrayList<String>();
                if (!reported_events.isNull("serious_events")) {
                    events.add("serious_events");
                }
                
                //if (!reported_events.isNull("other_events")) {
                //  events.add("other_events");
                // }
                
                try {
                    
                    PrintWriter out = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0624serious_subtitle_category.txt", true), true);
                    PrintWriter trial = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0624serious_trial.txt", true), true);
                    PrintWriter trial_death = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0624serious_death_trial.txt", true), true);
                    PrintWriter con = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0624serious_con.txt", true), true);
                    PrintWriter inter = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0624serious_inter.txt", true), true);
                    PrintWriter category = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/0624serious_category.csv", true), true);
                    
                    
                    Map<String,String> map_cat = new HashMap<String,String>();
                    
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
                                    Map<String,String> event_ca = new HashMap<String, String>();
                                    
                                    int subjects_affected_size = 0;

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
                                                if (iCount < event.getJSONArray("counts").length()) {
                                                    
                                                    if (event.getJSONArray("counts").getJSONObject(iCount).has("subjects_affected")) {
                                                        subjects_affected_size = event.getJSONArray("counts").getJSONObject(iCount)
                                                        .getInt("subjects_affected");
                                                    }
                                                }
                                                
                                                
                                            } else if (event.get("counts") instanceof JSONObject)
                                                
                                            {
                                                if (event.getJSONObject("counts").has("subjects_affected")) {
                                                    subjects_affected_size = event.getJSONObject("counts").getInt("subjects_affected");
                                                }
                                                
                                            }
                                            
                                            if (subjects_affected_size != 0)
                                            {
                                                
                                                trial.write(nct_id + "\n");
                                                
                                                //sub_title is String
                                                if (event.get("sub_title") instanceof String) {
                                                    
                                                    if(arr_category.getJSONObject(j).has("title")) {
                                                        
                                                        category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                        category.append(",");
                                                        category.append(event.getString("sub_title").trim().replace(" ", "_").replaceAll("(?i:.*Death.*)", "Death").toString().replace(",", ""));
                                                        category.append(",");
                                                        category.append("\n");
                                                        
                                                        //subtitle - category
                                                        arr_subtitle.add(event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                         arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                        
                                                        
                                                        map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                    .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                        
                                                        
                                                    }
                                                    //sub_title is JSONObject
                                                } else if (event.get("sub_title") instanceof JSONObject) {
                                                    
                                                    if(arr_category.getJSONObject(j).has("title")) {
                                                        
                                                        category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",","").replace(" ","_"));
                                                        category.append(",");
                                                        
                                                        category.append(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                        replaceAll("(?i:.*Death.*)", "Death").toString().replace(",",""));
                                                        category.append(",");
                                                        category.append("\n");
                                                        arr_subtitle.add(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                         replaceAll("(?i:.*Death.*)", "Death")+"-"+
                                                                         arr_category.getJSONObject(j).getString("title").toString().replace(",","").replace(" ","_"));
                                                        
                                                        map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                    replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                    .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                        
                                                    }
                                                }
                                                
                                            }
                                            
                                            
                                            //else if event is JSONArray
                                        } else if (event_list.get("event") instanceof JSONArray) {
                                            
                                            JSONArray event = event_list.getJSONArray("event");
                                            category.append("\n");
                                            
                                            //traverse event array size
                                            for (int iEvent = 0; iEvent < event.length(); iEvent++) {
                                                //**************** to fix NCT00088881 issue: the third group exists but counts only has two groups content
                                                if (event.getJSONObject(iEvent).get("counts") instanceof JSONArray) {
                                                    
                                                    JSONArray counts = event.getJSONObject(iEvent).getJSONArray("counts");
                                                    
                                                    if (iCount < counts.length()) {
                                                        if (counts.getJSONObject(iCount).has("subjects_affected")) {
                                                            subjects_affected_size = counts.getJSONObject(iCount).getInt("subjects_affected");
                                                        }
                  
                                                    }
                                                } else if (event.getJSONObject(iEvent).get("counts") instanceof JSONObject) {
                                                    
                                                    if (event.getJSONObject(iEvent).getJSONObject("counts").has("subjects_affected")) {
                                                        subjects_affected_size = event.getJSONObject(iEvent).getJSONObject("counts").getInt("subjects_affected");
                                                    }
                                                    
                                                    
                                                }
                                                if (subjects_affected_size != 0)
                                                {
                                                    
                                                    trial.write(nct_id + "\n");
                                                    
                                                    if (event.getJSONObject(iEvent).has("sub_title")) {
                                                        
                                                        if (event.getJSONObject(iEvent).get("sub_title") instanceof String) {
                                                                                                                   
                                                            if(arr_category.getJSONObject(j).has("title")) {
                                                                
                                                                category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                category.append(",");
                                                                
                                                                category.append(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                                replaceAll("(?i:.*Death.*)", "Death").toString().replace(",", ""));
                                                                category.append("\n");
                                                                arr_subtitle.add(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                                 replaceAll("(?i:.*Death.*)", "Death")+"-"+arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                
                                                                map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                            replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                            .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                                
                                                                
                                                            }
                                                            
                                                        } else if (event.getJSONObject(iEvent).get("sub_title") instanceof JSONObject) {
                                                            
                                                            
                                                            if(arr_category.getJSONObject(j).has("title")) {
                                                                
                                                                category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",","").replace(" ","_"));
                                                                category.append(",");
                                                                
                                                                category.append(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                                .replaceAll("(?i:.*Death.*)", "Death").toString().replace(",",""));
                                                                category.append(",");
                                                                category.append("\n");
                                                                
                                                                arr_subtitle.add(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                                 .replaceAll("(?i:.*Death.*)", "Death")+"-"+arr_category.getJSONObject(j).getString("title").toString().replace(",","").replace(" ","_"));
                                                                
                                                                map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                            .replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                            .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                                
                                                                
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
                                        
                                        if(s.contains("Death"))
                                        {                  
                                            trial_death.write(nct_id+"\n"); 
                                            if (json.optJSONObject("clinical_study").has("condition")) {
                                                
                                                if(nct_temp1==nct_id){ //this part can be omitted
                                                    
                                                }else{
                                                    
                                                    nct_temp1=nct_id;
                                                    if(json.optJSONObject("clinical_study").get("condition") instanceof String){
                                                        
                                                        String con1 = json.optJSONObject("clinical_study").get("condition") + "\n";
                                                        
                                                        con.write(con1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                  .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[","")
                                                                  .replace("]",""));
                                                        
                                                    }else if(json.optJSONObject("clinical_study").get("condition") instanceof JSONArray){
                                                        
                                                        for(int c=0;c<json.optJSONObject("clinical_study").getJSONArray("condition").length();c++){
                                                            
                                                            String con1 = json.optJSONObject("clinical_study").getJSONArray("condition").getString(c)+"\n";
                                                            
                                                            con.write(con1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                      .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[","")
                                                                      .replace("]",""));
                                                            
                                                        }
                                                        
                                                    }
                                                    
                                                    
                                                }
                                                
                                                
                                                
                                            }
                                            
                                            if (json.optJSONObject("clinical_study").has("intervention")) {
                                                
                                                if(nct_temp2==nct_id){ //this part can be omitted
                                                    
                                                }else{
                                                    
                                                    nct_temp2=nct_id;
                                                    if(json.optJSONObject("clinical_study").get("intervention") instanceof JSONObject)
                                                    {
                                                        
                                                        String inter_name = json.optJSONObject("clinical_study").getJSONObject("intervention").getString("intervention_name") + "\n";
                                                        
                                                        inter.write(inter_name.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                    .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT"));
                                                        
                                                    } else if(json.optJSONObject("clinical_study").get("intervention") instanceof JSONArray)
                                                    {
                                                        
                                                        
                                                        for(int t=0;t<json.optJSONObject("clinical_study").getJSONArray("intervention").length();t++){
                                                            String inter_name = json.optJSONObject("clinical_study").getJSONArray("intervention").getJSONObject(t).getString("intervention_name")+"\n";
                                                            
                                                            inter.write(inter_name.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                        .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT"));
                                                        }
                                                        
                                                    }
                                                    
                                                }
                                                
                                                
                                                
                                                
                                            }
                                        }
                                        
                                        out.write(s + "\n");
                                        //out.append(s+"\n");
                                        
                                        
                                    }
                                    
                                }
                                
                            }
                            
                        }
                        
                    }
                    
                    out.close();
                    con.close();
                    inter.close();
                    trial.close();
                    trial_death.close();
                    category.close();
                    
         
                        Iterator<Map.Entry<String, String>> iterator = map_cat.entrySet().iterator();
                        
                        while (iterator.hasNext()) {
                            
                            Map.Entry<String, String> entry = iterator.next();
                            String key = entry.getKey().toString();
                            String value = entry.getValue();
                            System.out.println(key+","+value);
                        }
                    
                    
                } catch (IOException exp) {
                    exp.printStackTrace();
                }
                
            }
            cursor.close();
            
        } catch (JSONException e) {
            e.printStackTrace();
        }
         
    }
    
    
    public static void main(String[] args) {
        
        //connect to mongodb
        MongoClient mclient = new MongoClient("localhost", 27017);
        MongoDatabase db = mclient.getDatabase("clinicaltrial");
        MongoCollection dbc = db.getCollection("trials");
        System.out.println("MongoDB is connected by this test");
        
        //Initialize query
        Document query = new Document("$and", asList(new Document("clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.sub_title", new Document("$exists", true)),
                                                     new Document("clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.sub_title", new Document("$exists", true))));
        
        //return all documents#
        System.out.println("Number of documents: " + dbc.count(new Document()));
        
        MongoCursor<Document> cursor = dbc.find(query).projection(fields(include(
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
                                                                                 "clinical_study.clinical_results.reported_events.serious_events.category_list.category.title",
                                                                                 "clinical_study.intervention.intervention_name", "clinical_study.condition", "clinical_study.intervention.intervention_type", "clinical_study.condition_browse.mesh_term"
                                                                                 ), excludeId())).iterator();
        
        inputQuery(cursor);
        
    }
}
























































































































































































































