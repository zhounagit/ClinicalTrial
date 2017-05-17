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
 * Created by Na on 1/10/16.
 */

public class javaConnectMongo {
    
    public static void inputQuery(MongoCursor<Document> cursor)
    {
        
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject reported_events = json.optJSONObject("clinical_study").optJSONObject("clinical_results").optJSONObject("reported_events");
                String url = json.optJSONObject("clinical_study").optJSONObject("required_header").getString("url");
                
                JSONObject id_info = json.optJSONObject("clinical_study").optJSONObject("id_info");
                String nct_id = id_info.getString("nct_id");
                String nct_temp1 =null;
                String nct_temp2 =null;
                String events_type = null;
                ArrayList<String> arr_condition = new ArrayList<String>();
                ArrayList<String> arr_intervention = new ArrayList<String>();
                Map<String, String> event_arm = new HashMap<String, String>();
                Map<String, String> arm_iCount = new HashMap<String, String>();
                Map<String, String> arm_affected_risk = new HashMap<String, String>();
                Map<String, String> arm_affected = new HashMap<String, String>();
                Map<String, String> arm_at_risk = new HashMap<String, String>();
                
                // add serious, other event or both to events arraylist
                ArrayList<String> events = new ArrayList<String>();
                
                if (!reported_events.isNull("serious_events")) {
                    events.add("serious_events");
                }
                
                if (!reported_events.isNull("other_events")) {
                    events.add("other_events");
                }
                
                try {
                    
                    PrintWriter out = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/allergic_subtitle_category.txt", true), true);
                    PrintWriter trial = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/all_trial.txt", true), true);
                    PrintWriter trial_affected_risk = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/affected_risk_trial.csv", true), true);
                    PrintWriter trial_allergic = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/allergic_trial.txt", true), true);
                    PrintWriter con_allergic = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/allergic_con.txt", true), true);
                    PrintWriter inter_allergic = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/allergic_inter.txt", true), true);
                    PrintWriter category = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/allergic_category.csv", true), true);
                    PrintWriter allergic_table = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/complex_hypersensitivity.csv", true), true);
                    
                    Map<String,String> map_cat = new HashMap<String,String>();
                    
                    
                    int subjects_affected_size = 0;
                    int affected = 0;
                    int subjects_at_risk_size = 0;
                    int at_risk = 0;
                    int affected_temp=0;
                    int at_risk_temp=0;
                    String col_nct=null;
                    String allergic_event = null;
                    String allergic_category = null;
                    String arm_title = null;
                    Map<String,String> arms = new HashMap<String, String>();
                    
                    for (int e = 0; e < events.size(); e++)
                    {
                        
                        JSONObject category_list = reported_events.optJSONObject(events.get(e)).optJSONObject("category_list");
                        
                        //group categories ----- column names in Serious Adverse Events
                        if (!reported_events.isNull("group_list"))
                        {
                            int counts_group = 0;
                            JSONObject group_list = reported_events.optJSONObject("group_list");
                            
                            //if group is JSONArray, below counts will be JSONArray except NCT00401778
                            if (group_list.get("group") instanceof JSONArray)
                            {
                                
                                JSONArray arr_group = group_list.optJSONArray("group");
                                counts_group = arr_group.length();
                                
                            } else {
                                counts_group = 1;
                                
                            }
                            
                            if (category_list.getJSONArray("category") != null) {
                                
                                //arr_category is serious category JSONArray
                                JSONArray arr_category = category_list.getJSONArray("category");
                                
                                if (events_type == events.get(e)) {
                                    
                                } else
                                {
                                    
                                    events_type = events.get(e);
                                    
                                    //iCount is the column index of serious adverse events table
                                    for (int iCount = 0; iCount < counts_group; iCount++)
                                    {
                                        
                                        if (counts_group > 1) {
                                            //arm_title = group_list.getJSONArray("group").getJSONObject(iCount).getString("title").replace(",", "|");
                                            arm_title = group_list.getJSONArray("group").getJSONObject(iCount).getString("title").replace(",", "");
                                        } else {
                                            //arm_title = group_list.getJSONObject("group").getString("title").replace(",", "|");
                                            arm_title = group_list.getJSONObject("group").getString("title").replace(",","");
                                        }
                                        
                                        
                                        //add sub_title to array arr_subtitle
                                        ArrayList<String> arr_subtitle = new ArrayList<String>();
                                        
                                        // j is the row index of serious adverse events table,except j=0 (neglect total result)
                                        for (int j = 1; j < arr_category.length(); j++) {
                                            
                                            JSONObject event_list = arr_category.getJSONObject(j).getJSONObject("event_list");
                                            
                                            //if event is JSONObject
                                            if (event_list.get("event") instanceof JSONObject)
                                                
                                            {
                                                //what if counts is not array even group is an array? for NCT00401778
                                                JSONObject event = event_list.getJSONObject("event");
                                                
                                                if (event.get("counts") instanceof JSONArray) {
                                                    if (iCount < event.getJSONArray("counts").length()) {
                                                        
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
                                                
                                                //if (subjects_affected_size != 0)
                                                // {
                                                
                                                trial.write(nct_id + "\n");
                                                if (event.has("sub_title")) {
                                                    
                                                    //sub_title is String
                                                    if (event.get("sub_title") instanceof String)
                                                    {
                                                        
                                                        if (arr_category.getJSONObject(j).has("title")) {
                                                            
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
                                                        
                                                        //allergic specific event affected and at risk count
                                                        String sub_title = event.getString("sub_title");
                                                        
                                                        if (sub_title.contains("hypersensitivity") || sub_title.contains("Hypersensitivity") || sub_title.contains("HYPERSENSITIVITY")) {
                                                            
                                                            //add each arm
                                                            affected_temp = subjects_affected_size;
                                                            at_risk_temp = subjects_at_risk_size;
                                                            //event
                                                            allergic_event = event.getString("sub_title").trim().replace(",", "").replace(" ", "_").replaceAll("(?i:.*Death.*)", "Death");
                                                            
                                                            if (affected_temp > 0 && allergic_event.matches("(?i:hypersensitivity)")) {
                                                                
                                                                arms.put(events.get(e) + ":" + "arm " + iCount + "- " + arm_title, affected_temp + "/" + at_risk_temp);
                                                                at_risk = subjects_at_risk_size + at_risk;
                                                                affected = subjects_affected_size + affected;
                                                                event_arm.put(arm_title+events.get(e)+"table",events.get(e));
                                                                arm_iCount.put(arm_title+events.get(e)+"table","arm"+iCount);
                                                                arm_affected_risk.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp)+"|"+String.valueOf(at_risk_temp));
                                                                arm_affected.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp));
                                                                arm_at_risk.put(arm_title+events.get(e)+"table",String.valueOf(at_risk_temp));
                                                            }
                                                            
                                                            System.out.println("first -String");
                                                            
                                                            
                                                            
                                                            //event category
                                                            allergic_category = arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_");
                                                            
                                                            if (col_nct != nct_id) {
                                                                
                                                                col_nct = nct_id;
                                                                trial_affected_risk.append(col_nct);
                                                                trial_affected_risk.append(",");
                                                                
                                                            }
                                                            
                                                        }
                                                        
                                                        //sub_title is JSONObject
                                                    } else if (event.get("sub_title") instanceof JSONObject) {
                                                        
                                                        
                                                        if (arr_category.getJSONObject(j).has("title")) {
                                                            
                                                            category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                            category.append(",");
                                                            
                                                            category.append(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                            replaceAll("(?i:.*Death.*)", "Death").toString().replace(",", ""));
                                                            category.append(",");
                                                            category.append("\n");
                                                            arr_subtitle.add(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                             replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                             arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                            
                                                            map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                        replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                        .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                            
                                                        }
                                                        
                                                        //allergic specific event affected and at risk count
                                                        String sub_title = event.getJSONObject("sub_title").getString("content");
                                                        if (sub_title.contains("hypersensitivity") || sub_title.contains("Hypersensitivity") || sub_title.contains("HYPERSENSITIVITY")) {
                                                            
                                                            //add each arm
                                                            affected_temp = subjects_affected_size;
                                                            at_risk_temp = subjects_at_risk_size;
                                                            
                                                            //event
                                                            allergic_event = event.getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_").
                                                            replaceAll("(?i:.*Death.*)", "Death");
                                                            
                                                            if (affected_temp > 0 && allergic_event.matches("(?i:hypersensitivity)")) {
                                                                
                                                                arms.put(events.get(e) + ":" + "arm " + iCount + "- " + arm_title, affected_temp + "/" + at_risk_temp);
                                                                at_risk = subjects_at_risk_size + at_risk;
                                                                affected = subjects_affected_size + affected;
                                                                event_arm.put(arm_title+events.get(e)+"table",events.get(e));
                                                                arm_iCount.put(arm_title+events.get(e)+"table","arm"+iCount);
                                                                arm_affected_risk.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp)+"|"+String.valueOf(at_risk_temp));
                                                                arm_affected.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp));
                                                                arm_at_risk.put(arm_title+events.get(e)+"table",String.valueOf(at_risk_temp));
                                                                
                                                            }
                                                            
                                                            System.out.println("Second - JSONObject");
                                                            
                                                            
                                                            
                                                            //event category
                                                            allergic_category = arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_");
                                                            
                                                            if (col_nct != nct_id) {
                                                                
                                                                col_nct = nct_id;
                                                                trial_affected_risk.append(col_nct);
                                                                trial_affected_risk.append(",");
                                                                
                                                            }
                                                        }
                                                        
                                                    }
                                                }
                                                
                                                //}
                                                
                                                
                                                //else if event is JSONArray
                                            } else if (event_list.get("event") instanceof JSONArray)
                                            {
                                                
                                                JSONArray event = event_list.getJSONArray("event");
                                                category.append("\n");
                                                
                                                //traverse event array size
                                                for (int iEvent = 0; iEvent < event.length(); iEvent++)
                                                {
                                                    //**************** to fix NCT00088881 issue: the third group exists but counts only has two groups content
                                                    if (event.getJSONObject(iEvent).get("counts") instanceof JSONArray) {
                                                        
                                                        JSONArray counts = event.getJSONObject(iEvent).getJSONArray("counts");
                                                        
                                                        if (iCount < counts.length()) {
                                                            if (counts.getJSONObject(iCount).has("subjects_affected")) {
                                                                subjects_affected_size = counts.getJSONObject(iCount).getInt("subjects_affected");
                                                            }
                                                            
                                                            if (counts.getJSONObject(iCount).has("subjects_at_risk")) {
                                                                
                                                                subjects_at_risk_size = counts.getJSONObject(iCount).getInt("subjects_at_risk");
                                                            }
                                                            
                                                            
                                                        }
                                                    } else if (event.getJSONObject(iEvent).get("counts") instanceof JSONObject) {
                                                        
                                                        if (event.getJSONObject(iEvent).getJSONObject("counts").has("subjects_affected")) {
                                                            subjects_affected_size = event.getJSONObject(iEvent).getJSONObject("counts").getInt("subjects_affected");
                                                            
                                                        }
                                                        
                                                        if (event.getJSONObject(iEvent).getJSONObject("counts").has("subjects_at_risk")) {
                                                            
                                                            subjects_at_risk_size = event.getJSONObject(iEvent).getJSONObject("counts").getInt("subjects_at_risk");
                                                            
                                                        }
                                                        
                                                    }
                                                    
                                                    //if (subjects_affected_size != 0)
                                                    // {
                                                    
                                                    trial.write(nct_id + "\n");
                                                    
                                                    if (event.getJSONObject(iEvent).has("sub_title")) {
                                                        
                                                        if (event.getJSONObject(iEvent).get("sub_title") instanceof String)
                                                        {
                                                            
                                                            if (arr_category.getJSONObject(j).has("title"))
                                                            {
                                                                
                                                                category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                category.append(",");
                                                                
                                                                category.append(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                                replaceAll("(?i:.*Death.*)", "Death").toString().replace(",", ""));
                                                                category.append("\n");
                                                                arr_subtitle.add(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                                 replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                
                                                                map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                            replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                            .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                                
                                                                
                                                            }
                                                            
                                                            String sub_title = event.getJSONObject(iEvent).getString("sub_title");
                                                            if (sub_title.contains("hypersensitivity") || sub_title.contains("Hypersensitivity") || sub_title.contains("HYPERSENSITIVITY"))
                                                                
                                                            {
                                                                //add each arm
                                                                affected_temp = subjects_affected_size;
                                                                at_risk_temp = subjects_at_risk_size;
                                                                
                                                                //event
                                                                allergic_event = event.getJSONObject(iEvent).getString("sub_title").trim().replace(",", " ").replace(" ", "_").
                                                                replaceAll("(?i:.*Death.*)", "Death");
                                                                
                                                                if (affected_temp > 0 && allergic_event.matches("(?i:hypersensitivity)")) {
                                                                    
                                                                    arms.put(events.get(e) + ":" + "arm " + iCount, arm_title + ": " + affected_temp + "/" + at_risk_temp);
                                                                    at_risk = subjects_at_risk_size + at_risk;
                                                                    affected = subjects_affected_size + affected;
                                                                    event_arm.put(arm_title+events.get(e)+"table",events.get(e));
                                                                    arm_iCount.put(arm_title+events.get(e)+"table","arm"+iCount);
                                                                    arm_affected_risk.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp)+"|"+String.valueOf(at_risk_temp));
                                                                    arm_affected.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp));
                                                                    arm_at_risk.put(arm_title+events.get(e)+"table",String.valueOf(at_risk_temp));
                                                                    System.out.println("Hi, I am in Third array");
                                                                    
                                                                }
                                                                
                                                                
                                                                System.out.println("Third - Array String");
                                                                
                                                                
                                                                
                                                                
                                                                //event category
                                                                allergic_category = arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_");
                                                                
                                                                
                                                                //remove duplicated trial id
                                                                if (col_nct != nct_id) {
                                                                    
                                                                    col_nct = nct_id;
                                                                    trial_affected_risk.append(col_nct);
                                                                    trial_affected_risk.append(",");
                                                                    
                                                                }
                                                                
                                                            }
                                                            
                                                            
                                                        } else if (event.getJSONObject(iEvent).get("sub_title") instanceof JSONObject) {
                                                            
                                                            
                                                            if (arr_category.getJSONObject(j).has("title")) {
                                                                
                                                                category.append(arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                category.append(",");
                                                                
                                                                category.append(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                                .replaceAll("(?i:.*Death.*)", "Death").toString().replace(",", ""));
                                                                category.append(",");
                                                                category.append("\n");
                                                                
                                                                arr_subtitle.add(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                                 .replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));
                                                                
                                                                map_cat.put(arr_category.getJSONObject(j).getString("title"), event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                            .replaceAll("(?i:.*Death.*)", "Death").replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                                                            .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT"));
                                                                
                                                                
                                                            }
                                                            
                                                            String sub_title = event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content");
                                                            
                                                            if (sub_title.contains("hypersensitivity") || sub_title.contains("Hypersensitivity") || sub_title.contains("HYPERSENSITIVITY"))
                                                            {
                                                                
                                                                //add each arm
                                                                affected_temp = subjects_affected_size;
                                                                at_risk_temp = subjects_at_risk_size;
                                                                
                                                                //event
                                                                allergic_event = event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_")
                                                                .replaceAll("(?i:.*Death.*)", "Death");
                                                                
                                                                if (affected_temp > 0 && allergic_event.matches("(?i:hypersensitivity)")) {
                                                                    
                                                                    arms.put(events.get(e) + ":" + "arm " + iCount + "- " + arm_title, affected_temp + "/" + at_risk_temp);
                                                                    at_risk = subjects_at_risk_size + at_risk;
                                                                    affected = subjects_affected_size + affected;
                                                                    event_arm.put(arm_title+events.get(e)+"table",events.get(e));
                                                                    arm_iCount.put(arm_title+events.get(e)+"table","arm"+iCount);
                                                                    arm_affected_risk.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp)+"|"+String.valueOf(at_risk_temp));
                                                                    arm_affected.put(arm_title+events.get(e)+"table",String.valueOf(affected_temp));
                                                                    arm_at_risk.put(arm_title+events.get(e)+"table",String.valueOf(at_risk_temp));
                                                                    
                                                                }
                                                                
                                                                System.out.println("Fourth - Array JSONObject");
                                                                
                                                                
                                                                
                                                                //event category
                                                                allergic_category = arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_");
                                                                
                                                                if (col_nct != nct_id) {
                                                                    
                                                                    col_nct = nct_id;
                                                                    trial_affected_risk.append(col_nct);
                                                                    trial_affected_risk.append(",");
                                                                    
                                                                }
                                                                
                                                            }
                                                            
                                                        }
                                                    }
                                                    
                                                    //}
                                                    
                                                }
                                                
                                            }
                                            
                                        }
                                        
                                        
                                        String s = arr_subtitle.toString();
                                        s = s.substring(1, s.length() - 1).replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                        .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT");
                                        
                                        
                                        if (!s.isEmpty())
                                        {
                                            
                                            if (s.contains("hypersensitivity") || s.contains("Hypersensitivity") || s.contains("HYPERSENSITIVITY"))
                                            {
                                                //if(s.matches("(?i:hypersensitivity)"))
                                                // {
                                                trial_allergic.write(nct_id + "\n");
                                                
                                                if (json.optJSONObject("clinical_study").has("condition"))
                                                {
                                                    
                                                    if (nct_temp1 == nct_id) {
                                                        
                                                    } else {
                                                        
                                                        nct_temp1 = nct_id;
                                                        if (json.optJSONObject("clinical_study").get("condition") instanceof String) {
                                                            
                                                            String con1 = json.optJSONObject("clinical_study").get("condition") + "\n";
                                                            String con2 = json.optJSONObject("clinical_study").get("condition").toString();
                                                            
                                                            con_allergic.write(con1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                               .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[", "")
                                                                               .replace("]", ""));
                                                            
                                                            String condition = con2.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                            .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[", "")
                                                            .replace("]", "");
                                                            arr_condition.add(condition);
                                                            
                                                            
                                                        } else if (json.optJSONObject("clinical_study").get("condition") instanceof JSONArray) {
                                                            
                                                            for (int c = 0; c < json.optJSONObject("clinical_study").getJSONArray("condition").length(); c++) {
                                                                
                                                                String con1 = json.optJSONObject("clinical_study").getJSONArray("condition").getString(c) + "\n";
                                                                String con2 = json.optJSONObject("clinical_study").getJSONArray("condition").getString(c);
                                                                
                                                                con_allergic.write(con1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                                   .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[", "")
                                                                                   .replace("]", ""));
                                                                
                                                                String condition = con2.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[", "")
                                                                .replace("]", "");
                                                                arr_condition.add(condition);
                                                                
                                                            }
                                                            
                                                            
                                                        }
                                                        
                                                        
                                                    }
                                                    
                                                    
                                                }
                                                
                                                System.out.println("1");
                                                if (json.optJSONObject("clinical_study").has("intervention"))
                                                {
                                                    if (nct_temp2 == nct_id) {
                                                        
                                                    } else {
                                                        
                                                        nct_temp2 = nct_id;
                                                        if (json.optJSONObject("clinical_study").get("intervention") instanceof JSONObject) {
                                                            
                                                            String inter_name = json.optJSONObject("clinical_study").getJSONObject("intervention").getString("intervention_name") + "\n";
                                                            String inter_name1 = json.optJSONObject("clinical_study").getJSONObject("intervention").getString("intervention_name");
                                                            
                                                            inter_allergic.write(inter_name.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                                 .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT"));
                                                            
                                                            String intervention = inter_name1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                            .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT");
                                                            arr_intervention.add(intervention);
                                                            
                                                        } else if (json.optJSONObject("clinical_study").get("intervention") instanceof JSONArray) {
                                                            
                                                            for (int t = 0; t < json.optJSONObject("clinical_study").getJSONArray("intervention").length(); t++) {
                                                                
                                                                String inter_name = json.optJSONObject("clinical_study").getJSONArray("intervention").getJSONObject(t).getString("intervention_name") + "\n";
                                                                String inter_name1 = json.optJSONObject("clinical_study").getJSONArray("intervention").getJSONObject(t).getString("intervention_name");
                                                                
                                                                inter_allergic.write(inter_name.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                                     .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT"));
                                                                
                                                                String intervention = inter_name1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                                                .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT");
                                                                arr_intervention.add(intervention);
                                                                
                                                                
                                                            }
                                                            
                                                            
                                                        }
                                                        
                                                    }
                                                    
                                                }
                                                
                                            }
                                            
                                            out.write(s + "\n");
                                            
                                        }
                                        
                                        
                                    } //iCount
                                    
                                }
                                
                            }
                            
                        }
                        
                    }
                    
                    trial_affected_risk.append(String.valueOf(affected));
                    trial_affected_risk.append(",");
                    trial_affected_risk.append(String.valueOf(at_risk));
                    trial_affected_risk.append(",");
                    trial_affected_risk.append("\n");
                    
                    
                    
                    Iterator<Map.Entry<String, String>> iterator_event_arm = event_arm.entrySet().iterator();
                    
                    while (iterator_event_arm.hasNext())
                    {
                        
                        Map.Entry<String, String> entry = iterator_event_arm.next();
                        //Key is arm_title
                        String key = entry.getKey().toString();
                        //Value is event type, Serious/Other
                        String value = entry.getValue();
                        
                        //Event
                        allergic_table.append(allergic_event);
                        allergic_table.append(",");
                        //Category
                        allergic_table.append(allergic_category);
                        allergic_table.append(",");
                        //Trial Id
                        allergic_table.append(col_nct);
                        allergic_table.append(",");
                        //Trial URL
                        allergic_table.append(url);
                        allergic_table.append(",");
                        
                        //Condition
                        if(arr_condition.size()>1){
                            
                            for(int i=0; i<arr_condition.size();i++){
                                allergic_table.append(arr_condition.get(i) + "|");
                            }
                            
                            allergic_table.append(",");
                            
                        }else if (arr_condition.size()==1){
                            
                            allergic_table.append(arr_condition.get(0).toString());
                            allergic_table.append(",");
                        } else {
                            
                            allergic_table.append("Null");
                            allergic_table.append(",");
                            
                        }
                        
                        //Intervention
                        if(arr_intervention.size()>1){
                            
                            for(int j=0; j<arr_intervention.size();j++){
                                
                                allergic_table.append(arr_intervention.get(j)+"|");
                            }
                            
                            allergic_table.append(",");
                        }else if(arr_intervention.size()==1) {
                            
                            allergic_table.append(arr_intervention.get(0).toString());
                            allergic_table.append(",");
                        }else {
                            
                            allergic_table.append("Null");
                            allergic_table.append(",");
                            
                        }
                        
                        //Total Affected
                        allergic_table.append(String.valueOf(affected));
                        allergic_table.append(",");
                        //Total At Risk
                        allergic_table.append(String.valueOf(at_risk));
                        allergic_table.append(",");
                        
                        //Serious/Other
                        allergic_table.append(value);
                        allergic_table.append(",");
                        
                        //arm#
                        if(arm_iCount.keySet().contains(key)){
                            
                            allergic_table.append(arm_iCount.get(key));
                            allergic_table.append(",");
                        }
                        
                        //arm title
                        allergic_table.append(key);
                        allergic_table.append(",");
                        
                        //arm-affected/at risk
                        if(arm_affected_risk.keySet().contains(key)){
                            
                            allergic_table.append(arm_affected_risk.get(key).toString());
                            allergic_table.append(",");
                            
                        }
                        
                        if(arm_affected.keySet().contains(key)){
                            
                            allergic_table.append(arm_affected.get(key).toString());
                            allergic_table.append(",");
                            
                        }
                        
                        if(arm_at_risk.keySet().contains(key)){
                            
                            allergic_table.append(arm_at_risk.get(key).toString());
                            allergic_table.append(",");
                        }
                        
                        allergic_table.append("\n");
                    }
                    
                    
                    
                    
                    out.close();
                    con_allergic.close();
                    inter_allergic.close();
                    trial.close();
                    trial_allergic.close();
                    trial_affected_risk.close();
                    category.close();
                    allergic_table.close();
                    
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
        
        Document query1 = new Document("clinical_study.id_info.nct_id","NCT00001962");
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
                                                                                 "clinical_study.required_header.url", "clinical_study.link.url",
                                                                                 "clinical_study.clinical_results.reported_events.serious_events.category_list.category.title",
                                                                                 "clinical_study.intervention.intervention_name", "clinical_study.condition", "clinical_study.intervention.intervention_type", "clinical_study.condition_browse.mesh_term"
                                                                                 ), excludeId())).iterator();
        
        inputQuery(cursor);
        
    }
}
























































































































































































































