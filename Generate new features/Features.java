import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
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

/**
 * Created by Na on 1/10/16.
 * Following columns are generated : trial Id, phase, intervention, condition, high confidence event, death category, death(Y/N)
 */

public class Features {
    public static void inputQuery(MongoCursor<Document> cursor, List<String> serious_events_list) {
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject clinical_study = json.optJSONObject("clinical_study");
                JSONObject reported_events = clinical_study.optJSONObject("clinical_results").optJSONObject("reported_events");
                
                //generate trial Id
                String nct_id = json.optJSONObject("clinical_study").optJSONObject("id_info").getString("nct_id");
                int participant = 0;
                int mean_age = 0;
                int death_patient_number = 0;
                //record affected participant# on each arm
                int subjects_affected_size = 0;
                
                /********1. Divide Features to 6 parts, Number of participants, Mean Age, Phase, Intervention, Condition, Serious Adverse Events.
                 Combine Number of Participants, Mean Age and Phase as Basic group.
                 * 2. Create String_to_Integer_* and Integer_to_String_* HashMaps.
                 * 3. Convert above HashMaps to Integer_to_Integer HashMap, <integer label: integer value> .
                 * 4. Some Features are duplicated in 3040_features.csv, for example, "angina_unstable" appeared
                 * both as Condition and Serious Adverse Events, thus clarify each group range as below to prevent confusion ********/
                
                // String_to_Integer_*
                //Basic group- participants#, mean age and phase
                HashMap<String, Integer> String_to_Integer_Basic = new HashMap<>();
                for (int i = 0; i < 8; i++) {
                    String_to_Integer_Basic.put(serious_events_list.get(i), 0);
                }
                
                //Intervention Group
                HashMap<String, Integer> String_to_Integer_Intervention = new HashMap<>();
                for (int i = 8; i < 1097; i++) {
                    String_to_Integer_Intervention.put(serious_events_list.get(i), 0);
                }
                
                //Condition Group
                HashMap<String, Integer> String_to_Integer_Condition = new HashMap<>();
                for (int i = 1097; i < 2619; i++) {
                    String_to_Integer_Condition.put(serious_events_list.get(i), 0);
                }
                
                //Serious adverse events group
                HashMap<String, Integer> String_to_Integer_Events = new HashMap<>();
                for (int i = 2619; i < 3040; i++) {
                    String_to_Integer_Events.put(serious_events_list.get(i), 0);
                }
                
                
                //Integer_to_String_*
                HashMap<Integer, String> Integer_to_String_Basic = new HashMap<>();
                for (int j = 0; j < 8; j++) {
                    Integer_to_String_Basic.put(j + 1, serious_events_list.get(j));
                }
                
                HashMap<Integer, String> Integer_to_String_Intervention = new HashMap<>();
                for (int j = 8; j < 1097; j++) {
                    Integer_to_String_Intervention.put(j + 1, serious_events_list.get(j));
                }
                
                HashMap<Integer, String> Integer_to_String_Condition = new HashMap<>();
                for (int j = 1097; j < 2619; j++) {
                    Integer_to_String_Condition.put(j + 1, serious_events_list.get(j));
                }
                
                HashMap<Integer, String> Integer_to_String_Events = new HashMap<>();
                for (int j = 2619; j < 3040; j++) {
                    Integer_to_String_Events.put(j + 1, serious_events_list.get(j));
                }
                
                //combination of above Hash Maps
                HashMap<Integer, Integer> Integer_to_Integer = new HashMap<>();
                
                // Focus on serious events list, similarly you can add "other_events" list too.
                ArrayList<String> events = new ArrayList<>();
                if (!reported_events.isNull("serious_events")) {
                    events.add("serious_events");
                }
                
                try {

                    PrintWriter full_table = new PrintWriter(new FileWriter("/Users/HIA/Na/serious/3040features_full_table.txt", true), true);
                    
                    // *************************** GET PARTICIPANT NUMBER AND MEAN AGE*********************************************************
                    if (clinical_study.optJSONObject("clinical_results").has("baseline")) {
                        JSONArray measure = clinical_study.optJSONObject("clinical_results").getJSONObject("baseline").optJSONObject("measure_list").getJSONArray("measure");
                        
                        for (int i = 0; i < measure.length(); i++) {
                            if (measure.getJSONObject(i).getJSONObject("category_list").get("category") instanceof JSONObject) {
                                JSONObject measurement_list = measure.getJSONObject(i).getJSONObject("category_list").getJSONObject("category")
                                .getJSONObject("measurement_list");
                                
                                if (measurement_list.get("measurement") instanceof JSONArray) {
                                    JSONArray measurement = measurement_list.getJSONArray("measurement");
                                    if (measure.getJSONObject(i).get("title").equals("Number of Participants")) {
                                        participant = measurement.getJSONObject(measurement.length() - 1).getInt("value");
                                    }
                                    
                                    if (measure.getJSONObject(i).get("param").equals("Mean") || measure.getJSONObject(i).get("param").equals("Median")) {
                                        if (measure.getJSONObject(i).get("title").equals("Age")) {
                                            
                                            if (measurement.getJSONObject(measurement.length() - 1).has("value")) {
                                                if (measurement.getJSONObject(measurement.length() - 1).get("value") instanceof Integer) {
                                                    mean_age = measurement.getJSONObject(measurement.length() - 1).getInt("value");
                                                    
                                                } else if (measurement.getJSONObject(measurement.length() - 1).get("value") instanceof Double) {
                                                    mean_age = measurement.getJSONObject(measurement.length() - 1).getInt("value");
                                                }
                                            }
                                        }
                                    }
                                    
                                } else if (measurement_list.get("measurement") instanceof JSONObject) {
                                    JSONObject measurement = measurement_list.getJSONObject("measurement");
                                    
                                    if (measure.getJSONObject(i).get("title").equals("Number of Participants")) {
                                        participant = measurement.getInt("value");
                                    }
                                    
                                    if (measure.getJSONObject(i).get("param").equals("Mean") || measure.getJSONObject(i).get("param").equals("Median")) {
                                        if (measure.getJSONObject(i).get("title").equals("Age")) {
                                            
                                            if (measurement.get("value") instanceof Integer) {
                                                mean_age = measurement.getInt("value");
                                                
                                            } else {
                                                mean_age = measurement.getInt("value");
                                            }
                                        }
                                    }
                                }
                                
                                String_to_Integer_Basic.put("number of participants", participant);
                                String_to_Integer_Basic.put("mean age", mean_age);
                            }
                        }
                    }
                    
                    
                    //******************************** GET PHASE **************************//
                    if (!clinical_study.isNull("phase")) {
                        String phase = clinical_study.getString("phase").toLowerCase().trim();
                        String_to_Integer_Basic.put(phase, 1);
                    }
                    
                    //************************* GET CONDITION *****************************//
                    if (clinical_study.has("condition")) {
                        if (clinical_study.get("condition") instanceof String) {
                            
                            String con1 = clinical_study.getString("condition");
                            String condition_title = con1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                            .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[", "")
                            .replace("]", "");
                            
                            String_to_Integer_Condition.entrySet().stream().filter(entry2 -> condition_title.toLowerCase().equals(entry2.getKey())).
                            forEach(entry2 -> String_to_Integer_Condition.put(entry2.getKey(), 1));
                            
                        } else if (clinical_study.get("condition") instanceof JSONArray) {
                            
                            for (int c = 0; c < clinical_study.getJSONArray("condition").length(); c++) {
                                String con1 = clinical_study.getJSONArray("condition").getString(c);
                                String condition_title = con1.replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                                .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT").replace("[", "")
                                .replace("]", "");
                                
                                String_to_Integer_Condition.entrySet().stream().filter(entry -> condition_title.toLowerCase().equals(entry.getKey())).
                                forEach(entry -> String_to_Integer_Condition.put(entry.getKey(), 1));
                            }
                        }
                    }
                    
                    
                    //******* GET-INTERVENTION *****************************************************//
                    if (clinical_study.has("intervention")) {
                        if (clinical_study.get("intervention") instanceof JSONObject) {
                            String inter_name = clinical_study.getJSONObject("intervention").getString("intervention_name").replace(",", "").replace("{", "").replace("}", "").replace("\"", "")
                            .replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT");
                            
                            String_to_Integer_Intervention.entrySet().stream().filter(entry3 -> inter_name.toLowerCase().equals(entry3.getKey()))
                            .forEach(entry3 -> String_to_Integer_Intervention.put(entry3.getKey(), 1));
                            
                        } else if (clinical_study.get("intervention") instanceof JSONArray) {
                            
                            for (int t = 0; t < clinical_study.getJSONArray("intervention").length(); t++) {
                                String inter_name = clinical_study.getJSONArray("intervention").getJSONObject(t).getString("intervention_name")
                                .replace(",", "").replace("{", "").replace("}", "").replace("\"", "").replace("content:", "").replace(" ", "_").replace(",", " ").replace("*", "").replace("%", "_PERCENT");
                                
                                String_to_Integer_Intervention.entrySet().stream().filter(entry -> inter_name.toLowerCase().equals(entry.getKey())).
                                forEach(entry -> String_to_Integer_Intervention.put(entry.getKey(), 1));
                            }
                        }
                    }
                    
                    //******************** Serious Events Subtitle ********************************
                    
                    for (String event1 : events) {
                        JSONObject category_list = reported_events.optJSONObject(event1).optJSONObject("category_list");
                        
                        //group categories ----- column names in Serious Adverse Events
                        if (!reported_events.isNull("group_list")) {
                            int counts_group;
                            JSONObject group_list = reported_events.optJSONObject("group_list");
                            
                            //if group is JSONArray, below counts will be JSONArray except NCT00401778
                            if (group_list.get("group") instanceof JSONArray) {
                                
                                JSONArray arr_group = group_list.optJSONArray("group");
                                counts_group = arr_group.length();
                                
                            } else {
                                counts_group = 1;
                            }
                            
                            if (category_list.getJSONArray("category") != null) {
                                
                                //arr_category is serious category JSONArray
                                JSONArray arr_category = category_list.getJSONArray("category");
                                
                                //iCount is the column index of serious adverse events table
                                for (int iCount = 0; iCount < counts_group; iCount++) {
                                    
                                    // j is the row index of serious adverse events table,neglect the total result (j=0)
                                    for (int j = 1; j < arr_category.length(); j++) {
                                        JSONObject event_list = arr_category.getJSONObject(j).getJSONObject("event_list");
                                        
                                        //if event is JSONObject
                                        if (event_list.get("event") instanceof JSONObject) {
                                            //what if counts is not array even group is an array? for example NCT00401778
                                            JSONObject event = event_list.getJSONObject("event");
                                            
                                            if (event.get("counts") instanceof JSONArray) {
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
                                            
                                            if (subjects_affected_size != 0) {
                                                //sub_title is String
                                                if (event.get("sub_title") instanceof String) {
                                                    
                                                    String sub_title = event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "death").toLowerCase();
                                                    
                                                    for (Map.Entry<String, Integer> entry : String_to_Integer_Events.entrySet()) {
                                                        if (entry.getKey().equals(sub_title)) {
                                                            String_to_Integer_Events.put(sub_title, entry.getValue() + subjects_affected_size);
                                                        }
                                                    }
                                                    
                                                    if (sub_title.contains("death")) {
                                                        if (subjects_affected_size > 0) {
                                                            death_patient_number += subjects_affected_size;
                                                        }
                                                    }
                                                    
                                                    //sub_title is JSONObject
                                                } else if (event.get("sub_title") instanceof JSONObject) {
                                                    
                                                    String sub_title = event.getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_").
                                                    replaceAll("(?i:.*Death.*)", "death").toLowerCase();
                                                    
                                                    for (Map.Entry<String, Integer> entry : String_to_Integer_Events.entrySet()) {
                                                        if (entry.getKey().equals(sub_title)) {
                                                            String_to_Integer_Events.put(sub_title, entry.getValue() + subjects_affected_size);
                                                        }
                                                    }
                                                    
                                                    
                                                    if (sub_title.contains("death")) {
                                                        if (subjects_affected_size > 0) {
                                                            death_patient_number += subjects_affected_size;
                                                        }
                                                    }
                                                }
                                            }
                                            
                                            //else if event is JSONArray
                                        } else if (event_list.get("event") instanceof JSONArray) {
                                            
                                            JSONArray event = event_list.getJSONArray("event");
                                            
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
                                                
                                                if (subjects_affected_size != 0) {
                                                    if (event.getJSONObject(iEvent).has("sub_title")) {
                                                        
                                                        if (event.getJSONObject(iEvent).get("sub_title") instanceof String) {
                                                            
                                                            String sub_title = event.getJSONObject(iEvent).getString("sub_title").trim().replace(",", " ").replace(" ", "_").
                                                            replaceAll("(?i:.*Death.*)", "death").toLowerCase();
                                                            
                                                            for (Map.Entry<String, Integer> entry : String_to_Integer_Events.entrySet()) {
                                                                if (entry.getKey().equals(sub_title)) {
                                                                    String_to_Integer_Events.put(sub_title, entry.getValue() + subjects_affected_size);
                                                                }
                                                            }
                                                            
                                                            if (sub_title.contains("death")) {
                                                                if (subjects_affected_size > 0) {
                                                                    death_patient_number += subjects_affected_size;
                                                                }
                                                            }
                                                            
                                                        } else if (event.getJSONObject(iEvent).get("sub_title") instanceof JSONObject) {
                                                            
                                                            String sub_title = event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_")
                                                            .replaceAll("(?i:.*Death.*)", "death").toLowerCase();
                                                            
                                                            for (Map.Entry<String, Integer> entry : String_to_Integer_Events.entrySet()) {
                                                                if (entry.getKey().equals(sub_title)) {
                                                                    String_to_Integer_Events.put(sub_title, entry.getValue() + subjects_affected_size);
                                                                }
                                                            }
                                                            
                                                            if (sub_title.contains("death")) {
                                                                if (subjects_affected_size > 0) {
                                                                    death_patient_number += subjects_affected_size;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                } //end of each arm
                                
                            }
                        }
                    }
                    
                    
                    //convert String_to_Integer to Integer_to_Integer
                    for (Map.Entry<String, Integer> entry1 : String_to_Integer_Basic.entrySet()) {
                        Integer_to_String_Basic.entrySet().stream().filter(entry2 -> entry2.getValue().equals(entry1.getKey())).
                        forEach(entry2 -> Integer_to_Integer.put(entry2.getKey(), entry1.getValue()));
                    }
                    
                    for (Map.Entry<String, Integer> entry1 : String_to_Integer_Intervention.entrySet()) {
                        Integer_to_String_Intervention.entrySet().stream().filter(entry2 -> entry2.getValue().equals(entry1.getKey())).
                        forEach(entry2 -> Integer_to_Integer.put(entry2.getKey(), entry1.getValue()));
                    }
                    
                    for (Map.Entry<String, Integer> entry1 : String_to_Integer_Condition.entrySet()) {
                        Integer_to_String_Condition.entrySet().stream().filter(entry2 -> entry2.getValue().equals(entry1.getKey())).
                        forEach(entry2 -> Integer_to_Integer.put(entry2.getKey(), entry1.getValue()));
                    }
                    
                    for (Map.Entry<String, Integer> entry1 : String_to_Integer_Events.entrySet()) {
                        Integer_to_String_Events.entrySet().stream().filter(entry2 -> entry2.getValue().equals(entry1.getKey())).
                        forEach(entry2 -> Integer_to_Integer.put(entry2.getKey(), entry1.getValue()));
                    }
                    
                    if (death_patient_number > 0) {
                        full_table.append("1");
                        full_table.write(" ");
                        
                    } else {
                        full_table.append("0");
                        full_table.append(" ");
                    }
                    
                    
                    for (Map.Entry<Integer, Integer> entry_final : Integer_to_Integer.entrySet()) {
                        Integer key = entry_final.getKey();
                        Integer value = entry_final.getValue();
                        full_table.append(key + ":" + value);
                        full_table.append(" ");
                    }
                    
                    // full_table.append(nct_id);
                    full_table.append("\n");
                    full_table.close();
                    
                    
                } catch (IOException exp) {
                    exp.printStackTrace();
                }
                
            } //end of each trial
            
            cursor.close();
            
        } catch (JSONException e) {
            e.printStackTrace();
        }
        
    }
    
    
    public static void main(String[] args) {
        
        //connect to mongodb
        MongoClient mclient = new MongoClient("localhost", 27017);
        MongoDatabase db = mclient.getDatabase("clinicaltrial");
        MongoCollection<Document> dbc = db.getCollection("trials");
        System.out.println("MongoDB is connected by this test");
        
        //Initialize query
        Document query = new Document("$and", asList(new Document("clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.sub_title", new Document("$exists", true)),
                                                     new Document("clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.sub_title", new Document("$exists", true))));
        //check specific document
        Document query1 = new Document("clinical_study.id_info.nct_id", "NCT00330928");
        Document query2 = new Document("clinical_study.id_info.nct_id", "NCT00050986");
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
                                                                                 "clinical_study.clinical_results.reported_events.serious_events.category_list.category.title", "clinical_study.phase",
                                                                                 "clinical_study.intervention.intervention_name", "clinical_study.condition", "clinical_study.clinical_results.baseline.measure_list.measure.category_list.category.measurement_list.measurement.value",
                                                                                 "clinical_study.intervention.intervention_type", "clinical_study.clinical_results.baseline.measure_list.measure.param", "clinical_study.clinical_results.baseline.measure_list.measure.title",
                                                                                 "clinical_study.condition_browse.mesh_term"
                                                                                 ), excludeId())).iterator();
        
        List<String> serious_events_list = new ArrayList<>();
        //read excel file to get each event
        String csvFile = "/Users/HIA/Desktop/Final_Paper/3040_features.csv";
        BufferedReader br = null;
        String line;
        String csvSplitBy = ",";
        
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] serious_conf_event = line.split(csvSplitBy);
                //first column
                serious_events_list.add(serious_conf_event[0].toLowerCase());
            }
            
        } catch (IOException ee) {
            ee.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        inputQuery(cursor, serious_events_list);
    }
}












































































































































































































