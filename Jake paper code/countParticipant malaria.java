import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.util.Vector;

import static com.mongodb.client.model.Projections.*;
import static java.util.Arrays.asList;

/**
 * Created by Na on 1/10/16.
 */
public class countParticipant {
    
    public static void main(String[] args) {
        
        //connect to mongodb
        MongoClient mclient = new MongoClient("localhost", 27017);
        MongoDatabase db = mclient.getDatabase("clinicaltrial");
        System.out.println("MongoDB is connected by this test");
        int count = 0;
        Vector<String> nct_top = new Vector<String>();
        
        //DB collection
        MongoCollection dbc = db.getCollection("trials");
        
        //query all documents
        Document query = new Document();
        
        Document query2 = new Document("$and", asList(new Document("clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.sub_title", new Document("$exists", true)),
                                                      new Document("clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.sub_title", new Document("$exists", true))));
        
        //query specific document
        Document query1 = new Document("clinical_study.id_info.nct_id","NCT00131235");
        
        //query all documents: dbc.find(query); query specific document: dbc.find(query1)
        MongoCursor<Document> cursor = dbc.find(query).projection(fields(include(
                                                                                 "clinical_study.clinical_results.participant_flow.group_list.group.description",
                                                                                 "clinical_study.clinical_results.participant_flow.group_list.group.group_id",
                                                                                 "clinical_study.clinical_results.participant_flow.group_list.group.title",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.drop_withdraw_reason_list.drop_withdraw_reason.participants_list.participants.count",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.drop_withdraw_reason_list.drop_withdraw_reason.participants_list.participants.group_id",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.drop_withdraw_reason_list.drop_withdraw_reason.title",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.milestone_list.milestone.participants_list.participants.content",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.milestone_list.milestone.participants_list.participants.count",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.milestone_list.milestone.participants_list.participants.group_id",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.milestone_list.milestone.title",
                                                                                 "clinical_study.clinical_results.participant_flow.period_list.period.title",
                                                                                 "clinical_study.clinical_results.participant_flow.pre_assignment_details",
                                                                                 "clinical_study.clinical_results.participant_flow.recruitment_details",
                                                                                 "clinical_study.location.contact.email",
                                                                                 "clinical_study.location.contact.last_name",
                                                                                 "clinical_study.location.contact.phone", "clinical_study.id_info.nct_id",
                                                                                 "clinical_study.location.contact.phone_ext",
                                                                                 "clinical_study.location.contact_backup.email",
                                                                                 "clinical_study.location.contact_backup.last_name",
                                                                                 "clinical_study.location.contact_backup.phone",
                                                                                 "clinical_study.location.contact_backup.phone_ext",
                                                                                 "clinical_study.location.facility.address.city",
                                                                                 "clinical_study.location.facility.address.country",
                                                                                 "clinical_study.location.facility.address.state",
                                                                                 "clinical_study.location.facility.address.zip",
                                                                                 "clinical_study.location.facility.latitude",
                                                                                 "clinical_study.location.facility.longitude",
                                                                                 "clinical_study.location.facility.name",
                                                                                 "clinical_study.location.investigator.last_name",
                                                                                 "clinical_study.location.investigator.role",
                                                                                 "clinical_study.location.status",
                                                                                 "clinical_study.location_countries.country",
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
                                                                                 "clinical_study.enrollment",
                                                                                 "clinical_study.enrollment.content",
                                                                                 "clinical_study.id_info.nct_id",
                                                                                 "clinical_study.intervention.intervention_name", "clinical_study.intervention.intervention_type", "clinical_study.condition_browse.mesh_term",
                                                                                 "clinical_study.condition"
                                                                                 ), excludeId())).iterator();
        
        /**  count all docs number * */
        System.out.println("Number of return documents are: " + dbc.count(query));
        
        try {
            while (cursor.hasNext())
            {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject id_info = json.optJSONObject("clinical_study").optJSONObject("id_info");
                int enrollment=0;
                
                if( json.optJSONObject("clinical_study").has("enrollment")){
                    if(json.optJSONObject("clinical_study").get("enrollment") instanceof Integer){
                        
                        enrollment =json.optJSONObject("clinical_study").getInt("enrollment");
                        
                    }else if(json.optJSONObject("clinical_study").get("enrollment") instanceof JSONObject){
                        
                        enrollment = json.optJSONObject("clinical_study").getJSONObject("enrollment").getInt("content");
                    }
                    
                }
                
                String nct_id = id_info.getString("nct_id");
                
                
                if (json.optJSONObject("clinical_study").has("condition"))
                {
                    String con = "condition: " + json.optJSONObject("clinical_study").get("condition") + "\n";
                    
                    if (con.contains("Malaria"))
                    {
                        System.out.println("I find Malaria");
                        
                        if (!json.optJSONObject("clinical_study").isNull("location_countries") || !json.optJSONObject("clinical_study").isNull("location"))
                        {
                            
                            System.out.println("I find location");
                            
                            if (json.optJSONObject("clinical_study").get("location") instanceof JSONObject)
                            {
                                
                                JSONObject location = json.optJSONObject("clinical_study").optJSONObject("location");
                                
                                if (!location.optJSONObject("facility").optJSONObject("address").isNull("city")) {
                                    
                                    String s1 = location.optJSONObject("facility").optJSONObject("address").getString("city").replace(" ", "_");
                                    if (s1.equalsIgnoreCase("Bamako"))
                                    {
                                        
                                        System.out.println("I find the city bamako");
                                        
                                        if (!location.optJSONObject("facility").isNull("name"))
                                        {
                                            
                                            String s3=location.optJSONObject("facility").getString("name").replace(" ", "_");
                                            if(s3.equalsIgnoreCase("Malaria_Research_and_Training_Center") || s3.equalsIgnoreCase("Malaria_Research_and_Training_Centre"))
                                            {
                                                
                                                System.out.println("I find the facility name in Bamako");
                                                
                                                if (json.optJSONObject("clinical_study").has("clinical_results"))
                                                {
                                                    
                                                    System.out.println("trial has clinical result");
                                                    
                                                    if (json.optJSONObject("clinical_study").optJSONObject("clinical_results").has("participant_flow"))
                                                    {
                                                        
                                                        JSONObject participant_flow = json.optJSONObject("clinical_study").optJSONObject("clinical_results").optJSONObject("participant_flow");
                                                        if (participant_flow.optJSONObject("period_list").has("period")) {
                                                            
                                                            if (participant_flow.optJSONObject("period_list").get("period") instanceof JSONObject) {
                                                                
                                                                if (participant_flow.optJSONObject("period_list").optJSONObject("period").has("milestone_list"))
                                                                {
                                                                    JSONObject milestone_list = participant_flow.optJSONObject("period_list").optJSONObject("period").optJSONObject("milestone_list");
                                                                    
                                                                    if (!participant_flow.isNull("group_list")) {
                                                                        
                                                                        if (!milestone_list.isNull("milestone")) {
                                                                            
                                                                            if (milestone_list.get("milestone") instanceof JSONArray) {
                                                                                
                                                                                JSONArray arr_milestone = milestone_list.getJSONArray("milestone");
                                                                                JSONObject participants_list = arr_milestone.getJSONObject(0).getJSONObject("participants_list");
                                                                                
                                                                                if (participants_list.get("participants") instanceof JSONArray)
                                                                                {
                                                                                    
                                                                                    JSONArray arr_participants = participants_list.getJSONArray("participants");
                                                                                    nct_top.add(nct_id);
                                                                                    
                                                                                    for (int j = 0; j < arr_participants.length(); j++)
                                                                                    {
                                                                                        
                                                                                        count = count + arr_participants.getJSONObject(j).getInt("count");
                                                                                        
                                                                                    }
                                                                                    
                                                                                } else if (participants_list.get("participants") instanceof JSONObject) {
                                                                                    
                                                                                    nct_top.add(nct_id);
                                                                                    
                                                                                    
                                                                                    count = count + participants_list.getJSONObject("participants").getInt("count");
                                                                                    
                                                                                }
                                                                                
                                                                            } else if (milestone_list.get("milestone") instanceof JSONObject) {
                                                                                
                                                                                JSONObject obj_milestone = milestone_list.getJSONObject("milestone");
                                                                                JSONObject participants_list = obj_milestone.getJSONObject("participants_list");
                                                                                
                                                                                if (participants_list.get("participants") instanceof JSONArray)
                                                                                {
                                                                                    
                                                                                    JSONArray arr_participants = participants_list.getJSONArray("participants");
                                                                                    nct_top.add(nct_id);
                                                                                    
                                                                                    for (int j = 0; j < arr_participants.length(); j++) {
                                                                                        
                                                                                        count = count + arr_participants.getJSONObject(j).getInt("count");
                                                                                        
                                                                                    }
                                                                                    
                                                                                } else if (participants_list.get("participants") instanceof JSONObject) {
                                                                                    
                                                                                    nct_top.add(nct_id);
                                                                                    
                                                                                    count = count + participants_list.getJSONObject("participants").getInt("count");
                                                                                    
                                                                                }
                                                                                
                                                                            }
                                                                            
                                                                            
                                                                        }
                                                                        
                                                                    }
                                                                    
                                                                }
                                                                
                                                            }
                                                        }
                                                    }
                                                }else {
                                                    
                                                    nct_top.add(nct_id);
                                                    count = count + enrollment;
                                                    
                                                }
                                            }
                                        }
                                    }
                                }
                                
                            } else if (json.optJSONObject("clinical_study").get("location") instanceof JSONArray) {
                                
                                JSONArray location_arr = json.optJSONObject("clinical_study").optJSONArray("location");
                                
                                System.out.println(" I am here in location array");
                                
                                //what if there is more than one San_Antonio, then differentiate it by zip code
                                //NCT01617434
                                if(location_arr.toString().contains("Bamako"))
                                {
                                    
                                    System.out.println("I find bamako2");
                                    
                                    if(location_arr.toString().contains("Malaria Research and Training Center") || location_arr.toString().contains("Malaria_Research_and_Training_Centre"))
                                    {
                                        
                                        System.out.println("I find facility2");
                                        
                                        if (json.optJSONObject("clinical_study").has("clinical_results"))
                                        {
                                            
                                            System.out.println("I find clinical result2");
                                            
                                            if (json.optJSONObject("clinical_study").optJSONObject("clinical_results").has("participant_flow")) {
                                                
                                                
                                                JSONObject participant_flow = json.optJSONObject("clinical_study").optJSONObject("clinical_results").optJSONObject("participant_flow");
                                                if (participant_flow.optJSONObject("period_list").has("period")) {
                                                    
                                                    
                                                    
                                                    if (participant_flow.optJSONObject("period_list").get("period") instanceof JSONObject) {
                                                        
                                                        
                                                        
                                                        if (participant_flow.optJSONObject("period_list").optJSONObject("period").has("milestone_list")) {
                                                            
                                                            
                                                            JSONObject milestone_list = participant_flow.optJSONObject("period_list").optJSONObject("period").optJSONObject("milestone_list");
                                                            
                                                            if (!participant_flow.isNull("group_list")) {
                                                                
                                                                
                                                                
                                                                if (!milestone_list.isNull("milestone")) {
                                                                    
                                                                    System.out.println(" here 1");
                                                                    if (milestone_list.get("milestone") instanceof JSONArray) {
                                                                        
                                                                        System.out.println(" here 2");
                                                                        
                                                                        JSONArray arr_milestone = milestone_list.getJSONArray("milestone");
                                                                        JSONObject participants_list = arr_milestone.getJSONObject(0).getJSONObject("participants_list");
                                                                        
                                                                        if (participants_list.get("participants") instanceof JSONArray) {
                                                                            
                                                                            JSONArray arr_participants = participants_list.getJSONArray("participants");
                                                                            nct_top.add(nct_id);
                                                                            
                                                                            for (int j = 0; j < arr_participants.length(); j++) {
                                                                                
                                                                                count = count + arr_participants.getJSONObject(j).getInt("count");
                                                                                
                                                                            }
                                                                            
                                                                        } else if (participants_list.get("participants") instanceof JSONObject) {
                                                                            nct_top.add(nct_id);
                                                                            
                                                                            count = count + participants_list.getJSONObject("participants").getInt("count");
                                                                            
                                                                        }
                                                                        
                                                                    } else if (milestone_list.get("milestone") instanceof JSONObject) {
                                                                        
                                                                        JSONObject obj_milestone = milestone_list.getJSONObject("milestone");
                                                                        JSONObject participants_list = obj_milestone.getJSONObject("participants_list");
                                                                        
                                                                        if (participants_list.get("participants") instanceof JSONArray)
                                                                        {
                                                                            
                                                                            JSONArray arr_participants = participants_list.getJSONArray("participants");
                                                                            nct_top.add(nct_id);
                                                                            
                                                                            for (int j = 0; j < arr_participants.length(); j++) {
                                                                                
                                                                                count = count + arr_participants.getJSONObject(j).getInt("count");
                                                                                
                                                                            }
                                                                            
                                                                        } else if (participants_list.get("participants") instanceof JSONObject) {
                                                                            
                                                                            nct_top.add(nct_id);
                                                                            count = count + participants_list.getJSONObject("participants").getInt("count");
                                                                            
                                                                        }
                                                                    }
                                                                    
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }else {
                                            
                                            nct_top.add(nct_id);
                                            count = count + enrollment;
                                            
                                        }
                                        
                                    }
                                }
                            }
                        }
                    }
                }
                
                
            }
            
            cursor.close();
            
        } catch (JSONException e) {
            e.printStackTrace();
        }
        
        System.out.println("total count value: " + count);
        
        
        for(int m=0;m<nct_top.size();m++){
           
            System.out.println("nct_id: "+nct_top.get(m));
        }
        
    }
}












