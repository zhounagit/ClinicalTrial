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

import static com.mongodb.client.model.Projections.*;

/**
 * Created by Na on 1/10/16.
 */
public class Location {
    public static void main(String[] args) {
        
        //connect to mongodb
        MongoClient mclient = new MongoClient("localhost", 27017);
        MongoDatabase db = mclient.getDatabase("clinicaltrial");
        System.out.println("MongoDB is connected by this test");
        
        //DB collection
        MongoCollection dbc = db.getCollection("trials");
        
        //query all documents
        Document query = new Document();
        
        //query specific document
        Document query1 = new Document("clinical_study.id_info.nct_id","NCT01489254");
        
        //query all documents: dbc.find(query); query specific document: dbc.find(query1)
        MongoCursor<Document> cursor = dbc.find(query).projection(fields(include(
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
                                                                                 "clinical_study.condition",
                                                                                 "clinical_study.location_countries.country"
                                                                                 ), excludeId())).iterator();
        
        /**  count all docs number * */
        System.out.println("Number of return documents are: " + dbc.count(query));
        
        try {
            while (cursor.hasNext())
            {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject location_countries = json.optJSONObject("clinical_study").optJSONObject("location_countries");
                JSONObject id_info = json.optJSONObject("clinical_study").optJSONObject("id_info");
                String nct_id = id_info.getString("nct_id");
                
                if (json.optJSONObject("clinical_study").has("condition"))
                {
                    
                    String con = "condition: " + json.optJSONObject("clinical_study").get("condition") + "\n";
                    
                    if (con.contains("Malaria")) {
                        
                        try {
                            
                            PrintWriter out = new PrintWriter(new FileWriter("/Users/HIA/Desktop/paperdata/location.json", true), true);
                            PrintWriter no_out = new PrintWriter(new FileWriter("/Users/HIA/Desktop/paperdata/nolocation.json", true), true);
                            PrintWriter zip_out = new PrintWriter(new FileWriter("/Users/HIA/Desktop/paperdata/filter_by_city_malaria.txt", true), true);
                            
                            
                            if (!json.optJSONObject("clinical_study").isNull("location_countries") || !json.optJSONObject("clinical_study").isNull("location"))
                            {
                                
                                if (json.optJSONObject("clinical_study").get("location") instanceof JSONObject)
                                {
                                    
                                    JSONObject location = json.optJSONObject("clinical_study").optJSONObject("location");
                                    out.write("-nct id: " + nct_id + "\n");
                                    out.write("location_countries: " + location_countries + "\n" + "location: " + location + "\n");
                                    out.write("\n");
                                    
                                    if(!location.optJSONObject("facility").optJSONObject("address").isNull("city"))
                                    {
                                        String s1 = location.optJSONObject("facility").optJSONObject("address").getString("city").replace(" ", "_");
                                        
                                        if(s1.equalsIgnoreCase("Bamako"))
                                        {
                                            
                                            zip_out.write("-nct id: " + nct_id + "\n");
                                            zip_out.write("condition: " +con+"\n");
                                            zip_out.write("city: "+s1+"\n");
                                            
                                            if(!location.optJSONObject("facility").isNull("name")){
                                                
                                                zip_out.write("facility name: "+location.optJSONObject("facility").getString("name").replace(" ","_")+"\n");
                                                zip_out.write("country: " + location.optJSONObject("facility").optJSONObject("address").getString("country")+"\n");
                                                
                                            }
                                            
                                        }
                                        
                                    }
                                    
                                    
                                } else if (json.optJSONObject("clinical_study").get("location") instanceof JSONArray) {
                                    
                                    JSONArray location_arr = json.optJSONObject("clinical_study").optJSONArray("location");
                                    out.write("\n" + "-nct id: " + nct_id + "\n");
                                    out.write("location_countries: " + location_countries + "\n");
                                    
                                    for (int i = 0; i < location_arr.length(); i++) {
                                        
                                        out.write("location " + i + ": " + location_arr.getString(i));
                                        out.write("\n");
                                        if(!location_arr.getJSONObject(i).getJSONObject("facility").getJSONObject("address").isNull("city")) {
                                            
                                            String s2=location_arr.getJSONObject(i).getJSONObject("facility").getJSONObject("address").getString("city").replace(" ","_");
                                            
                                            if(s2.equalsIgnoreCase("Bamako"))
                                            {
                                                
                                                zip_out.write("-nct id: " + nct_id + "\n");
                                                zip_out.write("condition: " +con+"\n");
                                                zip_out.write("city: "+s2+"\n");
                                                
                                                if(! location_arr.getJSONObject(i).getJSONObject("facility").isNull("name")){
                                                    
                                                    zip_out.write("facility name: " + location_arr.getJSONObject(i).getJSONObject("facility").getString("name").replace(" ","_") + "\n");
                                                    zip_out.write("country: " + location_arr.getJSONObject(i).getJSONObject("facility").getJSONObject("address").getString("country")+"\n");
                                                    
                                                }
                                                
                                            }
                                            
                                        }
                                    }
                                    
                                }
                                
                                
                            } else {
                                
                                no_out.write("no location nct_id: " + nct_id + "\n");
                                
                            }
                            
                            out.close();
                            no_out.close();
                            zip_out.close();
                            
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        
                    }
                }
                
                
                
            }
            
            cursor.close();
            
        } catch (JSONException e) {
            e.printStackTrace();
        }
        
    }
}












