/**
 * Created by Na Zhou on 10/26/16.
 * This program will help to check whether with each single higher confidence event have impact to death, in a way that
 * with event and death in same arm, which can increase death occurrences in death-trials compare to
 * without such event. All data are based on all death trials.
 */
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.math3.stat.inference.TTest;
import org.bson.Document;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
        String event_satisfied = null;

        //Initialize query
        Document query = new Document("$and", asList(new Document("clinical_study.clinical_results.reported_events.serious_events.category_list.category.event_list.event.sub_title", new Document("$exists", true)),
                new Document("clinical_study.clinical_results.reported_events.other_events.category_list.category.event_list.event.sub_title", new Document("$exists", true))));

        Document query1 = new Document("$or",asList(new Document("clinical_study.id_info.nct_id","NCT00036270"),new Document("clinical_study.id_info.nct_id","NCT00089791")));
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


        List<Double> arr_event_with_death_trial = new ArrayList();
        List<Double> arr_no_event_with_death_trial = new ArrayList();


        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                JSONObject json = new JSONObject(doc.toJson());
                JSONObject reported_events = json.optJSONObject("clinical_study").optJSONObject("clinical_results").optJSONObject("reported_events");

                JSONObject id_info = json.optJSONObject("clinical_study").optJSONObject("id_info");
                String nct_id = id_info.getString("nct_id");

                //Following 2 lines are based on trial level
                Double trial_with_event_death_value = 0.0;
                Double trial_no_event_death_value = 0.0;
                int has_event_arm_counter = 0;
                int no_event_arm_counter = 0;

                // add serious or other event to events arraylist
                ArrayList<String> events = new ArrayList<String>();
                if (!reported_events.isNull("serious_events")) {
                    events.add("serious_events");
                }

                //if (!reported_events.isNull("other_events")) {
                //  events.add("other_events");
                // }

                try {

                    PrintWriter with_event_death_arm = new PrintWriter(new FileWriter("/Users/HIA/Na/c1/1113death_incidence_from_death_arm_with_event.csv", true), true);
                    PrintWriter no_event_death_arm = new PrintWriter(new FileWriter("/Users/HIA/Na/c1/1113death_incidence_from_death_arm_non_event.csv", true), true);


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

                                            if(! event.isNull("sub_title"))
                                           {

                                               // System.out.println(nct_id);
                                                if (event.get("sub_title") instanceof String)
                                                {

                                                    if(arr_category.getJSONObject(j).has("title"))
                                                    {

                                                        String sub_title = event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death");

                                                        if (sub_title.contains("Death"))
                                                        {

                                                            //add each arm
                                                            affected_temp = subjects_affected_size;
                                                            at_risk_temp = subjects_at_risk_size;

                                                            if (at_risk_temp > 0)
                                                            {

                                                                // System.out.println("1st - now iCount is: " + iCount);
                                                                float ratio = (float) (affected_temp) / at_risk_temp;
                                                                current_arm_incidence.add(ratio);

                                                            }

                                                            // System.out.println("first -String");

                                                            //subtitle - category
                                                            arr_subtitle.add(event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                    arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));


                                                        }

                                                        if (sub_title.contains(event_var))
                                                        {

                                                            if(subjects_affected_size != 0){

                                                                //subtitle - category
                                                                arr_subtitle.add(event.getString("sub_title").trim().replace(" ", "_").replace(",", "").replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                        arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));

                                                            }

                                                        }

                                                    }


                                                    //sub_title is JSONObject
                                                } else if (event.get("sub_title") instanceof JSONObject) {


                                                    if(arr_category.getJSONObject(j).has("title"))
                                                    {

                                                        String sub_title = event.getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_").
                                                                replaceAll("(?i:.*Death.*)", "Death");

                                                        if (sub_title.contains("Death"))
                                                        {

                                                            //add each arm
                                                            affected_temp = subjects_affected_size;
                                                            at_risk_temp = subjects_at_risk_size;

                                                            arr_subtitle.add(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                    replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                    arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));


                                                            if (at_risk_temp > 0)
                                                            {

                                                                //System.out.println("2nd - now iCount is: " + iCount);
                                                                float ratio = (float) (affected_temp) / at_risk_temp;
                                                                current_arm_incidence.add(ratio);

                                                            }

                                                            //System.out.println("Second -JSONObject");

                                                        }

                                                        if (sub_title.contains(event_var))
                                                        {

                                                            if(subjects_affected_size != 0) {

                                                                arr_subtitle.add(event.getJSONObject("sub_title").getString("content").trim().replace(" ", "_").
                                                                        replaceAll("(?i:.*Death.*)", "Death") + "-" +
                                                                        arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));

                                                            }
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




                                                    if (event.getJSONObject(iEvent).has("sub_title"))
                                                    {

                                                        if (event.getJSONObject(iEvent).get("sub_title") instanceof String) {


                                                            if(arr_category.getJSONObject(j).has("title"))
                                                            {


                                                                String sub_title = event.getJSONObject(iEvent).getString("sub_title").trim().replace(",", " ").replace(" ", "_").
                                                                        replaceAll("(?i:.*Death.*)", "Death");

                                                                if (sub_title.contains("Death"))
                                                                {

                                                                    //add each arm
                                                                    affected_temp = subjects_affected_size;
                                                                    at_risk_temp = subjects_at_risk_size;


                                                                    arr_subtitle.add(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                            replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));


                                                                    if (at_risk_temp > 0)
                                                                    {

                                                                        //System.out.println("3rd - now iCount is: " + iCount);
                                                                        float ratio = (float) (affected_temp) / at_risk_temp;
                                                                        current_arm_incidence.add(ratio);

                                                                    }

                                                                    //System.out.println("Third -JSONArray");
                                                                }

                                                                if (sub_title.contains(event_var))
                                                                {

                                                                    if(subjects_affected_size != 0) {

                                                                        arr_subtitle.add(event.getJSONObject(iEvent).getString("sub_title").trim().replace(" ", "_").
                                                                                replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));


                                                                    }

                                                                }


                                                            }



                                                        } else if (event.getJSONObject(iEvent).get("sub_title") instanceof JSONObject) {

                                                            if(arr_category.getJSONObject(j).has("title"))
                                                            {

                                                                String sub_title = event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(",", " ").replace(" ", "_")
                                                                        .replaceAll("(?i:.*Death.*)", "Death");


                                                                if (sub_title.contains("Death"))
                                                                {

                                                                    //add each arm
                                                                    affected_temp = subjects_affected_size;
                                                                    at_risk_temp = subjects_at_risk_size;


                                                                    arr_subtitle.add(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                            .replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));


                                                                    if (at_risk_temp > 0)
                                                                    {

                                                                        // System.out.println("4th - now iCount is: " + iCount);
                                                                        float ratio = (float) (affected_temp) / at_risk_temp;
                                                                        current_arm_incidence.add(ratio);

                                                                    }

                                                                    //System.out.println("Fourth -JSONObject");
                                                                }

                                                                if(sub_title.contains(event_var))
                                                                {

                                                                    if(subjects_affected_size != 0){

                                                                        arr_subtitle.add(event.getJSONObject(iEvent).getJSONObject("sub_title").getString("content").trim().replace(" ", "_")
                                                                                .replaceAll("(?i:.*Death.*)", "Death") + "-" + arr_category.getJSONObject(j).getString("title").toString().replace(",", "").replace(" ", "_"));

                                                                    }
                                                                }

                                                            }

                                                        }
                                                    }

                                               // }

                                            }


                                        }


                                    }


                                    String s = arr_subtitle.toString();
                                    s = s.substring(1, s.length() - 1).replace(", ", " ").replace("{", "").replace("}", "").replace("\"", "")
                                            .replace("content:", "").replace(",", "").replace("*", "").replace("%", "_PERCENT");


                                    if (!s.isEmpty() && s.contains("Death"))
                                    {

                                        if(s.contains(event_var))
                                        {
                                            Double sum_event = 0.0;
                                            has_event_arm_counter++;

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

                                                //add all death incidence from current arm to trial_with_event_death_value
                                                trial_with_event_death_value += sum_event;

                                            }


                                        }else
                                        {
                                            Double sum_no_event=0.0;
                                            no_event_arm_counter++;

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

                                                //add all death incidence from current arm to trial_no_event_death_value
                                                trial_no_event_death_value += sum_no_event;

                                            }

                                        }

                                        // out.write(s + "\n");

                                    }

                                } //iCount

                            }

                        }

                    }


                    no_event_death_arm.close();
                    with_event_death_arm.close();


                } catch (IOException exp) {
                    exp.printStackTrace();
                }


                double ave_has_event_all_arms_trial = 0.0;
                double ave_no_event_all_arms_trial =0.0;

                if(has_event_arm_counter>0 && no_event_arm_counter>0)
                {
                    ave_has_event_all_arms_trial = trial_with_event_death_value / has_event_arm_counter;
                    arr_event_with_death_trial.add(ave_has_event_all_arms_trial);
                    ave_no_event_all_arms_trial = trial_no_event_death_value / no_event_arm_counter;
                    arr_no_event_with_death_trial.add(ave_no_event_all_arms_trial);

/*                    if(no_event_arm_counter>0)
                    {
                        ave_no_event_all_arms_trial = trial_no_event_death_value / no_event_arm_counter;
                        arr_no_event_with_death_trial.add(ave_no_event_all_arms_trial);
                    }*/
/*
                    else{

                        ave_no_event_all_arms_trial =0.0;
                        arr_no_event_with_death_trial.add(ave_no_event_all_arms_trial);
                    }
*/

                    try {
                        PrintWriter ave_has_event_div_arm = new PrintWriter(new FileWriter("/Users/HIA/Na/c1/1113trial_with_event_div_arm.csv", true), true);
                        PrintWriter ave_no_event_div_arm = new PrintWriter(new FileWriter("/Users/HIA/Na/c1/1113trial_no_event_div_arm.csv", true), true);

                        ave_has_event_div_arm.append(nct_id);
                        ave_has_event_div_arm.append(",");
                        ave_has_event_div_arm.append(event_var);
                        ave_has_event_div_arm.append(",");
                        ave_has_event_div_arm.append(String.valueOf(ave_has_event_all_arms_trial));
                        ave_has_event_div_arm.append("\n");

                        ave_no_event_div_arm.append(nct_id);
                        ave_no_event_div_arm.append(",");
                        ave_no_event_div_arm.append(event_var);
                        ave_no_event_div_arm.append(",");
                        ave_no_event_div_arm.append(String.valueOf(ave_no_event_all_arms_trial));
                        ave_no_event_div_arm.append("\n");

                        ave_has_event_div_arm.close();
                        ave_no_event_div_arm.close();

                    }catch (IOException e){
                        e.printStackTrace();
                    }


                }

            }


            cursor.close();

        } catch (JSONException e) {
            e.printStackTrace();
        }


        //Get p value from trial level
        double [] valueA = new double[arr_event_with_death_trial.size()];
        double [] valueB = new double[arr_no_event_with_death_trial.size()];
        System.out.println("size of event: "+arr_event_with_death_trial.size());
        System.out.println("size of no-event: "+arr_no_event_with_death_trial.size());

        double ave_group_1 =0.0, ave_group_2 =0.0;
        double sum_group_1 =0.0, sum_group_2 =0.0;

        for(int i=0;i<arr_event_with_death_trial.size();i++){

            valueA[i] = arr_event_with_death_trial.get(i);
            sum_group_1 +=arr_event_with_death_trial.get(i);
        }

        ave_group_1 = sum_group_1/arr_event_with_death_trial.size();

        for(int j=0;j<arr_no_event_with_death_trial.size();j++){

            valueB[j]=arr_no_event_with_death_trial.get(j);
            sum_group_2 += arr_no_event_with_death_trial.get(j);
        }

        ave_group_2 = sum_group_2 /arr_no_event_with_death_trial.size();

        TTest naTTest = new TTest();
        double naResult = naTTest.pairedTTest(valueA,valueB);


        try{

            PrintWriter pValue_death_incidence_trial = new PrintWriter(new FileWriter("/Users/HIA/Na/c1/1113pValue_death_incidence.csv", true), true);

            pValue_death_incidence_trial.append(event_var);
            pValue_death_incidence_trial.append(",");
            pValue_death_incidence_trial.append(String.valueOf(naResult));
            pValue_death_incidence_trial.append(",");
            pValue_death_incidence_trial.append(String.valueOf(ave_group_1));
            pValue_death_incidence_trial.append(",");
            pValue_death_incidence_trial.append(String.valueOf(ave_group_2));
            pValue_death_incidence_trial.append(",");
            pValue_death_incidence_trial.append(String.valueOf(arr_no_event_with_death_trial.size()));
            pValue_death_incidence_trial.append("\n");

            pValue_death_incidence_trial.close();

        }catch(IOException e){

            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        List <String> incidence_test = new ArrayList<String>();

        //read excel file to array List.
        String csvFile = "/Users/HIA/Desktop/Final_Paper/ssae_confidence_findP.csv";
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

























































































































































































































