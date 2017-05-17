/**
 * Created by Na Zhou on 8/9/16.
 */
import org.apache.poi.hssf.usermodel.*;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;


public class ExcelReader {
    
    public static void main(String[] args) throws Exception {
        
        String filename = "/Users/HIA/Desktop/Intervention/Paper/0821intervention_serious_death_trial.xls";
        FileInputStream fis = null;
        Map<String, ArrayList<String>> incidence = new HashMap<String,ArrayList<String>>();
        
        try {
            
            PrintWriter output = new PrintWriter(new FileWriter("/Users/HIA/Desktop/Intervention/Paper/sorted_intervention_serious_average_incidence_code.csv", true),true);
            fis = new FileInputStream(filename);
            HSSFWorkbook workbook = new HSSFWorkbook(fis);
            HSSFSheet sheet = workbook.getSheetAt(0);
            Iterator rowIter = sheet.rowIterator();
            
            while(rowIter.hasNext())
            {
                
                HSSFRow myRow = (HSSFRow) rowIter.next();
                Iterator cellIter = myRow.cellIterator();
                Vector<String> cellStoreVector = new Vector<String>();
                
                while (cellIter.hasNext())
                {
                    HSSFCell myCell = (HSSFCell) cellIter.next();
                    final HSSFDataFormatter df = new HSSFDataFormatter();
                    String cell_value = df.formatCellValue(myCell);
                    cellStoreVector.addElement(cell_value);
                    
                }
                String firstcolumnValue = null;
                String fifthcolumnValue = null;
                
                int i = 0;
                firstcolumnValue = cellStoreVector.get(i).toString();
                System.out.println("2th column is: "+cellStoreVector.get(i + 2).toString());
                fifthcolumnValue = cellStoreVector.get(i + 5).toString();
                
                
                /********************************************************************************************/
                
                //add duplicate key, then keep key and add value to its list
                if(incidence.containsKey(firstcolumnValue)){
                    
                    ArrayList<String> value_temp = incidence.get(firstcolumnValue);
                    value_temp.add(fifthcolumnValue);
                    incidence.put(firstcolumnValue,value_temp);
                    
                }else {
                    //The first time to add firstcolumnValue
                    ArrayList<String> incidence_value = new ArrayList<String>();
                    incidence_value.add(fifthcolumnValue);
                    incidence.put(firstcolumnValue, incidence_value);
                    
                }
                
            }
            
            //output incidence hashmap to excel
            Iterator<Map.Entry<String, ArrayList<String>>> incidence_iterator = incidence.entrySet().iterator();
            while(incidence_iterator.hasNext())
            {
                
                Map.Entry<String,ArrayList<String>> entry = incidence_iterator.next();
                String key = entry.getKey().toString();
                ArrayList value = entry.getValue();
                
                Double single_value =0.0;
                
                for(int i=0;i<value.size();i++){
                    
                    single_value += Double.parseDouble(entry.getValue().get(i));
                    
                    
                }
                
                output.append(key);
                output.append(",");
                output.append(String.valueOf((single_value/value.size())*100)+"%");
                output.append("\n");
                
                
            }
            
            
            output.close();
            
            
        } catch (IOException e) {
            
            e.printStackTrace();
            
        } finally {
            
            if (fis != null) {
                
                fis.close();
                
            }
            
        }
        
    }
    
}

