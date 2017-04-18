package chiron;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import vs.database.M_DB;
import vs.database.M_Query;

/**
 *
 * @author vitor
 */
public class DataManager {
    
    public static void transferData(M_DB db, String outputRelation, String destinationPath, String Filename, String workflowExecTag) {
        try {
            if (db == null) {
                return;
            }
            
            ArrayList<String> validColumns = getValidColumns(db, outputRelation);
            String temp;
            for(int i=0;i<validColumns.size()/2;i++){
                temp = validColumns.get(i);
                validColumns.set(i, validColumns.get(validColumns.size()-i-1));
                validColumns.set(validColumns.size()-i-1, temp);
            }
            
            String relation = "";
            
            String SQL = "select ";
            boolean first = true;
            for(String col : validColumns){
                if(first){
                    first = false;
                }else{
                    SQL += ",";
                    relation += ";";
                }
                
                SQL += col + " ";
                relation += col;
            }
            
            SQL += "from " + outputRelation + 
                    " as r, eworkflow as w where w.ewkfid=r.ewkfid and w.tagexec=?;";
            
            String tuples = "";
            String check;
            
            M_Query q = db.prepQuery(SQL);
            q.setParString(1, workflowExecTag);
            
            ResultSet rs = q.openQuery();
            while (rs.next()) {
                String tuple = "";
                
                boolean firstCol = true;
                for(String col : validColumns){
                    if(firstCol){
                        firstCol = false;
                    }else{
                        tuple += ";";
                    }
                    
                    check = rs.getString(col);
                    
                    if(check.contains("/")){
                        copyFileUsingStream(new File(check),new File(destinationPath + check.substring(check.lastIndexOf("/"))));
                        check = destinationPath + check.substring(check.lastIndexOf("/"));
                    }
                    
                    tuple += check;
                    
                }
                
                tuples += "\n" + tuple;
            }
            
            String fileContent = relation + tuples;
            writeFile(fileContent, destinationPath + "/" + Filename);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            printArguments();
        }
    }

    private static ArrayList<String> getValidColumns(M_DB db, String outputRelation) throws SQLException {
        String SQL = "select column_name from information_schema.columns where table_name=?;";

        M_Query q = db.prepQuery(SQL);
        q.setParString(1, outputRelation);

        ArrayList<String> validCols = new ArrayList<String>();
        ResultSet rs = q.openQuery();
        while (rs.next()) {
            String colName = rs.getString(1);

            if(!colName.equals("ewkfid") && !colName.equals("ik") && !colName.equals("ok")){
                validCols.add(colName.toUpperCase());
            }
        }

        return validCols;
    }

    private static void writeFile(String fileContent, String destinationFile) {
        try{
            FileWriter fstream = new FileWriter(destinationFile);
            BufferedWriter out = new BufferedWriter(fstream);

            out.write(fileContent);
            out.close();
        }catch(Exception ex){
            ex.printStackTrace();
            printArguments();
        }
    }

    private static void printArguments() {
        System.out.println("\n----------------------------------\nArguments:");
        System.out.println("String database, String server, String port, String username, String password, String outputRelation, String destinationFile, String workflowExecTag");
    }
    
    private static void copyFileUsingStream(File source, File dest) throws IOException {
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
        } finally {
            is.close();
            os.close();
        }
    }
    
}
