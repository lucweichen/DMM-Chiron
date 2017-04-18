/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package chiron.DataBase;

import chiron.ChironUtils;
import chiron.EActivation;
import chiron.EFile;
import chiron.ERelation;
import chiron.EWorkflow;
import chiron.concept.CActivation;
import chiron.concept.CActivity;
import chiron.concept.CRelation;
import chiron.concept.CWorkflow;
import chiron.concept.Operator;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import vs.database.M_DB;
import vs.database.M_Query;

/**
 *
 * @author lucliuji
 */
public class EProvenance_inM {
    
    static public M_DB db = null;
    private static volatile Map<Integer,In_workflow> in_workflow_queue = null;
    private static volatile Map<Integer,In_activity> in_activity_queue = null;
    private static volatile Map<Integer,In_file> in_file_queue = null;
    private static volatile Map<Integer,In_activation> in_activation_queue = null;
    private static volatile Map<String,In_KeySpace> in_KeySpace_queue = null;
    public static CWorkflow workflow = null;
    public static EWorkflow eworkflow = null;
    public static String workflowIDField = "EWKFID";
    
    private class In_workflow{
        int wkfId;String tag;String exeTag;String expDir;String wfDir;boolean writen;
        public In_workflow(){
            writen = false;
        }
    }
    private class In_activity{
        int id;int wkfId;String tag;String status;Date startTime;Date endTime;int cactid;boolean writen;
        public In_activity(){
            writen = false;
        }
    }
    private class In_file{
        int fileID;int activityId;int activationid;boolean template;boolean instrumented;String fileDir;String name;int fileSize;Date fileDate;String fileOper;String fieldName;boolean writen;
        public In_file(){
            writen = false;
        }
    }
    private class In_activation{
        int id;int activityId;int processor;int exitStatus;String commandLine;String workspace;String stdErr;String stdOut;Date startTime;Date endTime;String status;String extractor;boolean constrained;String templateDir;int node;boolean writen;
        public In_activation(){
            writen = false;
        }
    }
    private class In_KeySpace{
        int id;int activityId;String relation_name;int inputFirstKey;int inputLastKey;String type;boolean writen;
        public In_KeySpace(){
            writen = false;
        }
    }
    
//    public EProvenance(){
//        if(EProvenance.in_workflow_queue == null){
//            EProvenance.in_workflow_queue = new HashMap<Integer,In_workflow>();
//        }
//        if(EProvenance.in_activity_queue == null){
//            EProvenance.in_activity_queue = new HashMap<Integer,In_activity>();
//        }
//        if(EProvenance.in_file_queue == null){
//            EProvenance.in_file_queue = new HashMap<Integer,In_file>();
//        }
//        if(EProvenance.in_KeySpace_queue == null){
//            EProvenance.in_KeySpace_queue = new HashMap<String,In_KeySpace>();
//        }
//        if(EProvenance.in_activation_queue == null){
//            EProvenance.in_activation_queue = new HashMap<Integer,In_activation>();
//        }
//        
//    }
//    
//    public void print_get(){
//        p_workflow();
//        p_activity();
//        p_activation();
//        p_file();
//        p_KeySpace();
//    }
//    
//    private void execute(String sql){
////        System.out.println(sql);
//        if( (sql !=null) && (db != null) ){
//            try {
//                M_Query q = db.prepQuery(sql);
//                q.executeUpdate();
//            } catch (SQLException ex) {
//                Logger.getLogger(EProvenance.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
////        
//    }
//    
//    public void p_workflow(){
//        String sql = "";
////        In_workflow in = in_workflow_queue.get(0);
//        for(In_workflow in : in_workflow_queue.values()){
//            if(!in.writen){
//                sql += "insert into eworkflow(ewkfid, tag, tagexec, expdir, wfdir) values(";
//                sql += "'" + in.wkfId + "',";
//                sql += "'" + in.tag + "',";
//                sql += "'" + in.exeTag + "',";
//                sql += "'" + in.expDir + "',";
//                sql += "'" + in.wfDir + "');";
//                in.writen = true;
//            }
//        }
//        execute(sql);
//    }
//    
//    public void p_activity(){
//        List<Integer> keys = new ArrayList<Integer>();
//        String sql = "";
////      In_activity in = in_activity_queue.get(0);
//        for(int i : in_activity_queue.keySet()){
//            In_activity in = in_activity_queue.get(i);
//            if( (!in.writen)&&(in.status.equals("FINISHED"))){
//                sql += "insert into eactivity(actid, wkfid, tag, status, starttime, endtime, cactid) values(";
//                sql += "'" + in.id + "',";
//                sql += "'" + in.wkfId + "',";
//                sql += "'" + in.tag + "',";
//                sql += "'" + in.status + "',";
//                sql += "'" + in.startTime + "',";
//                sql += "'" + in.endTime + "',";
//                sql += "'" + in.cactid + "');";
//                in.writen = true;
//                keys.add(i);
//            }
//        }
//        for(int i:keys){
//            in_activity_queue.remove(i);
//        }
//        execute(sql);
//    }
//    
//    public void p_activation(){
//        String sql = "";
//        List<Integer> keys = new ArrayList<Integer>();
//        for(int i : in_activation_queue.keySet()){
//            In_activation in = in_activation_queue.get(i);
//            if((!in.writen)&&(in.status.equals("FINISHED"))){
//                sql += "insert into eactivation(taskid, actid, processor, exitstatus, commandline, workspace, terr, tout, starttime, endtime, status, extractor, constrained, templatedir, node) values(";
//                sql += "'" + in.id + "', ";
//                sql += "'" + in.activityId + "', ";
//                sql += "'" + in.processor + "', ";
//                sql += "'" + in.exitStatus + "',"; 
//                sql += "'" + in.commandLine + "',"; 
//                sql += "'" + in.workspace + "',";
//                sql += "'" + in.stdErr.replace("'", "").replace("\n", "") + "',";
//                sql += "'" + in.stdOut + "',"; 
//                sql += "'" + in.startTime + "',"; 
//                sql += "'" + in.endTime + "',"; 
//                sql += "'" + in.status + "',"; 
//                sql += "'" + in.extractor + "',"; 
//                if(in.constrained)
//                    sql += "'T',"; 
//                else
//                    sql += "'F',"; 
//                sql += "'" + in.templateDir + "',"; 
//                sql += "'" + in.node + "');";
//                in.writen = true;
//                keys.add(i);
//            }
//        }
//        for(int i:keys){
//            in_activation_queue.remove(i);
//        }
//        execute(sql);
//    }
//    
//    public void p_file(){
//        String sql = "";
//        List<Integer> keys = new ArrayList<Integer>();
//        for(int i : in_file_queue.keySet()){
//            In_file in = in_file_queue.remove(i);
//            if(!in.writen){
//                sql += "insert into efile(fileid, actid, taskid, fdir, fname, fsize, fdata, ftemplate, finstrumented, foper, fieldname) values(";
//                sql += "'" + in.fileID + "', ";
//                sql += "'" + in.activityId + "', ";
//                sql += "'" + in.activationid + "', ";
//                sql += "'" + in.fileDir + "',"; 
//                sql += "'" + in.fieldName + "',"; 
//                sql += "'" + in.fileSize + "',";
//                if(in.fileDate == null){
//                    sql += "NULL,";
//                }
//                else
//                    sql += "'" + in.fileDate + "',";
//                if(in.template)
//                    sql += "'T',"; 
//                else
//                    sql += "'F',"; 
//                if(in.instrumented)
//                    sql += "'T',"; 
//                else
//                    sql += "'F',"; 
//                sql += "'" + in.fileOper + "',"; 
//                sql += "'" + in.fieldName + "');";
//                in.writen = true;
//                keys.add(i);
//            }
//        }
//        for(int i:keys){
//            if(in_activation_queue.get(in_file_queue.get(i).activationid)==null)
//                in_file_queue.remove(i);    
//        }
//        execute(sql);
//    }
//    
//    public void p_KeySpace(){
//        List<String> keys = new ArrayList<String>();
//        String sql = "";
//        for(String i : in_KeySpace_queue.keySet()){
//            In_KeySpace in = in_KeySpace_queue.get(i);
//            if(!in.writen){
//                sql += "insert into ekeyspace(taskid, actid, relationname, iik, fik, relationtype) values(";
//                sql += "'" + in.id + "', ";
//                sql += "'" + in.activityId + "', ";
//                sql += "'" + in.relation_name + "', ";
//                sql += "'" + in.inputFirstKey + "',"; 
//                sql += "'" + in.inputLastKey + "',"; 
//                sql += "'" + in.type + "');";
//                in.writen = true;
//                keys.add(i);
//            }
//        }
//        for(String i: keys){
//            if(in_activity_queue.get(in_KeySpace_queue.get(i).activityId)==null)
//                in_KeySpace_queue.remove(i);
//        }
//        execute(sql);
//    }
//
//    public synchronized void storeWorkflow(M_DB db, EWorkflow workflow) throws SQLException {
//        In_workflow in_workflow = new In_workflow();
//        if(!EProvenance.in_workflow_queue.isEmpty())
//            in_workflow.wkfId = EProvenance.in_workflow_queue.size();
//        else
//            in_workflow.wkfId = 0;
//        workflow.wkfId = in_workflow.wkfId;
//        in_workflow.tag = workflow.tag;
//        in_workflow.exeTag = workflow.exeTag;
//        in_workflow.expDir = workflow.expDir;
//        in_workflow.wfDir = workflow.wfDir;
//        EProvenance.in_workflow_queue.put(in_workflow.wkfId, in_workflow);
//    }
//
//    public synchronized void storeActivity(EActivity act) throws SQLException {
//        if(in_activity_queue == null){
//            new EProvenance();
//        }
//        In_activity in_activity = new In_activity();
//        if(!EProvenance.in_activity_queue.isEmpty()){
//            if(EProvenance.in_activity_queue.containsKey(act.id))
//                in_activity.id = act.id;
//            else
//                in_activity.id = EProvenance.in_activity_queue.size();
//        }else
//            in_activity.id = 0;
//        act.id = in_activity.id;
//        in_activity.wkfId = act.workflow.wkfId;
//        in_activity.tag = act.tag;
//        in_activity.status = act.status.toString();
//        in_activity.startTime = act.startTime;
//        in_activity.endTime = act.endTime;
//        in_activity.cactid = act.cactid;
//        EProvenance.in_activity_queue.put(in_activity.id, in_activity);
//
//    }
//
//    private synchronized void storeFile(EFile file, EActivation activation) throws SQLException {
//        In_file in_file = new In_file();
//        File f = new File(file.getPath());
//        if (f.exists()) {
//            file.fileSize = (int) f.length();
//            file.fileDate = new Date(f.lastModified());
//        }
//        
//        if(!EProvenance.in_file_queue.isEmpty()){
//            if(in_file_queue.containsKey(file.fileID)){
//                in_file.fileID = file.fileID;
////                System.out.println("0 id: " + in_file.fileID +" size:" + EProvenance.in_file_queue.size());
//            }else{
//                in_file.fileID = EProvenance.in_file_queue.size();
////                System.out.println("1 id: " + in_file.fileID +" size:" + EProvenance.in_file_queue.size());
//            }
//        }else{
//            in_file.fileID = 0;
////            System.out.println("2 id: " + in_file.fileID +" size:" + EProvenance.in_file_queue.size());
//        }
//        file.fileID = in_file.fileID;
//        in_file.activityId = activation.activityId;
//        in_file.activationid = activation.id;
//        in_file.template = file.template;
//        in_file.instrumented = file.instrumented;
//        in_file.fileDir = file.getFileDir();
//        in_file.name = file.getFileName();
//        if(file.fileSize != null)
//            in_file.fileSize = file.fileSize;
//        if(file.fileDate != null)
//            in_file.fileDate = file.fileDate;
//        if(file.fileOper != null)
//            in_file.fileOper = file.fileOper.toString();
//        if(file.fieldName != null)
//            in_file.fieldName = file.fieldName;
//        EProvenance.in_file_queue.put(in_file.fileID, in_file);
//
//    }
///*do not support pipeline*/
//    public synchronized void storeActivation(EActivation activation) throws SQLException, IOException {
//        
//        if (activation.pipelinedFrom != null) {
//            //if the activation was pipelined from another one, it needs to retrieve
//            //the generated OK for the last activation
//            String okSelect = "SELECT ok FROM " + activation.pipelinedFrom.outputRelation.name 
//                    + " " + getWhereClause(eworkflow.wkfId, activation.pipelinedFrom.outputRelation.getFirstKey());
//            M_Query okSelection = db.prepQuery(okSelect);
//            ResultSet okrs = okSelection.openQuery();
//            int newKey;
//            if(okrs.next()) {
//                newKey = okrs.getInt("ok");
//            } else {
//                throw new NullPointerException("No output inserted in previous activity, could not pipeline");
//            }
//            activation.inputRelation.resetKey(activation.inputRelation.getFirstKey(), newKey);
//            activation.outputRelation.resetKey(activation.outputRelation.getFirstKey(), newKey);
//            
//            if (!activation.inputRelation.isEmpty()) {
//                storeInputRelation(activation.inputRelation, activation);
//            }
//            
//        }
//        
//        //System.out.println("storeActivation: " + EProvenance.in_activation_queue.values().size() + ";");
//        In_activation in_activation = new In_activation();
//        if (!activation.outputRelation.isEmpty()) {
//            storeOutputRelation(activation.outputRelation, activation.inputRelation.getFirstKey());
//        }
//        
//        if(!EProvenance.in_activation_queue.isEmpty()){
//            if(EProvenance.in_activation_queue.containsKey(activation.id)){
//                in_activation.id = activation.id;
//                //System.out.println("id: " + in_activation.id + " 0 ; size: " + EProvenance.in_activity_queue.values().size());
//            }
//            else{
//                in_activation.id = EProvenance.in_activation_queue.values().size();
//                //System.out.println("id: " + in_activation.id + " 1 ; size: " + EProvenance.in_activity_queue.values().size());
//            }
//        }else{
//            in_activation.id = 0;
//           // System.out.println("id: " + in_activation.id + " 2 ; size: " + EProvenance.in_activity_queue.values().size());
//        }
//        activation.id = in_activation.id;
//        if(activation.activityId != null)
//            in_activation.activityId = activation.activityId;
//        if(activation.processor != null)
//            in_activation.processor = activation.processor;
//        if(activation.exitStatus != null)
//            in_activation.exitStatus = activation.exitStatus;
//        if(activation.commandLine != null)
//            in_activation.commandLine = activation.commandLine;
//        if(activation.workspace != null)
//            in_activation.workspace = activation.workspace;
//        if(activation.stdErr != null)
//            in_activation.stdErr = activation.stdErr;
//        if(activation.stdOut != null)
//            in_activation.stdOut = activation.stdOut;
//        if(activation.startTime != null)
//            in_activation.startTime = activation.startTime;
//        if(activation.endTime != null)
//            in_activation.endTime = activation.endTime;
//        if(activation.status != null)
//            in_activation.status = activation.status.toString();
//        if(activation.extractor != null)
//            in_activation.extractor = activation.extractor;
//        in_activation.constrained = activation.constrained;
//        if(activation.templateDir != null)
//            in_activation.templateDir = activation.templateDir;
//        if(activation.node != null)
//            in_activation.node = activation.node;
//        EProvenance.in_activation_queue.put(in_activation.id, in_activation);
//
//        for (int j = 0; j < activation.files.size(); j++) {
//            EFile file = activation.files.get(j);
//            storeFile(file, activation);
//        }
//
//        storeKeySpace(activation, activation.inputRelation, "INPUT");
//        storeKeySpace(activation, activation.outputRelation, "OUTPUT");
//    }
//
//    /**
//     * Stores the KeySpace of an EActivation. This is important because the
//     * generateActivations() method in CActivity first creates the activations
//     * and stores them in the database. Later, those activations are retrieved
//     * from the database. It is important to know which data each activation
//     * will consume from their input Relation.
//     *
//     * @param activation The activation related to the keyspace
//     * @param relation The relation that the KeySpace refers to
//     * @param type A String saying if the relatioin is and INPUT or an OUTPUT
//     * @throws SQLException
//     */
//    private synchronized void storeKeySpace(EActivation activation, ERelation relation, String type) throws SQLException {
//        In_KeySpace in_keyspace = new In_KeySpace();
//        in_keyspace.id = activation.id;
//        in_keyspace.activityId = activation.activityId;
//        in_keyspace.relation_name = relation.name;
//        if(relation.getFirstKey() != null)
//            in_keyspace.inputFirstKey = relation.getFirstKey();
//        if(relation.getLastKey() != null)
//            in_keyspace.inputLastKey = relation.getLastKey();
//        in_keyspace.type = type;
//        String key = in_keyspace.id+";"+in_keyspace.activityId+";"+in_keyspace.relation_name;
//        EProvenance.in_KeySpace_queue.put(key, in_keyspace);
//    }
//
//    static synchronized protected EActivation loadReadyActivation(M_DB db, EActivity act) throws SQLException {
//        if (EProvenance.in_activation_queue.isEmpty()) {
//            return null;
//        }
//        
//        EActivation activation = null;
//        for (In_activation in_activation : EProvenance.in_activation_queue.values()) {
//            if( ( in_activation.status.equals( EActivity.StatusType.READY.toString() ) ) && ( in_activation.activityId == (int) act.id )){
//                activation = loadActivation(act, in_activation.id);
//                in_activation.status = EActivity.StatusType.RUNNING.toString();
//                EProvenance.in_activation_queue.put(in_activation.id, in_activation);
//                return activation;
//            }
//        }
//        return activation;
//    }
//    
//    static synchronized protected void updateRunningActivations(M_DB db, EActivity act) throws SQLException{
//        for (In_activation in_activation : EProvenance.in_activation_queue.values()) {
//            if( ( in_activation.status.equals( EActivity.StatusType.RUNNING.toString() ) ) && ( in_activation.activityId == (int) act.id )){
//                in_activation.status = EActivity.StatusType.READY.toString();
//                EProvenance.in_activation_queue.put(in_activation.id, in_activation);
//            }
//        }
//    }
//
//    /**
//     * Load an activation of a given EActivity act with a given activationId.
//     *
//     * @param dbs
//     * @param act
//     * @param activationid
//     * @return
//     * @throws SQLException
//     */
//    static synchronized private EActivation loadActivation(EActivity act, int activationid) throws SQLException {
//        
//        EActivation activation = null;
//        
//        for (In_activation in_activation : EProvenance.in_activation_queue.values()) {
//            if( ( in_activation.id == activationid ) && ( in_activation.activityId == (int) act.id )){
//                activation = new EActivation();
//                activation.id = in_activation.id;
//                activation.activityId = act.id;
//                activation.commandLine = in_activation.commandLine;
//                activation.workspace = in_activation.workspace;
//                activation.extractor = in_activation.extractor;
//                activation.constrained = in_activation.constrained;
//                activation.templateDir = in_activation.templateDir;
//                for (In_file in_file : in_file_queue.values()) {
//                    if( (in_file.activityId == act.id) && ( in_file.activationid == activationid ) ){
//                        boolean inst = in_file.instrumented;
//                        EFile newFile = new EFile(inst, in_file.name, EFile.Operation.valueOf(in_file.fileOper));
//                        newFile.template = in_file.template;
//                        newFile.fileID = in_file.fileID;
//                        newFile.setFileDir(in_file.fileDir);
//                        if(in_file.name != null)
//                            newFile.setFileName(in_file.name);
//                        newFile.fileOper = EFile.Operation.valueOf(in_file.fileOper);
//                        newFile.fieldName = in_file.fieldName.toUpperCase();
//                        newFile.fileSize = in_file.fileSize;
//                        activation.files.add(newFile);
//                    }
//                }
//                for(In_KeySpace in_KeySpace : in_KeySpace_queue.values()) {
//                    if( (in_KeySpace.activityId == act.id) && ( in_KeySpace.id == activationid ) ){
//                        if ( in_KeySpace.type.equals("INPUT") ) {
//                            String relationName = in_KeySpace.relation_name;
//                            Integer firstKey = in_KeySpace.inputFirstKey;
//
//                            ResultSet relation = loadParameterSpace(relationName, firstKey);
//                            List<String> fields = retrieveFields(relationName);
//
//                            TreeMap<Integer, String[]> relMap = new TreeMap<Integer, String[]>();
//
//                            while (relation.next()) {
//                                Integer k = new Integer(relation.getInt("ik"));
//                                String[] values = new String[fields.size()];
//                                for (int i = 0; i < fields.size(); i++) {
//                                    String f = fields.get(i);
//                                    Object o = relation.getObject(f);
//                                    if (o.getClass().equals(Double.class)) {
//                                        int decimalPlaces = EProvenance.getDecimalPlaces(f, act.operation.getInputRelations().get(0));
//                                        String d = ChironUtils.formatFloat((Double) o, decimalPlaces);
//                                        values[i] = d;
//                                    } else {
//                                        values[i] = String.valueOf(relation.getObject(f));
//                                    }
//                                }
//                                relMap.put(k, values);
//                            }
//                            String[] fieldArray = new String[fields.size()];
//                            for (int i = 0; i < fields.size(); i++) {
//                                fieldArray[i] = fields.get(i);
//                            }
//                            ERelation inputRelation = new ERelation(relationName, fieldArray, relMap);
//                            activation.inputRelation = inputRelation;
//                            relation.close();
//                        }else{
//                            String relationName = in_KeySpace.relation_name;
//                            ERelation outputRelation = new ERelation(relationName, retrieveFields(relationName));
//                            activation.outputRelation = outputRelation;
//                        }
//                    }
//                }
//            }
//        }
//        
//        activation.activator = CActivation.newInstance(act);
//        return activation;
//    }
//
//    public synchronized static boolean checkIfAllActivationsFinished(M_DB db, EActivity act) throws SQLException {
//        for (In_activation in_activation : EProvenance.in_activation_queue.values()) {
//            if( ( !in_activation.status.equals( EActivity.StatusType.FINISHED.toString() ) ) && ( in_activation.activityId == (int) act.id )){
//                return false;
//            }
//        }
//        return true;
//    }
//
//    /**
//     * This method checks wether a EWorkflow with a given tag and tagexec exists
//     * in the database.
//     *
//     * @param tag The workflow tag in the database/XML
//     * @param tagexec The tagexec value in the database/XML
//     * @return If the workflow exists, returns its wkfId, else returns -1
//     * @throws SQLException
//     */
//    public synchronized static int matchEWorkflow(String tag, String tagexec) throws SQLException {
//        if(EProvenance.in_workflow_queue == null){
//            new EProvenance();
//        }
//        for( In_workflow in_workflow : EProvenance.in_workflow_queue.values()){
//            if( in_workflow.tag.equals(tag) && in_workflow.exeTag.equals(tagexec)){
//                return in_workflow.wkfId;
//            }
//        }
//        return -1;
//    }
//
//    /**
//     * Retrieves the conceptual workflow from the database based on its tag.
//     *
//     * @param db
//     * @param tag
//     * @return
//     * @throws SQLException
//     */
//    public static void matchCWorkflow(CWorkflow wf) throws SQLException {
//        String SQL = "SELECT wkfid FROM cworkflow WHERE tag=?";
//        M_Query q = db.prepQuery(SQL);
//        q.setParString(1, wf.tag);
//        ResultSet rs = q.openQuery();
//        if (rs.next()) {
//            int wkfId = rs.getInt(1);
//            SQL = "SELECT actid, tag, atype, activation, extractor, templatedir FROM cactivity WHERE wkfid=? ORDER BY actid";
//            q = db.prepQuery(SQL);
//            q.setParInt(1, wkfId);
//            ResultSet as = q.openQuery();
//            while (as.next()) {
//                Operator operator = Operator.valueOf(as.getString("atype").trim().toUpperCase());
//                CActivity activity = CActivity.newInstance(operator);
//                activity.id = as.getInt("actid");
//                activity.tag = as.getString("tag");
//                activity.activation = as.getString("activation");
//                activity.extractor = as.getString("extractor");
//                activity.templateDir = as.getString("templatedir");
//
//                String query = "SELECT relid, rtype, rname, dependency FROM crelation WHERE actid=? ORDER BY relid";
//                M_Query qq = db.prepQuery(query);
//                qq.setParInt(1, activity.id);
//                ResultSet relSet = qq.openQuery();
//                while (relSet.next()) {
//                    int relid = relSet.getInt("relid");
//                    String rtype = relSet.getString("rtype");
//                    String rname = relSet.getString("rname");
//                    int dep = relSet.getInt("dependency");
//                    CRelation relation = new CRelation(relid, rname, rtype, wf, dep);
//                    relation.fields = retrieveFields(rname);
//                    wf.relations.put(rname, relation);
//                    if (relation.type == CRelation.Type.INPUT) {
//                        activity.addInput(relation);
//                    } else {
//                        activity.addOutput(relation);
//                    }
//                }
//                wf.activities.put(activity.id, activity);
//            }
//
//        } else {
//            throw new SQLException("There is no Workflow in the database with the tag " + wf.tag + ".");
//        }
//    }
//
//    public synchronized static void matchActivities(M_DB db, EWorkflow workflow) throws SQLException {
//        for(In_activity in_activity : EProvenance.in_activity_queue.values()) {
//            if(in_activity.wkfId == workflow.wkfId){
//                EActivity act = workflow.getActivity(in_activity.tag);
//                if (act != null) {
//                    act.id = in_activity.id;
//                    act.status = EActivity.StatusType.valueOf(in_activity.status);
//                    act.startTime = in_activity.startTime;
//                    act.endTime = in_activity.endTime;
//                    act.numActivations = 0;
//                    for(In_activation in_activation : EProvenance.in_activation_queue.values()){
//                        if(in_activation.activityId == act.id){
//                            act.numActivations++;
//                        }
//                    }
//                }
//            }
//        }
//    }

    /**
     * Insert the data from the input CSV into the database relation. Must also
     * sets the keyspace in the relation class so the activations can be
     * generated properly. Uses COPY
     *
     * @param relation
     */
    static void insertRelationData(CRelation relation, String expdir){
        /**
         * TO DO: For now we are storing the KeySpace as a contiguous space,
         * getting the first and the last inserted IKs and understanding that
         * everything between that space belogs to this executing. For multi
         * tenancy approaches it will not work and needs to be changed.
         */
        try{
            BaseConnection test = (BaseConnection) db.getConn();
            CopyManager copyManager = new CopyManager(test);

            ChironUtils.writeFileWithWorkflowID(expdir, relation.filename, workflowIDField, eworkflow.wkfId);

            FileReader fR = new FileReader(expdir + "Temp_" + relation.filename);
            String sql = "COPY " + relation.name + "(";
            relation.fields = EProvenance.retrieveFields(relation.name);
            sql += workflowIDField;
            for (String field : relation.fields) {
                sql += "," + field;
            }
            sql += ") FROM STDIN WITH CSV DELIMITER AS ';' HEADER";
            copyManager.copyIn(sql, fR);

            ChironUtils.deleteFile("Temp_" + relation.filename, expdir);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * For a given Relation, retrieves its fields from the database.
     *
     * @param relationName
     * @return A List containing the fields of the relation.
     * @throws SQLException
     */
    public static List<String> retrieveFields(String relationName) throws SQLException {
        String q = "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position";

        M_Query query = db.prepQuery(q);
        query.setParString(1, relationName);
        ResultSet r = query.openQuery();
        List result = new ArrayList<String>();
        while (r.next()) {
            String column = r.getString("column_name");
            if (!column.equals("ik") && !column.equals("ok")&& !column.equals("ewkfid")) {
                result.add(column.toUpperCase());
            }

        }
        return result;
    }
    
    public static String getWhereClause(int wkfID) {
        String where = "WHERE ewkfid = " + wkfID;
        return where;
    }
    
    public static String getWhereClause(int wkfID, int ik) {
        String where = "WHERE ewkfid = " + wkfID + " and ik = " + ik;
        return where;
    }

    private static ResultSet loadParameterSpace(String relationName) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(eworkflow.wkfId);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }
    
    public static ResultSet loadParameterSpace(String relationName, int ik) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(eworkflow.wkfId, ik);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    public static ResultSet loadOrderedParameterSpace(String relationName, ArrayList<String> fields) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(eworkflow.wkfId) + getOrderBy(fields);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }
    
    public static String getOrderBy(ArrayList<String> fields){
        String orderBy = "";
        boolean first = true;
        for(String field : fields){
            if(!first){
                orderBy += ", ";
            }else{
                orderBy += " ORDER BY ";
                first = false;
            }
            
            orderBy += field;
        }
        
        return orderBy;
    }
    
    public static ResultSet loadOrderedParameterSpace(String relationName, int ik, ArrayList<String> fields) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(eworkflow.wkfId, ik) + getOrderBy(fields);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    public static int getDecimalPlaces(String fieldName, CRelation inputRelation) throws SQLException {
        int places = 0;
        String sql = "SELECT decimalplaces FROM cfield WHERE relid=? AND fname=?";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, inputRelation.id);
        q.setParString(2, fieldName.toLowerCase());
        ResultSet rs = q.openQuery();
        if (rs.next()) {
            places = rs.getInt("decimalplaces");
        }
        return places;
    }

    public static List<EFile> getFileFields(CRelation inputRelation) throws SQLException {
        String sql = "SELECT fname, fileoperation, instrumented FROM cfield WHERE relid=? AND ftype='file'";

        M_Query q = db.prepQuery(sql);
        q.setParInt(1, inputRelation.id);
        ResultSet rs = q.openQuery();
        List<EFile> fileFields = new ArrayList<EFile>();
        while (rs.next()) {
            String fname = rs.getString("fname");
            String op = rs.getString("fileoperation");
            String instrumented = rs.getString("instrumented");
            boolean ins = false;
            if (instrumented != null && instrumented.equalsIgnoreCase("true")) {
                ins = true;
            }
            if (op == null) {
                op = "MOVE";
            }
            EFile file = new EFile(ins, fname.toUpperCase(), EFile.Operation.valueOf(op));
            fileFields.add(file);
        }
        return fileFields;
    }

    public static void storeOutputRelation(ERelation outputRel, int inputKey) throws SQLException, IOException {
        CopyManager copyManager = new CopyManager((BaseConnection) db.getConn());
        String sql = "COPY " + outputRel.name + "(" + workflowIDField + ", ik";
        for (int i = 0; i < outputRel.fields.length; i++) {
            sql += ", " + outputRel.fields[i];
        }
        sql += ") FROM STDIN WITH CSV DELIMITER AS ';'";
        InputStream is = new ByteArrayInputStream(outputRel.getCSV(inputKey,eworkflow.wkfId).getBytes());
        copyManager.copyIn(sql, is);
    }

    public static void storeInputRelation(ERelation relation, EActivation activation) throws SQLException {
        int ik = relation.getFirstKey();
        String select = "SELECT ik FROM " + relation.name + " " + getWhereClause(eworkflow.wkfId, ik);
        M_Query selection = db.prepQuery(select);
        int ewkfid = eworkflow.wkfId;

        ResultSet rs = selection.openQuery();
        if (!rs.next()) {
            
            String sql = "INSERT INTO " + relation.name + " (ewkfid, ik";
            for (int i = 0; i < relation.fields.length; i++) {
                sql += ", " + relation.fields[i];
            }
            sql += ") VALUES ";
            //Integer newKey = activation.pipelinedFrom.outputRelation.getFirstKey();
            //activation.inputRelation.resetKey(activation.outputRelation.getFirstKey(), newKey);
            for (Integer key : relation.values.keySet()) {
                sql += "(" + ewkfid + ", " + key;

                String[] tuple = relation.getTupleArray(key);

                for (int i = 0; i < tuple.length; i++) {

                    String value = tuple[i];
                    try {
                        Double.parseDouble(value);
                        sql += ", " + value;
                    } catch (NumberFormatException ex) {
                        sql += ", '" + value + "'";
                    }
                }
                sql += ")";
            }

            M_Query q = db.prepQuery(sql);
            q.executeUpdate();
        }
    }

    public static ResultSet loadParameterSpace(CRelation inputRelation) throws SQLException {
        return loadParameterSpace(inputRelation.name);
    }

    public static ResultSet loadOrderedParameterSpace(CRelation inputRelation, ArrayList<String> fields) throws SQLException {
        return loadOrderedParameterSpace(inputRelation.name, fields);
    }

    /**
     * Feeds the input relation of an activity using the output data from
     * previous activity. Thus this method selects the fields of the dependency
     * output and insert it into the input of the activity that now gonna be
     * able to execute.
     *
     * @param dependencyOutput
     * @param input
     * @throws SQLException
     */
    public static void propagateData(CRelation dependencyOutput, CRelation input) throws SQLException {
        String sql = "INSERT INTO " + input.name + " (ewkfid, ik, " + input.getFieldNames() + ")"
                + " SELECT ewkfid, ok AS ik, " + input.getFieldNames() + " FROM " + dependencyOutput.name + " " + getWhereClause(eworkflow.wkfId);
        M_Query q = db.prepQuery(sql);
        q.executeUpdate();
    }

    public static void executeSQLActivation(String SQL, List<CRelation> input, CRelation output) throws SQLException {
        if (checkForSqlInjection(SQL)) {
            throw new SQLException("The clause \"" + SQL + "\" ss suspicious of SQL injection. Activation aborted.");
        } else {
//            for (CRelation inputRelation : input) {
//                SQL = SQL.replace(inputRelation.name, inputRelation.name);
//            }
            int ewkfid = eworkflow.wkfId;
            String newQuery = SQL.replace("%=EWKFID%", String.valueOf(ewkfid));
            newQuery = newQuery.replace("%=KEY%", "IK");
            
            String sortFields = "";
            String[] splits = newQuery.toUpperCase().split("SELECT");
            if(splits.length == 2){
                splits = splits[1].split("FROM");
                if(splits.length == 2){
                    splits = splits[0].split(",");
            
                    String prefix = "";
                    for(String field : splits){
                        if(!sortFields.isEmpty()){
                            prefix = ", ";
                        }
                        
                        String strField = field.trim();
                        String[] tsplit = strField.split("\\.");
                        if(tsplit.length == 2){
                            strField = tsplit[1].trim();
                        }
                        
                        String[] fsplit = strField.split(" AS ");
                        if(fsplit.length == 2){
                            strField = fsplit[1].trim();
                        }
                        
                        sortFields += prefix + strField;
                    }
                }
            }
            
            if(sortFields.isEmpty()){
                sortFields = "ewkfid, ik, " + output.getFieldNames();
            }

            String insertQuery = "INSERT INTO " + output.name + " (" + sortFields + ") " + newQuery;
            M_Query q = db.prepQuery(insertQuery);
            q.executeUpdate();
        }
    }

    /**
     * Checks the sql string to avoid SQL Injections TO DO: Need to be improved
     * for smarter scans to detect injections.
     *
     * @param sql
     * @return
     */
    private static boolean checkForSqlInjection(String sql) {
        if (sql.contains("INSERT") || sql.contains("insert") || sql.contains("drop") || sql.contains("DROP") || sql.contains("ALTER") || sql.contains("alter")) {
            return true;
        } else {
            return false;
        }
    }

    public static ArrayList<String> getTextOperand(int id) throws SQLException {
        ArrayList<String> operands = new ArrayList<String>();
        
        String sql = "SELECT textvalue FROM coperand WHERE actid=?";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, id);
        ResultSet rs = q.openQuery();
        while (rs.next()) {
            operands.add(rs.getString("textvalue"));
        }
        
        return operands;
    }

    public static Double getNumericOperand(int id) throws SQLException {
        String sql = "SELECT numericvalue FROM coperand WHERE actid=?";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, id);
        ResultSet rs = q.openQuery();
        if (rs.next()) {
            return rs.getDouble("numericvalue");
        } else {
            throw new SQLException("There is no numeric-type operand defined for activity with id=" + String.valueOf(id));
        }
    }

}
