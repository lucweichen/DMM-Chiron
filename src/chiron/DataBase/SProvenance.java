package chiron.DataBase;

import Multisite.ConnectionSite;
import Multisite.Site;
import Multisite.VM;
import cache.FileInfoCache;
import chiron.Chiron;
import chiron.ChironUtils;
import chiron.EActivation;
import chiron.EActivity;
import chiron.EConfiguration;
import chiron.EFile;
import chiron.ERelation;
import chiron.EWorkflow;
import chiron.concept.*;
import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import vs.database.M_DB;
import vs.database.M_Query;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.util.PSQLException;

/**
 * Provenance data storage class. Stores and retrieves data from the Chiron's
 * provenance database.
 *
 * @author Vítor, Eduardo, Jonas
 * @since 2010-12-25
 */
public class SProvenance {

    public M_DB db = null;
    public Integer wid;
    private static String workflowIDField = "EWKFID";
//    public static CWorkflow workflow = null;
//    public static EWorkflow eworkflow = null;
//    public static String workflowIDField = "EWKFID";
//    private static double[][] queue = {{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}};
//    private static double weight = 1600.0;

    public SProvenance(M_DB db, int wid) {
        this.db = db;
        this.wid = wid;
        SDBKeepAlive sdba = new SDBKeepAlive(this);
        sdba.start();
    }

    public SProvenance(String server, String port, String nameOfDatabase, String username, String password) {
        String connection = "jdbc:postgresql://" + server + ":" + port + "/" + nameOfDatabase + "?chartset=UTF8";
        db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, username, password, true);
        SDBKeepAlive sdba = new SDBKeepAlive(this);
        sdba.start();
    }

    public static void storeWorkflow(M_DB db, EWorkflow workflow) throws SQLException {
        if (db == null) {
            return;
        }
        String SQL = "select f_workflow(?,?,?,?,?);";

        M_Query q = db.prepQuery(SQL);
        q.setParInt(1, workflow.wkfId);
        q.setParString(2, workflow.tag);
        q.setParString(3, workflow.exeTag);
        q.setParString(4, workflow.expDir);
        q.setParString(5, workflow.wfDir);
        ResultSet rs = q.openQuery();
        if (rs.next()) {
            workflow.wkfId = rs.getInt(1);
        }
        rs.close();
    }

    public int storeActivity(EActivity act) throws SQLException {
        System.out.println("store activity" + act.tag);
        if (db == null) {
            return -1;
        }
        String SQL = "select f_activity(?,?,?,?,?,?,?);";

        M_Query q = db.prepQuery(SQL);
        q.setParInt(1, act.id);
        q.setParInt(2, act.workflow.wkfId);
        q.setParString(3, act.tag);
        q.setParString(4, act.status.toString());
        q.setParDate(5, act.startTime);
        q.setParDate(6, act.endTime);
        q.setParInt(7, act.cactid);
        ResultSet rs = q.openQuery();
        if (rs.next()) {
            act.id = rs.getInt(1);
        }
        System.out.println("end store activity" + act.tag);
        return act.id;
    }

    public int getActivityId(String tag) throws SQLException {
        String sql = "SELECT actid,status FROM eactivity where tag = '" + tag + "'";
        System.out.println(sql);
        try {
            M_Query query = db.prepQuery(sql);
            ResultSet rs = query.openQuery();
            if (rs.next()) {
                if (!rs.getString("status").equals(EActivity.StatusType.RUNNING.name())) {
                    try {
                        Thread.sleep(1000);
                        return getActivityId(tag);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(SProvenance.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    return rs.getInt("actid");
                }
            }
        } catch (PSQLException psqe) {
            psqe.printStackTrace();
            System.out.println("reconnect database get Activity Id right exception");
            EConfiguration.reconnect();
            return getActivityId(tag);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("reconnect database get Activity Id");
            EConfiguration.reconnect();
            return getActivityId(tag);
        }
        return -1;
    }

    public void storeActivation(EActivation activation) throws SQLException, IOException {

        boolean stored = false;

        System.out.println("[activation exec] id: " + activation.id + " step: storeActivation");

        if (db == null) {
            return;
        }
        if (activation.pipelinedFrom != null) {
            //if the activation was pipelined from another one, it needs to retrieve
            //the generated OK for the last activation
            int newKey = 0;
            synchronized (db) {
                String okSelect = "SELECT ok FROM " + activation.pipelinedFrom.outputRelation.name
                        + " " + getWhereClause(wid, activation.pipelinedFrom.outputRelation.getFirstKey());
                M_Query okSelection = db.prepQuery(okSelect);
                ResultSet okrs = okSelection.openQuery();
                if (okrs.next()) {
                    newKey = okrs.getInt("ok");
                } else {
                    throw new NullPointerException("No output inserted in previous activity, could not pipeline");
                }
                okSelection.closeQuery();
            }
            activation.inputRelation.resetKey(activation.inputRelation.getFirstKey(), newKey);
            activation.outputRelation.resetKey(activation.outputRelation.getFirstKey(), newKey);

            if (!activation.inputRelation.isEmpty()) {
                storeInputRelation(activation.inputRelation, activation);
            }

        }
        if (activation.id != null && activation.outputRelation != null && !activation.outputRelation.isEmpty()) {
            storeOutputRelation(activation.outputRelation, activation.inputRelation.getFirstKey());
        }

        if (activation.ofiles != null) {
//            System.err.println("store file num: " + activation.ofiles.size());
            for (EFile ef : activation.ofiles) {
                insertFileSite(ef);
            }
        }

        if (activation.id != null) {

            for (EFile file : activation.files) {
                System.out.println("[debug]: file: " + file.getFileName());
                if (Chiron.cache.equals("cache")) {
                    FileInfoCache.cacheFile(file, activation);
                } else {
                    (new EFileProv(db)).storeFile(file, activation);
                }
            }

            stored = true;

        }
        synchronized (db) {
            String SQL = "select f_activation(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
            M_Query q = db.prepQuery(SQL);
            q.setParInt(1, activation.id);
            q.setParInt(2, activation.activityId);
            q.setParInt(3, activation.processor);
            q.setParInt(4, activation.exitStatus);
            q.setParString(5, activation.commandLine);
            q.setParString(6, activation.workspace);
            q.setParString(7, activation.stdErr);
            q.setParString(8, activation.stdOut);
            q.setParDate(9, activation.startTime);
            q.setParDate(10, activation.endTime);
            q.setParString(11, activation.status.toString());
            q.setParString(12, activation.extractor);
            if (activation.constrained) {
                q.setParString(13, "T");
            } else {
                q.setParString(13, "F");
            }
            q.setParString(14, activation.templateDir);
            q.setParInt(15, activation.node);
            q.setParInt(16, activation.site);
            ResultSet rs = q.openQuery();//meta operation
            if (rs.next()) {
                activation.id = (rs.getInt(1));
            }
            q.closeQuery();
        }

        System.out.println("[debug]" + activation.activityId + " file size: " + activation.files.size());

        if (!stored) {

            for (EFile file : activation.files) {
                System.out.println("[debug]: file: " + file.getFileName());
                if (Chiron.cache.equals("cache")) {
                    FileInfoCache.cacheFile(file, activation);
                } else {
                    (new EFileProv(db)).storeFile(file, activation);
                }
            }

        }

        storeKeySpace(activation, activation.inputRelation, "INPUT");
        storeKeySpace(activation, activation.outputRelation, "OUTPUT");

    }

    public void storeActivationLocal(EActivation activation) throws SQLException, IOException {

        boolean stored = false;

        System.out.println("[activation exec] id: " + activation.id + " step: storeActivation");

        if (db == null) {
            return;
        }
        if (activation.pipelinedFrom != null) {
                //if the activation was pipelined from another one, it needs to retrieve
            //the generated OK for the last activation
            int newKey = 0;
            synchronized (db) {
                String okSelect = "SELECT ok FROM " + activation.pipelinedFrom.outputRelation.name
                        + " " + getWhereClause(wid, activation.pipelinedFrom.outputRelation.getFirstKey());
                M_Query okSelection = db.prepQuery(okSelect);
                ResultSet okrs = okSelection.openQuery();

                if (okrs.next()) {
                    newKey = okrs.getInt("ok");
                } else {
                    throw new NullPointerException("No output inserted in previous activity, could not pipeline");
                }
                okSelection.closeQuery();
            }
            activation.inputRelation.resetKey(activation.inputRelation.getFirstKey(), newKey);
            activation.outputRelation.resetKey(activation.outputRelation.getFirstKey(), newKey);

            if (!activation.inputRelation.isEmpty()) {
                storeInputRelation(activation.inputRelation, activation);
            }

        }
        if ((activation.outputRelation != null) && (!activation.outputRelation.isEmpty())) {
            storeOutputRelation(activation.outputRelation, activation.inputRelation.getFirstKey());
        }

        if (activation.ofiles != null) {
//            System.err.println("store file num: " + activation.ofiles.size());
            for (EFile ef : activation.ofiles) {
                insertFileSite(ef);
            }
        }

        if (activation.id != null) {

            for (EFile file : activation.files) {
                System.out.println("[debug]: file: " + file.getFileName());
                if (Chiron.cache.equals("cache")) {
                    FileInfoCache.cacheFile(file, activation);
                } else {
                    (new EFileProv(db)).storeFileLocal(file, activation);
                }
            }

            stored = true;

        }
        synchronized (db) {
            String SQL = "select f_activation(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
            M_Query q = db.prepQuery(SQL);
            q.setParInt(1, activation.id);
            q.setParInt(2, activation.activityId);
            q.setParInt(3, activation.processor);
            q.setParInt(4, activation.exitStatus);
            q.setParString(5, activation.commandLine);
            q.setParString(6, activation.workspace);
            q.setParString(7, activation.stdErr);
            q.setParString(8, activation.stdOut);
            q.setParDate(9, activation.startTime);
            q.setParDate(10, activation.endTime);
            q.setParString(11, activation.status.toString());
            q.setParString(12, activation.extractor);
            if (activation.constrained) {
                q.setParString(13, "T");
            } else {
                q.setParString(13, "F");
            }
            q.setParString(14, activation.templateDir);
            q.setParInt(15, activation.node);
            q.setParInt(16, activation.site);
            q.openQuery();//meta operation
            q.closeQuery();
        }
        System.out.println("[debug]" + activation.activityId + " file size: " + activation.files.size());

        if (!stored) {

            for (EFile file : activation.files) {
                System.out.println("[debug]: file: " + file.getFileName());
                if (Chiron.cache.equals("cache")) {
                    FileInfoCache.cacheFile(file, activation);
                } else {
                    (new EFileProv(db)).storeFileLocal(file, activation);
                }
            }

        }

        storeKeySpace(activation, activation.inputRelation, "INPUT");
        storeKeySpace(activation, activation.outputRelation, "OUTPUT");

    }

    public void createActivation(int taskid, int actid, int site, EActivity.StatusType status) throws SQLException, IOException {
        synchronized (db) {
            String sqlUpd = "INSERT INTO eactivation (taskid, actid, site, status)\n"
                    + "    VALUES ('" + taskid + "', '" + actid + "', '" + site + "', '" + status.toString() + "')";
            System.out.println("[debug] create activation: " + sqlUpd);
            M_Query q = db.prepQuery(sqlUpd);
            q.executeUpdate();
            q.closeQuery();
        }
    }

    /**
     * Stores the KeySpace of an EActivation. This is important because the
     * generateActivations() method in CActivity first creates the activations
     * and stores them in the database. Later, those activations are retrieved
     * from the database. It is important to know which data each activation
     * will consume from their input Relation.
     *
     * @param activation The activation related to the keyspace
     * @param relation The relation that the KeySpace refers to
     * @param type A String saying if the relatioin is and INPUT or an OUTPUT
     * @throws SQLException
     */
    private void storeKeySpace(EActivation activation, ERelation relation, String type) throws SQLException {
        synchronized (db) {
            String sql = "SELECT f_ekeyspace(?,?,?,?,?,?)";
            M_Query q = db.prepQuery(sql);
            q.setParInt(1, activation.id);
            q.setParInt(2, activation.activityId);
            q.setParString(3, relation.name);

            Integer inputFirstKey = relation.getFirstKey();
            if (inputFirstKey != null) {
                q.setParInt(4, inputFirstKey);
            } else {
                q.setParInt(4, null);
            }
            Integer inputLastKey = relation.getLastKey();
            if (inputLastKey != null) {
                q.setParInt(5, inputLastKey);
            } else {
                q.setParInt(5, null);
            }
            q.setParString(6, type);
            q.openQuery();
            q.closeQuery();
        }
    }

    public List<Integer> loadActivationId(int act) throws SQLException {
        List<Integer> actIds = new ArrayList<>();
        if (db == null) {
            return null;
        }

        String sql = "select taskid from eactivation where actid = ?";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, (int) act);
        ResultSet rs = q.openQuery();

        while (rs.next()) {
            actIds.add(rs.getInt("taskid"));
        }
        return actIds;
    }

    public EActivation loadReadyActivation(M_DB db, EActivity act) throws SQLException {
        if (db == null) {
            return null;
        }

        EActivation activation = null;
        synchronized (db) {
            String sql = "select taskid from eactivation where status = ? and actid = ? and site = ? order by taskid limit 1";
            M_Query q = db.prepQuery(sql);
            q.setParString(1, EActivity.StatusType.READY.toString());
            q.setParInt(2, (int) act.id);
            q.setParInt(3, Chiron.site); //site
            ResultSet rs = q.openQuery();//meta operation
            if (rs.next()) {
                int activationid = rs.getInt("taskid");
                activation = loadActivation(act, activationid);
                String sqlUpd = "update eactivation set status = ? where actid = ? and taskid = ? and site = ?";
                M_Query qryUpd = db.prepQuery(sqlUpd);
                qryUpd.setParString(1, EActivity.StatusType.RUNNING.toString());
                qryUpd.setParInt(2, (int) activation.activityId);
                qryUpd.setParInt(3, (int) activation.id);
                qryUpd.setParInt(4, Chiron.site); //site
                qryUpd.executeUpdate();//meta operation
            }
        }
        return activation;
    }

    public void updateRunningActivations(M_DB db, EActivity act) throws SQLException {
        EActivation activation = null;
        String sql = "select taskid from eactivation where status = ? and actid = ? and site = ? order by taskid";
        M_Query q = db.prepQuery(sql);
        q.setParString(1, EActivity.StatusType.RUNNING.toString());
        q.setParInt(2, (int) act.id);
        q.setParInt(3, 0);
        ResultSet rs = q.openQuery();//meta operation

        while (rs.next()) {
            int activationid = rs.getInt("taskid");
            activation = loadActivation(act, activationid);
            String sqlUpd = "update eactivation set status = ? where actid = ? and taskid = ? and site = ?";
            M_Query qryUpd = db.prepQuery(sqlUpd);
            qryUpd.setParString(1, EActivity.StatusType.READY.toString());
            qryUpd.setParInt(2, (int) activation.activityId);
            qryUpd.setParInt(3, (int) activation.id);
            qryUpd.setParInt(4, Chiron.site);
            qryUpd.executeUpdate();//meta operation
        }
    }

//    static public EActivation loadActivationMock(EActivity act, int activationid) throws SQLException {
//        EActivation activation = null;
//        //get data from eactivation table:
//        String sql = "select taskid, commandline, workspace, extractor, constrained, templatedir, machine, site from eactivation where actid = ? and taskid = ?";
//        M_Query q = db.prepQuery(sql);
//        q.setParInt(1, act.id);
//        q.setParInt(2, activationid);
//        ResultSet rs = q.openQuery();
//        if (rs.next()) {
//            //sets the values
//            activation = new EActivation();
//            activation.id = rs.getInt("taskid");
//            activation.activityId = act.id;
//            activation.commandLine = rs.getString("commandline");
//            activation.workspace = rs.getString("workspace");
//            activation.extractor = rs.getString("extractor");
//            if (rs.getString("constrained").equals("T")) {
//                activation.constrained = true;
//            }
//            activation.templateDir = rs.getString("templatedir");
//            activation.node = rs.getInt("machine");
//            activation.site = rs.getInt("site");
//
//            //gets file information:
//            String fsql = "SELECT fileid, ftemplate, finstrumented, fdir, fname, fsize, foper, fieldname, fsite FROM efile WHERE actid = ? and taskid = ?";
//            M_Query fq = db.prepQuery(fsql);
//            fq.setParInt(1, act.id);
//            fq.setParInt(2, activationid);
//            ResultSet frs = fq.openQuery();
//            while (frs.next()) {
//                boolean inst = false;
//                if (frs.getString("finstrumented").equals("T")) {
//                    inst = true;
//                }
//                EFile newFile = new EFile(inst, frs.getString("fname"), EFile.Operation.valueOf(frs.getString("foper")));
//                if (frs.getString("ftemplate").equals("T")) {
//                    newFile.template = true;
//                }
//                newFile.fileID = frs.getInt("fileid");
//                newFile.setFileDir(frs.getString("fdir"));
//                String filename = frs.getString("fname");
//                if (filename != null) {
//                    newFile.setFileName(frs.getString("fname"));
//                }
//                newFile.fileOper = EFile.Operation.valueOf(frs.getString("foper"));
//                newFile.fieldName = frs.getString("fieldname").toUpperCase();
//                newFile.fileSize = frs.getInt("fsize");
//                newFile.fsite = frs.getInt("fsite");
//                activation.files.add(newFile);
//            }
//        }
//        return activation;
//    }
    /**
     * Load an activation of a given EActivity act with a given activationId.
     *
     * @param act
     * @param activationid
     * @return
     * @throws SQLException
     */
    public EActivation loadActivation(EActivity act, int activationid) throws SQLException {
        EActivation activation = null;
        //get data from eactivation table:
        String sql = "select taskid, commandline, workspace, extractor, constrained, templatedir, machine, site from eactivation where actid = ? and taskid = ?";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, act.id);
        q.setParInt(2, activationid);
        ResultSet rs = q.openQuery();//meta operation
        if (rs.next()) {
            //sets the values
            activation = new EActivation();
            activation.id = rs.getInt("taskid");
            activation.activityId = act.id;
            activation.commandLine = rs.getString("commandline");
            activation.workspace = rs.getString("workspace");
            activation.extractor = rs.getString("extractor");
            if (rs.getString("constrained").equals("T")) {
                activation.constrained = true;
            }
            activation.templateDir = rs.getString("templatedir");
            activation.node = rs.getInt("machine");
            activation.site = rs.getInt("site");
            if (Chiron.cache.equals("cache")) {
                activation.files.addAll(FileInfoCache.getFile(activation));
            } else {
                activation.files.addAll((new EFileProv(db)).getTaskFiles(act.id, activationid));
            }
            //gets keyspace data
            String ksql = "SELECT * FROM ekeyspace WHERE actid = ? AND taskid = ?";
            M_Query kq = db.prepQuery(ksql);
            kq.setParInt(1, act.id);
            kq.setParInt(2, activationid);
            ResultSet krs = kq.openQuery();
            while (krs.next()) {
                if (krs.getString("relationtype").equals("INPUT")) {
                    String relationName = krs.getString("relationname");
                    Integer firstKey = new Integer(krs.getInt("iik"));

                    ResultSet relation = loadParameterSpace(relationName, firstKey);
                    List<String> fields = retrieveFields(relationName);

                    TreeMap<Integer, String[]> relMap = new TreeMap<Integer, String[]>();

                    while (relation.next()) {
                        Integer k = new Integer(relation.getInt("ik"));
                        String[] values = new String[fields.size()];
                        for (int i = 0; i < fields.size(); i++) {
                            String f = fields.get(i);
                            Object o = relation.getObject(f);
                            if (o.getClass().equals(Double.class)) {
                                int decimalPlaces = getDecimalPlaces(f, act.operation.getInputRelations().get(0));
                                String d = ChironUtils.formatFloat((Double) o, decimalPlaces);
                                values[i] = d;
                            } else {
                                values[i] = String.valueOf(relation.getObject(f));
                            }
                        }
                        relMap.put(k, values);
                    }
                    String[] fieldArray = new String[fields.size()];
                    for (int i = 0; i < fields.size(); i++) {
                        fieldArray[i] = fields.get(i);
                    }
                    ERelation inputRelation = new ERelation(relationName, fieldArray, relMap);
                    activation.inputRelation = inputRelation;
                    relation.close();
                } else {
                    String relationName = krs.getString("relationname");
                    ERelation outputRelation = new ERelation(relationName, retrieveFields(relationName));
                    activation.outputRelation = outputRelation;
                }
            }
            krs.close();
//            frs.close();
            rs.close();
        }
        activation.activator = CActivation.newInstance(act);
        if (activation.files == null && activation.files.size() != 0) {
            System.out.println("activation id: " + activation.id + " has no file");
        } else {
            System.out.println("begin activation id: " + activation.id + " has files");
            for (EFile ef : activation.files) {
                System.out.println(ef.fileID + "file: " + ef.getFileName() + " fdir: " + ef.getFileDir());
            }
            System.out.println("end activation id: " + activation.id + " has files");
        }
        return activation;
    }

    public EActivation loadActivation(int act, int activationid) throws SQLException {
        EActivation activation = null;
        //get data from eactivation table:
        String sql = "select taskid, commandline, workspace, extractor, constrained, templatedir, machine, site from eactivation where actid = ? and taskid = ?";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, act);
        q.setParInt(2, activationid);
        ResultSet rs = q.openQuery();//meta operation
        if (rs.next()) {
            //sets the values
            activation = new EActivation();
            activation.id = rs.getInt("taskid");
            activation.activityId = act;
            activation.commandLine = rs.getString("commandline");
            activation.workspace = rs.getString("workspace");
            activation.extractor = rs.getString("extractor");
            if (rs.getString("constrained").equals("T")) {
                activation.constrained = true;
            }
            activation.templateDir = rs.getString("templatedir");
            activation.node = rs.getInt("machine");
            activation.site = rs.getInt("site");

            if (Chiron.cache.equals("cache")) {
                activation.files.addAll(FileInfoCache.getFile(activation));
            } else {
                (new EFileProv(db)).getTaskFiles(act, activationid);
            }
        }
        return activation;
    }

    public boolean checkIfAllActivationsFinished(M_DB db, EActivity act, int site) throws SQLException {
        boolean result = false;
        String sql = "SELECT count(*) FROM eactivation WHERE status <> ? AND actid = ? AND site = ?";
        M_Query q = db.prepQuery(sql);
        q.setParString(1, EActivity.StatusType.FINISHED.toString());
        q.setParInt(2, (int) act.id);
        q.setParInt(3, (int) site);
        ResultSet rs = q.openQuery();//meta operation
        if (rs.next()) {
            int counter = rs.getInt(1);
            if (counter == 0) {
                result = true;
            }
            rs.close();
        }
        return result;
    }

    public boolean checkIfActivationsFinished(List<EActivation> acts) throws SQLException {
        boolean result = false;
        String sql = "SELECT count(*) FROM eactivation WHERE status <> '" + EActivity.StatusType.FINISHED.toString() + "'";
        for (int i = 0; i < acts.size(); i++) {
            if (i == 0) {
                sql += " AND (taskid = " + acts.get(i).id;
            } else {
                sql += " OR taskid = " + acts.get(i).id;
            }
        }
        sql += ")";
        M_Query q = db.prepQuery(sql);
        ResultSet rs = q.openQuery();//meta operation
        if (rs.next()) {
            int counter = rs.getInt(1);
            if (counter == 0) {
                result = true;
            }
            rs.close();
        }
        return result;
    }

    /**
     * This method checks wether a EWorkflow with a given tag and tagexec exists
     * in the database.
     *
     * @param tag The workflow tag in the database/XML
     * @param tagexec The tagexec value in the database/XML
     * @return If the workflow exists, returns its wkfId, else returns -1
     * @throws SQLException
     */
    public int matchEWorkflow(String tag, String tagexec) throws SQLException {
        String SQL = "SELECT ewkfid FROM eworkflow WHERE tag=? and tagexec=?";
        M_Query q = db.prepQuery(SQL);
        q.setParString(1, tag);
        q.setParString(2, tagexec);
        ResultSet rs = q.openQuery();
        int wkfId = -1;
        if (rs.next()) {
            wkfId = rs.getInt(1);
        }
        return wkfId;
    }

    /**
     * Retrieves the conceptual workflow from the database based on its tag.
     *
     * @param wf
     * @throws SQLException
     */
    public void matchCWorkflow(CWorkflow wf) throws SQLException {
        String SQL = "SELECT wkfid FROM cworkflow WHERE tag=?";
        M_Query q = db.prepQuery(SQL);
        q.setParString(1, wf.tag);
        ResultSet rs = q.openQuery();
        if (rs.next()) {
            int wkfId = rs.getInt(1);
            SQL = "SELECT actid, tag, atype, activation, extractor, templatedir, workload FROM cactivity WHERE wkfid=? ORDER BY actid";
            q = db.prepQuery(SQL);
            q.setParInt(1, wkfId);
            ResultSet as = q.openQuery();
            while (as.next()) {
                Operator operator = Operator.valueOf(as.getString("atype").trim().toUpperCase());
                CActivity activity = CActivity.newInstance(operator);
                activity.id = as.getInt("actid");
                activity.tag = as.getString("tag");
                activity.activation = as.getString("activation");
                activity.extractor = as.getString("extractor");
                activity.templateDir = as.getString("templatedir");
                if ((!operator.equals(Operator.SR_QUERY)) && (!operator.equals(Operator.MR_QUERY))) {
                    activity.workload = as.getDouble("workload");
                }

                String query = "SELECT relid, rtype, rname, dependency FROM crelation WHERE actid=? ORDER BY relid";
                M_Query qq = db.prepQuery(query);
                qq.setParInt(1, activity.id);
                ResultSet relSet = qq.openQuery();
                while (relSet.next()) {
                    int relid = relSet.getInt("relid");
                    String rtype = relSet.getString("rtype");
                    String rname = relSet.getString("rname");
                    int dep = relSet.getInt("dependency");
                    CRelation relation = new CRelation(relid, rname, rtype, wf, dep);
                    relation.fields = retrieveFields(rname);
                    wf.relations.put(rname, relation);
                    if (relation.type == CRelation.Type.INPUT) {
                        activity.addInput(relation);
                    } else {
                        activity.addOutput(relation);
                    }
                }
                wf.activities.put(activity.id, activity);
            }

        } else {
            throw new SQLException("There is no Workflow in the database with the tag " + wf.tag + ".");
        }
    }

    public void matchActivities(M_DB db, EWorkflow workflow) throws SQLException {
        //Load the workflow's activities:
        String SQL = "select actid, tag, status, starttime, endtime, (select count(*) from eactivation t where eactivity.actid = t.actid) as activations from eactivity where wkfId = ? ";
        M_Query q = db.prepQuery(SQL);
        q.setParInt(1, workflow.wkfId);
        ResultSet actRs = q.openQuery();//meta operation

        while (actRs.next()) {
            int actID = actRs.getInt("actid");
            String tag = actRs.getString("tag");
            String status = actRs.getString("status");
            Date start = actRs.getDate("starttime");
            Date end = actRs.getDate("endtime");
            int numActivations = actRs.getInt("activations");
            EActivity act = workflow.getActivity(tag);
            if (act != null) {
                act.id = actID;
                act.status = EActivity.StatusType.valueOf(status);
                act.startTime = start;
                act.endTime = end;
                act.numActivations = numActivations;
            }
        }
        actRs.close();
    }

    /**
     * Insert the data from the input CSV into the database relation. Must also
     * sets the keyspace in the relation class so the activations can be
     * generated properly. Uses COPY
     *
     * @param relation
     */
    public List<String> insertRelationData(CRelation relation, String expdir) throws SQLException, FileNotFoundException, IOException, InterruptedException {
        /**
         * TO DO: For now we are storing the KeySpace as a contiguous space,
         * getting the first and the last inserted IKs and understanding that
         * everything between that space belogs to this executing. For multi
         * tenancy approaches it will not work and needs to be changed.
         */

        List<String> fields;

        CopyManager copyManager = new CopyManager((BaseConnection) db.getConn());

        ChironUtils.writeFileWithWorkflowID(expdir, relation.filename, workflowIDField, wid);

        FileReader fR = new FileReader(expdir + "Temp_" + relation.filename);
        String sql = "COPY " + relation.name + "(";
        fields = retrieveFields(relation.name);
        sql += workflowIDField;
        for (String field : relation.fields) {
            sql += "," + field;
        }
        sql += ") FROM STDIN WITH CSV DELIMITER AS ';' HEADER";
        copyManager.copyIn(sql, fR);

        ChironUtils.deleteFile("Temp_" + relation.filename, expdir);

        return fields;
    }

    public void insertInputData(CRelation relation, String expdir) throws SQLException, FileNotFoundException, IOException, InterruptedException {
        if (Chiron.cache.equals("cache")) {
            BufferedReader br = null;
            int ifsid, fsize, fsite;
            String fdir, fname;
            EFile file = null;

            try {

                String sCurrentLine;

                br = new BufferedReader(new FileReader(expdir + "ifile_" + relation.filename));
                br.readLine();
                while ((sCurrentLine = br.readLine()) != null) {
                    ifsid = Integer.parseInt(sCurrentLine.substring(0, sCurrentLine.indexOf(";")));
                    sCurrentLine = sCurrentLine.substring(sCurrentLine.indexOf(";") + 1);
                    fdir = sCurrentLine.substring(0, sCurrentLine.indexOf(";"));
                    sCurrentLine = sCurrentLine.substring(sCurrentLine.indexOf(";") + 1);
                    fname = sCurrentLine.substring(0, sCurrentLine.indexOf(";"));
                    sCurrentLine = sCurrentLine.substring(sCurrentLine.indexOf(";") + 1);
                    fsize = Integer.parseInt(sCurrentLine.substring(0, sCurrentLine.indexOf(";")));
                    sCurrentLine = sCurrentLine.substring(sCurrentLine.indexOf(";") + 1);
                    fsite = Integer.parseInt(sCurrentLine);
                    file = new EFile(fdir, fname, fsite);
                    file.fileID = ifsid;
                    file.fileSize = Long.valueOf(fsize);
                    FileInfoCache.cacheIFile(file);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null) {
                        br.close();
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        } else {
            (new EFileProv(db)).insertInputData(relation, expdir);
        }

    }

    public List<String> getfileds(CRelation relation) throws SQLException {
        return retrieveFields(relation.name);
    }

    /**
     * For a given Relation, retrieves its fields from the database.
     *
     * @param relationName
     * @return A List containing the fields of the relation.
     * @throws SQLException
     */
    public List<String> retrieveFields(String relationName) throws SQLException {
        String q = "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position";

        M_Query query = db.prepQuery(q);
        query.setParString(1, relationName);
        ResultSet r = query.openQuery();
        List result = new ArrayList<String>();
        while (r.next()) {
            String column = r.getString("column_name");
            if (!column.equals("ik") && !column.equals("ok") && !column.equals("ewkfid")) {
                result.add(column.toUpperCase());
            }

        }
        return result;
    }

    public static String getWhereClause(int wkfID) {
        String where = "WHERE ewkfid = " + wkfID;
        return where;
    }

    public static String getWhereClause(int wkfID, int from, int to) {
        String where = "WHERE ewkfid = " + wkfID + " and ik >= " + from + " and ik <= " + to;
        return where;
    }

    public static String getWhereClause(int wkfID, int ik) {
        String where = "WHERE ewkfid = " + wkfID + " and ik = " + ik;
        return where;
    }

    private ResultSet loadParameterSpace(String relationName) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(wid);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    private ResultSet loadParameterSpace(String relationName, int tsites, int site) throws SQLException {
        int from = 0, to = 0, total;
        total = loadParameterSpaceNum(relationName);
//        if(site == 0)
//            to = total;
        if (total < tsites) {
            if (site == 0) {
                from = 1;
                to = total;
            }
        } else {
            if (site == 0) {
                from = 1;
                to = total * 10 / 16;
            } else if (site == 1) {
                from = total * 10 / 16 + 1;
                to = total * 15 / 16;
            } else if (site == 2) {
                from = total * 15 / 16 + 1;
                to = total;
            }
        }
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(wid, from, to);
//        System.out.println(sql);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    private int loadParameterSpaceNum(String relationName) throws SQLException {
        String sql = "SELECT count(*) as num FROM " + relationName + " " + getWhereClause(wid);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        if (rs.next()) {
            return rs.getInt("num");
        } else {
            return -1;
        }
    }

    public EFile get_file_para(EFile f) throws SQLException {
        if (Chiron.cache.equals("cache")) {
            return FileInfoCache.getFile(f);
        } else {
            return (new EFileProv(db)).get_file_para(f);
        }
    }

    public ResultSet loadParameterSpace(String relationName, int ik) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(wid, ik);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    public ResultSet loadOrderedParameterSpace(String relationName, ArrayList<String> fields) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(wid) + getOrderBy(fields);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    public ResultSet loadReduceKeys(String relationName, ArrayList<String> fields, int site) throws SQLException {
        int from = 0, to = 0, total;
        total = loadParameterSpaceNum(relationName, fields);
        if (site == 0) {
            to = total;
        }
//        if (total < 3) {
//            if (site == 0) {
//                from = 1;
//                to = total;
//            }
//        } else {
//            if (site == 0) {
//                from = 1;
//                to = total * 10 / 16;
//            } else if (site == 1) {
//                from = total * 10 / 16 + 1;
//                to = total * 15 / 16;
//            } else if (site == 2) {
//                from = total * 15 / 16 + 1;
//                to = total;
//            }
//        }
        String sql = "SELECT * from (SELECT" + getSelect(fields) + ", count(*) as numbers FROM " + relationName + " " + getWhereClause(wid) + getGroupBy(fields) + getOrderBy(fields) + " limit " + to + ") as space " + getOrderBy(fields) + " desc limit " + (to - from + 1);
//        System.out.println(sql);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    public int loadReduceBeginNum(String relationName, ArrayList<String> fields, int site) throws SQLException {
        int from = 0, total;
        total = loadParameterSpaceNum(relationName, fields);
        if (total < 3) {
            if (site == 0) {
                from = 1;
            } else {
                from = -1;
            }
        } else {
            if (site == 0) {
                from = 1;
            } else if (site == 1) {
                from = total * 10 / 16 + 1;
            } else if (site == 2) {
                from = total * 15 / 16 + 1;
            }
        }
        return from;
    }

    public ResultSet loadOrderedParameterSpace(String relationName, ArrayList<String> fields, ArrayList<String> keys) {
        ResultSet rs = null;
        try {
            String sql = "SELECT * FROM " + relationName + " " + getWhereClause(wid, fields, keys);
            M_Query query = db.prepQuery(sql);
            rs = query.openQuery();
        } catch (SQLException ex) {
            Logger.getLogger(SProvenance.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rs;
    }

    public String getWhereClause(int wkfID, ArrayList<String> fields, ArrayList<String> keys) {
        String where = "WHERE ewkfid = " + wkfID;
        for (int i = 0; i < fields.size(); i++) {
            where += " and " + fields.get(i) + " = '" + keys.get(i) + "'";
        }
        return where;
    }

    public int loadParameterSpaceNum(String relationName, ArrayList<String> fields) throws SQLException {
        String sql = "SELECT count(*) as num FROM (select count(*) from " + relationName + " " + getWhereClause(wid) + getGroupBy(fields) + ") as number";
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        if (rs.next()) {
            return rs.getInt("num");
        } else {
            return -1;
        }
    }

    public String getSelect(ArrayList<String> fields) {
        String select = "";
        boolean first = true;
        for (String field : fields) {
            if (!first) {
                select += ", ";
            } else {
                select += " ";
                first = false;
            }

            select += field;
        }

        return select + " ";
    }

    public String getGroupBy(ArrayList<String> fields) {
        String GroupBy = "";
        boolean first = true;
        for (String field : fields) {
            if (!first) {
                GroupBy += ", ";
            } else {
                GroupBy += " GROUP BY ";
                first = false;
            }

            GroupBy += field;
        }

        return GroupBy + " ";
    }

    public String getOrderBy(ArrayList<String> fields) {
        String orderBy = "";
        boolean first = true;
        for (String field : fields) {
            if (!first) {
                orderBy += ", ";
            } else {
                orderBy += " ORDER BY ";
                first = false;
            }

            orderBy += field;
        }

        return orderBy + " ";
    }

    public ResultSet loadOrderedParameterSpace(String relationName, int ik, ArrayList<String> fields) throws SQLException {
        String sql = "SELECT * FROM " + relationName + " " + getWhereClause(wid, ik) + getOrderBy(fields);
        M_Query query = db.prepQuery(sql);
        ResultSet rs = query.openQuery();
        return rs;
    }

    public int getDecimalPlaces(String fieldName, CRelation inputRelation) throws SQLException {
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

    public List<EFile> getFileFields(CRelation inputRelation) throws SQLException {
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

    public java.util.Map isFile(String[] fields, int actid) throws SQLException {
        java.util.Map isfile = new HashMap<>();
        String sql = "select fname, ftype from cfield where relid in \n"
                + "( select relid from crelation where rtype='OUTPUT' and actid in \n"
                + "( select cactid from eactivity where actid = ?\n"
                + ")\n"
                + ")";
        M_Query q = db.prepQuery(sql);
        q.setParInt(1, actid);
        ResultSet rs = q.openQuery();
        while (rs.next()) {
            String fname = rs.getString("fname");
            String ftype = rs.getString("ftype");
            isfile.put(fname, ftype);
        }
        return isfile;
    }

    public void storeOutputRelation(ERelation outputRel, int inputKey) throws SQLException, IOException {
//        CopyManager copyManager = new CopyManager((BaseConnection) db.getConn());
//        String sql = "COPY " + outputRel.name + "(" + workflowIDField + ", ik";
//        for (int i = 0; i < outputRel.fields.length; i++) {
//            sql += ", " + outputRel.fields[i];
//        }
//        sql += ") FROM STDIN WITH CSV DELIMITER AS ';'";
//        InputStream is = new ByteArrayInputStream(outputRel.getCSV(inputKey, wid).getBytes());
//        copyManager.copyIn(sql, is);
        EProvenance.storeOutputRelation(outputRel, inputKey);
    }

    public void storeInputRelation(ERelation relation, EActivation activation) throws SQLException {
        int ik = relation.getFirstKey();
        String select = "SELECT ik FROM " + relation.name + " " + getWhereClause(wid, ik);
        M_Query selection = db.prepQuery(select);
        int ewkfid = wid;

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

    public ResultSet loadParameterSpace(CRelation inputRelation, int tsites, int site) throws SQLException {
        return loadParameterSpace(inputRelation.name, tsites, site);
    }

    public ResultSet loadParameterSpace(CRelation inputRelation) throws SQLException {
        return loadParameterSpace(inputRelation.name);
    }

    public int loadParameterSpaceNum(CRelation inputRelation) throws SQLException {
        return loadParameterSpaceNum(inputRelation.name);
    }

    public ResultSet loadOrderedParameterSpace(CRelation inputRelation, ArrayList<String> fields) throws SQLException {
        return loadOrderedParameterSpace(inputRelation.name, fields);
    }

    public ResultSet loadOrderedParameterSpace(CRelation inputRelation, ArrayList<String> fields, int site) throws SQLException {
        return loadReduceKeys(inputRelation.name, fields, site);
    }

    public int loadBeginNum(CRelation inputRelation, ArrayList<String> fields, int site) throws SQLException {
        return loadReduceBeginNum(inputRelation.name, fields, site);
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
    public void propagateData(CRelation dependencyOutput, CRelation input) throws SQLException {
        String sql = "INSERT INTO " + input.name + " (ewkfid, ik, " + input.getFieldNames() + ")"
                + " SELECT ewkfid, ok AS ik, " + input.getFieldNames() + " FROM " + dependencyOutput.name + " " + getWhereClause(wid);
        M_Query q = db.prepQuery(sql);
        q.executeUpdate();
    }

    public ResultSet getOrelation(CRelation dependencyOutput, CRelation input) throws SQLException {
        String sql = "SELECT ewkfid, ok AS ik, " + input.getFieldNames() + " from " + dependencyOutput.name;
        M_Query selection = db.prepQuery(sql);
        return selection.openQuery();
    }

    public void insertSite(Site s) throws SQLException {
        String sql = "INSERT INTO site (id, region, publicip,cpunumber,provbw) values (" + s.getId() + ",'" + s.getRegion() + "','" + s.getIp()
                + "'," + s.getCPUnumber() + "," + s.getProvBW() + ")";
        M_Query q = db.prepQuery(sql);
        q.executeUpdate();
    }

    public void insertVM(VM vm) throws SQLException {
        String sql = "INSERT INTO vm (id, siteid, mpiid, privateip, cpunumber, type, comcap, price) values (" + vm.id + "," + vm.siteid + "," + vm.mpiid
                + ",'" + vm.privateip + "'," + vm.cpu + ",'" + vm.type + "'," + vm.comcap + "," + vm.price + ")";
        M_Query q = db.prepQuery(sql);
        q.executeUpdate();
    }

    public void insertConnection(ConnectionSite cs) throws SQLException {
        String sql = "INSERT INTO connection (ositeid, oport, tsiteid, tport, databw) values (" + cs.ositeid + ",'" + cs.oport + "'," + cs.tsiteid
                + ",'" + cs.tport + "'," + cs.databw + ")";
        M_Query q = db.prepQuery(sql);
        q.executeUpdate();
    }

    public void insertFileSite(EFile ef) throws SQLException {
        if (Chiron.cache.equals("cache")) {
            FileInfoCache.cacheIFile(ef);
        } else {
            (new EFileProv(db)).insertFileSite(ef);
        }
    }

    public void insertFileSite(String fdir, String fname, int fsize, int fsite) throws SQLException {
        if (Chiron.cache.equals("cache")) {
            EFile file = new EFile(fdir, fname, fsite);
            file.fileSize = Long.valueOf(fsize);
            FileInfoCache.cacheIFile(file);
        } else {
            (new EFileProv(db)).insertFileSite(fdir, fname, fsize, fsite);
        }
    }

    public List<EFile> getFileOSite(String fdir) throws SQLException {
        if (Chiron.cache.equals("cache")) {
            return FileInfoCache.getIOFiles(fdir, Chiron.site);
        } else {
            return (new EFileProv(db)).getFileOSite(fdir);
        }
    }

    public void executeSQLActivation(String SQL, List<CRelation> input, CRelation output) throws SQLException {
        if (checkForSqlInjection(SQL)) {
            throw new SQLException("The clause \"" + SQL + "\" ss suspicious of SQL injection. Activation aborted.");
        } else {
//            for (CRelation inputRelation : input) {
//                SQL = SQL.replace(inputRelation.name, inputRelation.name);
//            }
            int ewkfid = wid;
            String newQuery = SQL.replace("%=EWKFID%", String.valueOf(ewkfid));
            newQuery = newQuery.replace("%=KEY%", "IK");

            String sortFields = "";
            String[] splits = newQuery.toUpperCase().split("SELECT");
            if (splits.length == 2) {
                splits = splits[1].split("FROM");
                if (splits.length == 2) {
                    splits = splits[0].split(",");

                    String prefix = "";
                    for (String field : splits) {
                        if (!sortFields.isEmpty()) {
                            prefix = ", ";
                        }

                        String strField = field.trim();
                        String[] tsplit = strField.split("\\.");
                        if (tsplit.length == 2) {
                            strField = tsplit[1].trim();
                        }

                        String[] fsplit = strField.split(" AS ");
                        if (fsplit.length == 2) {
                            strField = fsplit[1].trim();
                        }

                        sortFields += prefix + strField;
                    }
                }
            }

            if (sortFields.isEmpty()) {
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
    private boolean checkForSqlInjection(String sql) {
        if (sql.contains("INSERT") || sql.contains("insert") || sql.contains("drop") || sql.contains("DROP") || sql.contains("ALTER") || sql.contains("alter")) {
            return true;
        } else {
            return false;
        }
    }

    public ArrayList<String> getTextOperand(int id) throws SQLException {
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

    public Double getNumericOperand(int id) throws SQLException {
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

    public void keepAlive() {
        try {
            String sql = "SELECT * FROM cworkflow";
            if (db != null) {
                M_Query q = db.prepQuery(sql);
                ResultSet rs = q.openQuery();
                if (rs.next()) {
                    System.out.println("keepAlive" + rs.getInt("wkfid"));
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
//            try {
//                Thread.sleep(1000);
////                EConfiguration.reconnect();
////                keepAlive();
//            } catch (InterruptedException ex1) {
//                Logger.getLogger(EProvenance.class.getName()).log(Level.SEVERE, null, ex1);
//            }
        }
    }

}
