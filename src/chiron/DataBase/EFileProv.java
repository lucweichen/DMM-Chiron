/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chiron.DataBase;

import chiron.Chiron;
import chiron.EActivation;
import chiron.EFile;
import chiron.concept.CRelation;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import vs.database.M_DB;
import vs.database.M_Query;

/**
 *
 * @author luc
 */
public class EFileProv {

    private M_DB db = null;

    public EFileProv(M_DB db) {
        this.db = db;
    }

    protected void storeFile(EFile file, EActivation activation) throws SQLException {
        synchronized (db) {
            if (!Chiron.mainNode) {
                return;
            }
            String SQL = "select f_file(?,?,?,?,?,?,?,?,?,?,?,?);";

            System.out.println("[debug] [efileprov]: file: " + file.getFileName() + " fsite: " + file.getFsite());

            File f = new File(file.getPath());
            if (f.exists()) {
                file.fileSize = (Long) f.length();
                file.fileDate = new Date(f.lastModified());
            }

            M_Query q = db.prepQuery(SQL);
            q.setParInt(1, file.fileID);
            q.setParInt(2, activation.activityId);
            q.setParInt(3, activation.id);
            q.setParString(4, file.template ? "T" : "F");
            q.setParString(5, file.instrumented ? "T" : "F");
            q.setParString(6, file.getFileDir());
            q.setParString(7, file.getFileName());
            q.setParInt(8, (int) (file.fileSize + 0));
            q.setParDate(9, file.fileDate);
            q.setParString(10, file.fileOper.toString());
            q.setParString(11, file.fieldName);
            q.setParInt(12, file.getFsite());

            ResultSet rs = q.openQuery();//meta operation
            if (rs.next()) {
                file.fileID = (rs.getInt(1));
            }
        }
    }

    protected void storeFileLocal(EFile file, EActivation activation) throws SQLException {
        synchronized (db) {
            if (!Chiron.mainNode) {
                return;
            }
            String SQL = "select f_file(?,?,?,?,?,?,?,?,?,?,?,?);";

            System.out.println("[debug] [efileprov]: file id: " + file.fileID + " file: " + file.getFileName() + " fsite: " + file.getFsite());

            File f = new File(file.getPath());
            if (f.exists()) {
                file.fileSize = (Long) f.length();
                file.fileDate = new Date(f.lastModified());
            }

            M_Query q = db.prepQuery(SQL);
            q.setParInt(1, null);
            q.setParInt(2, activation.activityId);
            q.setParInt(3, activation.id);
            q.setParString(4, file.template ? "T" : "F");
            q.setParString(5, file.instrumented ? "T" : "F");
            q.setParString(6, file.getFileDir());
            q.setParString(7, file.getFileName());
            q.setParInt(8, (int) (file.fileSize + 0));
            q.setParDate(9, file.fileDate);
            q.setParString(10, file.fileOper.toString());
            q.setParString(11, file.fieldName);
            q.setParInt(12, file.getFsite());
            ResultSet rs = q.openQuery();//meta operation
            if (rs.next()) {
//                System.out.println(rs.getInt(1));
            }
        }
    }

    public EFile get_file_para(EFile f) throws SQLException {
        boolean got = false;
        String sql = "SELECT fsite,fsize FROM ifsite where fdir = '" + f.getFileDir() + "' and fname = '" + f.getFileName() + "'";
//        System.out.println(sql);
        synchronized (db) {
            M_Query query = db.prepQuery(sql);
            ResultSet rs = query.openQuery();//meta operation
            System.out.println("[debug prov sql]" + sql);
            while (rs.next()) {
                f.setFsite(rs.getInt("fsite"));
                f.fileSize = Long.valueOf(rs.getInt("fsize"));
                got = true;
                System.out.println("[debug prov site 0] file site: " + f.getFsite());
                if (f.getFsite() == Chiron.site) {
                    break;
                }
            }
            if (!got) {
                sql = "SELECT fsite,fsize FROM efile where fdir = '" + f.getFileDir() + "' and fname = '" + f.getFileName() + "'";
                query = db.prepQuery(sql);
                System.out.println("[debug prov ]" + sql);
                rs = query.openQuery();//meta operation
                if (rs.next()) {
                    f.setFsite(rs.getInt("fsite"));
                    f.fileSize = Long.valueOf(rs.getInt("fsize"));
                    System.out.println("[debug prov site 1] file site: " + f.getFsite());
                } else {
                    f.setFsite(-1);
                    f.fileSize = -1L;
                    System.out.println("[debug prov site 2] file site: " + f.getFsite());
                }
            }
        }
        return f;
    }

    public void insertFileSite(EFile ef) throws SQLException {
        synchronized (db) {
            String sql = "INSERT INTO ifsite (fdir, fname, fsize, fsite) values ('" + ef.getFileDir() + "','" + ef.getFileName() + "'," + ef.fileSize + "," + ef.getFsite() + ")";
//        System.out.println(sql);
            M_Query q = db.prepQuery(sql);//meta operation
            q.executeUpdate();
        }
    }

    public void insertFileSite(String fdir, String fname, int fsize, int fsite) throws SQLException {
        synchronized (db) {
            String sql = "INSERT INTO ifsite (fdir, fname, fsize, fsite) values ('" + fdir + "','" + fname + "'," + fsize + "," + fsite + ")";
            M_Query q = db.prepQuery(sql);
            q.executeUpdate();//meta operation
        }
    }

    public List<EFile> getFileOSite(String fdir) throws SQLException {

//        System.out.println("[getFileOSite] fdir: " + fdir);
        List<EFile> files = new ArrayList<>();
        String sql = "SELECT fname,fsite FROM ifsite WHERE fdir=? and fsite<>?";
        synchronized (db) {
            M_Query q = db.prepQuery(sql);
//        System.out.println(sql);
            q.setParString(1, fdir);
            q.setParInt(2, Chiron.site);
            ResultSet rs = q.openQuery();//meta operation
            while (rs.next()) {
                files.add(new EFile(fdir, rs.getString("fname"), rs.getInt("fsite")));
//            System.out.println("[getFileOSite] got fdir: " + fdir + " fname: " + rs.getString("fname") + " fsite: " + rs.getInt("fsite"));
            }
        }
        return files;

    }

    public List<EFile> getTaskFiles(int actid, int taskid) throws SQLException {
        List<EFile> files = new ArrayList<>();
        String fsql = "SELECT fileid, ftemplate, finstrumented, fdir, fname, fsize, foper, fieldname, fsite FROM efile WHERE actid = ? and taskid = ?";
        synchronized (db) {
            M_Query fq = db.prepQuery(fsql);
            fq.setParInt(1, actid);
            fq.setParInt(2, taskid);
            ResultSet frs = fq.openQuery();//meta operation
            while (frs.next()) {
                boolean inst = false;
                if (frs.getString("finstrumented").equals("T")) {
                    inst = true;
                }
                EFile newFile = new EFile(inst, frs.getString("fname"), EFile.Operation.valueOf(frs.getString("foper")));
                if (frs.getString("ftemplate").equals("T")) {
                    newFile.template = true;
                }
                newFile.fileID = frs.getInt("fileid");
                newFile.setFileDir(frs.getString("fdir"));
                String filename = frs.getString("fname");
                if (filename != null) {
                    newFile.setFileName(frs.getString("fname"));
                }
                newFile.fileOper = EFile.Operation.valueOf(frs.getString("foper"));
                newFile.fieldName = frs.getString("fieldname").toUpperCase();
                newFile.fileSize = Long.valueOf(frs.getInt("fsize"));
                newFile.setFsite(frs.getInt("fsite"));
                files.add(newFile);
            }
        }
        return files;
    }

    public void insertInputData(CRelation relation, String expdir) throws SQLException, FileNotFoundException, IOException, InterruptedException {
        /**
         * TO DO: For now we are storing the KeySpace as a contiguous space,
         * getting the first and the last inserted IKs and understanding that
         * everything between that space belogs to this executing. For multi
         * tenancy approaches it will not work and needs to be changed.
         */
        synchronized (db) {
            CopyManager copyManager = new CopyManager((BaseConnection) db.getConn());
            System.out.println(expdir + "ifile_" + relation.filename);
            FileReader fR = new FileReader(expdir + "ifile_" + relation.filename);
            String sql = "COPY ifsite(ifsid,fdir,fname,fsize,fsite) FROM STDIN WITH CSV DELIMITER AS ';' HEADER";
            copyManager.copyIn(sql, fR);//meta operation
        }

    }

}
