/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chiron.DataBase;

import chiron.EFile;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import vs.database.M_DB;

/**
 *
 * @author luc
 */
public class PProv extends Thread {

    private M_DB db = null;
    private String func = null;
    public EFile iefile;
    public int actid, taskid;
    public String fdir;
    public List<EFile> results;
    public EFile result;
    public int site;

    public PProv(M_DB db, String func) {
        this.db = db;
        this.func = func;
    }

    @Override
    public void run() {
        switch (func) {
            case "get_file_para":
                 {
                    try {
                        result = (new EFileProv(db)).get_file_para(iefile);
                        System.out.println("[pprov debug] site id: " + site + " fname:" + result.getFileName());
                    } catch (SQLException ex) {
                        Logger.getLogger(PProv.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                break;
            case "getTaskFiles": {
                try {
                    results = (new EFileProv(db)).getTaskFiles(actid, taskid);
                } catch (SQLException ex) {
                    Logger.getLogger(PProv.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            break;
            case "getFileOSite": {
                try {
                    results = (new EFileProv(db)).getFileOSite(fdir);
                } catch (SQLException ex) {
                    Logger.getLogger(PProv.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            break;
        }
    }

}
