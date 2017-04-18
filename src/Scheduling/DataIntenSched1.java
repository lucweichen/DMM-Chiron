/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.DataBase.EProvenance;
import static chiron.DataBase.EProvenance.db;
import chiron.EActivation;
import chiron.EFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import vs.database.M_DB;
import vs.database.M_Query;

/**
 *
 * @author luc
 */
public class DataIntenSched1 implements Scheduler1 {

    private static double[][] queue = {{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}};
    private static double[] pro = { 0.0027, 0.0253, 0.1117 };
    private static double weight = 100.0;
    private static Map<Integer, List<Integer>> schedulePlan = new HashMap<>();
    
    public DataIntenSched1(){
        List<Integer> tasks1 = new ArrayList<Integer>();
        List<Integer> tasks2 = new ArrayList<Integer>();
        List<Integer> tasks3 = new ArrayList<Integer>();
        schedulePlan.put(0, tasks1);
        schedulePlan.put(1, tasks2);
        schedulePlan.put(2, tasks3);
    }

    private static int getDataSite(EActivation activation) {
        long maxSize = 0;
        int site = 0;
        if (!activation.files.isEmpty()) {
            for (EFile f : activation.files) {
//                System.out.println("getDataSite: " + f.getFileDir() + f.getFileName());
                if ((f != null && f.fileSize != null) && (maxSize == 0 || (maxSize < f.fileSize))) {
                    maxSize = f.fileSize;
                    site = f.getFsite();
                }
            }
        }
        return site;
    }

    private static void change(int siteF, int siteT, int num) {
        List<Integer> taskF = schedulePlan.get(siteF);
        List<Integer> taskT = schedulePlan.get(siteT);
        if(num>taskF.size())
            num = taskF.size();
        System.out.println("begin sitef: " + siteF + " array num: " + taskF.size() + " siteT: " + siteT + "array num: " + taskT.size() + " number: " + num);
        for(int i = 0; i<num; i++){
            taskT.add(taskF.get(0));
            taskF.remove(0);
        }
        System.out.println("end sitef: " + siteF + " array num: " + taskF.size() + " siteT: " + siteT + "array num: " + taskT.size() + " number: " + num);
    }
    
    private static void update(){
        try {
            List<Integer> tasks;
            String sqlUpd = "";
            for(int i = 0; i<3; i++){
                sqlUpd += "update eactivation set site = " + i + " where taskid in (";
                tasks = schedulePlan.get(i);
                for(int j = 0; j< tasks.size(); j++){
                    sqlUpd += tasks.get(j);
                    if(j!= tasks.size()-1)
                        sqlUpd += ", ";
                    else
                        sqlUpd += ");";
                }
            }
            System.out.println(sqlUpd);
            M_Query qryUpd = db.prepQuery(sqlUpd);
            qryUpd.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(EProvenance.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args){
        String connection = "jdbc:postgresql://104.40.223.165:5432/buzzall?chartset=UTF8";
        EProvenance.db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, "liu", "4vzkvdy9", true);
        DataIntenSched1 dis = new DataIntenSched1();
        dis.schedule(1342);
    }

    @Override
    public void schedule(int act) {
        firstSchedule(act);
//        mctSchedule(act);
//        update();
        showschedule();
    }
    
    private void olbSchedule(int act){
        try {
            Random randomGenerator = new Random();
            List<Integer> ids = EProvenance.loadActivationId(act);
            for(int id:ids){
                insertId(randomGenerator.nextInt(3), id);
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataIntenSched.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void mctSchedule(int act){
        try {
            List<Integer> ids = EProvenance.loadActivationId(act);
            int site;
            double estExectime;
            int fileAtSite = 0;
            double[] estExec = {0.0, 0.0, 0.0};
            for(int id:ids){
                EActivation activation = EProvenance.loadActivation(act, id);
                if (activation.commandLine.contains("mv")) {
                    if (activation.workspace.contains("/Buzz/")) {
                        fileAtSite = getDataSite(activation);
                        estExec[0] = 1.12 / 32.0;
                        estExec[1] = 1.12 / 32.0;
                        estExec[2] = 1.12 / 32.0;
                        if (fileAtSite == 0) {
                            estExec[1] += 1.0;
                            estExec[2] += 2.0;
                        } else if (fileAtSite == 1) {
                            estExec[0] += 1.0;
                            estExec[2] += 2.0;
                        } else {
                            estExec[0] += 2.0;
                            estExec[1] += 2.0;
                        }
                        site = 0;
                        estExectime = queue[0][0] + estExec[0];
                        for (int i = 1; i < 3; i++) {
                            if ( queue[0][i] < estExectime) {
                                site = i;
                                estExectime = queue[0][i];
                            }
                        }
                        insertId(site, activation.id);
                        queue[0][site] += estExec[site];
                        System.out.println("exec: " + estExec[site]);
                    } else if (activation.workspace.contains("/BuzzHistory/")) {
                        estExec[0] = 0.384 / 32.0;
                        estExec[1] = 0.384 / 32.0;
                        estExec[2] = 0.384 / 32.0;
                        estExec[1] += 1.0;
                        estExec[2] += 2.0;
                        site = 0;
                        estExectime = queue[1][0] + estExec[0];
                        for (int i = 1; i < 3; i++) {
                            if ( queue[1][i] < estExectime) {
                                site = i;
                                estExectime = queue[1][i];
                            }
                        }
                        insertId(site, activation.id);
                        queue[1][site] += estExec[site];
                    } else if (activation.workspace.contains("/HistogramCreator/")) {
                        fileAtSite = getDataSite(activation);
                        estExec[0] = 2.13 / 32.0;
                        estExec[1] = 2.13 / 32.0;
                        estExec[2] = 2.13 / 32.0;
                        if (fileAtSite == 0) {
                            estExec[1] += 1.0;
                            estExec[2] += 2.0;
                        } else if (fileAtSite == 1) {
                            estExec[0] += 1.0;
                            estExec[2] += 2.0;
                        } else {
                            estExec[0] += 2.0;
                            estExec[1] += 2.0;
                        }
                        site = 0;
                        estExectime = queue[2][0] + estExec[0];
                        for (int i = 1; i < 3; i++) {
                            if ( queue[2][i] < estExectime) {
                                site = i;
                                estExectime = queue[2][i];
                            }
                        }
                        insertId(site, activation.id);
                        queue[2][site] += estExec[site];
                    } 
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataIntenSched1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void myalgo(int act){
        firstSchedule(act);
        reschedule(act);
    }
    
    public void showschedule(){
        for(int i=0;i<3;i++){
            List<Integer> taskIds = schedulePlan.get(i);
            System.out.println("Schedule for site: " + i + " count: " + taskIds.size());
            for(int id: taskIds){
                System.out.print(id + ", ");
            }
            System.out.println("");
        }
    }

    private void firstSchedule(int act) {
        try {
            List<Integer> ids = EProvenance.loadActivationId(act);
            int site;
            double estExectime, queueTime;
            int fileAtSite = 0;
            double[] estExec = {0.0, 0.0, 0.0};
            double[] estQueue = {0.0, 0.0, 0.0};
            for(int id:ids){
                EActivation activation = EProvenance.loadActivation(act, id);
                if (activation.commandLine.contains("mv")) {
                    if (activation.workspace.contains("/Buzz/")) {
                        fileAtSite = getDataSite(activation);
                        estQueue[0] = queue[0][0];
                        estQueue[1] = queue[0][1];
                        estQueue[2] = queue[0][2];
                        estExec[0] = 1.12 / 32.0 + pro[0] * weight;
                        estExec[1] = 1.12 / 32.0 + pro[1] * weight;
                        estExec[2] = 1.12 / 32.0 + pro[2] * weight;
                        if (fileAtSite == 0) {
                            estExec[1] += 1.0;
                            estExec[2] += 2.0;
                            estQueue[1] += 1.0;
                            estQueue[2] += 2.0;
                        } else if (fileAtSite == 1) {
                            estExec[0] += 1.0;
                            estExec[2] += 2.0;
                            estQueue[0] += 1.0;
                            estQueue[2] += 2.0;
                        } else {
                            estExec[0] += 2.0;
                            estExec[1] += 2.0;
                            estQueue[0] += 2.0;
                            estQueue[1] += 2.0;
                        }
//                        site = 0;
//                        queueTime = estQueue[0];
//                        estExectime = estExec[0];
//                        for (int i = 1; i < 3; i++) {
//                            if (estQueue[i] < queueTime) {
//                                site = i;
//                                queueTime = estQueue[i];
//                                estExectime = estExec[i];
//                            }
//                        }
                        site = fileAtSite;
                        insertId(site, activation.id);
                        queue[0][site] += estExec[site];
                        System.out.println("exec: " + estExec[site]);
                    } else if (activation.workspace.contains("/BuzzHistory/")) {
                        estQueue[0] = queue[1][0];
                        estQueue[1] = queue[1][1];
                        estQueue[2] = queue[1][2];
                        estExec[0] = 0.384 / 32.0 + pro[0] * weight;
                        estExec[1] = 0.384 / 32.0 + pro[1] * weight;
                        estExec[2] = 0.384 / 32.0 + pro[2] * weight;
                        estExec[1] += 1.0;
                        estExec[2] += 2.0;
                        estQueue[1] += 1.0;
                        estQueue[2] += 2.0;
//                        site = 0;
//                        queueTime = estQueue[0];
//                        estExectime = estExec[0];
//                        for (int i = 1; i < 3; i++) {
//                            if (estQueue[i] < queueTime) {
//                                site = i;
//                                queueTime = estQueue[i];
//                                estExectime = estExec[i];
//                            }
//                        }
                        site = 0;
                        insertId(site, activation.id);
                        System.out.println("site: " + site + "time: " + queue[1][site] + "estExectime: " + estExec[site]);
                        queue[1][site] += estExec[site];
                        System.out.println("site: " + site + "time: " + queue[1][site] + "estExectime: " + estExec[site]);
                    } else if (activation.workspace.contains("/HistogramCreator/")) {
                        fileAtSite = getDataSite(activation);
                        estQueue[0] = queue[2][0];
                        estQueue[1] = queue[2][1];
                        estQueue[2] = queue[2][2];
                        estExec[0] = 2.13 / 32.0 + pro[0] * weight;
                        estExec[1] = 2.13 / 32.0 + pro[1] * weight;
                        estExec[2] = 2.13 / 32.0 + pro[2] * weight;
                        if (fileAtSite == 0) {
                            estExec[1] += 1.0;
                            estExec[2] += 2.0;
                            estQueue[1] += 1.0;
                            estQueue[2] += 2.0;
                        } else if (fileAtSite == 1) {
                            estExec[0] += 1.0;
                            estExec[2] += 2.0;
                            estQueue[0] += 1.0;
                            estQueue[2] += 2.0;
                        } else {
                            estExec[0] += 2.0;
                            estExec[1] += 2.0;
                            estQueue[0] += 2.0;
                            estQueue[1] += 2.0;
                        }
//                        site = 0;
//                        queueTime = estQueue[0];
//                        estExectime = estExec[0];
//                        for (int i = 1; i < 3; i++) {
//                            if (estQueue[i] < queueTime) {
//                                site = i;
//                                queueTime = estQueue[i];
//                                estExectime = estExec[i];
//                            }
//                        }
                        site = fileAtSite;
                        insertId(site, activation.id);
                        queue[2][site] += estExec[site];
                    }else if (activation.workspace.contains("/Correlate/")) {
                        fileAtSite = getDataSite(activation);
                        estQueue[0] = queue[3][0];
                        estQueue[1] = queue[3][1];
                        estQueue[2] = queue[3][2];
                        estExec[0] = 0.375 / 32.0 + pro[0] * weight;
                        estExec[1] = 0.375 / 32.0 + pro[1] * weight;
                        estExec[2] = 0.375 / 32.0 + pro[2] * weight;
                        if (fileAtSite == 0) {
                            estExec[1] += 1.0;
                            estExec[2] += 2.0;
                            estQueue[1] += 1.0;
                            estQueue[2] += 2.0;
                        } else if (fileAtSite == 1) {
                            estExec[0] += 1.0;
                            estExec[2] += 2.0;
                            estQueue[0] += 1.0;
                            estQueue[2] += 2.0;
                        } else {
                            estExec[0] += 2.0;
                            estExec[1] += 2.0;
                            estQueue[0] += 2.0;
                            estQueue[1] += 2.0;
                        }
//                        site = 0;
//                        queueTime = estQueue[0];
//                        estExectime = estExec[0];
//                        for (int i = 1; i < 3; i++) {
//                            if (estQueue[i] < queueTime) {
//                                site = i;
//                                queueTime = estQueue[i];
//                                estExectime = estExec[i];
//                            }
//                        }
                        site = fileAtSite;
                        insertId(site, activation.id);
                        queue[3][site] += estExec[site];
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataIntenSched1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void insertId(int site, int taskid){
        List<Integer> tasks = schedulePlan.get(site);
        tasks.add(taskid);
    }

    private void reschedule(int act) {
        int actid = 0;
        actid = 1;
        int maxSite, minSite, numb;
        maxSite = getMaxSite(queue, actid);
        minSite = getMinSite(queue, actid);
        while (checkBalance(maxSite, minSite, actid)) {
            numb = getTaskNum(maxSite, minSite, actid);
            change(maxSite, minSite, numb);
            maxSite = getMaxSite(queue, actid);
            minSite = getMinSite(queue, actid);
        }
    }

    private static boolean checkBalance(int maxSite, int minSite, int act) {
        System.out.println("[check balance] maxsite: " + maxSite + " minsite: " + minSite + " act: " + act + " maxtime: " + queue[act][maxSite] + " mintime: " + queue[act][minSite]);
        double diff01, diff02, diff12;
        if (act == 0) {
            diff01 = 2 * 1.12 / 32.0 + pro[0] * weight + pro[1] * weight + 1.0;
            diff02 = 2 * 1.12 / 32.0 + pro[0] * weight + pro[2] * weight + 2.0;
            diff12 = 2 * 1.12 / 32.0 + pro[1] * weight + pro[2] * weight + 2.0;
        } else if (act == 1) {
            diff01 = 2 * 0.384 / 32.0 + pro[0] * weight + pro[1] * weight + 1.0;
            diff02 = 2 * 0.384 / 32.0 + pro[0] * weight + pro[2] * weight + 2.0;
            diff12 = 2 * 0.384 / 32.0 + pro[1] * weight + pro[2] * weight + 2.0;
        } else {
            diff01 = 2 * 2.13 / 32.0 + pro[0] * weight + pro[1] * weight + 1.0;
            diff02 = 2 * 2.13 / 32.0 + pro[0] * weight + pro[2] * weight + 2.0;
            diff12 = 2 * 2.13 / 32.0 + pro[1] * weight + pro[2] * weight + 2.0;
        }
        if (maxSite == 0) {
            if (minSite == 1) {
                if (queue[act][maxSite] - queue[act][minSite] > diff01 * 2) {
                    return true;
                }
            } else if (minSite == 2) {
                if (queue[act][maxSite] - queue[act][minSite] > diff02 * 2) {
                    return true;
                }
            }
        } else if (maxSite == 1) {
            if (minSite == 0) {
                if (queue[act][maxSite] - queue[act][minSite] > diff01 * 2) {
                    return true;
                }
            } else if (minSite == 2) {
                if (queue[act][maxSite] - queue[act][minSite] > diff12 * 2) {
                    return true;
                }
            }
        } else {
            if (minSite == 0) {
                if (queue[act][maxSite] - queue[act][minSite] > diff02 * 2) {
                    return true;
                }
            } else if (minSite == 1) {
                if (queue[act][maxSite] - queue[act][minSite] > diff12 * 2) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int getTaskNum(int maxSite, int minSite, int act) {
        double exec = 0.0;
        switch (act) {
            case 0:
                exec = 1.12 / 32.0;
                break;
            case 1:
                exec = 0.384 / 32.0;
                break;
            case 2:
                exec = 2.13 / 32.0;
                break;
        }
        double diff01 = 2 * exec + pro[0] * weight + pro[1] * weight + 1.0;
        double diff02 = 2 * exec + pro[0] * weight + pro[2] * weight + 2.0;
        double diff12 = 2 * exec + pro[1] * weight + pro[2] * weight + 2.0;
        double diff = queue[act][maxSite] - queue[act][minSite];
        double exec0 = exec + pro[0] * weight;
        double exec1 = exec + pro[1] * weight;
        double exec2 = exec + pro[2] * weight;
        if (maxSite == 0) {
            if (minSite == 1) {
                queue[act][maxSite] -= (double) ((int) (diff / diff01)) * exec0;
                queue[act][minSite] += (double) ((int) (diff / diff01)) * ( exec1 + 1.0 );
                return (int) (diff / diff01);
            } else if (minSite == 2) {
                queue[act][maxSite] -= (double) ((int) (diff / diff02)) * exec0;
                queue[act][minSite] += (double) ((int) (diff / diff02)) * ( exec2 + 2.0 );
                return (int) (diff / diff02);
            }
        } else if (maxSite == 1) {
            if (minSite == 0) {
                queue[act][maxSite] -= (double) ((int) (diff / diff01)) * exec1;
                queue[act][minSite] += (double) ((int) (diff / diff01)) * ( exec0 + 1.0 );
                return (int) (diff / diff01);
            } else if (minSite == 2) {
                queue[act][maxSite] -= (double) ((int) (diff / diff12)) * exec1;
                queue[act][minSite] += (double) ((int) (diff / diff12)) * ( exec2 + 2.0 );
                return (int) (diff / diff12);
            }
        } else {
            if (minSite == 0) {
                queue[act][maxSite] -= (double) ((int) (diff / diff02)) * exec2;
                queue[act][minSite] += (double) ((int) (diff / diff02)) * ( exec0 + 2.0 );
                return (int) (diff / diff02);
            } else if (minSite == 1) {
                queue[act][maxSite] -= (double) ((int) (diff / diff12)) * exec2;
                queue[act][minSite] += (double) ((int) (diff / diff12)) * ( exec1 + 2.0 );
                return (int) (diff / diff12);
            }
        }
        return 0;
    }

    private static int getMaxSite(double[][] queue, int act) {
        double max = -1;
        int site = 0;
        for (int i = 0; i < 3; i++) {
            if (max == -1) {
                max = queue[act][i];
                site = i;
            } else {
                if (max < queue[act][i]) {
                    max = queue[act][i];
                    site = i;
                }
            }
        }
        return site;
    }

    private static int getMinSite(double[][] queue, int act) {
        double min = -1;
        int site = 0;
        for (int i = 0; i < 3; i++) {
            if (min == -1) {
                min = queue[act][i];
                site = i;
            } else {
                if (min > queue[act][i]) {
                    System.out.println("min time: " + min + " site: " + i + " time: " + queue[act][i]);
                    min = queue[act][i];
                    site = i;
                }
            }
        }
        System.out.println("min site: " + site);
        return site;
    }

}
