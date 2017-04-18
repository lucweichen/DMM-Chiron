/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.Chiron;
import chiron.DataBase.EProvenance;
import static chiron.DataBase.EProvenance.db;
import chiron.EActivity;
import chiron.concept.Operator;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import vs.database.M_Query;

/**
 *
 * @author luc min-min: select eactivation.taskid, sum(fsize) from eactivation,
 * efile where eactivation.actid = 169 and eactivation.taskid = efile.taskid
 * group by eactivation.taskid order by sum max-min: select eactivation.taskid,
 * sum(fsize) from eactivation, efile where eactivation.actid = 169 and
 * eactivation.taskid = efile.taskid group by eactivation.taskid order by sum
 */
public class ActScheduleO implements Scheduler {

    private Map<Integer, List<Integer>> schedulePlan = new HashMap<>();
    private final List<Esite> sites;
    private final List<EsiteO> sitesO;
    private static boolean begin =false;
    private static long st = 0;

    public ActScheduleO() {
        sites = getSites();
        sitesO = getSitesO();
        List<Integer> tasks;
        for(Esite s:sites){
            tasks = new ArrayList<>();
            schedulePlan.put(s.getId(), tasks);
        }
    }
    
    private List<Esite> getSites(){
        List<Esite> tsites = new ArrayList<>();
        Map<Integer, Double> network = new HashMap<>();
        Esite s0 = new Esite("West Europe", 0);
        network.put(0, 94371840.0);
        network.put(1, 2097152.0);
        network.put(2, 1048576.0);
        s0.setNetwork(network);
        s0.setTimeProv(0.0027);
        s0.setCpuCap(24.0 * 2.4 * 4.0);
        s0.setCpuNO(24);
        Esite s1 = new Esite("North Europe", 1);
        network = new HashMap<>();
        network.put(0, 2097152.0);
        network.put(1, 94371840.0);
        network.put(2, 1048576.0);
        s1.setNetwork(network);
        s1.setTimeProv(0.0253);
        s1.setCpuCap(24.0 * 2.4 * 4.0);
        s1.setCpuNO(24);
        Esite s2 = new Esite("Centrale US", 2);
        network = new HashMap<>();
        network.put(0, 1048576.0);
        network.put(1, 1048576.0);
        network.put(2, 94371840.0);
        s2.setNetwork(network);
        s2.setTimeProv(0.1117);
        s2.setCpuCap(24.0 * 2.4 * 4.0);
        s2.setCpuNO(24);
        tsites.add(s0);
        tsites.add(s1);
        tsites.add(s2);
        return tsites;
    }
    
    private List<EsiteO> getSitesO(){
        List<EsiteO> tsites = new ArrayList<>();
        Map<Integer, Double> network = new HashMap<>();
        EsiteO s0 = new EsiteO("West Europe", 0);
        network.put(0, 94371840.0);
        network.put(1, 2097152.0);
        network.put(2, 1048576.0);
        s0.setNetwork(network);
        s0.setTimeProv(0.0027);
        s0.setCpuCap(24.0 * 2.4 * 4.0);
        s0.setCpuNO(24);
        EsiteO s1 = new EsiteO("North Europe", 1);
        network = new HashMap<>();
        network.put(0, 2097152.0);
        network.put(1, 94371840.0);
        network.put(2, 1048576.0);
        s1.setNetwork(network);
        s1.setTimeProv(0.0253);
        s1.setCpuCap(24.0 * 2.4 * 4.0);
        s1.setCpuNO(24);
        EsiteO s2 = new EsiteO("Centrale US", 2);
        network = new HashMap<>();
        network.put(0, 1048576.0);
        network.put(1, 1048576.0);
        network.put(2, 94371840.0);
        s2.setNetwork(network);
        s2.setTimeProv(0.1117);
        s2.setCpuCap(24.0 * 2.4 * 4.0);
        s2.setCpuNO(24);
        tsites.add(s0);
        tsites.add(s1);
        tsites.add(s2);
        return tsites;
    }
    
//    public static void main(String[] args){
//        EActivity act = new EActivity();
//        act.id = 15;
//        act.operation = CActivity.newInstance(Operator.MAP);
//        act.operation.workload = 1.8432;
//        Scheduler scheduler = new ActSchedule();
//        Chiron.schedule = "DIM";
//        String connection = "jdbc:postgresql://104.40.149.109:5432/dataexec?chartset=UTF8";
//        EProvenance.db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, "liu", "4vzkvdy9", true);
//        scheduler.schedule(act);
//    }
    
    @Override
    public void schedule(EActivity act) {
        long between = 0;
        java.util.Date date = new java.util.Date();
        System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with " + Chiron.schedule + " algorithm");
        between = date.getTime();
        if(!begin){
            schedulePlan = MRSchedule.MRSchedule(act, schedulePlan);
            System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with MR algorithm");
            update();
//            showschedule();
            begin = true;
        }else if (!isQuery(act)) {
            switch(Chiron.schedule){
                case "OneSite":schedulePlan = OneSite.oneSite(act, schedulePlan);
                System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with OneSite algorithm");
                break;
                case "OLB":schedulePlan = OLBSchedule.olbSchedule(act, schedulePlan, sites);
                System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with OLB algorithm");
                break;
                case "MR":schedulePlan = MRSchedule.MRSchedule(act, schedulePlan); 
                System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with MR algorithm");
                break;
                case "MCT":schedulePlan = MCTScheduleO.mctSchedule(act, act.operation.workload, schedulePlan, sitesO); 
                System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with MCT algorithm");
                break;
                case "DIM":schedulePlan = DIMScheduleO.DIM(act, act.operation.workload, schedulePlan, sitesO);
                    System.out.println("schedule begin: " + new Timestamp(date.getTime()) + " with DIM algorithm");
            }
            update();
//            showschedule();
        }
        date = new java.util.Date();
        System.out.println("schedule end: " + new Timestamp(date.getTime()));
        between = date.getTime() - between;
        st += between;
        System.out.println("[Scheculing time:] " + between + "; other format: " + new Timestamp(between));
    }
    
    private boolean isQuery(EActivity act){
        return act.operation.type.equals(Operator.SR_QUERY) || act.operation.type.equals(Operator.MR_QUERY);
    }
    
    
    public void showschedule() {
        for (int i = 0; i < 3; i++) {
            List<Integer> taskIds = schedulePlan.get(i);
            System.out.println("Schedule for site: " + i + " count: " + taskIds.size());
            for (int id : taskIds) {
                System.out.print(id + ", ");
            }
            System.out.println("");
        }
    }

    private void update() {
        try {
            List<Integer> tasks;
            String sqlUpd = "";
            for (int i = 0; i < 3; i++) {
                tasks = schedulePlan.get(i);
                if (tasks.size() > 0) {
                    sqlUpd += "update eactivation set site = " + i + " where taskid in (";
                    for (int j = 0; j < tasks.size(); j++) {
                        sqlUpd += tasks.get(j);
                        if (j != tasks.size() - 1) {
                            sqlUpd += ", ";
                        } else {
                            sqlUpd += ");";
                        }
                    }
                }
            }
            System.out.println(sqlUpd);
            M_Query qryUpd = db.prepQuery(sqlUpd);
            qryUpd.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(EProvenance.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
