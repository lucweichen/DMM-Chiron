/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.DataBase.EProvenance;
import chiron.EActivation;
import chiron.EActivity;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class MCTScheduleO {
    
    private static List<EActivation> tasks;
    private static Map<Integer, EActivation> taskMap;
    
    public static Map<Integer, List<Integer>> mctSchedule(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites) {
        tasks = new ArrayList<>();
        taskMap = new HashMap<>();
        try {
            List<Integer> ids = EProvenance.loadActivationId(act.id);
            int scheduleSite;
            for (int id : ids) {
                EActivation activation = EProvenance.loadActivation(act, id);  // to modify
                tasks.add(activation);
                taskMap.put(id, activation);
                scheduleSite = getScheduleSite(activation, workload, schedulePlan, sites);
                schedulePlan = ScheduleTools.insertId(scheduleSite, id, schedulePlan);
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataIntenSched1.class.getName()).log(Level.SEVERE, null, ex);
        }
        return schedulePlan;
    }
    
    private static int getScheduleSite(EActivation activation, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites){
        double minExecTime = 0;
        double ExecTime;
        int scheduleSite = 0;
//        System.out.println("for task: " + activation.id + ":");
        for(EsiteO esite: sites){
//            System.out.print("for site: " + esite.getId() + ":");
            schedulePlan = ScheduleTools.insertId(esite.getId(), activation.id, schedulePlan);
            ExecTime = esite.execTime(schedulePlan.get(esite.getId()), activation.activityId, workload, taskMap);
//            System.out.print("exec time: " + ExecTime + ":");
            if( (minExecTime == 0) || (minExecTime > ExecTime) ){
                minExecTime = ExecTime;
                scheduleSite = esite.getId();
            }
            schedulePlan = ScheduleTools.removeId(esite.getId(), activation.id, schedulePlan);
        }
//        System.out.println("for task: " + activation.id + ": choose site: " + scheduleSite);
        return scheduleSite;
    }
    
}
