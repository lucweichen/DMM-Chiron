/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.Chiron;
import chiron.EActivation;
import chiron.EActivity;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author luc
 */
public class DIMScheduleO {
    
    private static final double weight = Chiron.weight;
    private static int maxSite;
    private static int minSite;
    private static List<EActivation> tasks;
    private static Map<Integer, EActivation> taskMap;

    public static Map<Integer, List<Integer>> DIM(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites) {
        tasks = new ArrayList<>();
        taskMap = new HashMap<>();
        schedulePlan = MRScheduleO.MRSchedule(act, schedulePlan, tasks, taskMap);
        System.out.println("end MR scheduling");
        schedulePlan = balanceSchedule(act, workload, schedulePlan, sites);
        System.out.println("end balance scheduling");
        return schedulePlan;
    }
    
    private static Map<Integer, List<Integer>> balanceSchedule(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites) {
        double oexecTime, texecTime;
        while (!checkBalance(act, workload, schedulePlan, sites)) {
            oexecTime = execTime(act, workload, schedulePlan, sites);
            schedulePlan = exChange(act, workload, schedulePlan, sites);
            texecTime = execTime(act, workload, schedulePlan, sites);
            if(texecTime >= oexecTime)
                break;
        }
        return schedulePlan;
    }
    
    private static double execTime(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites){
        double maxExecTime = -1.0, execTime;
        for(EsiteO esite:sites){
            execTime = esite.execTime(schedulePlan.get(esite.getId()), act.id, workload, weight, taskMap);
            if(maxExecTime == -1.0){
                maxExecTime = execTime;
            }else if(maxExecTime < execTime){
                maxExecTime = execTime;
            }
        }
        return maxExecTime;
    }
    
    private static boolean checkBalance(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites){
        double maxTime = 0.0, minTime = 0.0, ExecTime, oneTimeExecMax = 0.0, oneTimeExecMin = 0.0, epsilon;
        for(EsiteO esite:sites){
            ExecTime = esite.execTime(schedulePlan.get(esite.getId()), act.id, workload, weight, taskMap);
            if(maxTime == 0.0 || maxTime < ExecTime){
                maxTime = ExecTime;
                maxSite = esite.getId();
                oneTimeExecMax = esite.oneTimeExec(workload);
            }
            if(minTime == 0.0 || minTime > ExecTime){
                minTime = ExecTime;
                minSite = esite.getId();
                oneTimeExecMin = esite.oneTimeExec(workload);
            }
        }
        epsilon = oneTimeExecMax > oneTimeExecMin ? oneTimeExecMin : oneTimeExecMax;
        return maxTime - minTime <= epsilon;
    }
    
    private static Map<Integer, List<Integer>> exChange(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<EsiteO> sites) {
        double diffo, diffa;
        EsiteO mSite = sites.get(0);
        int exchangeActivationId;
        diffo = Math.abs(sites.get(maxSite).execTime(schedulePlan.get(maxSite), act.id, workload, weight, taskMap) - sites.get(minSite).execTime(schedulePlan.get(minSite), act.id, workload, weight, taskMap));
        diffa = diffo;
        do{
            diffo = diffa;
            for(EsiteO es: sites){
                if(es.getId() == minSite)
                    mSite = es;
            }
            System.out.println("get 9");
            exchangeActivationId = sites.get(maxSite).getActivationId(act, schedulePlan, mSite, taskMap);
            schedulePlan = sites.get(maxSite).removeActivation(schedulePlan, exchangeActivationId);
            schedulePlan = ScheduleTools.insertId(minSite, exchangeActivationId, schedulePlan);
            diffa = sites.get(maxSite).execTime(schedulePlan.get(maxSite), act.id, workload, weight, taskMap) - sites.get(minSite).execTime(schedulePlan.get(minSite), act.id, workload, weight, taskMap);
            System.out.println("diff: " + diffa );
            System.out.println("max exec time: " + sites.get(maxSite).execTime(schedulePlan.get(maxSite), act.id, workload, weight, taskMap) );
            System.out.println("max exec task numbers: " + schedulePlan.get(maxSite).size());
            System.out.println("min exec time: " + sites.get(minSite).execTime(schedulePlan.get(minSite), act.id, workload, weight, taskMap) );
            System.out.println("min exec task numbers: " + schedulePlan.get(minSite).size());
        }while(diffa > 0 && diffa < diffo);
        return schedulePlan;
    }

}