/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.Chiron;
import chiron.EActivity;
import java.util.List;
import java.util.Map;

/**
 *
 * @author luc
 */
public class DIMSchedule {
    
    private static final double weight = Chiron.weight;
    private static int maxSite;
    private static int minSite;

    public static Map<Integer, List<Integer>> DIM(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<Esite> sites) {
        schedulePlan = MRSchedule.MRSchedule(act, schedulePlan);
        System.out.println("end MR scheduling");
        schedulePlan = balanceSchedule(act, workload, schedulePlan, sites);
        System.out.println("end balance scheduling");
        return schedulePlan;
    }
    
    private static Map<Integer, List<Integer>> balanceSchedule(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<Esite> sites) {
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
    
    private static double execTime(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<Esite> sites){
        double maxExecTime = -1.0, execTime;
        for(Esite esite:sites){
            execTime = esite.execTime(schedulePlan.get(esite.getId()), act.id, workload, weight);
            if(maxExecTime == -1.0){
                maxExecTime = execTime;
            }else if(maxExecTime < execTime){
                maxExecTime = execTime;
            }
        }
        return maxExecTime;
    }
    
    private static boolean checkBalance(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<Esite> sites){
        double maxTime = 0.0, minTime = 0.0, ExecTime, oneTimeExecMax = 0.0, oneTimeExecMin = 0.0, epsilon;
        for(Esite esite:sites){
            ExecTime = esite.execTime(schedulePlan.get(esite.getId()), act.id, workload, weight);
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
    
    private static Map<Integer, List<Integer>> exChange(EActivity act, double workload, Map<Integer, List<Integer>> schedulePlan, List<Esite> sites) {
        double diffo, diffa;
        Esite mSite = sites.get(0);
        int exchangeActivationId;
        diffo = Math.abs(sites.get(maxSite).execTime(schedulePlan.get(maxSite), act.id, workload, weight) - sites.get(minSite).execTime(schedulePlan.get(minSite), act.id, workload, weight));
        diffa = diffo;
        do{
            diffo = diffa;
            for(Esite es: sites){
                if(es.getId() == minSite)
                    mSite = es;
            }
            System.out.println("get 9");
            exchangeActivationId = sites.get(maxSite).getActivationId(act, schedulePlan, mSite);
            schedulePlan = sites.get(maxSite).removeActivation(schedulePlan, exchangeActivationId);
            schedulePlan = ScheduleTools.insertId(minSite, exchangeActivationId, schedulePlan);
            diffa = sites.get(maxSite).execTime(schedulePlan.get(maxSite), act.id, workload, weight) - sites.get(minSite).execTime(schedulePlan.get(minSite), act.id, workload, weight);
            System.out.println("diff: " + diffa );
            System.out.println("max exec time: " + sites.get(maxSite).execTime(schedulePlan.get(maxSite), act.id, workload, weight) );
            System.out.println("max exec task numbers: " + schedulePlan.get(maxSite).size());
            System.out.println("min exec time: " + sites.get(minSite).execTime(schedulePlan.get(minSite), act.id, workload, weight) );
            System.out.println("min exec task numbers: " + schedulePlan.get(minSite).size());
        }while(diffa > 0 && diffa < diffo);
        return schedulePlan;
    }

}