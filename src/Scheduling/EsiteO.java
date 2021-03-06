/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.Chiron;
import chiron.EActivation;
import chiron.EActivity;
import chiron.EFile;
import java.util.List;
import java.util.Map;

/**
 *
 * @author luc
 */
public class EsiteO {
    
    private String name;
    private int id;
    private double timeProv;
    private double cpuCap;
    private int cpuNO;
    private Map<Integer, Double> network;
    
    public EsiteO(String name, int id){
        this.name =name;
        this.id = id;
    }
    
    public EsiteO (String name, Map<Integer,Double> network){
        this.name = name;
        this.network = network;
    }
    
    public void setNetwork(Map<Integer, Double> network){
        this.network = network;
    }
    
    public double oneTimeExec(double workload){
        return workload / ( cpuCap / ((double)cpuNO) );
    }
    
    public double execTime(List<Integer> taskIds, int act, double workload, Map<Integer, EActivation> taskMap){
        EActivation activation;
        int runTimes = (int) Math.ceil( ( (double) (taskIds.size()) ) / ((double) cpuNO ) );
        double executionTime = ( (double) runTimes ) * workload / ( getCpuCap() / ((double) cpuNO));
        double transferTime = 0.0;
        
        for(int taskId:taskIds){
            activation = taskMap.get(taskId);
            transferTime += dtTime(activation);
            
        }
//        System.out.println("For Site " + id + " execution time: " + executionTime + " data transfer time: " + transferTime);
        return executionTime + transferTime;
    }
    
    public double execTime(List<Integer> taskIds, int act, double workload, double weight, Map<Integer, EActivation> taskMap){
        double provTime = taskIds.size() * weight * timeProv;
        return execTime(taskIds, act, workload, taskMap) + provTime;
    }
    
    public int getActivationId(EActivity act, Map<Integer, List<Integer>> schedulePlan, EsiteO tSite, Map<Integer, EActivation> taskMap){
        int resultId, resultSite, cSite;
        EActivation cactivation, ractivation;
        List<Integer> tasks = schedulePlan.get(id);
        resultId = tasks.get(0);
        ractivation = taskMap.get(resultId);
        resultSite = ScheduleTools.getDataSite(ractivation);
        for (int taskid : tasks) {
            cactivation = taskMap.get(taskid);
            cSite = ScheduleTools.getDataSite(cactivation);
            if(cSite == tSite.getId() ){
                return cactivation.id;
            }else if( ((cSite != Chiron.site)&&(resultSite == Chiron.site))
                    || ((cSite != Chiron.site)&&(resultSite != Chiron.site)&&(tSite.isFaster(cactivation, ractivation)))
                    || ((cSite == Chiron.site)&&(resultSite == Chiron.site)&&(tSite.isFaster(cactivation, ractivation)))
                    ){
                resultId = cactivation.id;
                resultSite = ScheduleTools.getDataSite(cactivation);
                ractivation = cactivation;
            }
        }
        return resultId;
    }
    
    public boolean isFaster(EActivation cTask, EActivation rTask){
        return dtTime(cTask) < dtTime(rTask);
    }
    
    private double dtTime(EActivation activation){
        double fileDtTime, taskDtTime = 0.0;
//        System.out.println("for activation: " + activation.id + " file size: " + activation.files.size());
        if (!activation.files.isEmpty()) {
            for (EFile f : activation.files) {
                if ((f != null) && (f.fileSize != null) && (f.getFsite() != Chiron.site)) {
                    System.out.println("for activation: " + activation.id + " file: " + f.getFileName() + " site: " + f.getFsite());
                    fileDtTime = f.fileSize / getNetwork().get(f.getFsite());
                    taskDtTime += fileDtTime;
                }
            }
        }
        return taskDtTime;
    }
    
    public Map<Integer, List<Integer>> removeActivation(Map<Integer, List<Integer>> schedulePlan, int ActivationId){
        List<Integer> tasks = schedulePlan.get(this.id);
        int location = 0;
        for(Integer taskid: tasks){
            if(!taskid.equals(ActivationId)){
                location++;
            }else{
                break;
            }
        }
        tasks.remove(location);
        schedulePlan.put(id, tasks);
        return schedulePlan;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the timeProv
     */
    public double getTimeProv() {
        return timeProv;
    }

    /**
     * @param timeProv the timeProv to set
     */
    public void setTimeProv(double timeProv) {
        this.timeProv = timeProv;
    }

    /**
     * @return the cpuCap
     */
    public double getCpuCap() {
        return cpuCap;
    }

    /**
     * @param cpuCap the cpuCap to set
     */
    public void setCpuCap(double cpuCap) {
        this.cpuCap = cpuCap;
    }

    /**
     * @return the network
     */
    public Map<Integer, Double> getNetwork() {
        return network;
    }

    /**
     * @return the cpuNO
     */
    public int getCpuNO() {
        return cpuNO;
    }

    /**
     * @param cpuNO the cpuNO to set
     */
    public void setCpuNO(int cpuNO) {
        this.cpuNO = cpuNO;
    }
    
}
