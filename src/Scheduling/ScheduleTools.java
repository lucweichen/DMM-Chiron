/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.EActivation;
import chiron.EFile;
import java.util.List;
import java.util.Map;

/**
 *
 * @author luc
 */
public class ScheduleTools {
    
    public static int getDataSite(EActivation activation) {
        long maxSize = 0;
        int site = 0;
        if (!activation.files.isEmpty()) {
            for (EFile f : activation.files) {
                if ((f != null && f.fileSize != null) && (maxSize == 0 || (maxSize < f.fileSize))) {
                    maxSize = f.fileSize;
                    site = f.getFsite();
                    System.out.println("activation: " + activation.id + " file: " + f.getFileName() + " fsite: " + f.getFsite() + " site: " + site);
                }
            }
        }
        System.out.println("activation: " + activation.id + " site: " + site);
        return site;
    }
    
    public static Map<Integer, List<Integer>> insertId(int site, int taskid, Map<Integer, List<Integer>> schedulePlan) {
        List<Integer> tasks = schedulePlan.get(site);
        tasks.add(taskid);
        return schedulePlan;
    }
    
    public static Map<Integer, List<Integer>> removeId(int site, int taskid, Map<Integer, List<Integer>> schedulePlan) {
        List<Integer> tasks = schedulePlan.get(site);
        for(int i=0; i<tasks.size(); i++){
            if(tasks.get(i).equals(taskid)){
                tasks.remove(i);
            }
        }
        return schedulePlan;
    }
    
    public static int getMasterSiteId(){
        return 0;
    }
    
}
