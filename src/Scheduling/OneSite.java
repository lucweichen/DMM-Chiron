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
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class OneSite {
    
    public static Map<Integer, List<Integer>> oneSite(EActivity act, Map<Integer, List<Integer>> schedulePlan) {
        try {
            List<Integer> ids = EProvenance.loadActivationId(act.id);
            for (int id : ids) {
                EActivation activation = EProvenance.loadActivation(act, id);
                schedulePlan = ScheduleTools.insertId(ScheduleTools.getMasterSiteId(), activation.id, schedulePlan);
            }
        } catch (SQLException ex) {
            Logger.getLogger(ActSchedule.class.getName()).log(Level.SEVERE, null, ex);
        }
        return schedulePlan;
    }
    
}
