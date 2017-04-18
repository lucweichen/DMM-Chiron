/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Scheduling;

import chiron.DataBase.EProvenance;
import chiron.EActivity;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class OLBSchedule {
    
    public static Map<Integer, List<Integer>> olbSchedule(EActivity act, Map<Integer, List<Integer>> schedulePlan, List<Esite> sites) {
        try {
            Random randomGenerator = new Random();
            List<Integer> ids = EProvenance.loadActivationId(act.id);
            for (int id : ids) {
                schedulePlan = ScheduleTools.insertId(sites.get(randomGenerator.nextInt(3)).getId(), id, schedulePlan);
            }
        } catch (SQLException ex) {
            Logger.getLogger(ActSchedule.class.getName()).log(Level.SEVERE, null, ex);
        }
        return schedulePlan;
    }
    
}
