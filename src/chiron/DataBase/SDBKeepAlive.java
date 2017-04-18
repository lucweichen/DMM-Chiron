/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chiron.DataBase;

import chiron.Chiron;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class SDBKeepAlive extends Thread{
    
    private static boolean run = true;
    private SProvenance spr;
    
    public SDBKeepAlive(SProvenance s){
        this.spr = s;
    }
    
    public void run() {
        while (run) {
            try {
                spr.keepAlive();
                Thread.sleep(Chiron.keepAliveInterval);
            } catch (InterruptedException ex) {
                Logger.getLogger(SDBKeepAlive.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void arret(){
        run = false;
    }
    
}
