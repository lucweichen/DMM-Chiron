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
public class DBKeepAlive extends Thread{
    
    private static boolean run = true;
    
    public void run() {
        while (run) {
            try {
                EProvenance.keepAlive();
                Thread.sleep(Chiron.keepAliveInterval);
                System.out.println("keep provenance database alive");
            } catch (InterruptedException ex) {
                Logger.getLogger(DBKeepAlive.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void arret(){
        run = false;
    }
    
}
