/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite;

import chiron.Chiron;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class FileKeepAlive extends Thread{
    
    private static boolean run = true;
    
    public void run() {
        while (run) {
            try {
                ComInit.keepFileSiteAlive();
                Thread.sleep(Chiron.keepAliveInterval);
            } catch (InterruptedException ex) {
                Logger.getLogger(FileKeepAlive.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void arret(){
        run = false;
    }
    
}
