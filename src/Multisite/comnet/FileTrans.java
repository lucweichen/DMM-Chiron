/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite.comnet;

import Multisite.Site;

/**
 *
 * @author luc
 */
public class FileTrans extends Thread{
    private Site s;
    
    FileTrans(Site s){
        this.s = s;
    }
    
    public void run(){
        while(true){
            String file = s.getString();
            if(file==null || file.equals("keep alive")){
                System.out.println("except message: " + file);
                continue;
            }else if(file.equals("stop")){
                break;
            }
            s.sendFile(file);
        }
    }
    
}
