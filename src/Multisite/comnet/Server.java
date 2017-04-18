/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite.comnet;

import Multisite.ComInit;
import Multisite.Site;
import Multisite.comnet.GreetingClient.Type;
import chiron.Chiron;
import chiron.ChironUtils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class Server extends Thread{
    private int port;
    private boolean sleep = false;
    private ServerSocket serverSocket;
    private boolean run = true;
    private List<Connection> connections;
    private List<Site> sites;
    private Site msite;
    private static int inport = Chiron.MSitePort;
    
    public Server(int port, List<Site> sites, Site msite) throws IOException{
        this.port = port;
        this.sites = sites;
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        serverSocket.setSoTimeout(1000);
        connections = new ArrayList<Connection>();
        this.msite = msite;
    }
    
    public void run(){
        while(run){
            while(sleep){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            try {
                Socket server = serverSocket.accept();
                DataInputStream in =
                      new DataInputStream(server.getInputStream());
                DataOutputStream out =
                     new DataOutputStream(server.getOutputStream());
                String ip = server.getRemoteSocketAddress() + "";
                ip = ip.substring(1,ip.indexOf(":"));
                String type = in.readUTF();
                Connection c = null;
                System.out.println("type: " + type);
                if(type.equals("connect"))
                    c = new Connection(ip, Chiron.MSiteMPort , register(ip),Type.connect);
                else if(type.equals("file"))
                    c = new Connection(ip, ComInit.getPort(ip) , new Site(ip),Type.file);
                c.start();
                while(!c.start){
                    this.sleep(50);
                }
                out.writeUTF(c.getPort()+"");
                connections.add(c);
                server.close();
            } catch (java.net.SocketTimeoutException ex) {
//                ex.printStackTrace();
            } catch (Exception ex){
                
            }
        }
    }
    
    public Site register(String ip){
        for(Site s:sites){
            if(ip.equals(s.getIp())){
                s.setConnect(true);
                return s;
            }
        }
        return null;
    }
    
    public boolean connected(){
        if(ChironUtils.isMasterSite()){
            for(Site s: sites){
                if((s.getId()!=Chiron.site)&&(!s.getConnect()))
                    return false;
            }
        }else{
            if(!msite.getConnect())
                return false;
        }
        return true;
    }
    
    public void finish(){
        this.run = false;
    }
    
    public void slep(){
        this.sleep = true;
    }
    
    public void weekup(){
        this.sleep = false;
    }
    
}
