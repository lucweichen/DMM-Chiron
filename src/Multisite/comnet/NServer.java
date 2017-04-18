/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite.comnet;

import Multisite.ComInit;
import Multisite.Site;
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
public class NServer extends Thread{
    
    private int port;
    private boolean run = true;
    private boolean sleep = false;
    private ServerSocket serverSocket;
    private List<Connection> connections;
    
    public NServer(int port) throws IOException{
        this.port = port;
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        serverSocket.setSoTimeout(1000);
        connections = new ArrayList<Connection>();
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
                c = new Connection(ip, ComInit.getPort(ip) , new Site(ip),GreetingClient.Type.file);
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
    
}
