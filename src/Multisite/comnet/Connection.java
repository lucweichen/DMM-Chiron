/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite.comnet;

import Multisite.Site;
import Multisite.comnet.GreetingClient.Type;
import chiron.Chiron;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class Connection extends Thread {
    
    private int port;
    private String cip;
    public boolean start;
    private Site s;
    private Type t;
    
    
    public Connection(String cip,int port,Site s, Type t){
        this.cip = cip;
        this.port = port;
        this.start = false;
        this.s = s;
        System.out.println("creating connection : " + s.getIp() + " port: " + port + "type: " + t.name() );
        this.t = t;
    }
    
    public int getPort(){
        return port;
    }
    
    @Override
    public void run(){
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            serverSocket.setSoTimeout(0);
            start = true;
            Socket server = serverSocket.accept();
            String remoteip = server.getRemoteSocketAddress() + "";
            remoteip = remoteip.substring(1,remoteip.indexOf(":"));
            while(!remoteip.equals(cip)){
                server.close();
                serverSocket.close();
                serverSocket = new ServerSocket(port);
                serverSocket.setReuseAddress(true);
                serverSocket.setSoTimeout(0);
                server = serverSocket.accept();
            }
            if(Chiron.debug){
                System.out.println("Just connected to " + server.getRemoteSocketAddress());
            }
            DataInputStream in =
                      new DataInputStream(server.getInputStream());
            DataOutputStream out =
                     new DataOutputStream(server.getOutputStream());
            s.setStream(in, out, server);
            s.setss(serverSocket);
            s.setPort(port);
            if(t==Type.file){
                FileTrans filetrans = new FileTrans(s);
                filetrans.start();
//                System.out.println("Server port: " + server.getPort());
//                    server.close();
            }
//            String input = null;
//            while(input==null||(!input.equals("stop"))){
//                input = in.readUTF();
//                System.out.println(input);
//                out.writeUTF("Got:\t" + input);
//            }
//            server.close();
        } catch (IOException ex) {
            System.out.println("port number: " + port);
            System.out.println("[exception] connection excpetion for ip: " + cip + " port: " + port);
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
}
