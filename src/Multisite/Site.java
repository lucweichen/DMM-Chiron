/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite;

import chiron.Chiron;
import chiron.DataBase.SProvenance;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class Site {

    private String ip;
    private static int idp = 0;
    private int id;
    private boolean connect;
    private DataInputStream in;
    private DataOutputStream out;
    private Socket server;
    private int port;
    private static int sid = 0;
    private static ServerSocket servers = null;
    private boolean busy = false;
    private static long transferdata = 0;
    private String region;
    private int CPUnumber;
    private double provBW;
    public SProvenance db = null;

    public Site(String ip) {
        this.ip = ip;
        this.id = idp++;
    }

    public Site(String ip, int id) {
        this.ip = ip;
        this.id = id;
    }

    public static int getidp() {
        return idp;
    }

    public String getIp() {
        return ip;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setss(ServerSocket ser) {
        this.servers = ser;
    }

    public void setConnect(boolean c) {
        connect = c;
    }

    public boolean getConnect() {
        return connect;
    }

    public DataOutputStream getOut() {
        return out;
    }

    public void setStream(DataInputStream in, DataOutputStream out, Socket server) {
        this.server = server;
        this.in = in;
        this.out = out;
        init();
    }

    private void init() {
        try {
            this.server.setKeepAlive(true);
        } catch (SocketException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public boolean isalive() {
        if (server != null) {
            return server.isConnected();
        } else {
            return false;
        }
    }

    public synchronized void sendString(String s) {
        try {
            System.out.println(s + ";" + (sid++) + " sent");
            if (out == null) {
                System.out.println("out is null");
            }
            if (server.isOutputShutdown()) {
                System.out.println("socket output is shut down");
            } else {
                System.out.println("socket output is alive");
            }
            out = new DataOutputStream(server.getOutputStream());
            out.writeBytes(s + ";" + (sid++) + "\n");
            out.flush();
        } catch (IOException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void sendInt(int s) {
        try {
            out.writeInt(s);
            out.flush();
        } catch (IOException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public int getInt() {
        try {
            return in.readInt();
        } catch (IOException ex) {
//            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Connection lost! port: " + port);
        }
        return -1;
    }

    public synchronized String getString() {
        try {
            System.out.println("getting string");
            if (server.isInputShutdown()) {
                System.out.println("socket input is shut down");
            } else {
                System.out.println("socket input is alive");
            }
            in = new DataInputStream(server.getInputStream());
            String results = in.readLine();
            System.out.println("got string: " + results);
            if (results != null) {
                return results.substring(0, results.lastIndexOf(";"));
            } else {
                return null;
            }
        } catch (IOException ex) {
//            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
            System.out.println("Connection lost! port: " + port);
        }
        return null;
    }

    protected void discconect() {
        try {
            server.close();
            setConnect(false);
            if (servers != null) {
                servers.close();
            }
        } catch (IOException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        server.close();
        super.finalize();
    }

    public boolean isbusy() {
        return busy;
    }

    public synchronized void sendFile(String f) {
        java.util.Date date = new java.util.Date();
        System.out.println("[" + new Timestamp(date.getTime()) + "] begin send File: " + f);
        busy = true;
        System.out.println("send file: " + f);
        File myFile = new File(f);
        transferdata += myFile.length();
        System.out.println("[transfered data size]: " + transferdata);
        byte[] mybytearray = new byte[(int) myFile.length()];
        sendString(f + ";" + mybytearray.length);
        FileInputStream fis;
        BufferedInputStream bis;
        try {
            fis = new FileInputStream(myFile);
            bis = new BufferedInputStream(fis);
            bis.read(mybytearray, 0, mybytearray.length);
            out.write(mybytearray, 0, mybytearray.length);
            out.flush();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        }
        busy = false;
        date = new java.util.Date();
        System.out.println("[" + new Timestamp(date.getTime()) + "] end send File: " + f + " size: " + myFile.length());

    }

    public synchronized void getFile(String file, String dest) {
        java.util.Date date = new java.util.Date();
        System.out.println("[" + new Timestamp(date.getTime()) + "] begin receive File: " + file);
        busy = true;
        sendString(file);
        String fsize = getString();
        int size = Integer.parseInt(fsize.substring(fsize.indexOf(";") + 1));
        System.out.println(file + " transfer begin");
        System.out.println(fsize + " size: " + size);
        System.out.println(file + " transfer end.");
        byte[] mybytearray = new byte[size];
        FileOutputStream fos;
        BufferedOutputStream bos;
        int bytesRead, current;
        try {
            File f = new File(dest);
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
            if (!f.exists()) {
                f.createNewFile();
            }
//            fos = new FileOutputStream(file);
            fos = new FileOutputStream(dest);
            bos = new BufferedOutputStream(fos);
            System.out.println("end: read");
            bytesRead = in.read(mybytearray, 0, mybytearray.length);
            current = bytesRead;
            do {
                if (Chiron.debug) {
                    System.out.println("getting files: " + file + "; current: " + current + "; bytesRead: " + bytesRead);
                }
                bytesRead = in.read(mybytearray, current, (mybytearray.length - current));
                if (bytesRead > 0) {
                    current += bytesRead;
                }
            } while (bytesRead > 0);
            System.out.println("end: write");
            bos.write(mybytearray, 0, size);
            System.out.println("end: flush");
            bos.flush();
            System.out.println("end: end");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Site.class.getName()).log(Level.SEVERE, null, ex);
        }
        busy = false;
        date = new java.util.Date();
        System.out.println("[" + new Timestamp(date.getTime()) + "] end receive File: " + file + " size: " + fsize);

    }

    /**
     * @return the region
     */
    public String getRegion() {
        return region;
    }

    /**
     * @param region the region to set
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * @return the CPUnumber
     */
    public int getCPUnumber() {
        return CPUnumber;
    }

    /**
     * @param CPUnumber the CPUnumber to set
     */
    public void setCPUnumber(int CPUnumber) {
        this.CPUnumber = CPUnumber;
    }

    /**
     * @return the provBW
     */
    public double getProvBW() {
        return provBW;
    }

    /**
     * @param provBW the provBW to set
     */
    public void setProvBW(double provBW) {
        this.provBW = provBW;
    }

}
