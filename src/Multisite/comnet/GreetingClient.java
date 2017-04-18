package Multisite.comnet;

// File Name GreetingClient.java
import Multisite.Site;
import java.net.*;
import java.io.*;

public class GreetingClient {

    public enum Type {

        connect, file
    }

    public static boolean connect(Site msite, int port, Type t) {
//        int localport = 8891;
        try {
            System.out.println("connect to site: ip: " + msite.getIp() + " port: " + port);
            Socket client = new Socket(msite.getIp(), port);
            DataOutputStream out = new DataOutputStream(client.getOutputStream());
            if (t == Type.connect) {
                out.writeUTF("connect");
//                localport = 8892;
            } else if (t == Type.file) {
                out.writeUTF("file");
//                localport = 8893;
            }
            DataInputStream in = new DataInputStream(client.getInputStream());
            port = Integer.parseInt(in.readUTF());
            client.close();
            client = new Socket(msite.getIp(), port);
            out = new DataOutputStream(client.getOutputStream());
            in = new DataInputStream(client.getInputStream());
//          ServerSay ss = new ServerSay(client);
//          ss.start();
//          ss.setIn(client);
            msite.setStream(in, out, client);
            msite.setPort(port);
        } catch (ConnectException e) {
//           e.printStackTrace();
            System.err.print("Connection error");
            return false;
        } catch (Exception e) {
//           e.printStackTrace();
            System.err.print("general Connection error");
            return false;
        }
        return true;
    }

}
