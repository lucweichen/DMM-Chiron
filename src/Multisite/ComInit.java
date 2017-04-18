/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Multisite;

import Multisite.comnet.GreetingClient;
import Multisite.comnet.NServer;
import ServiceBus.Queue;
import ServiceBus.Topic;
import chiron.Chiron;
import static chiron.Chiron.machine;
import chiron.DataBase.DBKeepAlive;
import chiron.DataBase.EProvenance;
import chiron.DataBase.SProvenance;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;
import nu.xom.ParsingException;

/**
 *
 * @author luc
 */
public class ComInit {

    public final static boolean remove = true;
    private static NServer s;
    private static String siteName;
    private static Queue q;
    private static Topic t;
    private static boolean isMSite;
    private static String msiteName;
    public static List<String> sitesName;
    private static List<Site> sites;
    private static List<Site> fileSites;
    private final static String btype = "broadcast";
    private static FileKeepAlive fka = null;
    private static DBKeepAlive postgresAlive = null;
    private static Map<Integer, Integer> ports;
    public static SProvenance mpro, lpro;

    public static void communicate(int port, String siteName, String msiteName, String conf, boolean isMsite) {
        try {
            ComInit.s = new NServer(port);
            ComInit.s.start();
            ComInit.q = new Queue(siteName);
            ComInit.siteName = siteName;
            readSites(conf);
            if (msiteName != null) {
                ComInit.t = new Topic(msiteName);
                for (String sName : sitesName) {
                    if (!sName.equals(msiteName)) {
                        ComInit.t.createSub(sName);
                    }
                }
            }
            ComInit.isMSite = isMsite;
            ComInit.msiteName = msiteName;
            fileSites = new ArrayList<>();
            fka = new FileKeepAlive();
            fka.start();
            postgresAlive = new DBKeepAlive();
            postgresAlive.start();
        } catch (IOException ex) {
            System.out.println("port number: " + port + "site name: " + siteName);
            Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static int getPort(int siteNumber) {
        System.out.println("getting siteNumber for: " + siteNumber);
        for (int id : ports.keySet()) {
            System.out.println("id: " + id + " port: " + ports.get(id));
        }
        System.out.println("got port number for " + siteNumber + " : " + ports.get(siteNumber));
        return ports.get(siteNumber);
    }

    public static int getPort(String ip) {
        System.out.println("getting port for: " + ip);
        return getPort(getId(ip));
    }

    public static void readSites(String conf) {
        try {
            Builder parser = new Builder();
            File file = new File(conf);
            
            Document xml = parser.build(file);
            Element root = xml.getRootElement();
            
            setSites(new ArrayList<Site>());
            ports = new HashMap<>();
            ComInit.sitesName = new ArrayList<>();
            
            //Obtain database information from XML
            Elements xmlsites = root.getChildElements("site");
            for(int i = 0; i<xmlsites.size(); i++){
                Element site = xmlsites.get(i);
                Site nsite = new Site(site.getAttributeValue("publicIP"), Integer.parseInt(site.getAttributeValue("id")));
                getSites().add(nsite);
                ComInit.sitesName.add(site.getAttributeValue("id"));
                Elements css = site.getChildElements("connection");
                Elements evms = site.getChildElements("VM");
                if(Chiron.cache.contains("dis")){
                    Element db = site.getChildElements("database").get(0);
                    SProvenance sprov = new SProvenance(db.getAttributeValue("server"), db.getAttributeValue("port"), db.getAttributeValue("name"), db.getAttributeValue("username"), db.getAttributeValue("password"));
                    nsite.db = sprov;
                    if( Chiron.site == nsite.getId()){
                        lpro = sprov;
                    }
                    if( site.getAttributeValue("msite") != null && site.getAttributeValue("role").equals("master") ){
                        mpro = sprov;
                        mpro.db = EProvenance.db;
                    }
                }
                nsite.setRegion(site.getAttributeValue("region"));
                nsite.setProvBW(Double.parseDouble(site.getAttributeValue("proBW")));
                nsite.setCPUnumber(Integer.parseInt(site.getAttributeValue("CPU")));
                if (Chiron.site == nsite.getId()) {
                    EProvenance.insertSite(nsite);
                    for(int j = 0; j < css.size(); j++){
                        Element cs = css.get(j);
                        ConnectionSite ccs = new ConnectionSite();
                        ccs.oport = Integer.parseInt(cs.getAttributeValue("thisport"));
                        ports.put(j, ccs.oport);
                        if(!cs.getAttributeValue("targetport").equals(""))
                            ccs.tport = Integer.parseInt(cs.getAttributeValue("targetport"));
                        ccs.tsiteid = Integer.parseInt(cs.getAttributeValue("target"));
                        ccs.databw = Double.parseDouble(cs.getAttributeValue("databw"));
                        EProvenance.insertConnection(ccs);
                    }
                    for(int j = 0; j< evms.size(); j++){
                        Element evm = evms.get(j);
                        VM vm = new VM();
                        vm.comcap = Double.parseDouble(evm.getAttributeValue("comcap"));
                        vm.cpu = Integer.parseInt(evm.getAttributeValue("CPU"));
                        vm.id = Integer.parseInt(evm.getAttributeValue("id"));
                        vm.mpiid = Chiron.MPI_rank;
                        vm.price = Double.parseDouble(evm.getAttributeValue("price"));
                        vm.privateip = evm.getAttributeValue("privateip");
                        vm.siteid = nsite.getId();
                        vm.type = evm.getAttributeValue("type");
                        EProvenance.insertVM(vm);
                    }
                }
                
            }
            Site stemp;
            for(int i = 0; i<sites.size(); i++){
                for(int j = i+1; j<sites.size(); j++){
                    if(sites.get(j).getProvBW() < sites.get(i).getProvBW()){
                        stemp = sites.get(j);
                        sites.set(j, sites.get(i));
                        sites.set(i, stemp);
                    }
                }
            }
//        Site thissite;
//        sites = new ArrayList<>();
//        ports = new HashMap<>();
//        ComInit.sitesName = new ArrayList<>();
//        BufferedReader br;
//        try {
//            br = new BufferedReader(new FileReader(conf));
//            String line = br.readLine();
//            String[] info;
//
//            while (line != null) {
//                info = line.split("\t");
//                System.out.println(Integer.parseInt(info[1]));
//                thissite = new Site(info[0], Integer.parseInt(info[1]));
//                sites.add(thissite);
//                ComInit.sitesName.add(info[1]);
//                if (Chiron.site == thissite.getId()) {
//                    for (int i = 0; i < info.length; i++) {
//                        if (i > 1) {
//                            ports.put(i - 2, Integer.parseInt(info[i]));
//                        }
//                    }
//                }
//                line = br.readLine();
//            }
//            for (int id : ports.keySet()) {
//                System.out.println("id: " + id + " port: " + ports.get(id));
//            }
//        } catch (IOException ex) {
//            Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
//        }
        } catch (ParsingException ex) {
            Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void setwfid(int id){
        for(Site site: sites){
            if(site.db != null){
                site.db.wid = id;
            }
        }
    }

    public static void sendString(String site, String s, String id) {
        Queue sq = new Queue(site);
        sq.sendMessage(s, id);
    }

    public static String getString(String id) {
        System.out.println("getting string: id: " + id);
        String message = q.getMessage(remove, id);
        System.out.println("got string:" + message + " id: " + id);
        while (message == null) {
            try {
                Thread.sleep(1000);
                message = q.getMessage(remove, id);
                System.out.println("got string: " + message + " id: " + id);
            } catch (InterruptedException ex) {
                Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("got string: " + message + " id: " + id);
        return message;
    }

    public static String getString(List<String> ids) {
        String message = q.getMessage(remove, ids);
        while (message == null) {
            try {
                Thread.sleep(1000);
                message = q.getMessage(remove, ids);
            } catch (InterruptedException ex) {
                Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return message;
    }

    public static void synchronize(String s, String id) {
        System.out.println("synchronize begin: " + s + " id: " + id);
        if (isMSite) {
            sendTString(s, id);
        } else {
            if (!getTString(s, id)) {
                try {
                    throw (new Exception("not synchronized"));
                } catch (Exception ex) {
                    Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                Queue mq = new Queue(msiteName);
                mq.sendMessage(s, siteName + id);
            }
        }
        if (isMSite) {
            for (String siteName : sitesName) {
                System.out.println("for site: " + siteName);
                if (siteName.equals(msiteName)) {
                    continue;
                }
                if (!getString(siteName + id).equals(s)) {
                    try {
                        throw (new Exception("not synchronized"));
                    } catch (Exception ex) {
                        Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        System.out.println("synchronize end: " + s + " id: " + id);
    }

    public static void broadCast(String s) {
        if (isMSite) {
            sendTString(s, btype);
        }
    }

    public static String getBroadCastString() {
        String message = t.getSubMessage(siteName, remove, btype);
        while (message == null) {
            try {
                Thread.sleep(1000);
                message = t.getSubMessage(siteName, remove, btype);
            } catch (InterruptedException ex) {
                Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return message;
    }

    private static void sendTString(String s, String id) {
        System.out.println("send string: " + s + " string id: " + id);
        t.sendMessage(s, id);
    }

    private static boolean getTString(String s, String id) {
        System.out.println("getting string: " + s + " string id: " + id);
        String message = t.getSubMessage(siteName, remove, id);
        while (message == null) {
            try {
                Thread.sleep(1000);
                message = t.getSubMessage(siteName, remove, id);
            } catch (InterruptedException ex) {
                Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (message.equals(s)) {
            System.out.println("got string: " + s + " string id: " + id);
            return true;
        } else {
            System.out.println("didn't get string: " + s + " string id: " + id);
            return false;
        }
    }

    public static synchronized void getFile(String file, String dest, int site) {
        if(site==-1){
            System.out.println("site is -1 file: " + file);
            return;
        }
        Boolean c = false;
        System.out.println("begin sending file from site: " + site);
        Site s = getFileSite(site);
        if (s == null) {
            System.out.println("site is null");
        }
        if (s != null && s.getOut() == null) {
            System.out.println("site id: " + s.getId() + " out is null");
        }
        if (s == null || s.getOut() == null) {
            s = new Site(getSite(site).getIp());
            s.setId(site);
            System.out.println("get File connection begin");
            while (!c) {
                c = GreetingClient.connect(s, Chiron.MSitePort, GreetingClient.Type.file);
                if (!c) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(ComInit.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            fileSites.add(s);
        } else {
            System.out.println("get File existing connection begin");
        }
        System.out.println("get File connection end");
        s.getFile(file, dest);
        System.out.println("ofile: " + file + "; dfile: " + dest);
    }

    public static void keepFileSiteAlive() {
        for (Site s : fileSites) {
            System.out.println("[keep site alive] keep alive for site: " + s.getId() + " site ip: " + s.getIp());
            if (!s.isbusy()) {
                s.sendString("keep alive");
            }
            System.out.println("[keep site alive] site free keep alive for site: " + s.getId() + " site ip: " + s.getIp());
        }
    }

    private static Site getFileSite(int site) {
        for (Site s : fileSites) {
            System.out.println("site id: " + s.getId() + " site ip: " + s.getIp());
            if (s.getId() == site) {
                return s;
            }
        }
        return null;
    }

    public static Site getSite(int id) {
        for (Site s : getSites()) {
            if (s.getId() == id) {
                return s;
            }
        }
        return null;
    }

    private static int getId(String ip) {
        for (Site s : getSites()) {
            if (s.getIp().equals(ip)) {
                return s.getId();
            }
        }
        return -1;
    }

    public static void delete() {
        if (fka != null) {
            fka.arret();
        }
        if (machine == 0) {
            ComInit.synchronize("end file transfer pipe begin", "delete");
            for (Site s : fileSites) {
                System.out.println("[end file transfer] end file transfer for site: " + s.getId() + " site ip: " + s.getIp());
                s.sendString("stop");
                System.out.println("[end file transfer] end file transfer for site: " + s.getId() + " site ip: " + s.getIp());
            }

            ComInit.synchronize("end file transfer pipe end", "delete");
        }

        ComInit.q.deleteQueue();
        if (isMSite) {
            ComInit.t.deleteTopic();
        }
        if (postgresAlive != null) {
            postgresAlive.arret();
        }
    }

    /**
     * @return the sites
     */
    public static List<Site> getSites() {
        return sites;
    }

    /**
     * @param aSites the sites to set
     */
    public static void setSites(List<Site> aSites) {
        sites = aSites;
    }

}
