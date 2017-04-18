package chiron;

import Multisite.FileKeepAlive;
import chiron.DataBase.EProvenance;
import chiron.DataBase.EProvenanceQueue;
import chiron.concept.CWorkflow;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nu.xom.*;
import vs.database.M_DB;

/**
 * Reads the execution XML for input configuration
 *
 * @author Eduardo, Vítor, Jonas
 * @since 2011-01-13
 */
public class EConfiguration {

    private final String configurationFileName;
    private CWorkflow conceptualWorkflow;
    private EWorkflow executionWorkflow;
    private static String nameOfDatabase;
    private static String server;
    private static String port;
    private static String username;
    private static String password;

    public EConfiguration(String configurationFile) {
        this.configurationFileName = configurationFile;
    }

    /**
     * Método que realiza a leitura do arquivo de configuração no formato XML
     *
     * @param chiron
     * @return
     * @throws ParsingException
     * @throws ValidityException
     * @throws IOException
     * @throws SQLException
     */
    public void readXMLConfiguration(Chiron chiron) throws ParsingException, ValidityException, IOException, SQLException {
        Builder parser = new Builder();
        File file = new File(configurationFileName);
        
        Document xml = parser.build(file);
        Element elementChiron = xml.getRootElement();

        //Obtain database information from XML
        Element elementDatabase = elementChiron.getChildElements("database").get(0);
        nameOfDatabase = elementDatabase.getAttributeValue("name");
        server = elementDatabase.getAttributeValue("server");
        port = elementDatabase.getAttributeValue("port");
        username = elementDatabase.getAttributeValue("username");
        password = elementDatabase.getAttributeValue("password");

        //Database connection configuration
        connect();
        
//        //Obtain environment configuration
//        Element elementEnv = elementChiron.getChildElements("machine").get(0);
//        String machineName = elementEnv.getAttributeValue("name");
//        String machineAddress = elementEnv.getAttributeValue("address");
//        String machineUsername = elementEnv.getAttributeValue("username");
//        String machinePassword = elementEnv.getAttributeValue("password");
        
        /**
         * TO DO: Implement the storeMachine method in EProvenance
         */
        //int machineId = EProvenance.storeMachine(machineName, machineAdrress, machineUsername, machinePassword);

        //Obtain CWorkflow information from XML
        Element elementWorkflow = elementChiron.getChildElements("Workflow").get(0);
        CWorkflow wf = new CWorkflow();
        wf.tag = elementWorkflow.getAttributeValue("tag");
        //retrive remaining meta-data from database
        EProvenance.matchCWorkflow(wf);
        //so here we have the conceptual workflow in the variable wf with all the activities and relations stuff

        String wfDir = elementWorkflow.getAttributeValue("wfdir");
        String expDir = ChironUtils.checkDir(elementWorkflow.getAttributeValue("expdir").replace(ChironUtils.workflowTag, wfDir));
        String exeTag = elementWorkflow.getAttributeValue("exectag");
        //derive the conceptual workflow to an executable workflow
        EWorkflow eWf = wf.derive(wfDir, expDir, exeTag);
        EProvenance.eworkflow = eWf;
        
        /**
         * TO DO: uncomment after implementing the storeMachine method in EProvenance
         */
        //eWf.machineId = machineId;
        
        String execmodel = elementWorkflow.getAttributeValue("execmodel");
        String verbose = elementWorkflow.getAttributeValue("verbose");

        if (execmodel != null) {
            eWf.model = EWorkflow.ExecModel.valueOf(execmodel);
        }
        if (verbose != null) {
            ChironUtils.verbose = Boolean.parseBoolean(verbose);
        }
        //insert the input relations data into database relations
        Elements childElements = elementWorkflow.getChildElements();
        for (int i = 0; i < childElements.size(); i++) {
            Element element = childElements.get(i);
            
            if(element.getLocalName().equals("Relation")){
                
                String relationName = element.getAttributeValue("name").toLowerCase();
                if (eWf.checkInputRelation(relationName)) {
                    String filename = element.getAttributeValue("filename");
                    eWf.getInputRelation(relationName).filename = filename;
                } else {
                    throw new ValidityException("The input relation named " + relationName + " is not defined in the concept of workflow " + wf.tag + ". Please check your workflow. Possible input relations are: " + wf.relations.toString());
                }
                
            }
        }
        
        this.conceptualWorkflow = wf;
        this.executionWorkflow = eWf;
        EProvenance.workflow = wf;
    }
    
    private void connect(){
        String connection = "jdbc:postgresql://" + server + ":" + port + "/" + nameOfDatabase + "?chartset=UTF8";
        if (Chiron.mainNode) {
            EProvenance.db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, username, password, true);
            EProvenanceQueue.queue = new EProvenanceQueue();
            EProvenanceQueue.queue.db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, username, password, true);
            EProvenanceQueue.queue.start();
        }
    }
    
    public static void reconnect(){
        String connection = "jdbc:postgresql://" + server + ":" + port + "/" + nameOfDatabase + "?chartset=UTF8";
        System.out.println("getting connection: " + connection + " username: " + username + ";psd: " + password);
        EProvenance.db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, username, password, true);
        EProvenanceQueue.queue.db = EProvenance.db;
    }
    
    public static void connect(String configurationFile){
        try {
            Builder parser = new Builder();
            File file = new File(configurationFile);
            
            Document xml = parser.build(file);
            Element elementChiron = xml.getRootElement();
            
            //Obtain database information from XML
            Element elementDatabase = elementChiron.getChildElements("database").get(0);
            nameOfDatabase = elementDatabase.getAttributeValue("name");
            server = elementDatabase.getAttributeValue("server");
            port = elementDatabase.getAttributeValue("port");
            username = elementDatabase.getAttributeValue("username");
            password = elementDatabase.getAttributeValue("password");
            String connection = "jdbc:postgresql://" + server + ":" + port + "/" + nameOfDatabase + "?chartset=UTF8";
            System.out.println("getting connection: " + connection + " username: " + username + ";psd: " + password);
            EProvenance.db = new M_DB(M_DB.DRIVER_POSTGRESQL, connection, username, password, true);
//            EProvenanceQueue.queue.db = EProvenance.db;
        } catch (ParsingException | IOException ex) {
            Logger.getLogger(EConfiguration.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void redisConf(String configurationFile){
        try {
            
            Builder parser = new Builder();
            File file = new File(configurationFile);
            
            Document xml = parser.build(file);
            Element elementChiron = xml.getRootElement();
            
            Element elementRedis = elementChiron.getChildElements("redis").get(0);
            Chiron.url = elementRedis.getAttributeValue("url");
            Chiron.psw = elementRedis.getAttributeValue("psw");
            Chiron.port = elementRedis.getAttributeValue("port");
        } catch (ParsingException | IOException ex) {
            Logger.getLogger(EConfiguration.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public CWorkflow getConceptualWorkflow() {
        return conceptualWorkflow;
    }

    public EWorkflow getExecutionWorkflow() {
        return executionWorkflow;
    }
    
}
