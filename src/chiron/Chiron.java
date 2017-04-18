package chiron;

import chiron.DataBase.EProvenance;
import chiron.DataBase.EProvenanceQueue;
import Multisite.ComInit;
import chiron.DataBase.DBKeepAlive;
import chiron.concept.CWorkflow;
import java.io.File;
import java.io.IOException;
import static java.lang.System.exit;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
//import monitoring.Average;
import mpi.MPI;
import nu.xom.ParsingException;
import nu.xom.ValidityException;

//Executar como single instance com x threads: x exp/chiron.xml
//Executar com MPJ no no y com x threads; y dir/mpj.conf niodev MPI x exp/chiron.xml
/**
 * Classe principal do chiron
 *
 * @author Eduardo, Jonas, Vítor
 * @version 0.8
 * @since 2011-2-25
 */
public class Chiron {

    //MPI Attributes
    private boolean isMPI = false;
    public static int MPI_rank = 0;
    protected static int MPI_size = 1;
    protected static int numberOfThreads = 0;
    public static int machine = 0;
    public static int site = 0;
    private EConfiguration config;
    public static CWorkflow cworkflow;
    public static EWorkflow eworkflow;
    private Thread listenerThread;
    private EListener listener = null;
    private EBody hBody;
    public static boolean mainNode = false;
    public static int MSitePort = 8888;
    public static int MSiteMPort = 8889;
    public static int MSitefPort = 8890;
    public static boolean debug = true;
    public static boolean connected = false;
    public static int keepAliveInterval = 30000;
    private static DBKeepAlive postgresAlive = null;
    public static String schedule;
    public static double weight;
    public static String url;
    public static String psw;
    public static String port;
    public static String cache;

    /**
     * Método principal do Chiron
     *
     * @param args argumentos para execução do Chiron
     * @return void
     */
    @SuppressWarnings({"CallToThreadDumpStack", "static-access"})
    public static void main(String[] args) {
        
        System.out.println("version 1.3");

        Chiron chiron = new Chiron();
        try {
            System.out.println("Chiron...");
            System.out.println("Prepare...");
            chiron.prepare(args);

            System.out.println("Open...");
            if (chiron.mainNode) {
                chiron.open();
            }
            System.out.println("Execute...");
////            Average m = new Average();
////            m.start();
            chiron.execute();
////            System.out.println("Current CPU usage : " + m.get_cpu() + " memory usage : " + m.get_mem() +" counter: " + m.get_counter());
////            m.interrupt();
            System.out.println("Close...");
            chiron.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Prepara para executar o Chiron
     *
     * @param args
     * @return void
     * @throws ParsingException
     * @throws ValidityException
     * @throws IOException
     * @throws SQLException
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    private void prepare(String[] args) throws ParsingException, ValidityException, IOException, SQLException, Exception {
        //Set up arguments
        String configurationFile = null;
        Map<String,Integer> locations = new HashMap();
        String[] init = {null,null,"niodev","MPI"};
        //discover wich argument is the MPI argument
        for (int i = 0; i < args.length; i++) {
            if (args[i].compareTo("MPI") == 0) {
                locations.put("mpi", i);
            }else if(args[i].equals("-site")){
                locations.put("site", i);
            }else if(args[i].equals("-threads")){
                locations.put("threads", i);
            }else if(args[i].equals("-EModel")){
                locations.put("emodel", i);
            }else if(args[i].equals("-mpjc")){
                locations.put("mpjc", i);
            }else if(args[i].equals("-mnumber")){
                locations.put("mnumber", i);
            }else if(args[i].equals("-sitec")){
                locations.put("sitec", i);
            }else if(args[i].equals("-schedule")){
                locations.put("schedule", i);
            }else if(args[i].equals("-weight")){
                locations.put("weight", i);
            }else if(args[i].equals("-db")){
                locations.put("cache", i);
            }
        }
        
        if(locations.containsKey("site")){
            this.site = Integer.parseInt( args[locations.get("site") + 1] );
            System.out.println("Site number: " + this.site);
        }else{
            System.out.println("Please set site");
            exit(-1);
        }
        
        if(locations.containsKey("emodel")){
            configurationFile = args[locations.get("emodel") + 1];
        }else{
            System.out.println("Please set Execution Model file.");
            exit(-1);
        }
        
        if(locations.containsKey("mpjc")){
            init[1] = args[locations.get("mpjc") + 1];
        }else{
            System.out.println("Please set mpj configuration file");
            exit(-1);
        }
        
        if(locations.containsKey("mnumber")){
            this.machine = Integer.parseInt( args[locations.get("mnumber") + 1] );
            init[0] = this.machine + "";
            System.out.println("Machine number: " + this.machine);
        }else{
            System.out.println("Please set machine number");
            exit(-1);
        }
        
        if(locations.containsKey("threads")){
            this.numberOfThreads = Integer.parseInt( args[locations.get("threads") + 1] );
        }else{
            this.numberOfThreads = Runtime.getRuntime().availableProcessors();
        }
        
        if(locations.containsKey("schedule")){
            this.schedule = args[locations.get("schedule") + 1];
            System.out.println("schedule: " + this.schedule);
        }else{
            this.schedule = "OLB";
            System.out.println("schedule: " + this.schedule);
        }
        
        if(locations.containsKey("weight")){
            this.weight = Double.parseDouble(args[locations.get("weight") + 1]);
        }else{
            this.weight = 0;
        }
        
        if(locations.containsKey("cache")){
            this.cache = args[locations.get("cache") + 1];
        }else{
            this.cache = null;
        }
        
        System.out.println("Number of threads: " + this.numberOfThreads);
        
        //if MPI is passed as argument ... setup MPI
        if (locations.containsKey("mpi")) {
            System.out.println("Init 1");
            MPI.Init(init);
            System.out.println("Init 2");
            this.MPI_size = MPI.COMM_WORLD.Size();
            this.MPI_rank = MPI.COMM_WORLD.Rank();
            this.isMPI = true;
        } else {
            this.MPI_size = 1;
        }
        
        if (!this.isMPI || this.MPI_rank == 0) {
            Chiron.mainNode = true;
            config = new EConfiguration(configurationFile);
            config.readXMLConfiguration(this);
            cworkflow = config.getConceptualWorkflow();
            eworkflow = config.getExecutionWorkflow();
        }else{
            EConfiguration.connect(configurationFile);
            postgresAlive = new DBKeepAlive();
            postgresAlive.start();
        }
        
        if(Chiron.cache.equals("cache")){
            EConfiguration.redisConf(configurationFile);
        }
        
        if(locations.containsKey("sitec")){
            if(machine==0){
                if(site == 0)
                    ComInit.communicate(MSitePort, site + "", "0", args[locations.get("sitec") + 1], true);
                else
                    ComInit.communicate(MSitePort, site + "", "0", args[locations.get("sitec") + 1], false);
            }
        }else{
            System.out.println("Please set site configuration file");
            exit(-1);
        }
        
//        System.out.println("Communicate");
        connected = true;
    }

    /**
     * Open
     *
     * @throws Exception
     */
    public void open() throws Exception {
        File direct = new File(eworkflow.expDir);
        if (!direct.exists()) {
            direct.mkdirs();
        }
        int tmpWkfId = EProvenance.matchEWorkflow(cworkflow.tag, eworkflow.exeTag);
        if (tmpWkfId >= 0) {
            //the workflow was partially executed before
            eworkflow.wkfId = tmpWkfId;
            EProvenance.matchActivities(EProvenance.db, eworkflow);
        }
        System.out.println("store workflow begin synchron");
        if(machine==0){
            ComInit.synchronize("store workflow", "begin Chiron");
            if(site==0){
                EProvenance.storeWorkflow(EProvenance.db, eworkflow);
                ComInit.setwfid(eworkflow.wkfId);
                ComInit.broadCast(eworkflow.wkfId + "");
                for (EActivity act : eworkflow.activities) {
                    int id = EProvenance.storeActivity(act);
                    ComInit.broadCast(act.tag + ":" + id);
                    EProvenance.updateRunningActivations(EProvenance.db, act);
                }
            }else{
                eworkflow.wkfId = Integer.parseInt(ComInit.getBroadCastString());
                ComInit.setwfid(eworkflow.wkfId);
                System.out.println("slave received: " + eworkflow.wkfId);
                for (EActivity act: eworkflow.activities){
                    String ids = ComInit.getBroadCastString();
                    if(act.tag.equals(ids.substring(0, ids.indexOf(":")))){
                        act.id = Integer.parseInt(ids.substring(ids.indexOf(":") + 1));
                    }
                }
            }
            System.out.println("store workflow end synchron");
            ComInit.synchronize("store workflow", "end Chiron");
        }
        System.out.println("workflow wkid: " + eworkflow.wkfId);
        eworkflow.evaluateDependencies(site);
        eworkflow.checkPipeline();
    }

    /**
     * Executa o Chiron
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws SQLException
     * @throws Exception
     */
    public void execute() throws IOException, InterruptedException, SQLException, Exception {
        hBody = new EBody(MPI_size, MPI_rank, numberOfThreads);
        //start Listener if is MPI and in main node
        if ((MPI_size > 1) && MPI_rank == 0) {
            listener = new EListener(hBody, MPI_size);
            listenerThread = new Thread(listener);
            listenerThread.start();
        }

        //initializes on main node
        if (Chiron.mainNode) {
            hBody.eWorkflow = eworkflow;
        }

        hBody.execute();

//        if ((MPI_size > 1) && (MPI_rank == 0)) {
//            while (listener.nodes > 0) {
//                ChironUtils.sleep();
//            }
//        }

        if (Chiron.mainNode) {
            EProvenanceQueue.queue.status = EActivity.StatusType.FINISHED;
        }
//        ChironUtils.sleep();
    }

    /**
     * Close
     *
     * @return void
     */
    private void close() {
        
        if(postgresAlive != null){
            postgresAlive.arret();
        }
        
        if (isMPI) {
            MPI.Finalize();
        }        
        if(machine==0)
            ComInit.delete();
        System.exit(0);
        
    }
}
