package chiron;

import chiron.DataBase.EProvenance;
import Multisite.ComInit;
import Multisite.Site;
import Scheduling.ActScheduleO;
import Scheduling.Scheduler;
import chiron.concept.CActivity;
import chiron.concept.CRelation;
import chiron.concept.Operator;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The EWorkflow is responsible to store the execution data of the workflow and
 * to control the execution following a given execution model.
 *
 * @author Eduardo, Vítor
 * @since 2010-12-25
 */
public class EWorkflow implements Serializable {

    public enum ExecModel {
        STA_FAF, DYN_FAF, STA_FTF, DYN_FTF
    }
    
    public ExecModel model = ExecModel.DYN_FAF;
    public String tag;
    public Integer wkfId = null;
    public String exeTag;
    public String expDir;
    public String wfDir;
    public ArrayList<EActivity> activities = new ArrayList<>();
    public ArrayList<EActivity> runnableActivities = new ArrayList<>();
    public ArrayList<CRelation> inputRelations = new ArrayList<>();
    
    public ArrayList<ActivityTransfer> activityTransfer = new ArrayList<>();

    /**
     * Construtor de um workflow
     * @param workflowDir
     */
    public EWorkflow(String workflowDir) {
        FileReader reader = null;
        try {
            reader = new FileReader(workflowDir + "/properties");
            Scanner in = new Scanner(reader);
            activityTransfer = new ArrayList<>();
            while(in.hasNextLine()){
                String line = in.nextLine();
                String[] splits = line.split(";");
                activityTransfer.add(new ActivityTransfer(splits[0], splits[1], splits[2]));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(EWorkflow.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                reader.close();
            } catch (IOException ex) {
                Logger.getLogger(EWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Realiza a interação de execução de cada atividade do workflow
     *
     * @return
     */
    private void addPipelineActivations(ArrayList<EActivation> resultList, EActivity activity, EActivation activation) throws SQLException, IOException {
        EActivity dep = activity.pipeline;
        EActivity act = activity;
        while (dep != null) {
            //activity already executed, 
            EActivation depActivation = dep.operation.createPipelineActivation(expDir, wfDir, act, dep, activation);
            (new EProvenance()).storeActivation(depActivation);
            resultList.add(depActivation);
            depActivation.pipelinedFrom = activation;
            act = dep;
            dep = dep.pipeline;
            activation = depActivation;
        }
    }
    
    private boolean isQuery(int id){
        CActivity act = Chiron.cworkflow.activities.get(id);
        return act.type.equals(Operator.SR_QUERY)||act.type.equals(Operator.MR_QUERY);
    }

    public EActivation[] iterateExecution() throws Exception {
        ArrayList<EActivation> resultList = new ArrayList<EActivation>();
        boolean returnWait = false;
        int i = 0;
        while ((runnableActivities.size() > 0) && (i < runnableActivities.size())) {
            EActivity act = runnableActivities.get(i);
            java.util.Date date= new java.util.Date();
            System.out.println("[" + new Timestamp(date.getTime()) + "] act: " + act.tag + "; status: " + act.status);
            if (act.status == EActivity.StatusType.READY) {
//                if(isQuery(act.cactid))
//                    (new EProvenance()).print_get();
                act.startTime = new Date();
                date= new java.util.Date();
                System.out.println("[" + new Timestamp(date.getTime()) + "]generate activity tasks begin");
                act.operation.generateActivations(act, this.wfDir, this.expDir);
//                EProvenance.rescheduling(act);
                date= new java.util.Date();
                System.out.println("[" + new Timestamp(date.getTime()) + "]generate activity tasks end");
                if (Chiron.machine == 0) {
                    ComInit.synchronize("schedule begin", "begin " + act.tag);
                }
                if(ChironUtils.isMasterSite() && !act.status.equals(EActivity.StatusType.FINISHED)){
                    date = new java.util.Date();
                    System.out.println("[" + new Timestamp(date.getTime()) + "] schedule begin");
//                    Scheduler sch = new ActSchedule();
                    Scheduler sch = new ActScheduleO();
                    sch.schedule(act);
                    date = new java.util.Date();
                    System.out.println("[" + new Timestamp(date.getTime()) + "] schedule end");
                }
                if (Chiron.machine == 0) {
                    ComInit.synchronize("schedule end", "end " + act.tag);
                }
                EActivity pipe = act.pipeline;
                while (pipe != null) {
                    pipe.status = EActivity.StatusType.PIPELINED;
                    pipe.startTime = new Date();
                    EProvenance.storeActivity(pipe);
                    pipe = pipe.pipeline;
                }
            }
            date= new java.util.Date();
            System.out.println("[" + new Timestamp(date.getTime()) + "] act: " + act.tag + "; status: " + act.status);
            if (act.status == EActivity.StatusType.RUNNING) {
                EActivation item = EProvenance.loadReadyActivation(EProvenance.db, act);
                if(item != null)
                    EProvenance.updateRunningActivation(item);
                System.out.println("[" + new Timestamp(date.getTime()) + "] item: " + item );
                if (item != null) {
                    int k = 1, n = act.numActivations / (Chiron.MPI_size * Chiron.numberOfThreads);
                    resultList.add(item);
                    addPipelineActivations(resultList, act, item);
                    if ((act.workflow.model == EWorkflow.ExecModel.STA_FAF) || (act.workflow.model == EWorkflow.ExecModel.STA_FTF)) {
                        while ((item != null) && (k < n)) {
                            item = EProvenance.loadReadyActivation(EProvenance.db, act);
                            if (item != null) {
                                resultList.add(item);
                                if(item != null)
                                    EProvenance.updateRunningActivation(item);
                                if (act.workflow.model == EWorkflow.ExecModel.STA_FTF) {
                                    addPipelineActivations(resultList, act, item);
                                }
                                k++;
                            }
                        }
                    }
                } else if (checkActFini(act)) {
                    act.status = EActivity.StatusType.FINISHED;
                    act.endTime = new Date();
                    if ( Chiron.site != 0) {
                        EProvenance.setFinish(act);
                    }
                } else {
                    returnWait = true;
                }
            }
            date= new java.util.Date();
            System.out.println("[" + new Timestamp(date.getTime()) + "] act: " + act.tag + "; status: " + act.status);
            if (act.status == EActivity.StatusType.FINISHED) {
                if(Chiron.site == 0)
                    EProvenance.storeActivity(act);
                transferRelationFromActivity(act);
                
                EActivity dep = act.pipeline;
                while (dep != null) {
                    dep.status = EActivity.StatusType.FINISHED;
                    dep.endTime = new Date();
                    if(Chiron.site == 0)
                        EProvenance.storeActivity(dep);
                    dep = dep.pipeline;
                }
                runnableActivities.remove(act);
                evaluateDependencies(Chiron.site);
                if (resultList.isEmpty()) {
                    i = 0;
                    continue;
                }
            }
            if (resultList.size() > 0) {
                break;
            }
            i++;
        }

        if ((resultList.isEmpty()) && returnWait) {
            resultList.add(EActivation.WAIT_ACTIVATION);
        }
        if (resultList.size() > 0) {
            EActivation[] result = new EActivation[resultList.size()];
            for (int j = 0; j < resultList.size(); j++) {
                result[j] = resultList.get(j);
            }
            return result;
        }else{
            evaluateDependencies(Chiron.site);
        }
        return null;
    }
    
    private boolean checkActFini(EActivity act) {
        if(Chiron.site == 0)
            try {
                return EProvenance.checkIfAllActivationsFinished(EProvenance.db, act);
            } catch (SQLException ex) {
                Logger.getLogger(EWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        else{
            if(Chiron.cache.equals("dislocal") || Chiron.cache.equals("disRepDyn") || Chiron.cache.equals("disRepDHT")){
                return checkActFiniDisLoc(act);
            }else if(Chiron.cache.equals("cache") || Chiron.cache.equals("centralized")){
                return checkActFiniCen(act);
            }else{
                return checkActFiniDHT(act);
            }
        }
        return false;
    }
    
    private boolean checkActFiniDisLoc(EActivity act){
        for (Site s : ComInit.getSites()) {
            try {
                if (!s.db.checkIfAllActivationsFinished(s.db.db, act, s.getId())) {
                    return false;
                }
            } catch (SQLException ex) {
                Logger.getLogger(EWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return true;
    }
    
    private boolean checkActFiniCen(EActivity act){
        try {
            return EProvenance.checkIfAllActivationsFinished(EProvenance.db, act);
        } catch (SQLException ex) {
            Logger.getLogger(EWorkflow.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    private boolean checkActFiniDHT(EActivity act){
        try {
            List<EActivation> acts = EProvenance.loadActivations(act.id);
            Map<Integer, List<EActivation>> siteActs = new HashMap<>();
            int sHash;
            List<EActivation> tmp;
            for(EActivation activation: acts){
                sHash = activation.hashCode()%3;
                if(siteActs.containsKey(sHash)){
                    tmp = siteActs.get(sHash);
                }else{
                    tmp = new ArrayList<>();
                }
                tmp.add(activation);
                siteActs.put(sHash, tmp);
            }
            for (Site s : ComInit.getSites()) {
                if( (siteActs.get(s.getId())!=null) && (!s.db.checkIfActivationsFinished(siteActs.get(s.getId()))) )
                    return false;
            }
            return true;
        } catch (SQLException ex) {
            Logger.getLogger(EWorkflow.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * Obtém a atividade a partir de sua tag
     *
     * @param actTag
     * @return
     */
    public EActivity getActivity(String actTag) {
        for (int i = 0; i < activities.size(); i++) {
            EActivity activity = activities.get(i);
            if (activity.tag.equals(actTag)) {
                return activity;
            }
        }
        return null;
    }

    public CRelation getInputRelation(String name) {
        for (CRelation rel : this.inputRelations) {
            if (rel.name.equals(name)) {
                return rel;
            }
        }
        return null;
    }

    /**
     * Calcula a dependência entre as atividades do workflow
     */
    @SuppressWarnings("CallToThreadDumpStack")
    public void evaluateDependencies(int site) throws SQLException, FileNotFoundException, IOException, InterruptedException {

        for (EActivity activity : activities) {
            if (activity.status == EActivity.StatusType.BLOCKED) {
                EActivity.StatusType newStatus = EActivity.StatusType.BLOCKED;
                
                boolean finished = activity.hasFinishedDependentActivities();
                
                System.out.println("activity: " + activity.id + " status: " + finished);
                
                if(!finished){
                    newStatus = EActivity.StatusType.BLOCKED;
                    break;
                } else {
                    newStatus = EActivity.StatusType.READY;
                    
                    System.out.println("activity: " + activity.id + " relation: " + activity.relations.size());
                    
                    for(CRelation relation : activity.relations){
                        
                        System.out.println("activity: " + activity.id + " relation: " + relation.filename + " type: " + relation.type.equals(CRelation.Type.INPUT));
                        //the relation can be an input relation, must store input data into database and set activity to READY
                        if (relation.type.equals(CRelation.Type.INPUT)) {
                            if(relation.filename != null){
                                System.out.println("file: " + this.expDir + relation.filename + " type: " + ChironUtils.checkFile(this.expDir + relation.filename));
                                if(ChironUtils.checkFile(this.expDir + relation.filename)){
                                    if(Chiron.machine == 0){
                                    
                                        ComInit.synchronize("insert relation data","begin" + relation.id + "");
                                    
                                        if(site==0){
                                            relation.fields = EProvenance.insertRelationData(relation, this.expDir);
                                        }else{
                                            relation.fields = EProvenance.getfileds(relation);
                                        }

                                        ComInit.synchronize("insert relation data","end" + relation.id + "");
                                    }
                                    
                                }else{
                                    newStatus = EActivity.StatusType.BLOCKED;
                                }
                                if(ChironUtils.checkFile(this.expDir + "ifile_" + relation.filename)){
                                    EProvenance.insertInputData(relation, expDir);
                                }
                            }
                        }
                    }
                }
                
                activity.status = newStatus;
            }
        }

        for (int i = activities.size() - 1; i >= 0; i--) {
            EActivity activity = activities.get(i);
            if ((activity.status == EActivity.StatusType.READY) || (activity.status == EActivity.StatusType.RUNNING)) {
                if (runnableActivities.indexOf(activity) < 0) {
                    runnableActivities.add(activity);
                }
            }
        }
    }

    public void checkPipeline() {
        if ((model != ExecModel.DYN_FTF) && (model != ExecModel.STA_FTF)) {
            return;
        }
        for (int i = activities.size() - 1; i >= 0; i--) {
            EActivity activity = activities.get(i);
            if (activity.operation.type != Operator.MAP) {
                continue;
            }
            CRelation relation = activity.relations.get(0);
            if (relation.dependency != null) {
                CActivity dependency = relation.dependency;
                EActivity actDependent = this.getActivity(dependency.tag);
                if (actDependent.operation.type == Operator.MAP) {
                    actDependent.pipeline = activity;
                }
            }
        }
    }

    public boolean checkInputRelation(String relationName) {
        for (CRelation r : this.inputRelations) {
            if (r.name.equals(relationName)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean hasWorkflowFinished(){
        for(EActivity eact : activities){
            if(eact.status != EActivity.StatusType.FINISHED){
                try {
                    evaluateDependencies(Chiron.site);
                    return false;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        
        return true;
    }
    
    private void transferRelationFromActivity(EActivity act) throws IOException, InterruptedException {
        for (ActivityTransfer actTransfer : activityTransfer) {
            System.out.println("Activity name: "+actTransfer.activityName+" act.tag: "+act.tag);
            if(actTransfer.activityName.equals(act.tag)){
                System.out.println("write data");
                DataManager.transferData(EProvenance.db,
                        act.getOutputRelation().toLowerCase(),
                        actTransfer.destinationPath , actTransfer.destinationFilename, 
                        exeTag);
                ChironUtils.runCommand("touch READY_" + actTransfer.destinationFilename, actTransfer.destinationPath);
            }
        }
    }
}
