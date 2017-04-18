package chiron;

import chiron.DataBase.EProvenance;
import chiron.EActivity.StatusType;
import chiron.concept.CActivation;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The EActivation class represents the minimal unit of data necessary to run an
 * instance of an EActivity of the EWorkflow. It stores its own input and output
 * data, the references to its files and general provenance data. Since this is
 * the class that is distributed over MPI, it should be as small as possible.
 *
 * @author Jonas, Eduardo, Vítor
 * @since 2010-12-25
 *
 *
 */
public class EActivation implements Serializable {

    public static EActivation WAIT_ACTIVATION = new EActivation();
    public EActivation pipelinedFrom = null;
    public Integer id = null;
    public Integer activityId = null;
    public Integer processor = null;
    public Integer exitStatus = null;
    public String commandLine = null;
    public String extractor = null;
    public String workspace = null;
    public String templateDir = null;
    public String stdErr = null;
    public String stdOut = null;
    public Date startTime = null;
    public Date endTime = null;
    public StatusType status = StatusType.READY;
    public List<EFile> files = new ArrayList<>();
    public List<EFile> ofiles = new ArrayList<>();
    public ERelation inputRelation = null;
    public ERelation outputRelation = null;
    public boolean constrained = false;
    public CActivation activator;
    public boolean instrumented = false;
    public int node = -1;
    public int site = -1;

    private void preparePipeline() {
        if (pipelinedFrom != null) {
            activator.pipelineData(this);
        }
    }

    public void executeActivation() {
        System.out.println("[activation exec] id: " + this.id + " step: begin");
        startTime = new Date();
        if (!this.instrumented) {
            initExec();
        }
//        transfer();
        System.out.println("[activation exec] id: " + this.id + " step: execute");
        synchronized (this) {
            activator.execute(this);
        }
        System.out.println("[activation exec] id: " + this.id + " step: extract");
        synchronized (this) {
            activator.extract(this);
        }
        status = EActivity.StatusType.FINISHED;
//        System.out.println("Activation Running: id: " + id);
        endTime = new Date();
        System.out.println("[activation exec] id: " + this.id + " step: end");
    }

    public boolean isConstrained() {
        return constrained;
    }

    private void initExec() {
        this.preparePipeline();
        System.out.println("[activation exec] id: " + this.id + " step: instrument");
        activator.instrument(this);
        this.instrumented = true;
    }

    public void prepare() {
        try {
            ChironUtils.createDirectory(this.workspace);
            initExec();
            manipulateFile(this.files, this.workspace);
        } catch (IOException ex) {
            Logger.getLogger(EActivation.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(EActivation.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void manipulateFile(List<EFile> files, String destination) throws IOException, InterruptedException {
        try {
            for (EFile f : files) {
                System.out.println("manipulatefile: " + f.getFileDir() + f.getFileName() + " for activation: " + this.id + " (work space: " + this.workspace + ")");
                if (f.getFileName() != null) {
                    if (f.getFsite() != Chiron.site) {
                        System.out.println("manipulatefile: " + f.getFileDir() + f.getFileName() + " for activation: " + this.id + " (work space: " + this.workspace + ") is not in this site");
//                    this.preparePipeline();
//                    activator.instrument(this);
//                    this.instrumented = true;
                        f.transfer(destination);
                        f.setFsite(Chiron.site);
                        this.commandLine = this.commandLine.replace(f.getFileDir(), destination);
                    } else {
                        String origin = f.getPath();

                        if (f.fileOper != null && (f.fileOper.equals(EFile.Operation.COPY) || f.fileOper.equals(EFile.Operation.COPY_DELETE))) {
                            System.out.println("copy file: " + origin + " to " + destination);
                            ChironUtils.copyFile(origin, destination);
                        } else {
                            System.out.println("move file: " + origin + " to " + destination);
                            ChironUtils.moveFile(origin, destination);
                        }
                    }
                    f.setFileDir(destination);
                }
            }

            files = EProvenance.getFileOSite(destination);
            System.out.println("manipulatefile (ofile) workspace: " + destination);
            for (EFile f : files) {
                System.out.println("manipulatefile (ofile): " + f.getFileDir() + f.getFileName() + " for activation: " + this.id + " (work space: " + this.workspace + ") is not in this site");
                f.transfer(destination);
            }

        } catch (SQLException ex) {
            Logger.getLogger(EActivation.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

//    private void transfer(){
//        for(EFile ef: files){
//            if(ef.fsite != Chiron.site){
//                if(ef.getFileName()!=null)
//                ef.transfer(workspace);
//                ef.fsite = Chiron.site;
//            }
//        }
//    }
    public boolean hasFile(String fieldName) {
        for (EFile file : files) {
            if ((file.fieldName != null) && (file.fieldName.equals(fieldName))) {
                return true;
            }
        }
        return false;
    }

    public EFile getFile(String fieldName) {
        for (EFile file : files) {
            if (file.fieldName.equalsIgnoreCase(fieldName)) {
                return file;
            }
        }
        return null;
    }
    
    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof EActivation))
            return false;
        if (obj == this)
            return true;

        EActivation oact = (EActivation) obj;
        return this.id.equals(oact.id)&&this.site == oact.site;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.id);
        hash = 59 * hash + this.site;
        if(hash < 0)
            hash = -1 * hash;
        return hash;
    }
    
}
