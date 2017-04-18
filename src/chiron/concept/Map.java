package chiron.concept;

import chiron.DataBase.EProvenance;
import Multisite.ComInit;
import chiron.*;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author jonasdias
 */
public class Map extends CActivity {

    CRelation input;
    CRelation output;

    protected Map(Operator type) {
        super(type);
    }

    public Map() {
        this(Operator.MAP);
    }

    public static class MapActivation extends CActivation implements Serializable {

        MapActivation(String wfDir, String expDir) {
            super(wfDir, expDir);
        }

        @Override
        @SuppressWarnings("CallToThreadDumpStack")
        public void instrument(EActivation activation) {
            activation.stdErr = "";
            activation.stdOut = "";
            try {
                HashMap<String, String> tuple = activation.inputRelation.getFirst();
                for (EFile file : activation.files) {
                    tuple.put(file.fieldName, file.getPath());
                }

                activation.templateDir = processTags(activation.templateDir, wfDir, expDir, tuple);
                if (activation.templateDir != null && !activation.templateDir.equals("")) {
                    ChironUtils.copyTemplateFiles(activation.templateDir, activation.workspace);
                }

                instrumentFiles(activation.files, wfDir, tuple);
                System.out.println("[commandLine] " + activation.commandLine);
                activation.commandLine = processTags(activation.commandLine, wfDir, expDir, tuple);
                System.out.println("[commandLine] " + activation.commandLine);
                if (activation.extractor != null) {
                    activation.extractor = processTags(activation.extractor, wfDir, expDir, tuple);
                }
//                ChironUtils.deleteFile(ChironUtils.relationFile, activation.workspace);
            } catch (Exception ex) {
                activation.stdErr += ex.getStackTrace();
                ex.printStackTrace();
            }
        }

        @Override
        @SuppressWarnings("CallToThreadDumpStack")
        public void execute(EActivation activation) {
            String workspace = activation.workspace;
            String command = activation.commandLine + " > " + workspace + ChironUtils.resultFile + " 2> " + workspace + ChironUtils.errorFile;
            try {
                activation.exitStatus = ChironUtils.runCommand(command, workspace);
                activation.stdErr += ChironUtils.ReadFile(workspace + ChironUtils.errorFile);
                activation.stdOut += ChironUtils.ReadFile(workspace + ChironUtils.resultFile);
            } catch (Exception ex) {
                activation.stdErr += ex.getStackTrace();
                ex.printStackTrace();
            }
        }

        @Override
        @SuppressWarnings("CallToThreadDumpStack")
        public void extract(EActivation activation) {
            try {
                Scanner s = this.runExtractor(activation);

                String line = s.nextLine();
                String fields[] = line.split(ChironUtils.relationSeparator);
                line = s.nextLine();
                String values[] = line.split(ChironUtils.relationSeparator);
                if (fields.length != values.length) {
                    throw new Exception("The number of extracted values do not "
                            + "correspond to the number of extracted fields. "
                            + "Check the ERelation.txt extracted file.");
                }
                //extractedTuple stores the values extracted to ERelation.txt
                HashMap<String, String> extractedTuple = new HashMap<String, String>();
                for (int i = 0; i < fields.length; i++) {
                    extractedTuple.put(fields[i], values[i]);
                }

                //outputTuple stores the tuple of the output Relation
                LinkedHashMap<String, String> outputTuple = new LinkedHashMap<String, String>();
                for (String field : activation.outputRelation.fields) {
                    field = field.toUpperCase();
                    if (extractedTuple.containsKey(field)) {
                        outputTuple.put(field, extractedTuple.get(field));
                    } else if (activation.inputRelation.getFirst().containsKey(field)) {
                        outputTuple.put(field, activation.inputRelation.getFirst().get(field));
                    } else {
                        throw new Exception("The value for the " + field + " is missing.");
                    }
                }
                for (EFile file : activation.files) {
                    System.out.println("[extract 1] activation: " + activation.id + "file: " + file.getFileName());
                    if (outputTuple.containsKey(file.fieldName.toUpperCase())) {
                        if (file.getFileName() == null) {
                            file.setFileName(outputTuple.get(file.fieldName.toUpperCase()));
                        }
                        System.out.println("[extract 2] activation: " + activation.id + "file: " + file.getFileName() + " workspace: " + activation.workspace + " fieldname: " + file.fieldName);
                        file.setFileDir(activation.workspace);
                        file.setFsite(activation.site);
                        file.getSize();
                        System.out.println("[extract 3] file: " + file.getFileName() + " file dir: " + file.getFileDir());
                        outputTuple.put(file.fieldName.toUpperCase(), file.getPath());
                    } //else if (activation.outputRelation.fields){
                    //  outputTuple.put(file.fieldName, file.getPath());
                    //}
                }
                for (EFile file: activation.files){
                    System.out.println("[file debug] File name: " + file.getFileName() + " file dir: " + file.getFileDir() + " file site: " + file.getFsite());
                }
                activation.outputRelation = new ERelation(activation.outputRelation.name, activation.inputRelation.getFirstKey(), outputTuple);
            } catch (Exception ex) {
                activation.stdErr += "Extractor Error:" + ex.getStackTrace();
                ex.printStackTrace();
            }
        }

        @Override
        public void pipelineData(EActivation activation) {
            HashMap<String, String> pipedOutput = activation.pipelinedFrom.outputRelation.getFirst();
            Integer k = activation.pipelinedFrom.outputRelation.getFirstKey();
            Integer newKey = k;
            HashMap<String, String> inputTuple = new HashMap<String, String>();
            for (String field : activation.inputRelation.fields) {
                inputTuple.put(field, pipedOutput.get(field));
                EFile f = activation.getFile(field);
                if (f != null) {
                    f.setFileName(pipedOutput.get(field));
                }
            }
            activation.inputRelation = new ERelation(activation.inputRelation.name, newKey, inputTuple);
        }
    }

    @Override
    public void addInput(CRelation relations) {
        input = relations;
    }

    @Override
    public void addOutput(CRelation relations) {
        output = relations;
    }

    @Override
    public List<CRelation> getInputRelations() {
        List ret = new ArrayList();
        ret.add(input);
        return ret;
    }

    @Override
    public List<CRelation> getOutputRelations() {
        List ret = new ArrayList();
        ret.add(output);
        return ret;
    }

    @Override
    @SuppressWarnings("CallToThreadDumpStack")
    public void generateActivations(EActivity act, String wfDir, String expDir) throws Exception {
        if (input == null) {
            //something is wrong
            throw new NullPointerException("The input relation for activity" + act.tag + " is not available in the list of relations.");
        } else {
            if (Chiron.machine == 0) {
                ComInit.synchronize("generate begin", "begin " + act.tag);
            }
            if (ChironUtils.isMasterSite()) {
                checkDependencies(input);
            }
            if (Chiron.machine == 0) {
                ComInit.synchronize("generate begin", "end " + act.tag);
            }
            String folderActivity = expDir + this.tag + ChironUtils.SEPARATOR;
            ChironUtils.createDirectory(folderActivity);
            if (ChironUtils.isMasterSite()) {
//            ResultSet rs = EProvenance.loadParameterSpace(input, ComInit.sitesName.size(), Chiron.site);
                ResultSet rs = EProvenance.loadParameterSpace(input);
                boolean this_site = false, begin = false;

//                int generateNum = EProvenance.loadParameterSpaceNum(input);
//                System.out.println("parameter number: " + generateNum);
                while (rs.next()) {
                    this_site = false;
                    begin = false;
                //this new ChironUtils methods can be reimplemented to support a large number of folders creation. For now
                    //it is still simple, creating a folder for each activation in the activity directory.
                    String activationFolder = ChironUtils.getActivationFolder(rs.getInt("ik") - 1, folderActivity);
                    System.out.println("activation id: " + (rs.getInt("ik") - 1));
                    EActivation newActivation = this.createActivation(act, activationFolder);

                    newActivation.inputRelation = new ERelation(this.input.name, this.input.fields);//inputRel;
                    newActivation.inputRelation.putKey(rs.getInt("ik"));

                    List<EFile> fileFields = EProvenance.getFileFields(this.getInputRelations().get(0));

                    //CActivity dependency = input.dependency;
                    for (EFile f : fileFields) {
                        
                        f.setFileName(rs.getString(f.fieldName));
                        EFile tempf = f;
                        f = EProvenance.get_file_para(f);
//                        if(f.getFileName() == null){
//                        System.out.println("[debug0] filename: " + f.getFileName() + " fsite: " + f.getFsite());
                        if (f != null) {
                            if (!this_site && !begin) {
//                            System.out.println("[debug1] filename: " + f.getFileName() + " fsite: " + f.getFsite());
                                if (f.getFsite() == Chiron.site) {
//                                System.out.println("[debug1.5] filename: " + f.getFileName() + " fsite: " + f.getFsite());
                                    ChironUtils.manipulateFile(f, newActivation.workspace);
//                                System.out.println("[debug2] filename: " + f.getFileName() + " fsite: " + f.getFsite());
                                    f.setFileDir(newActivation.workspace);
//                                System.out.println("[debug3] filename: " + f.getFileName() + " fsite: " + f.getFsite());
                                    this_site = true;
                                } else {
                                    this_site = false;
                                }
//                            System.out.println("[debug4] filename: " + f.getFileName() + " fsite: " + f.getFsite());
                                begin = true;
                            } else {
                                if (this_site) {
                                    if (f.getFsite() != -1 && f.getFsite() != Chiron.site) {
//                                    System.out.println("end file name: " + f.getFileDir() + "/" + f.getFileName() + " site: " + f.fsite);
//                                    f.transfer();
                                    } else {
                                        ChironUtils.manipulateFile(f, newActivation.workspace);
                                        f.setFileDir(newActivation.workspace);
                                    }
                                }
                            }
                        } else {
                            f = tempf;
                        }
//                        System.out.println("[debug5] filename: " + f.getFileName() + " fsite: " + f.getFsite());
//                        }
                        newActivation.files.add(f);
                    }

                    ERelation outputRel = new ERelation(output.name, output.fields);
                    newActivation.outputRelation = outputRel;
                    List<EFile> outputfileFields = EProvenance.getFileFields(this.getOutputRelations().get(0));
                    for (EFile f : outputfileFields) {
                        if (!newActivation.hasFile(f.fieldName)) {
                            f.setFileDir(newActivation.workspace);
                            f.setFsite(Chiron.site);
                            f.fileSize = 0L;
                            newActivation.files.add(f);
                        }
                    }
//                if (this_site) {
                    EProvenance.storeActivation(newActivation);
//                }
//                numActivations++;
                }
//                act.numActivations = numActivations;
            }
            act.numActivations = EProvenance.loadParameterSpaceNum(input);
            act.status = EActivity.StatusType.RUNNING;
            if (Chiron.machine == 0) {
                ComInit.synchronize("generate end begin", "begin " + act.tag);
            }
            if (ChironUtils.isMasterSite()) {
                EProvenance.storeActivity(act);
            }
            if (Chiron.machine == 0) {
                ComInit.synchronize("generate end middle", "middle" + act.tag);
            }
            if (Chiron.site != 0) {
                act.id = EProvenance.getActivityId(act.tag);
            }
            if (Chiron.machine == 0) {
                ComInit.synchronize("generate end end", "end " + act.tag);
            }
        }
    }

    @Override
    @SuppressWarnings("CallToThreadDumpStack")
    public EActivation createPipelineActivation(String expDir, String wfDir, EActivity act, EActivity dep, EActivation activation) {
        String folderActivation = activation.workspace.replace(act.tag, dep.tag);
        ChironUtils.createDirectory(expDir + dep.operation.tag);

        EActivation depActivation = this.createActivation(dep, folderActivation);

        depActivation.inputRelation = new ERelation(this.input.name, this.input.fields);
        depActivation.outputRelation = new ERelation(this.output.name, this.output.fields);
        try {
            List<EFile> fileFields = EProvenance.getFileFields(this.getInputRelations().get(0));
            for (EFile f : fileFields) {
                depActivation.files.add(f);
            }
            fileFields = EProvenance.getFileFields(this.getOutputRelations().get(0));
            for (EFile f : fileFields) {
                if (!depActivation.hasFile(f.fieldName)) {
                    depActivation.files.add(f);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        depActivation.activator = CActivation.newInstance(act);
        return depActivation;
    }
}
