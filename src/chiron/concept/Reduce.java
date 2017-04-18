package chiron.concept;

import chiron.DataBase.EProvenance;
import Multisite.ComInit;
import chiron.*;
import static chiron.DataBase.EProvenance.loadOrderedParameterSpace;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * TO DO: Reduce Operator still needs to be implemented.
 *
 * @author jonasdias, vitor
 */
class Reduce extends CActivity {

    CRelation input;
    CRelation output;
    ArrayList<String> aggregationFields;

    static class ReduceActivation extends CActivation implements Serializable {

        public ReduceActivation(String wfDir, String expDir) {
            super(wfDir, expDir);
        }

        @Override
        @SuppressWarnings("CallToThreadDumpStack")
        public void instrument(EActivation activation) {
            activation.stdErr = "";
            activation.stdOut = "";
            try {

                activation.templateDir = processTags(activation.templateDir, wfDir, expDir, activation.inputRelation.getFirst());
                if (activation.templateDir != null && !activation.templateDir.equals("")) {
                    ChironUtils.copyTemplateFiles(activation.templateDir, activation.workspace);
                }

                activation.commandLine = processTags(activation.commandLine, wfDir, expDir, activation.inputRelation.getFirst());
                if (activation.extractor != null) {
                    activation.extractor = processTags(activation.extractor, wfDir, expDir, activation.inputRelation.getFirst());
                }
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
                    if (extractedTuple.containsKey(field)) {
                        outputTuple.put(field, extractedTuple.get(field));
                    } else if (activation.inputRelation.getFirst().containsKey(field.toLowerCase())) {
                        outputTuple.put(field, activation.inputRelation.getFirst().get(field.toLowerCase()));
                    } else {
                        throw new Exception("The value for the " + field + " is missing.");
                    }
                }
                for (EFile file : activation.files) {
                    if (outputTuple.containsKey(file.fieldName.toUpperCase())) {
                        if (file.getFileName() == null) {
                            file.setFileName(outputTuple.get(file.fieldName.toUpperCase()));
                        }
                        file.setFileDir(activation.workspace);
                        file.setFsite(activation.site);
                        file.getSize();
                        outputTuple.put(file.fieldName.toUpperCase(), file.getPath());
                    }//else{
                    //  outputTuple.put(file.fieldName, file.getPath());
                    //}
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
            HashMap<String, String> inputTuple = new HashMap<>();
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

    public Reduce() {
        this(Operator.REDUCE);
    }

    private Reduce(Operator type) {
        super(type);
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
        List<CRelation> ret = new ArrayList<>();
        ret.add(input);
        return ret;
    }

    @Override
    public List<CRelation> getOutputRelations() {
        List<CRelation> ret = new ArrayList<>();
        ret.add(output);
        return ret;
    }

    @Override
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
            aggregationFields = EProvenance.getTextOperand(this.id);
            int numberFolder = EProvenance.loadBeginNum(input, aggregationFields, Chiron.site);
            if (numberFolder != -1) {
                ArrayList<String> keys;
                ResultSet rss;
                ResultSet rs = EProvenance.loadOrderedParameterSpace(input, aggregationFields, Chiron.site);
                
                while (rs.next()) {
                    
                    ERelation inputRelation = new ERelation(input.name, input.fields);
                    List<String> fields = EProvenance.retrieveFields(inputRelation.name);
                    String[] fValues = new String[fields.size()];
                    List<EFile> fileFields = EProvenance.getFileFields(this.getInputRelations().get(0));
                    List<EFile> outputfileFields = EProvenance.getFileFields(this.getOutputRelations().get(0));
                    String activationFolder = ChironUtils.getActivationFolder(numberFolder, folderActivity);
                    EActivation newActivation = this.createActivation(act, activationFolder);
                    newActivation.files = new ArrayList<>();

                    keys = new ArrayList<>();
                    for (String af : aggregationFields) {
                        keys.add(String.valueOf(rs.getObject(af)));
                    }

                    rss = loadOrderedParameterSpace(input.name, aggregationFields, keys);

                    while (rss.next()) {
                        for (int i = 0; i < fields.size(); i++) {
                            String f = fields.get(i);
                            Object o = rss.getObject(f);
                            if (o.getClass().equals(Double.class)) {
                                int decimalPlaces = EProvenance.getDecimalPlaces(f, act.operation.getInputRelations().get(0));
                                String d = ChironUtils.formatFloat((Double) o, decimalPlaces);
                                fValues[i] = d;
                            } else {
                                fValues[i] = String.valueOf(rss.getObject(f));
                            }
                        }
                        inputRelation.values.put(rss.getInt("ik"), fValues);

                        boolean this_site = false, begin = false;
                        EFile nefile;
                        

                        for (EFile f : fileFields) {
                            nefile = new EFile(f.instrumented,f.fieldName,f.fileOper);
                            nefile.setFileName(rss.getString(f.fieldName));
                            nefile = EProvenance.get_file_para(nefile);
                            if (!this_site && !begin) {
                                if (nefile.getFsite() == Chiron.site) {
                                    ChironUtils.manipulateFile(nefile, newActivation.workspace);
                                    nefile.setFileDir(newActivation.workspace);
                                    this_site = true;
                                } else {
                                    this_site = false;
                                }
                                begin = true;
                            } else {
                                if (this_site) {
                                    if (nefile.getFsite() != -1 && nefile.getFsite() != Chiron.site) {
                                        System.out.println("end file name: " + nefile.getFileDir() + "/" + nefile.getFileName() + " site: " + nefile.getFsite());
                                    } else {
                                        ChironUtils.manipulateFile(nefile, newActivation.workspace);
                                        f.setFileDir(newActivation.workspace);
                                    }
                                }
                            }
                            newActivation.files.add(nefile);
                            System.out.println("[debug]" + act.cactid + " file size: " + newActivation.files.size() + " add file: " + nefile.getFileName());
                        }
                    }
                    for(EFile f: newActivation.files){
                        System.out.println("[debug] reduce file: " + f.getFileName());
                    }
                    System.out.println("[debug]" + act.cactid + " file size: " + newActivation.files.size());
                    newActivation.outputRelation = new ERelation(output.name, output.fields);
                    for (EFile f : outputfileFields) {
                        if (!newActivation.hasFile(f.fieldName)) {
                            f.setFileDir(newActivation.workspace);
                            f.setFsite(Chiron.site);
                            f.fileSize = 0L;
                            newActivation.files.add(f);
                        }
                    }
                    newActivation.inputRelation = inputRelation;
                    EProvenance.storeActivation(newActivation);
                    ChironUtils.createDirectory(newActivation.workspace);
                    ChironUtils.deleteFile(ChironUtils.relationFile, newActivation.workspace);
                    String csv = newActivation.inputRelation.getCSVHeader() + newActivation.inputRelation.getCSV();
                    ChironUtils.WriteFile(newActivation.workspace + newActivation.inputRelation.name + ".hfrag", csv);
                    numberFolder++;
                }
            }
            act.numActivations = EProvenance.loadParameterSpaceNum(input.name, aggregationFields);
            act.status = EActivity.StatusType.RUNNING;
            if (Chiron.machine == 0) {
                ComInit.synchronize("generate end begin", "begin" + act.tag);
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
                ComInit.synchronize("generate begin", "end " + act.tag);
            }
        }
    }

    @Override
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

    private boolean checkAggregationValues(ArrayList<String> values, ArrayList<String> lastValues) {
        for (int i = 0; i < values.size(); i++) {
            if (!values.get(i).equals(lastValues.get(i))) {
                return false;
            }
        }

        return true;
    }

}
