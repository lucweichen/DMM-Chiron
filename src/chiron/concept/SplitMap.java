package chiron.concept;

import chiron.ChironUtils;
import chiron.DataBase.EProvenance;
import chiron.EActivation;
import chiron.EFile;
import chiron.ERelation;
import java.io.Serializable;
import java.util.*;

/**
 *
 * @author jonasdias.
 */
public class SplitMap extends Map {

    protected SplitMap(Operator type) {
        super(type);
    }

    public SplitMap() {
        this(Operator.SPLIT_MAP);
    }

    public static class SplitMapActivation extends MapActivation implements Serializable {
        
        SplitMapActivation(String workflowDir, String experimentDir) {
            super(workflowDir, experimentDir);
        }

        @Override
        @SuppressWarnings("CallToThreadDumpStack")
        public void extract(EActivation activation) {
            try {
                Scanner s = this.runExtractor(activation);
                String line = s.nextLine();
                String fields[] = line.split(ChironUtils.relationSeparator);
                java.util.Map isfile = EProvenance.isFile(fields, activation.activityId);
                TreeMap<Integer, String[]> relation = new TreeMap<Integer, String[]>();
                activation.ofiles = new ArrayList<EFile>();
                int t = 0;
                //for each line of the relation it adds a pair Key->Tuple to the TreeMap.
                while (s.hasNextLine()) {
                    line = s.nextLine();
                    String values[] = line.split(ChironUtils.relationSeparator);
                    if (fields.length != values.length) {
                        throw new Exception("The number of extracted values do not "
                                + "correspond to the number of extracted fields. "
                                + "Check the ERelation.txt extracted file.");
                    }

                    HashMap<String, String> extractedTuple = new HashMap<String, String>();
                    for (int i = 0; i < fields.length; i++) {
                        extractedTuple.put(fields[i], values[i]);
                        if(isfile.get(fields[i].toLowerCase()).equals("file")){
                            EFile f = new EFile(values[i].substring(0,values[i].lastIndexOf("/")),values[i].substring(values[i].lastIndexOf("/")+1), activation.site);
                            activation.ofiles.add(f);
                        }
                    }
                    //outputTuple stores the tuple of the output Relation
                    LinkedHashMap<String, String> outputTuple = new LinkedHashMap<String, String>();
                    for (String field : activation.outputRelation.fields) {
                        if (extractedTuple.containsKey(field)) {
                            outputTuple.put(field, extractedTuple.get(field));
                        } else if (activation.inputRelation.getFirst().containsKey(field)) {
                            outputTuple.put(field, activation.inputRelation.getFirst().get(field));
                        } else {
                            throw new Exception("The value for the " + field + " is missing.");
                        }
                    }
                    List<EFile> files = activation.files;
                    activation.files = new ArrayList<EFile>();
                    for (EFile file : files) {
                        if (outputTuple.containsKey(file.fieldName.toUpperCase())) {
                            EFile f = new EFile(false, file.fieldName.toUpperCase(), EFile.Operation.MOVE);
                            f.setFileName(outputTuple.get(file.fieldName.toUpperCase()));
                            f.setFileDir(activation.workspace);
                            f.setFsite(activation.site);
                            f.getSize();
                            activation.files.add(f);
                            outputTuple.put(f.fieldName.toUpperCase(), f.getPath());
                        }
                        if(file.getFileName()!=null&&file.getFileDir()!=null){
                            file.setFsite(activation.site);
                            activation.ofiles.add(file);
                        }
                    }
                    String[] tuple = new String[outputTuple.values().size()];
                    int i = 0;
                    for (String tupleValue : outputTuple.values()) {
                        tuple[i++] = tupleValue;
                    }
                    relation.put(t++, tuple);
                }
                activation.outputRelation = new ERelation(activation.outputRelation.name, activation.outputRelation.fields, relation);
//                System.err.println("file number: " + activation.ofiles.size());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}