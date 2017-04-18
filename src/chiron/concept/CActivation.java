package chiron.concept;

import chiron.Chiron;
import chiron.ChironUtils;
import chiron.EActivation;
import chiron.EActivity;
import chiron.EFile;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author jonasdias
 */
public abstract class CActivation implements Serializable {

    String wfDir;
    String expDir;

    public abstract void instrument(EActivation activation);

    public abstract void execute(EActivation activation);

    public abstract void extract(EActivation activation);

    public abstract void pipelineData(EActivation activation);

    protected CActivation(String wfDir, String expDir) {
        this.wfDir = wfDir;
        this.expDir = expDir;
    }

    public static CActivation newInstance(EActivity act) {
        switch (act.operation.type) {
            case MAP:
                return new Map.MapActivation(act.getWorkflowDir(), act.getExperimentDir());
            case SPLIT_MAP:
                return new SplitMap.SplitMapActivation(act.getWorkflowDir(), act.getExperimentDir());
            case REDUCE:
                return new Reduce.ReduceActivation(act.getWorkflowDir(), act.getExperimentDir());
            case FILTER:
                return new Filter.FilterActivation(act.getWorkflowDir(), act.getExperimentDir());
        }
        return null;
    }

    protected void instrumentFiles(List<EFile> files, String workspace, HashMap<String, String> fieldValues) throws IOException {
        for (int i = 0; i < files.size(); i++) {
            EFile templateFile = files.get(i);

            String fileName = templateFile.getPath();

            if (templateFile.instrumented) {
                String textFile = ChironUtils.ReadFile(fileName);
                textFile = processTags(textFile, wfDir, expDir, fieldValues);
                ChironUtils.WriteFile(fileName, textFile);
            }
        }
    }

    protected String processTags(String text, String wfDir, String expDir, HashMap<String, String> fieldValues) {
        String result = text;
        
        if(result!=null){
            for (String key : fieldValues.keySet()) {
//                System.out.println("[CActivation] key: " + key + " value: " + fieldValues.get(key));
                String value = fieldValues.get(key);
                String tagValue = "%=" + key.toUpperCase() + "%";
                result = result.replaceAll(tagValue, value);
            }

            String directoryConverted = ChironUtils.correctPath(wfDir);
            result = result.replaceAll(ChironUtils.workflowTag, directoryConverted);

            if (expDir.endsWith(ChironUtils.SEPARATOR)) {
                directoryConverted = expDir.substring(0, expDir.length() - 1);
            } else {
                directoryConverted = expDir;
            }

            directoryConverted = ChironUtils.correctPath(expDir);
            result = result.replaceAll(ChironUtils.experimentTag, directoryConverted);

            if (ChironUtils.isWindows()) {
                result = result.replaceAll("/", "\\\\");
            }
        }

        return result;
    }

    protected Scanner runExtractor(EActivation activation) throws IOException, InterruptedException {
        if (activation.extractor != null) {
            if (activation.extractor.startsWith("Chiron.FileExtractor")) {
                /**
                 * TO DO: add code to call ChironUtils.fileExtractor using file
                 * extractor code already made.
                 */
            } else {
                String command = activation.extractor;
                ChironUtils.runCommand(command, activation.workspace);
                return new Scanner(new File(activation.workspace + ChironUtils.relationFile));
            }
        } else {
            return new Scanner(new File(activation.workspace + ChironUtils.relationFile));
        }
        return null;
    }
}
