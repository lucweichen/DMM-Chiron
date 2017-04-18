package chiron;

import Multisite.ComInit;
import java.io.File;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * Represents a file to be consumed by an activation.
 *
 * @author Eduardo, Jonas, Vítor.
 * @since 2010-12-25
 */
public class EFile implements Serializable, Cloneable{

    /**
     * @return the fsite
     */
    public int getFsite() {
        return fsite;
    }

    /**
     * @param fsite the fsite to set
     */
    public void setFsite(int fsite) {
        this.fsite = fsite;
    }

    public enum Operation {
        MOVE, MOVE_DELETE, COPY, COPY_DELETE
    };
    public Integer fileID = null;
    public boolean template = false;
    public boolean instrumented;
    private String fileDir = "";
    private String fileName = null;
    public Long fileSize = 0L;
    public Date fileDate = null;
    public String fieldName;
    public Operation fileOper;
    private int fsite = -1;

    public EFile(boolean instrumented, String fieldName, Operation fileOper) {
        this.instrumented = instrumented;
        this.fieldName = fieldName;
        this.fileOper = fileOper;
    }
    
    public EFile(String fileDir,String fileName, int fsite) {
        this.fileDir = fileDir;
        this.fileName = fileName;
        this.fsite = fsite;
    }

    /**
     * @param fileDir the fileDir to set
     */
    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

    /**
     * @param fileName the fileName to set
     */
    public void setFileName(String fileName) {
        if (fileName.contains(ChironUtils.SEPARATOR)) {
            fileDir = "";
            String[] split = fileName.split(ChironUtils.SEPARATOR);
            this.fileName = split[split.length - 1];
            fileDir += "/";
            for (int i = 0; i < split.length - 1; i++) {
                split[i] = split[i].trim();
                if(split[i].equals("")) {
                    continue;
                }
                fileDir += split[i] + ChironUtils.SEPARATOR;
            }
            ChironUtils.checkDir(fileDir);
        } else {
            this.fileName = fileName;
        }
    }

    public String getFileDir() {
        return fileDir;
    }

    public String getFileName() {
        return fileName;
    }

    public String getPath() {
        return fileDir + fileName;
    }
    
    public void transfer(String ddir) {
        System.out.println("filename: " + fileDir+fileName);
        System.out.println("fsite: " + getFsite());
        ComInit.getFile(fileDir+fileName, ddir + fileName, getFsite());
    } 
    
    public void getSize(){
        if(!fileDir.equals("") && fileName != null){
            File myFile = new File (fileDir+fileName);
            fileSize = myFile.length();
        }
    }
    
    
    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof EFile))
            return false;
        if (obj == this)
            return true;

        EFile ofile = (EFile) obj;
        return this.fileDir.equals(ofile.fileDir)&&this.fileName.equals(ofile.fileName);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 71 * hash + Objects.hashCode(this.fileDir);
        hash = 71 * hash + Objects.hashCode(this.fileName);
        if(hash < 0)
            hash = -1 * hash;
        return hash;
    }
    
    @Override
    public EFile clone() throws CloneNotSupportedException {
        return (EFile) super.clone();
    }
    
}
