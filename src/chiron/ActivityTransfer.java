package chiron;

/**
 *
 * @author vitor
 */
class ActivityTransfer {
    
    String activityName;
    String destinationPath;
    String destinationFilename;
    
    public ActivityTransfer(String activityName, String destinationPath, String destinationFilename){
        this.activityName = activityName;
        this.destinationPath = destinationPath;
        this.destinationFilename = destinationFilename;
    }
}
