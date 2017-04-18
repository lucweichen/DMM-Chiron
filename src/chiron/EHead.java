package chiron;

import chiron.EActivity.StatusType;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The EHeads are threads responsible to run the activations.
 *
 * @author Jonas, Eduardo
 * @since 2010-12-25
 */
public class EHead extends Thread {

    public EActivation[] activation = null;
    public EActivation[] activationFinished = null;
    public StatusType status = StatusType.READY;
    public boolean constrained = false;
    public int core = 1;
    private boolean running;

    public EHead(int core) {
        super();
        this.core = core;
    }

    @Override
    @SuppressWarnings({"SleepWhileHoldingLock"})
    public void run() {
        while (status != StatusType.FINISHED) {
            if(activation == null){
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException ex) {
                        Logger.getLogger(EHead.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            running = true;
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance().getTime());
            System.out.println(timeStamp + "[core] core number: " + core);
            if (!this.gotConstrained()) {
                if (status != StatusType.BLOCKED) {
                    /**
                     * The Head is blocked while constrained equals TRUE and
                     * the EBody threads are still busy processing previous
                     * activations.
                     */
                    if ((activation != null)) {
                        for (int i = 0; i < activation.length; i++) {
                            if (activation[i].status == StatusType.READY) {
//                                System.out.println("Thread Running: id: " + activation[i].id + "; process: " + core);
                                activation[i].status = EActivity.StatusType.RUNNING;
//                                exe = new Executor(activation[i]);
//                                exe.start();
//                                System.out.println("files for " + activation[i].id);
//                                for(EFile ef: activation[i].files){
//                                    System.out.println("file: " + ef.getFileName());
//                                    if(ef.fsite != Chiron.site){
//                                        ef.transfer();
//                                    }
//                                }
//                                System.out.println("end files for " + activation[i].id );
                                activation[i].executeActivation();
                                System.out.println("activation id: " + activation[i].id + " head is null core: " + core + " activation num: " + activation.length);
                            }
                        }
//                        try {
//                            exe.join();
//                        } catch (InterruptedException ex) {
//                            Logger.getLogger(EHead.class.getName()).log(Level.SEVERE, null, ex);
//                        }
                        if (this.constrained) {
                            this.constrained = false;
                        }
                        activationFinished = activation;
                        activation = null;
                    } else {
//                        ChironUtils.sleep();
                    }
                } else {
//                    ChironUtils.sleep();
                }
            }
            running = false;
        }
    }

    private synchronized boolean gotConstrained() {
        if (!this.constrained && activation != null) {
            for (EActivation t : activation) {
                if (t.isConstrained()) {
                    this.constrained = true;
                    this.status = StatusType.BLOCKED;
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized boolean isWaiting() {
        if (this.status.equals(StatusType.BLOCKED)) {
            return true;
        } else if (this.activation == null && this.activationFinished == null) {
            return true;
        } else {
            return false;
        }
    }
    
    public boolean isrunning(){
        return running;
    }
    
}
