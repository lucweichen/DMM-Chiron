package chiron.DataBase;

import Multisite.ComInit;
import chiron.Chiron;
import chiron.EActivation;
import chiron.EActivity.StatusType;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import vs.database.M_DB;

/**
 *
 * @author Eduardo
 */
public class EProvenanceQueue extends Thread {

    public static EProvenanceQueue queue;

    public enum ActStoreType {

        ADD_ACTIVATIONS, REMOVE_ACTIVATIONS
    }
    public M_DB db = null;
    public StatusType status = StatusType.READY;
    ArrayList<EActivation> activations = new ArrayList<>();

    public synchronized EActivation[] handleActivations(ActStoreType Oper, EActivation[] chironActivations) {
        EActivation result[] = null;
        if (Oper == ActStoreType.ADD_ACTIVATIONS) {
            if (chironActivations != null) {
                for (EActivation act : chironActivations) {
                    System.out.println("handleActivations activation id: " + act.id);
                }
            }
            activations.addAll(Arrays.asList(chironActivations));
        } else if (Oper == ActStoreType.REMOVE_ACTIVATIONS) {
            result = new EActivation[activations.size()];
            for (int i = 0; i < activations.size(); i++) {
                result[i] = activations.get(i);
            }
            activations.clear();
        }
        return result;
    }

    @Override
    @SuppressWarnings({"SleepWhileHoldingLock"})
    public void run() {
        while (status != StatusType.FINISHED) {
            storeActivations();
//            ChironUtils.sleep();
        }
    }

    @SuppressWarnings("CallToThreadDumpStack")
    public void storeActivations() {
        if (db == null) {
            return;
        }
        EActivation[] activationList = handleActivations(ActStoreType.REMOVE_ACTIVATIONS, null);
        if (activationList.length != 0) {
            System.out.println("activation List number: " + activationList.length);
        }
        for (EActivation activationList1 : activationList) {
            java.util.Date date = new java.util.Date();
            System.out.println("[" + new Timestamp(date.getTime()) + "] storing activation begin: id: " + activationList1.id);
            try {
                if (Chiron.cache.contains("disRepDyn") || Chiron.cache.equals("dislocal")) {
                    if(Chiron.site !=0)
                        ComInit.lpro.storeActivationLocal(activationList1);
                    else
                        ComInit.lpro.storeActivation(activationList1);
                } else {
                    if (Chiron.cache.contains("DHT")) {
                        int site = activationList1.hashCode() % 3;
                        if(site == 0)
                            ComInit.getSite(site).db.storeActivation(activationList1);
                        else
                            ComInit.getSite(site).db.storeActivationLocal(activationList1);
                        if( (activationList1.site == 0) && (site != 0)){
                            EProvenance.setActivationFinish(activationList1.id);
                        }
                    }
                    if (Chiron.cache.contains("disRep")) {
                        if( activationList1.hashCode() % 3 != Chiron.site ){
                            activationList1.outputRelation.values.clear();
                            if(Chiron.site !=0)
                                ComInit.lpro.storeActivationLocal(activationList1);
                            else
                                ComInit.lpro.storeActivation(activationList1);
                        }
                    }
                    if (Chiron.cache.contains("centralized")) {
                        EProvenance.storeActivation(activationList1);
                    }
                }
                date = new java.util.Date();
                System.out.println("[" + new Timestamp(date.getTime()) + "] storing activation end: id: " + activationList1.id);
            } catch (SQLException | IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
