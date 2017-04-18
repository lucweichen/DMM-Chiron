package chiron;

import chiron.DataBase.EProvenanceQueue;
import chiron.EActivity.StatusType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import mpi.MPI;

/**
 * This class control the threads on a compute node running Chiron.
 *
 * @author Jonas, Eduardo, Vítor
 * @since 2011-01-13
 */
public class EBody {

    private int MPI_size;
    private int MPI_rank;
    private List<EHead> heads;
    private int threads;
    protected EWorkflow eWorkflow;
    private EHead constrained = null;
    private int blocked = 0;

    /**
     * Construtor da classe EBody
     *
     * @param MPI_rank
     * @param threads
     */
    public EBody(int MPI_size, int MPI_rank, int threads) {
        this.MPI_size = MPI_size;
        this.MPI_rank = MPI_rank;
        heads = new ArrayList<EHead>();
        this.threads = threads;
    }

    /**
     * Esse método inicia as threads e as mantém alimentadas com tarefas até que
     * todas as tarefas sejam finalizadas
     *
     * @throws InterruptedException
     * @throws SQLException
     */
    @SuppressWarnings("SleepWhileHoldingLock")
    public void execute() throws InterruptedException, SQLException, Exception {
        //create the threads
        for (int i = 0; i < threads; i++) {
            EHead head = new EHead(i + 1);
            heads.add(head);
            head.start();
        }

        //do it until you have threads available on your list of threads
        boolean receivedWait = false;
        int WaitCounter = 0;
        int WaitThreshold = 0;
        while (heads.size() > 0) {
//            System.out.println("size: " + heads.size());
            Iterator<EHead> iter = heads.iterator();
            while (iter.hasNext()) {
//                System.out.println("tic");
                EHead head = iter.next();
                if (head.isrunning()) {
                    continue;
                }
                if (this.constrained == null) {
                    if (head.constrained == true) {
                        this.constrained = head;
                        continue;
                    }
                } else {
                    if (head.isWaiting() && this.constrained != head) {
                        head.status = StatusType.BLOCKED;
                        this.blocked++;
                    }
                    if (blocked == heads.size() - 1) {
                        this.constrained.status = StatusType.READY;
                        blocked = 0;
                    }
                }
                System.out.println("for core: " + head.core + "receivedWait: " + receivedWait + " head activation:" + Arrays.toString(head.activation) + "head activationFinished:" + Arrays.toString(head.activationFinished));
                if ((head.activation == null) && (!receivedWait || head.activationFinished != null)) {
                    System.out.println("for core: " + head.core + " sent activations");
                    EMessage sendMsg = new EMessage();
                    sendMsg.type = EMessage.Type.REQUEST_ACTIVATION;
                    sendMsg.activation = head.activationFinished;
                    if (sendMsg.activation != null) {
                        for (EActivation ea : sendMsg.activation) {
                            System.out.println("for core: " + head.core + " sent activations id: " + ea.id);
                        }
                    }
                    head.activationFinished = null;
                    sendMsg.MPI_rank = this.MPI_rank;

                    //se o body está no modo constrained, mas a thread constrained já terminou, liberar outras threads
                    if (this.constrained == head) {
                        this.constrained = null;
                        this.blocked = 0;
                        if (!aHeadIsConstrained()) {
                            for (EHead h : heads) {
                                if (!h.constrained) {
                                    h.status = StatusType.READY;
                                }
                            }
                        }
                    }
                    //se alguma head está como constrained, bloquear as threads que terminaram suas tarefas
                    if (this.constrained != null && head.status != StatusType.BLOCKED) {
                        head.status = StatusType.BLOCKED;
                        sendMsg.type = EMessage.Type.STORE;
                    }
                    String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance().getTime());
                    System.out.println("send request begin: " + timeStamp );
                    EMessage recvMsg = this.sendRequest(sendMsg);
                    timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance().getTime());
                    System.out.println(timeStamp + "receive message: " + recvMsg.type );
                    if (recvMsg.type.equals(EMessage.Type.PROCESS_ACTIVATION)) {
                        synchronized (head) {
                            for (EActivation activation : recvMsg.activation) {
//                            recvMsg.activation[i].processor = this.MPI_rank*threads + head.core;
                                activation.processor = head.core;
                                System.out.println("Processor: " + head.core + "; id: " + activation.id);
                                activation.node = Chiron.machine;
                            }
                            head.activation = recvMsg.activation;
                            head.notify();
                        }
                    } else if (recvMsg.type.equals(EMessage.Type.FINISH)) {
                        head.status = EActivity.StatusType.FINISHED;
                        iter.remove();
                    } else {
                        head.activation = null;
                    }
                    if (recvMsg.type.equals(EMessage.Type.WAIT)) {
                        receivedWait = true;
                    }
                }
            }
            
            iter = heads.iterator();
            boolean hastask = false;
            while (iter.hasNext()) {
                EHead head = iter.next();
                if( (head.activation != null) || (head.activationFinished != null)){
                    hastask = true;
                    break;
                }
            }
            if(!hastask){
                Thread.sleep(1000);
            }
            if (receivedWait) {
                WaitCounter++;
//                Thread.sleep(1000);
                java.util.Date date= new java.util.Date();
                System.out.println("[" + new Timestamp(date.getTime()) + "] ebody wait");
                if (WaitCounter > WaitThreshold) {
                    receivedWait = false;
                    WaitCounter = 0;
                }
            }
        }
//        System.out.println("fini tac");
        if (MPI_rank > 0) {
            EMessage sendMsg = new EMessage();
            sendMsg.type = EMessage.Type.FINISH;
            sendMsg.activation = null;
            this.sendRequest(sendMsg);
        }
//        ChironUtils.sleep();
//        (new EProvenance()).print_get();
    }

    /**
     * Método responsável pelo envio de um pedido
     *
     * @param sendMsg
     * @return
     * @throws SQLException
     * @throws Exception
     */
    private EMessage sendRequest(EMessage sendMsg) throws SQLException, Exception {
        EMessage recvMsg = null;
        //send request locally
        if (MPI_rank == 0) {
            recvMsg = answerRequest(sendMsg);
        } else {
            //ask the listener (Listener changes the status of the activations to 1 already)
            EMessage sendMsgArray[] = new EMessage[1];
            sendMsgArray[0] = sendMsg;
            MPI.COMM_WORLD.Send(sendMsgArray, 0, 1, MPI.OBJECT, 0, 0);

            if (sendMsg.type != EMessage.Type.FINISH) {
                EMessage recvMsgArray[] = new EMessage[1];
                MPI.COMM_WORLD.Recv(recvMsgArray, 0, 1, MPI.OBJECT, 0, 0);
                recvMsg = recvMsgArray[0];
            }
        }
        return recvMsg;
    }

    /**
     * Método responsável pela resposta do pedido
     *
     * @param questionMsg
     * @return
     * @throws SQLException
     * @throws Exception
     */
    public synchronized EMessage answerRequest(EMessage questionMsg) throws SQLException, Exception {
        EMessage answerMsg = new EMessage();
        if (questionMsg.type.equals(EMessage.Type.REQUEST_ACTIVATION)) {
            if (questionMsg.activation != null) {
                if (questionMsg.activation != null) {
                    for (EActivation act : questionMsg.activation) {
                        System.out.println("Answer: activation id: " + act.id + ";MPI_rank: " + questionMsg.MPI_rank);
                    }
                }
                EProvenanceQueue.queue.handleActivations(EProvenanceQueue.ActStoreType.ADD_ACTIVATIONS, questionMsg.activation);
            }
            EActivation[] activation = eWorkflow.iterateExecution();
            if (activation != null) {
                if ((activation[0] != EActivation.WAIT_ACTIVATION)) {
                    answerMsg.type = EMessage.Type.PROCESS_ACTIVATION;
                    answerMsg.activation = activation;
                    answerMsg.MPI_rank = questionMsg.MPI_rank;
                    for (EActivation act : activation) {
                        act.prepare();
                    }
//                        System.out.println("Requestion: act id: " + act.id + ";MPI_rank: " + questionMsg.MPI_rank);
                } else if (activation[0] == EActivation.WAIT_ACTIVATION) {
                    answerMsg.type = EMessage.Type.WAIT;
                }
            } else if (activation == null && eWorkflow.hasWorkflowFinished()) {
                answerMsg.type = EMessage.Type.FINISH;
            } else {
                answerMsg.type = EMessage.Type.WAIT;
            }
        } else if (questionMsg.type.equals(EMessage.Type.STORE)) {
            if (questionMsg.activation != null) {
                EProvenanceQueue.queue.handleActivations(EProvenanceQueue.ActStoreType.ADD_ACTIVATIONS, questionMsg.activation);
                if (questionMsg.activation != null) {
                    for (EActivation act : questionMsg.activation) {
                        System.out.println("Answer: act id: " + act.id + ";MPI_rank: " + questionMsg.MPI_rank);
                    }
                }
            }
        }
//        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance().getTime());
//        System.out.println("finish request end: " + timeStamp );
        return answerMsg;
    }

    private boolean aHeadIsConstrained() {
        boolean constrain = false;
        for (EHead h : heads) {
            if (h.constrained) {
                constrain = true;
                this.constrained = h;
            }
        }
        return constrain;
    }
}
