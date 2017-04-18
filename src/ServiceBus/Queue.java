/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ServiceBus;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.services.servicebus.*;
import com.microsoft.windowsazure.services.servicebus.models.*; 
import com.microsoft.windowsazure.exception.ServiceException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class Queue {
    
    public final static String queueName = "msitechiron";
    public final static String issuer = "owner";
    public final static String key = "kMn2iXiM89BByXTUt0gP5ujxiN2YmnxqIQNkWr5+QSE=";
    public final static String sasKey = ".servicebus.windows.net";
    public final static String serviceBusRootUri = "-sb.accesscontrol.windows.net/WRAPv0.9";
    public static ServiceBusContract service = null;
    private String name;
    
    public Queue(String name){
        this.name = name + "queue";
        init(this.name);
    }
    
    public String getName(){
        return name;
    }
    
    private static void init(String name){
        getService();
        createQueue(name);
    }
    
    public static void getService(){
//        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader(); 
        // Change context classloader to class context loader   
//        Thread.currentThread().setContextClassLoader(AzureManagementServiceDelegate.class.getClassLoader));
        try{
            Configuration config = ServiceBusConfiguration.configureWithSASAuthentication(
                queueName,
                "RootManageSharedAccessKey",
                key,
                sasKey);

            service = ServiceBusService.create(config);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
    private static CreateQueueResult createQueue(String name){
        try {
            if(!isIn(name)){
                QueueInfo queueInfo = new QueueInfo(name);
                return service.createQueue(queueInfo);
            }
        } catch (NoService ex) {
            Logger.getLogger(Queue.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ServiceException ex) {
            Logger.getLogger(Queue.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    private static boolean isIn(String name) throws NoService, ServiceException{
        if(service!=null){
            ListQueuesOptions options = new ListQueuesOptions();
            options.setTop(100);
            options.setSkip(0);
            ListQueuesResult queuesResult = new ListQueuesResult();
            queuesResult = service.listQueues(options);
            List<QueueInfo> list = queuesResult.getItems();
            for (QueueInfo info : list)
            {
               if(info.getPath().equals(name))
                   return true;
            }
        }else{
            throw new NoService();
        }
        return false;
    }
    
    public void sendMessage(String message,String id){
        try{
            System.out.println("send message: " + message + " id: " + id);
            BrokeredMessage sendMessage = new BrokeredMessage(message);
            sendMessage.setLabel(id);
            service.sendQueueMessage(name, sendMessage);
        }
        catch (ServiceException e) 
        {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
    
    public String getMessage(boolean delete){
        String result =  null;
        try{
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            if(delete)
                opts.setReceiveMode(ReceiveMode.RECEIVE_AND_DELETE);
            else
                opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
            
            ReceiveQueueMessageResult resultQM = 
                        service.receiveQueueMessage(name, opts);
            BrokeredMessage message = resultQM.getValue();
            if (message != null && message.getMessageId() != null){
                byte[] b = new byte[200];
                int numRead = message.getBody().read(b);
                while (-1 != numRead){
                    if(result == null)
                        result = (new String(b)).trim();
                    else
                        result += (new String(b)).trim();
                    numRead = message.getBody().read(b);
                }
            }
            if(message!=null && message.getLabel()!=null)
                System.out.println("get queue message: " + result + " id: " + message.getLabel().trim());
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        return result;
    }
    
    public String getMessage(boolean delete, String id){
        System.out.println("getting message id: " + id);
        String result =  null;
        try{
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
            
            ReceiveQueueMessageResult resultQM = 
                        service.receiveQueueMessage(name, opts);
            BrokeredMessage message = resultQM.getValue();
            while(message != null && message.getLabel() != null && !message.getLabel().trim().equals(id)){
                System.out.println("got message id: " + message.getLabel().trim());
                resultQM = service.receiveQueueMessage(name, opts);
                message = resultQM.getValue();
            }
            if (message != null && message.getMessageId() != null){
                byte[] b = new byte[200];
                int numRead = message.getBody().read(b);
                while (-1 != numRead){
                    if(result == null)
                        result = (new String(b)).trim();
                    else
                        result += (new String(b)).trim();
                    numRead = message.getBody().read(b);
                }
            }
            if(delete && message!=null && service!=null && message.getLabel() != null)
                service.deleteMessage(message);
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        System.out.println("get queue message: " + result);
        return result;
    }
    
    public String getMessage(boolean delete, List<String> ids){
        String result =  null;
        try{
            boolean labelEqual = false;
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
            
            ReceiveQueueMessageResult resultQM = 
                        service.receiveQueueMessage(name, opts);
            BrokeredMessage message;
            do{
                message = resultQM.getValue();
                if(message != null){
                    labelEqual = false;
                    for(String id: ids){
                        if(message.getLabel().trim().equals(id))
                            labelEqual = true;
                    }
                }else{
                    labelEqual = false;
                }
//                System.out.println("got message id: " + message.getLabel().trim());
            }while(!labelEqual);
            if (message != null && message.getMessageId() != null){
                byte[] b = new byte[200];
                int numRead = message.getBody().read(b);
                while (-1 != numRead){
                    if(result == null)
                        result = (new String(b)).trim();
                    else
                        result += (new String(b)).trim();
                    numRead = message.getBody().read(b);
                }
            }
            if(delete && message!=null)
                service.deleteMessage(message);
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        System.out.println("get queue message: " + result);
        return result;
    }

    public void deleteQueue(){
        try {
            while(getMessage(true)!=null){
                getMessage(true);
            }
            service.deleteQueue(name);
        } catch (ServiceException ex) {
            Logger.getLogger(Queue.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public List<String> scan(){
        List<String> result = null;
        String message = getMessage(false);
        while(message != null){
            if(result == null)
                result = new ArrayList<String>();
            result.add(message);
            message = getMessage(false);
        }
        return result;
    }
    
    public void deleteAll(){
        String message = getMessage(true);
        while(message != null){
            message = getMessage(false);
        }
    }
    
}
