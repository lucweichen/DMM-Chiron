/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ServiceBus;

import static ServiceBus.Queue.service;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ListSubscriptionsResult;
import com.microsoft.windowsazure.services.servicebus.models.ListTopicsOptions;
import com.microsoft.windowsazure.services.servicebus.models.ListTopicsResult;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveSubscriptionMessageResult;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luc
 */
public class Topic {
    
    private String name;
    
    public Topic(String name){
        this.name = name + "topic";
        init(this.name);
    }
    
    private void init(String name){
        if(Queue.service == null){
            Queue.getService();
        }
        createTopic(name);
    }
    
    private boolean hasTopic(String name){
        try {
            ListTopicsOptions options = new ListTopicsOptions();
            options.setTop(100);
            options.setSkip(0);
            ListTopicsResult topicsResult = new ListTopicsResult();
            topicsResult = service.listTopics(options);
            List<TopicInfo> list = topicsResult.getItems();
            for (TopicInfo info : list)
            {
                if(info.getPath().equals(name))
                   return true;
            }
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    private void createTopic(String name){
        try{
            if(!hasTopic(name)){
                TopicInfo topicInfo = new TopicInfo(name);
                service.createTopic(topicInfo);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
    private boolean hasSubName(String subName){
        try {
            ListSubscriptionsResult subs = service.listSubscriptions(name);
            for(SubscriptionInfo sub: subs.getItems()){
                if(sub.getName().equals(subName))
                    return true;
            }
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    public void deleteTopic(){
        try {
            ListSubscriptionsResult subs = service.listSubscriptions(name);
            for(SubscriptionInfo sub: subs.getItems()){
                while(getSubMessage(sub.getName(),true)!=null){
                    getSubMessage(sub.getName(),true);
                }
            }
            service.deleteTopic(name);
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void createSub(String subName){
        try {
            if(!hasSubName(subName)){
                SubscriptionInfo subInfo = new SubscriptionInfo(subName);
                service.createSubscription(name, subInfo);
            }
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void sendMessage(String s,String id){
        try {
            BrokeredMessage message = new BrokeredMessage(s);
            message.setLabel(id);
            service.sendTopicMessage(name, message);
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String getSubMessage(String subName,boolean delete){
        String result =  null;
        try {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            if(delete)
                opts.setReceiveMode(ReceiveMode.RECEIVE_AND_DELETE);
            else
                opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
            ReceiveSubscriptionMessageResult resultQM = service.receiveSubscriptionMessage(name, subName, opts);
            BrokeredMessage message = resultQM.getValue();
            
            if (message != null && message.getMessageId() != null){
                byte[] b = new byte[200];
                int numRead;
                try {
                    numRead = message.getBody().read(b);
                    while (-1 != numRead){
                        if(result == null)
                            result = (new String(b)).trim();
                        else
                            result += (new String(b)).trim();
                        numRead = message.getBody().read(b);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("get sub message: " + result);
        return result;
    }
    
    public String getSubMessage(String subName,boolean delete, String id){
        String result =  null;
        try {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
            ReceiveSubscriptionMessageResult resultQM = service.receiveSubscriptionMessage(name, subName, opts);
            BrokeredMessage message = resultQM.getValue();
            while(message != null && message.getLabel()!=null && !message.getLabel().equals(id))
                message = resultQM.getValue();
            if (message != null && message.getMessageId() != null){
                byte[] b = new byte[200];
                int numRead;
                try {
                    numRead = message.getBody().read(b);
                    while (-1 != numRead){
                        if(result == null)
                            result = (new String(b)).trim();
                        else
                            result += (new String(b)).trim();
                        numRead = message.getBody().read(b);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if(delete && message != null && service!=null){
                try{
                    service.deleteMessage(message);
                }catch(Exception e){
                }
            }
        } catch (ServiceException ex) {
            Logger.getLogger(Topic.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("get sub message: " + result);
        return result;
    }
    
    private List<String> scan(String subName){
        List<String> result = null;
        String message = getSubMessage(subName,false);
        while(message != null){
            if(result == null)
                result = new ArrayList<String>();
            result.add(message);
            message = getSubMessage(subName,false);
        }
        return result;
    }
    
}