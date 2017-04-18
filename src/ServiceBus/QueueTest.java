/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ServiceBus;

import java.util.List;
import junit.framework.*;

/**
 *
 * @author luc
 */
public class QueueTest extends TestCase{
    
    public static void main(String[] args) {
//        testQueueSend();
//        testQueueGet();
//        testQueueScan();
//        test1Topic();
//        test2Topic();
//        test3Topic();
        Topic t = new Topic("0");
        t.deleteTopic();
        Queue q = new Queue("0");
        q.deleteQueue();
        q = new Queue("1");
        q.deleteQueue();
        q = new Queue("2");
        q.deleteQueue();
    }
    
    private static void testQueueSend(){
        Queue q = new Queue("testqueue");
        q.deleteAll();
        q.sendMessage("test1","1");
        assertEquals(q.getMessage(true), "test1");
        q.deleteAll();
    }
    
    private static void testQueueGet(){
        Queue q = new Queue("testqueue");
        q.deleteAll();
        assertEquals(q.getMessage(true), null);
        q.sendMessage("test1","1");
        assertEquals(q.getMessage(true), "test1");
        q.sendMessage("test2","2");
        assertEquals(q.getMessage(false), "test2");
        assertEquals(q.getMessage(true), null);
        q.deleteAll();
    }
    
    private static void testQueueScan(){
        Queue q = new Queue("testqueue");
        q.deleteAll();
        assertEquals(q.scan(), null);
        q.sendMessage("test1","1");
        q.sendMessage("test2","2");
        List<String> messages = q.scan();
        int id = 1;
        for(String message:messages){
            assertEquals(message, "test"+id++);
        }
        assertEquals(q.getMessage(true), null);
        q.deleteAll();
    }
    
    private static void test1Topic(){
        Topic t = new Topic("test");
        t.createSub("sub1");
        t.sendMessage("test", "2");
        assertEquals(t.getSubMessage("sub1", true), "test");
        assertEquals(t.getSubMessage("sub1", true), null);
    }
    
    private static void test2Topic(){
        Topic t = new Topic("test");
        t.createSub("sub1");
        t.createSub("sub2");
        t.sendMessage("test", "1");
        assertEquals(t.getSubMessage("sub1", true), "test");
        assertEquals(t.getSubMessage("sub1", true), null);
        assertEquals(t.getSubMessage("sub2", true), "test");
        assertEquals(t.getSubMessage("sub2", true), null);
    }
    
    private static void test3Topic(){
        Topic t = new Topic("test");
        t.createSub("sub1");
        t.createSub("sub2");
        t.sendMessage("test1", "1");
        t.sendMessage("test2", "1");
        assertEquals(t.getSubMessage("sub1", true), "test1");
        assertEquals(t.getSubMessage("sub1", true), "test2");
        assertEquals(t.getSubMessage("sub1", true), null);
        assertEquals(t.getSubMessage("sub2", true), "test1");
        assertEquals(t.getSubMessage("sub2", true), "test2");
        assertEquals(t.getSubMessage("sub2", true), null);
    }
    
}
