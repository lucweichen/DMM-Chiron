/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package monitoring;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
/**
 *
 * @author lucliuji
 */
public class CPU_m{
    
    public void main(String[] args) throws InstanceNotFoundException, MalformedObjectNameException, ReflectionException, InterruptedException {
        
        Average m = new Average();
        m.start();
        while(true){
            m.re_set();
            Thread.sleep(2000);
            System.out.println("Current CPU usage : " + m.get_cpu() + " memory usage : " + m.get_mem() +" counter: " + m.get_counter());
        }
    }
    
}

