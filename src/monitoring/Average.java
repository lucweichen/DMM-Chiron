/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package monitoring;

/**
 *
 * @author lucliuji
 */
public class Average extends Thread{
        private double cpu_load;
        private double mem_load;
        private double counter = 0.0;
        
        @Override
        public void run(){
            Monitor m = new Monitor();
            while(true){
                m.up_date();
                if(counter == 0.0){
                    counter = counter + 1.0;
                    cpu_load = m.get_cpu();
                    mem_load = m.get_mem();
//                    System.out.println("0 cpu: " + m.get_cpu() +" mem: " + m.get_mem() + " counter: " + counter);
                }else{
                    counter = counter + 1.0;
                    cpu_load = (cpu_load * (double)( counter - 1.0 ) + m.get_cpu()) / counter ;
                    mem_load = (mem_load * (double)( counter - 1.0 ) + m.get_mem()) / counter ;
//                    System.out.println("a: " + (double)( counter - 1.0 ));
//                    System.out.println("b: " + (cpu_load * (double)( counter - 1.0 )));
//                    System.out.println("c: " + ((cpu_load * (double)( counter - 1.0 ) + m.get_cpu())));
//                    System.out.println("d: " + counter);
//                    System.out.println("d: " + ((cpu_load * (double)( counter - 1.0 ) + m.get_cpu()) / counter));
//                    System.out.println("1 cpu: " + m.get_cpu() +" mem: " + m.get_mem() + " counter: " + counter);
                }
            }
        }
        
        public void re_set(){
            counter = 0.0;
        }
        
        public double get_cpu(){
            return cpu_load;
        }
        
        public double get_mem(){
            return mem_load;
        }
        
        public double get_counter(){
            return counter;
        }
        
}