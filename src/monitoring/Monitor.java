/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package monitoring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 * @author lucliuji
 */

public class Monitor{
    
    private final static String cmdTop = "top -n 2 -b -d 0.2";
    private double cpu_load = -1;
    private double mem_load = -1;
    private String time = null;
            
    public void up_date(){
        int total, used;
            try
            {
                Process child = Runtime.getRuntime().exec(cmdTop);

                // hook up child process output to parent
                InputStream lsOut = child.getInputStream();
                InputStreamReader r = new InputStreamReader(lsOut);
                BufferedReader in = new BufferedReader(r);

                // read the child process' output
                String line;
                line = in.readLine();
                while(line.length()<1)
                {
                    line = in.readLine();
                }
                line = line.substring(line.indexOf(":")-2);
                time = line.substring(0,line.indexOf(" ")).trim();
                in.readLine();
                line = in.readLine();
                String delims = "%";
                String[] parts = line.split(delims);
                String[] parts1;
                delims =" ";
                parts1 = parts[0].split(delims);
                cpu_load = Double.parseDouble(parts1[parts1.length-1]);
                parts1 = parts[1].split(delims);
                cpu_load = cpu_load + Double.parseDouble(parts1[parts1.length-1]);
                line = in.readLine();
                line =  line.substring(line.indexOf(":")+1).trim();
                total = Integer.parseInt(line.substring(0,line.indexOf("k")));
                line = line.substring(line.indexOf(",")+1).trim();
                used = Integer.parseInt(line.substring(0,line.indexOf("k")));
                mem_load = used * 1.0 / total;
            }
            catch (IOException e)
            { // exception thrown
                e.printStackTrace();
                System.out.println("Command failed!");
            } catch (NumberFormatException e) {
                // exception thrown
                e.printStackTrace();
                System.out.println("Command failed!");
            }
    }
    
    public double get_cpu(){
        return cpu_load;
    }
    
    public double get_mem(){
        return mem_load;
    }
    
    public String get_time(){
        return time;
    }
    
}
