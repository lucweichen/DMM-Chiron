/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package chiron;

/**
 *
 * @author lucliuji
 */
public class Executor extends Thread {
    EActivation activation;
    protected Executor(EActivation activation){
        this.activation = activation;
    }
    
    @Override
    public void run() {
        activation.executeActivation();
    }
    
}
