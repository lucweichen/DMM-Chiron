/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ServiceBus;

/**
 *
 * @author luc
 */
public class NoService extends Exception{
    
    public NoService() {
        // TODO Auto-generated constructor stub
    }

    public NoService(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public NoService(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

    public NoService(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }
    
}
