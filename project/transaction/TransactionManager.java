package transaction;

import java.rmi.*;

/** 
 * Interface for the Transaction Manager of the Distributed Travel
 * Reservation System.
 * <p>
 * Unlike WorkflowController.java, you are supposed to make changes
 * to this file.
 */

public interface TransactionManager extends Remote {

    public boolean dieNow()
	throws RemoteException;
    
    /** The RMI names */
    public static final String RMINameFlights = "RMFlights";
    public static final String RMINameRooms = "RMRooms";
    public static final String RMINameCars = "RMCars";
    public static final String RMINameCustomers = "RMCustomers";
    
    
    public boolean start(int  xid)
    		throws RemoteException;
    public boolean commit(int  xid)
    		throws RemoteException, TransactionAbortedException;
    public boolean abort(int  xid)
    		throws RemoteException;
    public boolean enlist(int  xid, String  RMIName)
    		throws RemoteException, InvalidTransactionException;
    
    /** The RMI name a TransactionManager binds to. */
    public static final String RMIName = "TM";

    public boolean dieTMBeforeCommit()
	throws RemoteException;

    public boolean dieTMAfterCommit()
	throws RemoteException;



	
}
