package transaction;

import java.rmi.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

/** 
 * Transaction Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl
    extends java.rmi.server.UnicastRemoteObject
    implements TransactionManager {
    
	protected Hashtable<Integer, HashSet<String>> TransTrace = null;
    
    protected ResourceManager rmFlights = null;
    protected ResourceManager rmRooms = null;
    protected ResourceManager rmCars = null;
    protected ResourceManager rmCustomers = null;
    
    protected boolean flag_ref = false;
	
    public static void main(String args[]) {
	System.setSecurityManager(new RMISecurityManager());

	String rmiPort = System.getProperty("rmiPort");
	if (rmiPort == null) {
	    rmiPort = "";
	} else if (!rmiPort.equals("")) {
	    rmiPort = "//:" + rmiPort + "/";
	}

	try {
	    TransactionManagerImpl obj = new TransactionManagerImpl();
	    Naming.rebind(rmiPort + TransactionManager.RMIName, obj);
	    System.out.println("TM bound");
	} 
	catch (Exception e) {
	    System.err.println("TM not bound:" + e);
	    System.exit(1);
	}
    }
    
    
    public TransactionManagerImpl() throws RemoteException {
    	TransTrace = new Hashtable<Integer, HashSet<String>> ();
    	flag_ref = false;
    }


    public boolean dieNow() 
	throws RemoteException {
	System.exit(1);
	return true; // We won't ever get here since we exited above;
	             // but we still need it to please the compiler.
    }

	@Override
	public boolean start(int xid) 
			throws RemoteException{
		// TODO Auto-generated method stub
		HashSet<String> newtable = new HashSet<String>();
		TransTrace.put(xid,newtable);
		return true;
	}
	
	@Override
	public boolean commit(int xid) 
			throws RemoteException{
		// TODO Auto-generated method stub
		HashSet<String> curtable = TransTrace.get(xid);
		if(curtable==null) return false;
		for(String RMIName: curtable){
			try {
				if(RMIName.equals(RMINameFlights))
					rmFlights.commit(xid);
				if(RMIName.equals(RMINameCars))
					rmCars.commit(xid);
				if(RMIName.equals(RMINameRooms))
					rmRooms.commit(xid);
				if(RMIName.equals(RMINameCustomers))
					rmCustomers.commit(xid);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
		TransTrace.remove(curtable);
		return true;
	}


	@Override
	public boolean abort(int xid) 
			throws RemoteException{
		HashSet<String> curtable = TransTrace.get(xid);
		if(curtable==null) return false;
		for(String RMIName: curtable){
		
			try {
				if(RMIName.equals(RMINameFlights))
					rmFlights.abort(xid);
				else if(RMIName.equals(RMINameCars))
					rmCars.abort(xid);
				else if(RMIName.equals(RMINameRooms))
					rmRooms.abort(xid);
				else if(RMIName.equals(RMINameCustomers))
					rmCustomers.abort(xid);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		TransTrace.remove(curtable);
		// TODO Auto-generated method stub
		return true;
	}

	protected boolean connect()
			throws RemoteException{
		String rmiPort = System.getProperty("rmiPort");
		if (rmiPort == null) {
		    rmiPort = "";
		} else if (!rmiPort.equals("")) {
		    rmiPort = "//:" + rmiPort + "/";
		}
		try {
		    rmFlights =
			(ResourceManager)Naming.lookup(rmiPort +
						       ResourceManager.RMINameFlights);
		    System.out.println("TM bound to RMFlights");
		    rmRooms =
			(ResourceManager)Naming.lookup(rmiPort +
						       ResourceManager.RMINameRooms);
		    System.out.println("TM bound to RMRooms");
		    rmCars =
			(ResourceManager)Naming.lookup(rmiPort +
						       ResourceManager.RMINameCars);
		    System.out.println("TM bound to RMCars");
		    rmCustomers =
			(ResourceManager)Naming.lookup(rmiPort +
						       ResourceManager.RMINameCustomers);
		    System.out.println("TM bound to RMCustomers");

		} 
		catch (Exception e) {
		    System.err.println("TM cannot bind to some component:" + e);
		    return false;
		}
		return true;
	}
	@Override
	public boolean enlist(int xid, String RMIName) 
			throws RemoteException,InvalidTransactionException{
		// TODO Auto-generated method stub
		HashSet<String> curtable = TransTrace.get(xid);
		if(curtable==null) throw new InvalidTransactionException(xid, RMIName);
		if(!flag_ref){
			if(connect())
				flag_ref = true;
		}
			
		curtable.add(RMIName);
		
		return true;
	}




}
