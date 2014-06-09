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
			throws RemoteException, TransactionAbortedException{
		// TODO Auto-generated method stub
		System.out.println("committing "+xid);
		tryconnect();
		HashSet<String> curtable = TransTrace.get(xid);
		if(curtable==null) return false;
		boolean vote = true;
		
		//send prepare and see what happen
		try{
		for(String RMIName: curtable){
				System.out.println("send prepare to "+RMIName);
				if(RMIName.equals(RMINameFlights))
					rmFlights.prepare(xid);
						
				if(RMIName.equals(RMINameCars))
					rmCars.prepare(xid);
						
				if(RMIName.equals(RMINameRooms))
					rmRooms.prepare(xid);
						
				if(RMIName.equals(RMINameCustomers))
					rmCustomers.prepare(xid);
						
		}} catch(TransactionAbortedException tbrt){
			vote = false;
		}

		//send commit or abort  //here should be logged
		if(vote){
			System.out.println(xid+"everyone votes yes");
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
		}else 	{
			System.out.println(xid+"someone vote no");
			this.abort(xid);
			throw new TransactionAbortedException(xid,"can't commit");
		}
			
		TransTrace.remove(curtable);
		return true;
	}


	@Override
	public boolean abort(int xid) 
			throws RemoteException{
		System.out.println("aborting "+xid);
		tryconnect();
		HashSet<String> curtable = TransTrace.get(xid);
		if(curtable==null) return false;
		for(String RMIName: curtable){

				if(RMIName.equals(RMINameFlights))
					rmFlights.abort(xid);
				else if(RMIName.equals(RMINameCars))
					rmCars.abort(xid);
				else if(RMIName.equals(RMINameRooms))
					rmRooms.abort(xid);
				else if(RMIName.equals(RMINameCustomers))
					rmCustomers.abort(xid);
	
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
	
	protected void tryconnect() throws RemoteException{
		try{
			rmFlights.tryconnect();
			rmCars.tryconnect();
			rmRooms.tryconnect();
			rmCustomers.tryconnect();
		}catch(RemoteException re){
			System.err.println("Some RM is lost");
			connect();
		}
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

	boolean dieTMBeforeCommit = false;
	boolean dieTMAfterCommit = false;

	
    public boolean dieTMBeforeCommit()
	throws RemoteException {
    	dieTMBeforeCommit = true;
	return true;
    }
    public boolean dieTMAfterCommit()
	throws RemoteException {
    	dieTMAfterCommit = true;
	return true;
    }







}
