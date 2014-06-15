package transaction;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

/** 
 * Transaction Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl
    extends java.rmi.server.UnicastRemoteObject
    implements TransactionManager {
    
	protected Hashtable<Integer, HashSet<String>> TransTrace = null;
	protected Hashtable<Integer, HashSet<String>> TransTraceReadonly = null;
    
    protected ResourceManager rmFlights = null;
    protected ResourceManager rmRooms = null;
    protected ResourceManager rmCars = null;
    protected ResourceManager rmCustomers = null;
    
    protected boolean flag_ref = false;
	
    ////roughly implement a hashtable for 2PC's coordinate database, second parameter used for state
    protected Hashtable<Integer, Integer> TransStates = null;
    public static final int PREPARING = 0;
    public static final int COMMITTING = 1;
    public static final int ABORTING = 2;
    
    private TMLogManager TML = null; 
    
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
    
    
    public TransactionManagerImpl() 
    		throws RemoteException, IOException, ClassNotFoundException {
    	TransTrace = new Hashtable<Integer, HashSet<String>> ();
    	TransTraceReadonly = new Hashtable<Integer, HashSet<String>> ();
    	TransStates = new Hashtable<Integer, Integer> ();
    	flag_ref = false;
    	TML = new TMLogManager("TMLog");
    	recover();
    	System.out.println("After TM recovery, " + TransStates.size() + "is recoverd");
    }

    /*
     * Reconstruct only TransStates Table
     */

    private void recover() throws IOException, ClassNotFoundException {
    	// reconstruct only TransStates Table
    	ArrayList<TMLog> stateLog = TML.LogSequenceInFile();
    	for (TMLog log : stateLog) {
    		if (log.type == TMLog.COMMIT) {
    			TransStates.put(log.xid, this.COMMITTING);
    		} else if (log.type == TMLog.END) {
    			TransStates.remove(log.xid);
    		}
    	}
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
		if(!flag_ref){  //refer to all RMs at very beginning
			if(connect())
				flag_ref = true;
		}else
			tryconnect();  //reconnect if in need
		TransTrace.put(xid,new HashSet<String>());
		TransTraceReadonly.put(xid,new HashSet<String>());
		System.out.println("Start Success in TM");
		return true;
	}
	
	@Override
	public boolean commit(int xid) 
			throws RemoteException, TransactionAbortedException {
		// TODO Auto-generated method stub
		System.out.println("committing "+xid);
		tryconnect();
		
		if(this.TransTrace == null || this.TransTraceReadonly == null) 
			throw new TransactionAbortedException(xid, "Missing Inforamtion");
		
		HashSet<String> curtable = TransTrace.get(xid);
		HashSet<String> curtable_readonly = TransTraceReadonly.get(xid);
		
		if(curtable == null || curtable_readonly == null) 
			throw new TransactionAbortedException(xid, "Missing Inforamtion");
		
		boolean vote = true;
		boolean rmentry = true;
		
		//send prepare and see what happen
		TransStates.put(xid, PREPARING);
		try{
			for(String RMIName: curtable_readonly){  
				System.out.println("send prepare to "+RMIName);
				if(RMIName.equals(RMINameFlights))
					rmFlights.prepare(xid, true);
						
				else if(RMIName.equals(RMINameCars))
					rmCars.prepare(xid, true);
						
				else if(RMIName.equals(RMINameRooms))
					rmRooms.prepare(xid, true);
						
				else if(RMIName.equals(RMINameCustomers))
					rmCustomers.prepare(xid, true);
			}
			
			for(String RMIName: curtable){
				System.out.println("send prepare to "+RMIName);
				if(RMIName.equals(RMINameFlights))
					rmFlights.prepare(xid, false);
						
				else if(RMIName.equals(RMINameCars))
					rmCars.prepare(xid, false);
						
				else if(RMIName.equals(RMINameRooms))
					rmRooms.prepare(xid, false);
						
				else if(RMIName.equals(RMINameCustomers))
					rmCustomers.prepare(xid, false);		
			}
			
		} catch(TransactionAbortedException tbrt){
			vote = false;
		} catch(RemoteException re){
			//can't connect, should abort..
			vote = false;
		}

		//send commit or abort  //here should be logged
		if(vote){
			System.out.println(xid+"everyone votes yes");
			TransStates.put(xid, COMMITTING);
			
			// Write and Flush Commit Log
			ArrayList<String> transRMs = new ArrayList<String>();
			HashSet<String> sub = this.TransTrace.get(xid);
			if (sub != null) transRMs.addAll(sub);
			sub = this.TransTraceReadonly.get(xid);
			if (sub != null) transRMs.addAll(sub);
			try {
				TML.newLog(xid, TMLog.COMMIT, transRMs);
			} catch (IOException e) {
				return false;
			}
			
			for(String RMIName: curtable){
				try {
					if(RMIName.equals(RMINameFlights) && rmFlights!=null)
						rmFlights.commit(xid);
					else if(RMIName.equals(RMINameCars) && rmFlights!=null)
						rmCars.commit(xid);
					else if(RMIName.equals(RMINameRooms) && rmFlights!=null)
						rmRooms.commit(xid);
					else if(RMIName.equals(RMINameCustomers) && rmFlights!=null)
						rmCustomers.commit(xid);
					else{
						rmentry = false; //if reach here, means some RM is dead. shouldn't remove entry..
					}
					
				} catch (RemoteException e) {
					// If some RM is dead, should reach here
					rmentry = false;
					System.out.println("impossible to reach");  //never reach here.
				} catch (TransactionAbortedException e) {
					// TODO Auto-generated catch block
					System.out.println("impossible to abort in committing"); //never reach here.
					
				}	
			}
		} else 	{
			System.out.println(xid+"someone vote no");
			this.abort(xid);
			throw new TransactionAbortedException(xid,"can't commit");
			
		}
		
		if(rmentry){
			TransTrace.remove(xid);
			TransTraceReadonly.remove(xid);
		}
		return true;
	}


	@Override
	public boolean abort(int xid) 
			throws RemoteException{
		System.out.println("aborting "+xid);
		TransStates.put(xid, ABORTING);
		tryconnect();
		HashSet<String> curtable = TransTrace.get(xid);
		HashSet<String> curtable_readonly = TransTraceReadonly.get(xid);
		if(curtable==null||curtable_readonly ==null) return false;
		for(String RMIName: curtable_readonly){
			System.out.println("aborting readonly"+RMIName);
			if(RMIName.equals(RMINameFlights)&&rmFlights!=null)
				rmFlights.abort(xid);
			else if(RMIName.equals(RMINameCars)&&rmCars!=null)
				rmCars.abort(xid);
			else if(RMIName.equals(RMINameRooms)&&rmRooms!=null)
				rmRooms.abort(xid);
			else if(RMIName.equals(RMINameCustomers)&&rmCustomers!=null)
				rmCustomers.abort(xid);
		}
		for(String RMIName: curtable){
			System.out.println("aborting "+RMIName);
			if(RMIName.equals(RMINameFlights)&&rmFlights!=null)
				rmFlights.abort(xid);
			else if(RMIName.equals(RMINameCars)&&rmCars!=null)
				rmCars.abort(xid);
			else if(RMIName.equals(RMINameRooms)&&rmRooms!=null)
				rmRooms.abort(xid);
			else if(RMIName.equals(RMINameCustomers)&&rmCustomers!=null)
				rmCustomers.abort(xid);
		}
		TransTrace.remove(xid);
		TransTraceReadonly.remove(xid);
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
		String testing = "";
		//while(true)
		try{
			testing = RMINameFlights;
			rmFlights.tryconnect();
			testing = RMINameCars;
			rmCars.tryconnect();
			testing = RMINameRooms;
			rmRooms.tryconnect();
			testing = RMINameCustomers;
			rmCustomers.tryconnect();
			return;
		}catch(RemoteException re){
			System.err.println("Some RM is lost");
			reconnect(testing);  //maybe in a loop or just connect all ... in the case of multiple RMs failed
		}
		catch(NullPointerException e){
			System.err.println("Some RM already lost");
			reconnect(testing);  //
		}
		//try again
		try{
			testing = RMINameFlights;
			rmFlights.tryconnect();
			testing = RMINameCars;
			rmCars.tryconnect();
			testing = RMINameRooms;
			rmRooms.tryconnect();
			testing = RMINameCustomers;
			rmCustomers.tryconnect();
		}catch(RemoteException re){
			//..
			if(testing.equals(RMINameFlights)){
				 rmFlights = null; 
				 System.out.println("TM not bound to RMFlights");
			}else
			if(testing.equals(RMINameCars)){
				rmCars = null;
			    System.out.println("TM not bound to RMCars");
			}else
			if(testing.equals(RMINameRooms)){
				rmRooms = null;
			    System.out.println("TM not bound to RMRooms");
			}else
			if(testing.equals(RMINameCustomers)){
				rmCustomers = null;
			    System.out.println("TM not bound to RMCustomers");
			}
		}
	}
	
	protected boolean reconnect(String RMIName)
			throws RemoteException{
		String rmiPort = System.getProperty("rmiPort");
		if (rmiPort == null) {
		    rmiPort = "";
		} else if (!rmiPort.equals("")) {
		    rmiPort = "//:" + rmiPort + "/";
		}
		try {
			if(RMIName.equals(RMINameFlights)){
				 rmFlights =
					(ResourceManager)Naming.lookup(rmiPort +
								       ResourceManager.RMINameFlights);
			}
			if(RMIName.equals(RMINameCars)){
				rmCars =
				(ResourceManager)Naming.lookup(rmiPort +
							       ResourceManager.RMINameCars);
			}
			if(RMIName.equals(RMINameRooms)){
				rmRooms =
				(ResourceManager)Naming.lookup(rmiPort +
							       ResourceManager.RMINameRooms);
			}
			if(RMIName.equals(RMINameCustomers)){
				rmCustomers =
				(ResourceManager)Naming.lookup(rmiPort +
							       ResourceManager.RMINameCustomers);
			}
		} 
		catch (Exception e) {
		    System.err.println("TM cannot bind to:" + RMIName + e);
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
		if(TransTraceReadonly.get(xid).contains(RMIName))  //if in readonly list, delete;
			TransTraceReadonly.get(xid).remove(RMIName);
		curtable.add(RMIName);
		return true;
	}

	public boolean enlist_readonly(int xid, String RMIName) 
			throws RemoteException,InvalidTransactionException{
		// TODO Auto-generated method stub
		if(TransTrace.contains(xid))  //if in normal list, return;
			return true;
		HashSet<String> curtable = TransTraceReadonly.get(xid);
		if(curtable==null) 
			throw new InvalidTransactionException(xid, RMIName);
		curtable.add(RMIName);
		
		return true;
	}
	
	public int check_status(int xid) 
			throws RemoteException{
		if(!TransStates.containsKey(xid))
			return -1;
		return TransStates.get(xid);
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

class TMLog implements Serializable {
	public static final int COMMIT = 0;
	public static final int END = 1;
	
	public final int LSN;
	public final int xid;
	public final int type;
	public final ArrayList<String> RMs;
	
	public TMLog (int xid, int type, int LSN, ArrayList<String> RMs) throws IllegalArgumentException {
		if (type != COMMIT && type != END) 
			throw new IllegalArgumentException("wrong TMLog Type");
		this.xid = xid;
		this.type = type;
		this.LSN = LSN;
		this.RMs = RMs;
	}
}

class TMLogManager {
	 private static final String logSuffix = ".log";
	 private static final String dirName = "data/";


	 private String TMName;
	 private final String logName;


	 //output Stream
	 private FileOutputStream fos = null;
	 private ObjectOutputStream oos = null;


	 //input Stream
	 private FileInputStream fis = null;
	 private ObjectInputStream ois = null;
	 
	 private LinkedList<TMLog> logQueue = null; // the log sequence in the memory
	 private ArrayList <TMLog> logSeq = null;
	 
	 
	 public TMLogManager(String tmName) throws ClassNotFoundException, IOException {
		 TMName = tmName;
		 logName = dirName + TMName + logSuffix;
		 logSeq = this.LogSequenceInFile();
		 
		 logQueue = new LinkedList<TMLog>();
	 }
	 
	 // Add A new log
	 public void newLog(int xid, int type, ArrayList<String> RMs) throws IOException 
	 {
		 logQueue.addLast(new TMLog(xid, type, 0, RMs));
		 if (type == TMLog.COMMIT) {
			 flushLog(0);
		 }
	 }

	 public void close() {
		// TODO Auto-generated method stub
		try {
			this.closeInputStream();
			this.closeOutputStream();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//do nothing
		}
		
	}

	/*
	  * Set output stream to log file
	  * Try to create new file if not exists
	  */

	 private void setOutputStream() throws IOException, FileNotFoundException {
		 
		 try {
			 fos = new FileOutputStream(logName, true);
		 } catch (FileNotFoundException fe) {
			 File f = new File(logName);
			 f.mkdirs();
			 f.createNewFile();
			 fos = new FileOutputStream(f, true);
		 }
		 oos = new ObjectOutputStream(fos);


	 }

	 /*
	  * 
	  */

	 private void closeOutputStream() throws IOException {
		 if (oos != null)
			 oos.close();
		 if (fos != null)
			 fos.close(); // redundant
		 oos = null;
		 fos = null;
	 }

	 /*
	  * Set inputStream from logFile
	  * Throw FileNotFoundException if file not exists
	  */

	 private void setInputStream() throws FileNotFoundException, IOException {
		fis = new FileInputStream(logName);
		ois = new ObjectInputStream(fis);
	 }

	 /*
	  * 
	  */

	 private void closeInputStream() throws IOException {
		 if (ois != null)
			 ois.close();
		 if (fis != null)
			 fis.close(); // redundant
		 ois = null;
		 fis = null;
	 }

	 



	 /*
	  * FLush logs that < LSN to file
	  */


	 public void flushLog(int LSN) throws IOException {
		 this.closeInputStream();
		 this.setOutputStream();
		 
		 while(!logQueue.isEmpty() && logQueue.peekFirst().LSN <= LSN) {
			 oos.writeObject(logQueue.removeFirst());
		 }
		 oos.flush();
		 fos.flush();
		 
	 }
	 
	 public ArrayList<TMLog> logQueueInMem() {
		 return new ArrayList<TMLog>(this.logQueue);
	 }
	
	/*
	  * return log sequence on disk in ArrayList
	  *   Empty ArrayList if no logs on disk
	  */
	 public ArrayList<TMLog> LogSequenceInFile() throws IOException, ClassNotFoundException {
		 
		 this.closeOutputStream();
		 
		 try {
			 this.setInputStream();
		 } catch (FileNotFoundException fne) {
			 return new ArrayList<TMLog>();
		 } catch(IOException ie){  //last time may not close os
			 return new ArrayList<TMLog>();
		 }

		 ArrayList<TMLog> logList = new ArrayList<TMLog>();
		 while(true) {
			 TMLog tmlog = null;
			 try {
				tmlog = (TMLog)ois.readObject();
				//System.out.println("read a log"+rmlog.table+rmlog.key);
			 } catch (EOFException eofe) {
				 // End of File
				 break;
			 }
			 logList.add(tmlog);
		 }
		 ///not sure
		 //this.closeInputStream();
		 return logList;
	 }
}
