package transaction;

import java.rmi.*;
import java.util.*;

/** 
 * Workflow Controller for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the WC.  In the real
 * implementation, the WC should forward calls to either RM or TM,
 * instead of doing the things itself.
 */

public class WorkflowControllerImpl
    extends java.rmi.server.UnicastRemoteObject
    implements WorkflowController {

 //   protected int flightcounter, flightprice, carscounter, carsprice, roomscounter, roomsprice; 
    protected int xidCounter;
    
    protected ResourceManager rmFlights = null;
    protected ResourceManager rmRooms = null;
    protected ResourceManager rmCars = null;
    protected ResourceManager rmCustomers = null;
    protected TransactionManager tm = null;

    public static void main(String args[]) {
	System.setSecurityManager(new RMISecurityManager());

	String rmiPort = System.getProperty("rmiPort");
	if (rmiPort == null) {
	    rmiPort = "";
	} else if (!rmiPort.equals("")) {
	    rmiPort = "//:" + rmiPort + "/";
	}

	try {
	    WorkflowControllerImpl obj = new WorkflowControllerImpl();
	    Naming.rebind(rmiPort + WorkflowController.RMIName, obj);
	    System.out.println("WC bound");
	}
	catch (Exception e) {
	    System.err.println("WC not bound:" + e);
	    System.exit(1);
	}
    }
    
    
    public WorkflowControllerImpl() throws RemoteException {
//	flightcounter = 0;
//	flightprice = 0;
//	carscounter = 0;
//	carsprice = 0;
//	roomscounter = 0;
//	roomsprice = 0;
//	flightprice = 0;

	//xidCounter = 1;

	while (!reconnect()) {
	    // would be better to sleep a while
	} 
    }


    // TRANSACTION INTERFACE
    public int start()
	throws RemoteException {
    	xidCounter++;
    	if(!tm.start(xidCounter))
    		return -1;
    	return (xidCounter);
    }

    public boolean commit(int xid)
	throws RemoteException, 
	       TransactionAbortedException, 
	       InvalidTransactionException {
    	System.out.println("Committing");
    	if(!tm.commit(xidCounter))
    		return false;
    	return true;
    }

    public void abort(int xid)
	throws RemoteException, 
               InvalidTransactionException {
    	tm.abort(xid);
	return;
    }


    // ADMINISTRATIVE INTERFACE
    public boolean addFlight(int xid, String flightNum, int numSeats, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmFlights.addFlight(xid, flightNum, numSeats, price))
    		return false;
    	tm.enlist(xid, "RMFlights");
//	flightcounter += numSeats;
//	flightprice = price;
    	return true;
    }

    public boolean deleteFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmFlights.deleteFlight(xid, flightNum))
    		return false;
    	tm.enlist(xid, "RMFlights");
//	flightcounter = 0;
//	flightprice = 0;
	return true;
    }
		
    public boolean addRooms(int xid, String location, int numRooms, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmRooms.addRooms(xid, location, numRooms, price))
    		return false;
    	tm.enlist(xid, "RMRooms");
//	roomscounter += numRooms;
//	roomsprice = price;
	return true;
    }

    public boolean deleteRooms(int xid, String location, int numRooms) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmRooms.deleteRooms(xid, location, numRooms))
    		return false;
    	tm.enlist(xid, "RMRooms");
//	roomscounter = 0;
//	roomsprice = 0;
	return true;
    }

    public boolean addCars(int xid, String location, int numCars, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmCars.addCars(xid, location, numCars, price))
    		return false;
    	tm.enlist(xid, "RMCars");
//	carscounter += numCars;
//	carsprice = price;
	return true;
    }

    public boolean deleteCars(int xid, String location, int numCars) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmCars.deleteCars(xid, location, numCars))
    		return false;
    	tm.enlist(xid, "RMCars");
//	carscounter = 0;
//	carsprice = 0;
	return true;
    }

    public boolean newCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmCustomers.newCustomer(xid, custName))
    		return false;
    	tm.enlist(xid, "RMCustomers");
    	return true;
    }

    public boolean deleteCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(!rmCustomers.deleteCustomer(xid, custName))
    		return false;
    	tm.enlist(xid, "RMCustomers");
    	return true;
    }


    // QUERY INTERFACE
    public int queryFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMFlights");
    	return rmFlights.queryFlight(xid, flightNum);
    }

    public int queryFlightPrice(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMFlights");
    	return rmFlights.queryFlightPrice(xid, flightNum);
    }

    public int queryRooms(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMRooms");
    	return rmRooms.queryRooms(xid, location);
    }

    public int queryRoomsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMRooms");
    	return rmRooms.queryRoomsPrice(xid, location);
    }

    public int queryCars(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMCars");
    	return rmCars.queryCars(xid, location);
    }

    public int queryCarsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMCars");
    	return rmCars.queryCarsPrice(xid, location);
    }

    public int queryCustomerBill(int xid, String custName)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	tm.enlist(xid, "RMCustomers");
    	tm.enlist(xid, "RMCars");
    	tm.enlist(xid, "RMFlights");
    	return 0;
    }


    // RESERVATION INTERFACE
    public boolean reserveFlight(int xid, String custName, String flightNum) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(rmFlights.reserveFlight(xid, custName, flightNum))
    		if(rmCustomers.reserveFlight(xid, custName, flightNum)){
    	    	tm.enlist(xid, "RMCustomers");
    	    	tm.enlist(xid, "RMFlights");
    			return true;
    		}
    	//flightcounter--;
	return false;
    }
 
    public boolean reserveCar(int xid, String custName, String location) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(rmCars.reserveCar(xid, custName, location))
    		if(rmCustomers.reserveCar(xid, custName, location)){
      	    	tm.enlist(xid, "RMCustomers");
    	    	tm.enlist(xid, "RMCars");
    	    	return true;
    		}
    			
    	return false;
	//carscounter--;
	
    }

    public boolean reserveRoom(int xid, String custName, String location) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	if(rmRooms.reserveRoom(xid, custName, location))
    		if(rmCustomers.reserveRoom(xid, custName, location)){
      	    	tm.enlist(xid, "RMCustomers");
    	    	tm.enlist(xid, "RMRooms");
    	    	return true;
    		}
    return false;
	//roomscounter--;
    }

    public boolean reserveItinerary(int xid, String custName, List flightNumList, String location, boolean needCar, boolean needRoom)
        throws RemoteException,
	TransactionAbortedException,
	InvalidTransactionException {
    	for(Object flightNum:flightNumList){
    		if(!reserveFlight(xid,custName,(String)flightNum))
    			return false;
    	}
    	if(needCar){
    		if(!reserveCar(xid,custName,location))
    			return false;
    	}
    	if(needRoom){
    		if(!reserveRoom(xid,custName,location))
    			return false;
    	}
	return true;
    }

    // TECHNICAL/TESTING INTERFACE
    public boolean reconnect()
	throws RemoteException {
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
	    System.out.println("WC bound to RMFlights");
	    rmRooms =
		(ResourceManager)Naming.lookup(rmiPort +
					       ResourceManager.RMINameRooms);
	    System.out.println("WC bound to RMRooms");
	    rmCars =
		(ResourceManager)Naming.lookup(rmiPort +
					       ResourceManager.RMINameCars);
	    System.out.println("WC bound to RMCars");
	    rmCustomers =
		(ResourceManager)Naming.lookup(rmiPort +
					       ResourceManager.RMINameCustomers);
	    System.out.println("WC bound to RMCustomers");
	    tm =
		(TransactionManager)Naming.lookup(rmiPort +
						  TransactionManager.RMIName);
	    System.out.println("WC bound to TM");
	} 
	catch (Exception e) {
	    System.err.println("WC cannot bind to some component:" + e);
	    return false;
	}

	try {
	    if (rmFlights.reconnect() && rmRooms.reconnect() &&
		rmCars.reconnect() && rmCustomers.reconnect()) {
		return true;
	    }
	} catch (Exception e) {
	    System.err.println("Some RM cannot reconnect:" + e);
	    return false;
	}

	return false;
    }

    public boolean dieNow(String who)
	throws RemoteException {
	if (who.equals(TransactionManager.RMIName) ||
	    who.equals("ALL")) {
	    try {
		tm.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameFlights) ||
	    who.equals("ALL")) {
	    try {
		rmFlights.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameRooms) ||
	    who.equals("ALL")) {
	    try {
		rmRooms.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameCars) ||
	    who.equals("ALL")) {
	    try {
		rmCars.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameCustomers) ||
	    who.equals("ALL")) {
	    try {
		rmCustomers.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(WorkflowController.RMIName) ||
	    who.equals("ALL")) {
	    System.exit(1);
	}
	return true;
    }
    public boolean dieRMAfterEnlist(String who)
	throws RemoteException {
	return true;
    }
    public boolean dieRMBeforePrepare(String who)
	throws RemoteException {
	return true;
    }
    public boolean dieRMAfterPrepare(String who)
	throws RemoteException {
	return true;
    }
    public boolean dieTMBeforeCommit()
	throws RemoteException {
	return true;
    }
    public boolean dieTMAfterCommit()
	throws RemoteException {
	return true;
    }
    public boolean dieRMBeforeCommit(String who)
	throws RemoteException {
	return true;
    }
    public boolean dieRMBeforeAbort(String who)
	throws RemoteException {
	return true;
    }
}
