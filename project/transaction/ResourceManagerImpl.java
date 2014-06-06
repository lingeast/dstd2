package transaction;

import lockmgr.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/** 
 * Resource Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl
    extends java.rmi.server.UnicastRemoteObject
    implements ResourceManager {
    
    protected String myRMIName = null; // Used to distinguish this RM from other RMs
    protected TransactionManager tm = null;
    protected RMLogManager RML = null;
    protected int xidCounter;
    protected String TableName = null; //
    LockManager lm = new LockManager();
    String[] file={"data/Pointer","data/Flights1","data/Flights2","data/Hotels1","data/Hotels2","data/Cars1","data/Cars2","data/Customers1","data/Customers2","data/Reservations1","data/Reservations2"};
	int [] pointer =new int [6];   //the 6th represents how many transactions ran
	
    // Use Hash Map to represent tables
    // flightNum as primary key
    HashMap <String, Flight> flights = null;// new HashMap <String, Flight>();
    
    // location as primary key
    HashMap <String, Car> cars = null;//new HashMap <String, Car>();
    
    // location as as primary key
    HashMap <String, Hotel> hotels = null;//new HashMap <String, Hotel>();
    
    // custName as primary key
    HashMap <String, Customer> customers = null;//new HashMap <String, Customer>();
    
    // custName as primary key, combined with customer table
    HashMap <String, ArrayList<Reservation>> reservations = null;//new HashMap <String, ArrayList<Reservation>>();
    
    
    public static void main(String args[]) {
    	System.setSecurityManager(new RMISecurityManager());

    	String rmiName = System.getProperty("rmiName");
    	if (rmiName == null || rmiName.equals("")) {
    	    System.err.println("No RMI name given");
    	    System.exit(1);
    	}

    	String rmiPort = System.getProperty("rmiPort");
    	if (rmiPort == null) {
    	    rmiPort = "";
    	} else if (!rmiPort.equals("")) {
    	    rmiPort = "//:" + rmiPort + "/";
    	}

    	try {
    	    ResourceManagerImpl obj = new ResourceManagerImpl(rmiName);
    	    Naming.rebind(rmiPort + rmiName, obj);
    	    System.out.println(rmiName + " bound");
    	} 
    	catch (Exception e) {
    	    System.err.println(rmiName + " not bound:" + e);
    	    System.exit(1);
    	}
	}
        
	public ResourceManagerImpl(String rmiName) throws RemoteException, IOException {
    	myRMIName = rmiName; 
    	xidCounter = 0;
    	
    	new File("data").mkdir(); //not yet exist
    	
    	
    	
    	switch(myRMIName){
    		case RMINameFlights:
    			flights = new HashMap <String, Flight>();
    			TableName = "flights";
    			break;
    		case RMINameRooms:
    			hotels = new HashMap <String, Hotel>();
    			TableName = "rooms";
    			break;
    		case RMINameCars:
    			cars = new HashMap <String, Car>();
    			TableName = "cars";
    			break;
    		case RMINameCustomers:
    			customers = new HashMap <String, Customer>();
    			reservations = new HashMap <String, ArrayList<Reservation>>();
    			TableName = "customers";
    			break;
    		default: 
    			throw new RemoteException("Wrong RMI name");
    	}
    	
    	RML = new RMLogManager(TableName);
    	//RML.recover();
    	

    	while (!reconnect()) {
    	    // would be better to sleep a while
    	} 
    }

	public boolean reconnect()
    	throws RemoteException {
    	String rmiPort = System.getProperty("rmiPort");
    	if (rmiPort == null) {
    	    rmiPort = "";
    	} else if (!rmiPort.equals("")) {
    	    rmiPort = "//:" + rmiPort + "/";
    	}

    	try {
    	    tm = (TransactionManager)Naming.lookup(rmiPort + TransactionManager.RMIName);
    	    System.out.println(myRMIName + " bound to TM");
    	} 
    	catch (Exception e) {
    	    System.err.println(myRMIName + " cannot bind to TM:" + e);
    	    return false;
    	}

    	return true;
	}



    // TRANSACTION INTERFACE
    
    private void push2file(Object obj,int type, int filenumber) throws FileNotFoundException, IOException{
    	ObjectOutputStream oos = null;
    	switch(type){
    	case 0: // push pointer
    	    FileWriter writer = new FileWriter(file[0]); 
    	    for(int i=0;i<6;i++)
    	    	writer.write(pointer[i]+'0');
    	    writer.flush();
    	    writer.close();
    	    break;
    	case FLIGHT: 
    		oos = new ObjectOutputStream(new FileOutputStream(file[filenumber]));
			oos.writeObject((HashMap <String, Flight>)obj);
			oos.flush();
			oos.close();
			break;
    	case HOTEL: 	
    		oos = new ObjectOutputStream(new FileOutputStream(file[filenumber]));
			oos.writeObject((HashMap <String, Hotel>)obj);
			oos.flush();
			oos.close();
			break;
    	case CAR: 
    		oos = new ObjectOutputStream(new FileOutputStream(file[filenumber]));
			oos.writeObject((HashMap <String, Car>)obj);
			oos.flush();
			oos.close();
			break;
    	case CUSTOMER: 
    		oos = new ObjectOutputStream(new FileOutputStream(file[filenumber]));
			oos.writeObject((HashMap <String, Customer>)obj);
			oos.flush();
			oos.close();
			break;
    	case RESERVATION: 	
    		oos = new ObjectOutputStream(new FileOutputStream(file[filenumber]));
			oos.writeObject((HashMap <String,  ArrayList<Reservation>>)obj);
			oos.flush();
			oos.close();
			break;
    	}
    }
    
    public boolean commit(int xid)
	throws RemoteException,TransactionAbortedException, 
	       InvalidTransactionException {
    	System.out.println("Committing");
    	
    	/*TransRes finished = trans.remove(xid);
    	if (finished == null) 
    		assert(false);
    	// update current to be shadow
    	
    	if (!finished.cars .isEmpty()) {
    		HashMap <String, Car> cars_shadowing = new HashMap <String, Car>(cars);
    		for (String key : finished.cars.keySet()) {
    			if(finished.cars.get(key)!=null)
    				cars_shadowing.put(key, finished.cars.get(key));
    			else
    				cars_shadowing.remove(key);
    		}
    		cars = cars_shadowing;
    		//push to file[5] or file[6]
			try{
				push2file(cars,CAR,6-pointer[2]);
			}catch(FileNotFoundException fnfe){
				return false;
			}catch(IOException io){
				return false;
			}
			pointer[2] = 1-pointer[2];
    	}
    	
    	if (!finished.hotels .isEmpty()){
    		HashMap <String, Hotel> hotels_shadowing = new HashMap <String, Hotel>(hotels);
    		for (String key : finished.hotels.keySet()) {
    			if(finished.hotels.get(key)!=null)
    				hotels_shadowing.put(key, finished.hotels.get(key));
    			else
    				hotels_shadowing.remove(key);
    		}
    		hotels = hotels_shadowing;
    		//push to file[3] or file[4]
			try{
				push2file(hotels,HOTEL,4-pointer[1]);
					
			}catch(FileNotFoundException fnfe){
				return false;
			}catch(IOException io){
				return false;
			}
			pointer[1] = 1-pointer[1];
    	}
    	
    	if (!finished.flights .isEmpty()) {
    		HashMap <String, Flight> flights_shadowing = new HashMap <String, Flight>(flights);
    		for (String key : finished.flights.keySet()) {
    			if(finished.flights.get(key)!=null)
    				flights_shadowing.put(key, finished.flights.get(key));
    			else
    				flights_shadowing.remove(key);
    		}
    		flights =  flights_shadowing;
    		//push to file[1] or file[2]
			try{
				push2file(flights,FLIGHT,2-pointer[0]);
			}catch(FileNotFoundException fnfe){
				return false;
			}catch(IOException io){
				return false;
			}
			pointer[0] = 1-pointer[0];
    	}
    	
    	if (!finished.customers .isEmpty()){
    		HashMap <String, Customer> customers_shadowing = new HashMap <String, Customer>(customers);
    		for (String key : finished.customers.keySet()) {
    			if(finished.customers.get(key)!=null)
    				customers_shadowing.put(key, finished.customers.get(key));
    			else
    				customers_shadowing.remove(key);
    		}
    		customers = customers_shadowing;
    		//push to file[7] or file[8]
			try{
				push2file(customers,CUSTOMER,8-pointer[3]);
			}catch(FileNotFoundException fnfe){
				return false;
			}catch(IOException io){
				return false;
			}
			pointer[3] = 1-pointer[3];
    	}
 
    	if (!finished.reservations .isEmpty()) {
    		HashMap <String, ArrayList<Reservation>> reservations_shadowing = new HashMap <String, ArrayList<Reservation>>(reservations);
    		for (String key : finished.reservations.keySet()) {
    			if(finished.reservations.get(key)!=null)
    				reservations_shadowing.put(key, finished.reservations.get(key));
    			else
    				reservations_shadowing.remove(key);
    		}
    		reservations = reservations_shadowing;
    		//push to file[9] or file[10]
			try{
				push2file(reservations,RESERVATION,10-pointer[4]);
			}catch(FileNotFoundException fnfe){
				return false;
			}catch(IOException io){
				return false;
			}
			pointer[4] = 1-pointer[4];
    	}*/

    	lm.unlockAll(xid);
    	
    	return true; //page shadowing implies page level locking, always return true
    }

    public void abort(int xid)  
	throws RemoteException, 
               InvalidTransactionException {
    	// releases its locks
    	lm.unlockAll(xid);
    	return;
    }


    // ADMINISTRATIVE INTERFACE
    public boolean addFlight(int xid, String flightNum, int numSeats, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
          //no XID check any more
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+flightNum, LockManager.WRITE);
		} catch (DeadlockException e) {
			e.printStackTrace();
			return false;
		}    
    	if(flights==null) return false;
    	Flight curFlight = flights.get(flightNum);
    	Flight oldFlight = null;
    	if(curFlight==null){
    		curFlight = new Flight(flightNum);
    		flights.put(flightNum, curFlight);
    		}else{
    			oldFlight = new Flight(curFlight);
    		}
        if(price>0)
        	curFlight.price = price;
        curFlight.numSeats+=numSeats;
        curFlight.numAvail+=numSeats;
        if(curFlight.numSeats<0||curFlight.numSeats<0)
        	return false;
        RML.newLog(0, TableName, flightNum, oldFlight, curFlight);
    	return true;
    }

    public boolean deleteFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
          //no XID check any more
        try {
			lm.lock(xid, String.valueOf(myRMIName)+flightNum, LockManager.WRITE);
		} catch (DeadlockException e) {
			e.printStackTrace();
			return false;
		}
        if(flights==null) return false;
        Flight curFlight = flights.get(flightNum);
        //if already reserved
        if (curFlight==null||curFlight.numSeats!=curFlight.numAvail) {
        	return false;
        }
        flights.remove(curFlight);
        RML.newLog(0, TableName, flightNum, curFlight, null);
    	return true;
    } 
		
    public boolean addRooms(int xid, String location, int numRooms, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	 //no XID check any more
        try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.WRITE);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        if(hotels==null) return false;
        Hotel curHotel = hotels.get(location);
        Hotel oldHotel = null;
    	if(curHotel==null){
    		curHotel = new Hotel(location);
    		hotels.put(location, curHotel);
    	}else{
    		oldHotel = new Hotel(curHotel);
    	}
        if(price>0)
        	curHotel.price = price;	// directly overwrite
        curHotel.numRooms += numRooms;
        curHotel.numAvail += numRooms;

        RML.newLog(0, TableName, location, oldHotel, curHotel);
        return true;
    }

    public boolean deleteRooms(int xid, String location, int numRooms) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	 //no XID check any more
        try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.WRITE);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        if(hotels==null) return false;
        Hotel curHotel = hotels.get(location); 
        Hotel oldHotel = null;
        if(curHotel==null||curHotel.numAvail<numRooms||curHotel.numRooms<numRooms)
        	return false;  
        else oldHotel = new Hotel(curHotel);
        curHotel.numRooms -= numRooms;
        curHotel.numAvail -= numRooms;
        if(curHotel.numRooms==0) {
            hotels.remove(location);
            curHotel = null;
        }
        RML.newLog(0, TableName, location, oldHotel, curHotel);

        return true;
    }

    public boolean addCars(int xid, String location, int numCars, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	 //no XID check any more
        try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.WRITE);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        if(cars==null) return false;
        Car curCar = cars.get(location);
        Car oldCar = null;
    	if(curCar==null){
    		curCar = new Car(location);
    		cars.put(location, curCar);
    	}else{
    		oldCar = new Car(curCar);
    	}
        if(price>0)
        	curCar.price = price; // should directly overwrite price
        curCar.numCars += numCars;
        curCar.numAvail += numCars;
        
        RML.newLog(0, TableName, location, oldCar, curCar);
        return true;
    }

    public boolean deleteCars(int xid, String location, int numCars) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	 //no XID check any more
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.WRITE);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        if(cars==null) return false;
        Car curCar = cars.get(location);
        Car oldCar = null;
        if(curCar==null||curCar.numAvail<numCars||curCar.numCars<numCars)
        	return false;
        else oldCar = new Car(curCar);
        curCar.numCars -= numCars;
        curCar.numAvail -= numCars;
        
        if(curCar.numCars==0) {
        	cars.remove(location);
            curCar = null;
        }
        
        RML.newLog(0, TableName, location, oldCar, curCar);
        return true;
    }

    public boolean newCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	 //no XID check any more
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+custName, LockManager.WRITE);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        if(customers==null) return false;
        Customer curCustomer = customers.get(custName);
    	if(curCustomer==null){
    		curCustomer = new Customer(custName);
    		customers.put(custName, curCustomer);
    	}
        RML.newLog(0, TableName, custName, null, curCustomer);
        return true;
    }

    public boolean deleteCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	// get transaction
    	 //no XID check any more
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+custName, LockManager.WRITE);
	    	lm.lock(xid, String.valueOf("reservations")+custName, LockManager.WRITE);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

        if(customers==null||reservations==null) 
        	return false;
        Customer curCustomer = customers.remove(custName);
        if(curCustomer==null)
        	return false;

        ArrayList<Reservation> curRevlist =reservations.remove(custName);
      //No longer to handle consistency of data by itself
        /* ArrayList<Reservation> curRevlist = reservations.get(custName);
        
        
        for(Reservation r:curRevlist) {
	   		switch(r.resvType){
	   			case FLIGHT:
	   				if (acqCurEntry(tr,FLIGHT,r.resvKey,true)) 
	   					++tr.flights.get(r.resvKey).numAvail;
	   				else
	   					abort(xid);
	   				break;
	   			case HOTEL:
	   				if (acqCurEntry(tr,HOTEL,r.resvKey,true)) 
	   					++tr.hotels.get(r.resvKey).numAvail;
	   				else
	   					abort(xid);
	   				break;
	   			case CAR:
	   				if (acqCurEntry(tr,CAR,r.resvKey,true)) 
	   					++tr.cars.get(r.resvKey).numAvail;
	   				else
	   					abort(xid);
	   				break;
	   			default: System.err.println("Unidentified reservation");
	   		}
        }*/
        RML.newLog(0, TableName, custName, curCustomer, null);
        RML.newLog(0, "reservations", custName, curRevlist, null);
        return true;
        
    }


    // QUERY INTERFACE
    public int queryFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+flightNum, LockManager.READ);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}    
    	if(flights==null) return -1;
    	Flight curFlight = flights.get(flightNum);
        
    	if(curFlight!=null)
			return curFlight.numAvail;
    	else
    		return -1;
    }	

    public int queryFlightPrice(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+flightNum, LockManager.READ);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}    
    	if(flights==null) return -1;
    	Flight curFlight = flights.get(flightNum);
        
    	if(curFlight!=null)
			return curFlight.price;
    	else
    		return -1;
    }

    public int queryRooms(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.READ);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}    
    	if(hotels==null) return -1;
    	Hotel curHotel = hotels.get(location);
        
    	if(curHotel!=null)
			return curHotel.numAvail;
    	else
    		return -1;
    }

    public int queryRoomsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.READ);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}    
    	if(hotels==null) return -1;
    	Hotel curHotel = hotels.get(location);
        
    	if(curHotel!=null)
			return curHotel.price;
    	else
    		return -1;
    }

    public int queryCars(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.READ);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}    
    	if(cars==null) return -1;
    	Car curCar = cars.get(location);
        
    	if(curCar!=null)
			return curCar.numAvail;
    	else
    		return -1;
    }

    public int queryCarsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try {
			lm.lock(xid, String.valueOf(myRMIName)+location, LockManager.READ);
		} catch (DeadlockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}    
    	if(cars==null) return -1;
    	Car curCar = cars.get(location);
        
    	if(curCar!=null)
			return curCar.price;
    	else
    		return -1;

    }

    public int queryCustomerBill(int xid, String custName)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	int total=0;
    	///////////not implement yet
    	return total;
    }




    // TECHNICAL/TESTING INTERFACE
    public boolean shutdown()
	throws RemoteException {
    	System.exit(0);
    	return true;
    }

    public boolean dieNow() 
	throws RemoteException {
    	System.exit(1);
    	return true; // We won't ever get here since we exited above;
	             // but we still need it to please the compiler.
    }


}

/////////////////////////////////////////////////////////////////////   
 class Flight implements Serializable {
	String flightNum;
	int price;
	int numSeats;
	int numAvail;
	
	Flight(String flightN){
		flightNum=flightN;
		price=0;
		numSeats=0;
		numAvail=0;
	}
	
	Flight(String flightN,int pri,int numS,int numA){
		flightNum=flightN;
		price=pri;
		numSeats=numS;
		numAvail=numA;
	}

	public Flight(Flight flight) {
		flightNum=flight.flightNum;
		price=flight.price;
		numSeats=flight.numSeats;
		numAvail=flight.numAvail;
		// TODO Auto-generated constructor stub
	}
}

 class Car implements Serializable{
	String location;
	int price;
	int numCars;
	int numAvail;
	
	Car(String loc){
		location=loc;	
	}
	Car(String loc,int pri,int numS,int numA){
		location=loc;
		price=pri;
		numCars=numS;
		numAvail=numA;
	}
	public Car(Car car) {
		// TODO Auto-generated constructor stub
		location=car.location;
		price=car.price;
		numCars=car.numCars;
		numAvail=car.numAvail;
	}

}

 class Hotel implements Serializable{
	
	String location;
	int price;
	int numRooms;
	int numAvail;
	
	Hotel(String loc){
		location=loc;
	}
	
	Hotel(String loc,int pri,int numS,int numA){
		location=loc;
		price=pri;
		numRooms=numS;
		numAvail=numA;
	}

	public Hotel(Hotel hotel) {
		// TODO Auto-generated constructor stub
		location=hotel.location;
		price=hotel.price;
		numRooms=hotel.numRooms;
		numAvail=hotel.numAvail;
	}
}

 class Customer implements Serializable{
	String custName;
	//int total;
	Customer(String name){
		custName=name;
		//total=0;
	}
	public Customer(Customer customer) {
		// TODO Auto-generated constructor stub
		custName=customer.custName;
	}
}

 class Reservation implements Serializable{
	String custName;
	int resvType;
	String resvKey;
	//int price; //for possible calculate
	Reservation(String name,int resvT,String resvK){//int pric
		custName=name;
		resvType=resvT;
		resvKey=resvK;
		//price=pric;
		
	}
}


 
class RMLog implements Serializable {
	 public static final int PUT = 0;
	 public static final int REMOVE = 1;
	 public static final int COMMIT = 2;
	 public static final int ABORT = 3;
	 public static final int CLR = 4;
	 public static final int PREPARE = 5;


	 int type;
	 public final int LSN;
	 /* Table Name
	  * "RMFlights";
	  *	"RMRooms";
	  *	"RMCars";
	  *	"RMCustomers";
	  */


	 public final String table;
	 public final String key;
	 public Object beforeVal;
	 public Object afterVal;


	 public RMLog(int LSN, int type,
			 String table, String key, Object beforeVal, Object afterVal) {
		 this.LSN = LSN;
		 this.type = type;
		 this.table = table;
		 this.key = key;
		 this.beforeVal = beforeVal;
		 this.afterVal = afterVal;
	 }


 }
 /*
  * Log Manager for a Resourse Manager.
  * 
  */
 
 class RMLogManager {
	 private static final String logSuffix = ".log";


	 private String RMName;


	 //output Stream
	 private FileOutputStream fos = null;
	 private ObjectOutputStream oos = null;


	 //input Stream
	 private FileInputStream fis = null;
	 private ObjectInputStream ois = null;


	 private int LSN = -1;	// Latest Log Sequence Number
	 private LinkedList<RMLog> logQueue = null; // the log sequence in the memory
	 public RMLogManager(String tableName) {
		 // 
		 RMName = tableName;
		 logQueue = new LinkedList<RMLog>();
	 }


	 private void setOutputStream() throws IOException, FileNotFoundException {
		 try {
			 fos = new FileOutputStream(RMName + logSuffix, true);
		 } catch (FileNotFoundException fe) {
			 File f = new File(RMName + logSuffix);
			 f.mkdirs();
			 f.createNewFile();
			 fos = new FileOutputStream(f, true);
		 }
		 oos = new ObjectOutputStream(fos);


	 }


	 private void closeOutputStream() throws IOException {
		 if (oos != null)
			 oos.close();
		 if (fos != null)
			 fos.close(); // redundant
	 }


	 private void setInputStream() throws FileNotFoundException, IOException {
		fis = new FileInputStream(RMName + logSuffix);
		ois = new ObjectInputStream(fis);
	 }


	 private void closeInputStream() throws IOException {
		 if (ois != null)
			 ois.close();
		 if (fis != null)
			 fis.close(); // redundant
	 }


	 public void newLog(int type, String table, String key, Object before, Object after) {
		 logQueue.addLast(new RMLog(++LSN, type, table, key, before, after));
	 }




	 public void flushLog(int LSN) throws IOException {
		 while(!logQueue.isEmpty() && logQueue.peekFirst().LSN <= LSN) {
			 oos.writeObject(logQueue.removeFirst());
		 }
	 }


	 // return the log sequence after a specific LSN, used for recovery
	 public ArrayList<RMLog> LogSequenceAfter(int LSN) 
			 throws IOException, ClassNotFoundException {
		 // TODO: read RMLog out from file stream
		 this.closeOutputStream();
		 try {
			 this.setInputStream();
		 } catch (FileNotFoundException fne) {
			 return new ArrayList<RMLog>();
		 }


		 ArrayList<RMLog> logList = new ArrayList<RMLog>();
		 while(true) {
			 RMLog rmlog = null;
			 try {
				rmlog = (RMLog)ois.readObject();
			 } catch (EOFException eofe) {
				 // End of File
				 break;
			 }
			 if (rmlog.LSN > LSN) {
				 logList.add(rmlog);
			 }
		 }
		 return logList;
	 }
 }
