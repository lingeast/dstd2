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
    

    public ResourceManagerImpl(String rmiName) throws IOException {
	myRMIName = rmiName;
	xidCounter = 0;
	FileReader[] ff = new FileReader[11];
	boolean flag_restore = true;
	
	new File("data").mkdir(); //not yet exist
		
	
	for(int i = 0;i<11;i++){  //reading file
    	try{
    		ff[i] = new FileReader(file[i]); 
    	}catch(FileNotFoundException FN){
    		File newfile = new File(file[i]);
    	      // creates the file
    		newfile.createNewFile();
    	    ff[i] = new FileReader(file[i]); 
    	
	    	if(i == 0){//if pointer not exists, create a new one and point to 1  //no need to restore
	    		flag_restore = false;
	    	    FileWriter writer = new FileWriter(file[0]); 
	  	      // Writes the content to the file
	    	    writer.write("000000");
	    	    writer.flush();
	    	    writer.close();
	    	}
    	}
	}

	char [] charb = new char[6];
	ff[0].read(charb);
	ff[0].close();
	ObjectInputStream ois = null;
	FileInputStream f = null;
	if(flag_restore){//input data
		//try{
		for(int i = 0; i<6;++i){
			pointer[i] =charb[i]-'0';
			try{
			switch(i){
	    		case 0:
	    			f= new FileInputStream(file[(1+pointer[i])]);
	    			if (f.read()!=-1){
	    				ois = new ObjectInputStream(f);
	    				flights = (HashMap <String, Flight>)ois.readObject();
	    			}
	    			break;
	    		case 1:
	    			f= new FileInputStream(file[(3+pointer[i])]);
	    			if (f.read()!=-1){
	    				ois = new ObjectInputStream(f);
	    				hotels = (HashMap <String, Hotel>)ois.readObject();
	    			}
	    			break;
	    		case 2:
	    			f= new FileInputStream(file[(5+pointer[i])]);
	    			if (f.read()!=-1){
	    				ois = new ObjectInputStream(f);
	    				cars = (HashMap <String, Car>)ois.readObject();
	    			}
	    			break;
	    		case 3:
	    			f= new FileInputStream(file[(7+pointer[i])]);
	    			if (f.read()!=-1){
	    				ois = new ObjectInputStream(f);
	    				customers = (HashMap <String, Customer>)ois.readObject();
	    			}
	    			break;
	    		case 4:
	    			f= new FileInputStream(file[(9+pointer[i])]);
	    			if (f.read()!=-1){
	    				ois = new ObjectInputStream(f);
	    				reservations = (HashMap<String, ArrayList<Reservation>>)ois.readObject();
	    			}
	    			break;
	    		case 5:
	    			xidCounter = pointer[i];
	    		default: break;
			}
			ois.close();
			}catch(EOFException  eo){
				break;
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			}
		}
		
	}

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

	//reservation 
    public static final int FLIGHT = 1;
    public static final int HOTEL = 2;
    public static final int CAR = 3;
    public static final int CUSTOMER = 4;
    public static final int RESERVATION = 5;
    String[] file={"data/Pointer","data/Flights1","data/Flights2","data/Hotels1","data/Hotels2","data/Cars1","data/Cars2","data/Customers1","data/Customers2","data/Reservations1","data/Reservations2"};
	LockManager lm = new LockManager();
	int [] pointer =new int [6];   //the 6th represents how many transactions ran.
	boolean flag_pointerbefore = false;
	boolean flag_pointerafter = false;
    // Mapping xid to transaction private resources
    HashMap <Integer, TransRes> trans = new HashMap<Integer, TransRes>();

    // Use Hash Map to represent tables
    // flightNum as primary key
    HashMap <String, Flight> flights = new HashMap <String, Flight>();
    
    // location as primary key
    HashMap <String, Car> cars = new HashMap <String, Car>();
    
    // location as as primary key
    HashMap <String, Hotel> hotels = new HashMap <String, Hotel>();
    
    // custName as primary key
    HashMap <String, Customer> customers = new HashMap <String, Customer>();
    
    // custName as primary key, combined with customer table
    HashMap <String, ArrayList<Reservation>> reservations = new HashMap <String, ArrayList<Reservation>>();
    
    protected int xidCounter;
    
    private boolean acqCurEntry(TransRes tr, int tableName, String Primarykey, boolean flag_wr)throws RemoteException, 
    TransactionAbortedException, InvalidTransactionException {
    	//check if it already be in table // if so and flag = false, return true directly..
    	if(!flag_wr){
	    	switch(tableName){
		    	case CAR:
		    		if(tr.cars.containsKey(Primarykey))
		    			return true;
		    		break;
				case HOTEL:
		    		if(tr.hotels.containsKey(Primarykey))
		    			return true;
		    		break;
			    case FLIGHT:
			    	if(tr.flights.containsKey(Primarykey))
			    		return true;
		    		break;
			    case CUSTOMER:
			    	if(tr.customers.containsKey(Primarykey))
			    		return true;
			    	break;
		    	case RESERVATION:
			    	if(tr.reservations.containsKey(Primarykey))
			    		return true;
			    	break;
				default: System.err.println("Unidentified " + tableName);
					return false;
	    		}
    	}
    	//locking flag = true means write
    	if(flag_wr){
	    	try {
				lm.lock(tr.xid, String.valueOf(tableName)+Primarykey, LockManager.WRITE);
	    	} catch(DeadlockException dle) {	// handle deadlock
				System.err.println(dle.getMessage());
				abort(tr.xid);
				throw new TransactionAbortedException(tr.xid,"Waiting for write key of"+String.valueOf(tableName)+Primarykey);
				//return false;
			}
    	}
    	else{
        	try {
    			lm.lock(tr.xid, String.valueOf(tableName)+Primarykey, LockManager.READ);
        	} catch(DeadlockException dle) {	// handle deadlock
    			System.err.println(dle.getMessage());
    			abort(tr.xid);
    			throw new TransactionAbortedException(tr.xid,"Waiting for read key of"+String.valueOf(tableName)+Primarykey);
    			//return false;
    		}
    		
    	}

    	if(flag_wr){
        	//create new entry shadowing/logging for write
		    switch(tableName){
		    	case CAR:
		    		if(tr.cars.containsKey(Primarykey))
		    			return true;
		    		if(cars.containsKey(Primarykey))
		    				tr.cars.put(Primarykey,new Car(cars.get(Primarykey)));
		    		else
		    				tr.cars.put(Primarykey,new Car(Primarykey));
			    	return true;
			    	
				case HOTEL:
		    		if(tr.hotels.containsKey(Primarykey))
		    			return true;
		    		if(hotels.containsKey(Primarykey))
		    				tr.hotels.put(Primarykey,new Hotel(hotels.get(Primarykey)));
		    		else
		    				tr.hotels.put(Primarykey,new Hotel(Primarykey));
			    	return true;
			
			    case FLIGHT:
			    	if(tr.flights.containsKey(Primarykey))
			    		return true;
			    	if(flights.containsKey(Primarykey))
		    				tr.flights.put(Primarykey,new Flight(flights.get(Primarykey)));
		    		else
		    				tr.flights.put(Primarykey,new Flight(Primarykey));
			    	return true;
			    	
			    case CUSTOMER:
			    	if(tr.customers.containsKey(Primarykey))
			    		return true;
			    	if(customers.containsKey(Primarykey))
							tr.customers.put(Primarykey,new Customer(customers.get(Primarykey)));
			    	else
							tr.customers.put(Primarykey,new Customer(Primarykey));
			    	return true;
			
		    	case RESERVATION:
		    		if(tr.reservations.containsKey(Primarykey))
			    		return true;
			    	if(reservations.containsKey(Primarykey)&&reservations.get(Primarykey)!=null)
							tr.reservations.put(Primarykey,new ArrayList<Reservation>(reservations.get(Primarykey)));
			    	else
							tr.reservations.put(Primarykey,new ArrayList<Reservation>());
			    	return true;
		
				default: System.err.println("Unidentified " + tableName);
	
			}
	    }else{
		    switch(tableName){
		    	case CAR:
		    		if(cars.containsKey(Primarykey))
		    				tr.cars.put(Primarykey,cars.get(Primarykey));
			    	return true;
			    	
				case HOTEL:
		    		if(hotels.containsKey(Primarykey))
		    				tr.hotels.put(Primarykey,hotels.get(Primarykey));
			    	return true;
			
			    case FLIGHT:
			    	if(flights.containsKey(Primarykey))
		    				tr.flights.put(Primarykey,flights.get(Primarykey));
			    	return true;
			    	
			    case CUSTOMER:
			    	if(customers.containsKey(Primarykey))
							tr.customers.put(Primarykey,customers.get(Primarykey));
			    	return true;
			
		    	case RESERVATION:
			    	if(reservations.containsKey(Primarykey))
							tr.reservations.put(Primarykey,reservations.get(Primarykey));
			    	return true;
		
				default: System.err.println("Unidentified " + tableName);

		}
	    }
		return false;
	}

    // TRANSACTION INTERFACE
    public int start()
    		throws RemoteException {
    	++xidCounter; 
    	trans.put(xidCounter, new TransRes(xidCounter));
    	pointer[5] = xidCounter;
    	try {
			push2file(null,0,0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return (xidCounter);
    	
    }
    
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
    	
    	TransRes finished = trans.remove(xid);
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
    	}
    	if(flag_pointerbefore)	//for test, be4 or after pointer switch
    		dieNow();
    	//swap pointer
		try{
	    	push2file(null,0,0);

		}catch(FileNotFoundException fnfe){
			return false;
		}catch(IOException io){
			return false;
		}
	    if(flag_pointerafter)	//for test, be4 or after pointer switch
	    	dieNow();
    	// releases its locks
    	lm.unlockAll(xid);
    	
    	return true; //page shadowing implies page level locking, always return true
    }

    public void abort(int xid)
	throws RemoteException, 
               InvalidTransactionException {
    	if(!trans.containsKey(xid)) 
    		throw new InvalidTransactionException(xid,"aborting");
    	trans.remove(xid);
    	// releases its locks
    	lm.unlockAll(xid);
    	return;
    }


    // ADMINISTRATIVE INTERFACE
    public boolean addFlight(int xid, String flightNum, int numSeats, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,flightNum);
            		else throw new TransactionAbortedException(xid,flightNum);
        Flight curFlight = null;
        if (acqCurEntry(tr,FLIGHT,flightNum,true)) {
        	curFlight = tr.flights.get(flightNum);
        } else {
        	abort(xid);
        }
        
        if(price>0)
        	curFlight.price = price;
        curFlight.numSeats+=numSeats;
        curFlight.numAvail+=numSeats;
        //tr.flights.put(flightNum,curFlight);   //no need

    	return true;
    }

    public boolean deleteFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,flightNum);
            		else throw new TransactionAbortedException(xid,flightNum);
        Flight curFlight = null;
        if (acqCurEntry(tr,FLIGHT,flightNum,true)) {
        	curFlight = tr.flights.get(flightNum);
        } else {
        	abort(xid);
        }
        //if already reserved
        if (curFlight.numSeats!=curFlight.numAvail) {
        	return false;
        }
        tr.flights.put(flightNum,null);
    	return true;
    } 
		
    public boolean addRooms(int xid, String location, int numRooms, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Hotel curHotel = null;
        if (acqCurEntry(tr,HOTEL,location,true)) {
        	curHotel = tr.hotels.get(location);
        } else {
        	abort(xid);
        }
    	
        //hotel.price=hotel.price<price?price:hotel.price;
        if(price>0)
        	curHotel.price = price;	// directly overwrite
        curHotel.numRooms += numRooms;
        curHotel.numAvail += numRooms;

        return true;
    }

    public boolean deleteRooms(int xid, String location, int numRooms) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Hotel curHotel = null;
        if (acqCurEntry(tr,HOTEL,location,true)) {
        	curHotel = tr.hotels.get(location);
        } else {
        	abort(xid);
        }
        
        if(curHotel.numAvail<numRooms||curHotel.numRooms<numRooms)
        	return false;  
        
        curHotel.numRooms -= numRooms;
        curHotel.numAvail -= numRooms;

        if(curHotel.numRooms==0) 
            tr.hotels.put(location,null);
        
        return true;
    }

    public boolean addCars(int xid, String location, int numCars, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Car curCar = null;
        if (acqCurEntry(tr,CAR,location,true)) {
        	curCar = tr.cars.get(location);
        } else {
        	abort(xid);
        }
        
        if(price>0)
        	curCar.price = price; // should directly overwrite price
        curCar.numCars += numCars;
        curCar.numAvail += numCars;
        
        return true;
    }

    public boolean deleteCars(int xid, String location, int numCars) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Car curCar = null;
        if (acqCurEntry(tr,CAR,location,true)) {
        	curCar = tr.cars.get(location);
        } else {
        	abort(xid);
        }
        
        if(curCar.numAvail<numCars||curCar.numCars<numCars)
        	return false;
        
        curCar.numCars -= numCars;
        curCar.numAvail -= numCars;
        
        if(curCar.numCars==0) 
            tr.cars.put(location,null);
        return true;
    }

    public boolean newCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,custName);
            		else throw new TransactionAbortedException(xid,custName);
        Customer curCustomer = null;
        if (acqCurEntry(tr,CUSTOMER,custName,true)) {
        	curCustomer = tr.customers.get(custName);
        } else {
        	abort(xid);
        }
        return true;
    }

    public boolean deleteCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,custName);
            		else throw new TransactionAbortedException(xid,custName);
        Customer curCustomer = null;
        ArrayList<Reservation> curRevlist = null;
        if (acqCurEntry(tr,CUSTOMER,custName,true)&&acqCurEntry(tr,RESERVATION,custName,true)) {
        	curCustomer = tr.customers.get(custName);
        	curRevlist = tr.reservations.get(custName);
        } else {
        	abort(xid);
        }
        
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
   		
        }
        tr.customers.put(custName,null);
        tr.reservations.put(custName,null);
        return true;
        
    }


    // QUERY INTERFACE
    public int queryFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
				TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5]+1)throw new InvalidTransactionException(xid,flightNum);
            		else throw new TransactionAbortedException(xid,flightNum);
        Flight curFlight = null;
        if (acqCurEntry(tr,FLIGHT,flightNum,false)) {
        	curFlight = tr.flights.get(flightNum);
        } else {
        	abort(xid);
        }
        
    	if(curFlight!=null)
			return curFlight.numAvail;
    	else
    		return -1;
    }	

    public int queryFlightPrice(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
				TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,flightNum);
            		else throw new TransactionAbortedException(xid,flightNum);
        Flight curFlight = null;
        if (acqCurEntry(tr,FLIGHT,flightNum,false)) {
        	curFlight = tr.flights.get(flightNum);
        } else {
        	abort(xid);
        }
        
    	if(curFlight!=null)
			return curFlight.price;
    	else
    		return -1;
    }

    public int queryRooms(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Hotel curHotel = null;
        if (acqCurEntry(tr,HOTEL,location,false)) {
        	curHotel = tr.hotels.get(location);
        } else {
        	abort(xid);
        }
    	
    	if(curHotel!=null)
    		return curHotel.numAvail;
    	else
    		return -1;
    }

    public int queryRoomsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Hotel curHotel = null;
        if (tr!=null&&acqCurEntry(tr,HOTEL,location,false)) {
        	curHotel = tr.hotels.get(location);
        } else {
        	abort(xid);
        }
    	
    	if(curHotel!=null)
    		return curHotel.price;
    	else
    		return -1;
    }

    public int queryCars(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Car curCar = null;
        if (tr!=null&&acqCurEntry(tr,CAR,location,false)) {
        	curCar = tr.cars.get(location);
        } else {
        	abort(xid);
        }
        
    	if(curCar!=null)
			return curCar.numAvail;
    	else
    		return -1;
    }

    public int queryCarsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Car curCar = null;
        if (tr!=null&&acqCurEntry(tr,CAR,location,false)) {
        	curCar = tr.cars.get(location);
        } else {
        	abort(xid);
        }
        
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
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,custName);
            		else throw new TransactionAbortedException(xid,custName);
    	ArrayList<Reservation> curRevlist = null;
         if (acqCurEntry(tr,RESERVATION,custName,false)) {
        	 curRevlist = tr.reservations.get(custName);
         } else {
         	 abort(xid);
         }
         
    	if(curRevlist==null)
    		return 0;
    	for(Reservation r:curRevlist) {
    		switch(r.resvType){
    			case FLIGHT:
    				if (acqCurEntry(tr,FLIGHT,r.resvKey,false)) 
    					total +=  tr.flights.get(r.resvKey).price;
    				else
    					abort(xid);
    				break;
    			case HOTEL:
    				if (acqCurEntry(tr,HOTEL,r.resvKey,false)) 
    					total +=  tr.hotels.get(r.resvKey).price;
    				else
    					abort(xid);
    				break;
    			case CAR:
    				if (acqCurEntry(tr,CAR,r.resvKey,false)) 
    					total +=  tr.cars.get(r.resvKey).price;
    				else
    					abort(xid);
    				break;
    			default: System.err.println("Unidentified reservation");
    		}
    		
    	}
    	return total;
    }


    // Reservation INTERFACE
    public boolean reserveFlight(int xid, String custName, String flightNum) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,custName+flightNum);
            		else throw new TransactionAbortedException(xid,custName+flightNum);
        Flight curFlight = null;
        if (acqCurEntry(tr,FLIGHT,flightNum,true)) {
        	curFlight = tr.flights.get(flightNum);
        } else {
        	abort(xid);
        	return false;
        }
        
    	ArrayList<Reservation> curRevlist = null;
        if (acqCurEntry(tr,RESERVATION,custName,true)) {
       	 curRevlist = tr.reservations.get(custName);
        } else {
        	abort(xid);
        	return false;
        }
        
    	if(curFlight != null&&curFlight.numAvail>0){
    		//price = flight.price;
    		--curFlight.numAvail;
  
    	} else 
    		return false;
    	
    	Reservation rev = new Reservation(custName, 1, flightNum); // 1 for a flight
        
    	curRevlist.add(rev);

    	return true;
    }
 
    public boolean reserveCar(int xid, String custName, String location) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Car curCar = null;
        if (acqCurEntry(tr,CAR,location,true)) {
        	curCar = tr.cars.get(location);
        } else {
        	abort(xid);
        }
        
    	ArrayList<Reservation> curRevlist = null;
        if (acqCurEntry(tr,RESERVATION,custName,true)) {
       	 curRevlist = tr.reservations.get(custName);
        } else {
        	abort(xid);
        }
        
    	if(curCar != null&&curCar.numAvail>0){
    		//price = flight.price;
    		--curCar.numAvail;

    	} else 
    		return false;
    	
    	Reservation rev = new Reservation(custName, 3, location); // 3 for a car
        
    	curRevlist.add(rev);

    	return true;
    }

    public boolean reserveRoom(int xid, String custName, String location) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	
    	// get transaction
        TransRes tr = trans.get(xid);
        if(tr==null) 
            if(xid>pointer[5])throw new InvalidTransactionException(xid,location);
            		else throw new TransactionAbortedException(xid,location);
        Hotel curHotel = null;
        if (acqCurEntry(tr,HOTEL,location,true)) {
        	curHotel = tr.hotels.get(location);
        } else {
        	abort(xid);
        }
        
    	ArrayList<Reservation> curRevlist = null;
        if (acqCurEntry(tr,RESERVATION,custName,true)) {
       	 curRevlist = tr.reservations.get(custName);
        } else {
        	abort(xid);
        }
        
    	if(curHotel != null&&curHotel.numAvail>0){
    		//price = flight.price;
    		--curHotel.numAvail;

    	} else 
    		return false;
    	
    	Reservation rev = new Reservation(custName, 2, location); // 2 for a room
    	curRevlist.add(rev);

    	return true;
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

    public boolean dieBeforePointerSwitch() 
    		throws RemoteException {
    	flag_pointerbefore = true;
    	return true;
    }

    public boolean dieAfterPointerSwitch() 
	throws RemoteException {
    	flag_pointerafter = true;
    	return true;
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

 class TransRes {
	 public final int xid;
	// Transaction's private current page for shadow paging
    public Map <String, Flight> flights;
    
    // location as primary key
    public Map <String, Car> cars;
    
    // location as as primary key
    public Map <String, Hotel> hotels;
    
    // custName as primary key
    public Map <String, Customer> customers ;
    
    // resvKey or custName? as primary key, combined with customer table
    public Map <String, ArrayList<Reservation>> reservations;
    
    public TransRes (int xid) {
    	this.xid = xid;
    	flights = new HashMap <String, Flight>();
    	cars = new HashMap <String, Car>();
    	hotels = new HashMap <String, Hotel>();
    	customers = new HashMap <String, Customer>();
    	reservations = new HashMap <String, ArrayList<Reservation>>();
    }
 }
