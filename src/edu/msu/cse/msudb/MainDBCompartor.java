package edu.msu.cse.msudb;

import java.util.Comparator;

import com.sleepycat.je.DatabaseEntry;

public class MainDBCompartor  implements Comparator {

	@Override
	public int compare(Object d1, Object d2) {
		 byte[] b1 = (byte[])d1;
	     byte[] b2 = (byte[])d2;
	     
	     DatabaseEntry theData1 = new DatabaseEntry(b1);
	     DatabaseEntry theData2 = new DatabaseEntry(b2);
	     
	     Record record1 = (Record) MServer.rb.entryToObject(theData1);
	     Record record2 = (Record) MServer.rb.entryToObject(theData2);
	     
	     //System.out.println("Inside comparator. record1.ut= " + record1.ut + " record2.ut= " + record2.ut);
	     //we want to put records with higher ut first:
	     if (record1.ut > record2.ut) return -1; 
	     else if (record1.ut == record2.ut)
	    	 {
	    	 //if timestamps are equal we give priority to the version with hgiher dc number. 
	    	 	if (record1.sr > record2.sr) return -1;
	    	 }
	     return 1;
	}
	

}
