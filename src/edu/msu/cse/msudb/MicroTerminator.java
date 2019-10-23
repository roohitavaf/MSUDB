package edu.msu.cse.msudb;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class MicroTerminator implements Runnable{
	int length = 0;
	public MicroTerminator(int t) {
		length = t;
	}
	
	public void run() {
		MServer.collect = false;
		MServer.microTerminatorTimer.shutdown();
		System.out.println("Number of Operations= " + MServer.numberOfOperations);
		int numberOfOperations = MServer.numberOfOperations;
		System.out.println("Throughput= " + new Float(numberOfOperations)/ length);
		
		PrintWriter writer;
		try {
			writer = new PrintWriter("MicroResults.txt", "UTF-8");
			writer.println("Number of Operations= " + MServer.numberOfOperations + "\n");
			writer.println("Throughput= " + new Float(numberOfOperations)/ length);
			writer.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

}
