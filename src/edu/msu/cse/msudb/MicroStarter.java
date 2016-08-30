package edu.msu.cse.msudb;

public class MicroStarter implements Runnable{

	@Override
	public void run() {
		System.out.println("Colleciton started!");
		MServer.collect = true;
		MServer.microStarterTimer.shutdown();
		
	}

}
