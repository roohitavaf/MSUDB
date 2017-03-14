package edu.msu.cse.msudb;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientListener implements Runnable {

	
	//Client Executors: 
	public static ExecutorService clientsExecutors = Executors.newFixedThreadPool(MServer.numberOfClientThreads);

	@Override
	public void run() {
		//Adding synchronous communication fro clients: 
		try {
			ServerSocket s = new ServerSocket(MServer.clientPort);
			System.out.println("Server is listening!");
			while (true) {
				Socket connection = s.accept();
				connection.setKeepAlive(true);
				System.out.println("New client arrived!");
				
				Thread newClientThread = new Thread (new ClientHandler(connection));
				newClientThread.start();
				
				//clientsExecutors.execute(worker);
				//System.out.println("added to executor queue!");

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
