package edu.msu.cse.msudb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Member;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler implements Runnable {

	Socket connection;
	String message;

	public ClientHandler(Socket c) {
		this.connection = c;

	}
	

	
	@Override
	public void run() {
		//System.out.println("A Client message received!:" + (String)msg);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			PrintWriter pw = new PrintWriter(connection.getOutputStream(), true);

			while (true) {
				message = br.readLine();
				Message mes = extractMessageString(message);
				if (mes.type.equals("GET")) {
					getMessageHandler((GetMessage) mes, new PrintWriter(connection.getOutputStream(), true));
				} else if (mes.type.equals("PUT")) {
					putMessageHandler((PutMessage) mes, new PrintWriter(connection.getOutputStream(), true));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		 * try { br = new BufferedReader(new
		 * InputStreamReader(connection.getInputStream())); message =
		 * br.readLine(); } catch (IOException e2) { // TODO Auto-generated
		 * catch block e2.printStackTrace(); }
		 * 
		 * System.out.println(message);
		 * 
		 * Message mes = extractMessageString(message); try { if
		 * (mes.type.equals("GET")) { getMessageHandler((GetMessage) mes, new
		 * PrintWriter(connection.getOutputStream(), true)); } else if
		 * (mes.type.equals("PUT")) { putMessageHandler((PutMessage) mes, new
		 * PrintWriter(connection.getOutputStream(), true)); } } catch
		 * (Exception e) { e.printStackTrace(); }
		 */

	}

	private Message extractMessageString(String message) {
		Message mes;
		String type = message.substring(0, message.indexOf(MServer.mainDelimiter));
		if (type.equals("GET")) {
			mes = new GetMessage(message);
		} else {
			mes = new PutMessage(message);
		}
		return mes;
	}

	/*
	 * A client sends a GET request with an item key and its GSTc to the server
	 * that serves the partition containing the item.
	 * 
	 * The server - first updates its GST if it is smaller than the client’s. -
	 * The server then obtains the latest version in the version chain of the
	 * requested item, which is either created by clients attached to the local
	 * datacenter or has an update timestamp no greater than the partition’s
	 * GST. Hence, a client always reads local updates without any delay and
	 * replicated updates from other datacenters once they are globally stable.
	 * 
	 * 
	 * 
	 * The partition returns the item value, its update timestamp, and its GST
	 * back to the client.
	 */

	private void getMessageHandler(GetMessage mes, PrintWriter pw) throws IOException {
		Record record = MServer.findTheNewestStableVersion(mes.key);
		if (record == null) {
			pw.println("Not found");
			//connection.close();
			//Test.....
			if (MServer.collect)
			{
				MServer.numberOfOperations++;
			}
			//.........
		} else {
			String responseForClient;
			try {
				responseForClient = record.ut + MServer.mainDelimiter + record.sr + MServer.mainDelimiter + record.key
						+ MServer.mainDelimiter + new String(record.value, "UTF-8");
				pw.println(responseForClient);
				//Test.....
				if (MServer.collect)
				{
					MServer.numberOfOperations++;
				}
				//.........
				//connection.close();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/*
	 * A client sends a PUT request, h PUTREQ k,v,DTc i , which includes the
	 * item key, the update value, and the client’s dependency time, to the
	 * server that manages the item.
	 * 
	 * The server then checks that the client’s dependency time is smaller than
	 * its physical clock time. -If it, it waits until the condition becomes
	 * true.
	 * 
	 * The server then updates the local element of its version vector with its
	 * physical clock time.
	 * 
	 * It creates a new version of the item by assigning it a tuple that
	 * consists of the key, value, update time, and its replica id, and inserts
	 * the newly created item version in the version chain of the item.
	 * 
	 * The server sends a reply with the update time of the newly created item
	 * version to the client.
	 */
	private void putMessageHandler(PutMessage mes, PrintWriter pw) throws IOException {

		//We don't need to wait when we use HCL
		/*
		 * 
		 * long currentTime = MServer.getCurrentTime(); //waiting if
		 * (currentTime < mes.dt) { try { Thread.sleep(mes.dt - currentTime); }
		 * catch (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * } currentTime = MServer.getCurrentTime();
		 */

		//creating the new version
		Record newVersion = new Record();
		newVersion.key = mes.key;
		newVersion.sr = MServer.dcn;
		//HLC
		newVersion.ut = MServer.creatNewTimestamp(mes.dt);
		newVersion.dv = new HashMap<Integer, Long>();
		try {
			newVersion.value = mes.value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Retrieving the previous key (the key with the highest timestamp) for conflict detection: 
		DatabaseEntry theKey = null;
		try {
			theKey = new DatabaseEntry(MServer.getHash(mes.key));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DatabaseEntry preData = new DatabaseEntry();
		Record preVersion = null;
		if (MServer.db.get(null, theKey, preData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			preVersion = (Record) MServer.rb.entryToObject(preData);
		}

		//writing the new version to the DB
		DatabaseEntry theData = new DatabaseEntry();
		MServer.rb.objectToEntry(newVersion, theData);

		synchronized (MServer.heartbeatTimer) {

			MServer.db.put(null, theKey, theData);
			//Sending reply to the client
			String response = Long.toString(newVersion.ut) + MServer.mainDelimiter + newVersion.sr
					+ MServer.mainDelimiter + newVersion.key;
			pw.println(response);
			
			//Test.....
			if (MServer.collect)
			{
				MServer.numberOfOperations++;
			}
			//.........
			//connection.close();
			// send replicate messages....
			//note that sending replicate message should be before put to guarantee that the replicate message
			//arives sooner than other possilbe replicate. 
			//We use heartbeadTimer lock to serialize all writes to the channel
			//We also shutdown the heartbeat and restart it to send after a heatbeatInterval. 
			//so we do this: 
			//First we lock the hearbeatTime, so other threads cannot send replicate (it serializes)
			//Then, we shutdown the timer. 
			//Send the replicate. 
			//One the reolicates are sent to the channel, we restart the time. 
			//release the lock. 

			//MServer.heartbeatTimer.shutdown();
			sendReplicates(newVersion, preVersion, mes.dv);
			//MServer.heartbeatTimer = Executors.newScheduledThreadPool(1);
			//MServer.heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(), MServer.heartbeatInterval,
				//	MServer.heartbeatInterval, TimeUnit.MILLISECONDS);
		}

		//updating the vv
		//I think it is not necessary, as in the gst compuation I use the actual clock not vv entry for this node. 
		synchronized (MServer.vvLock) {
			if (MServer.vv.get(MServer.dcn) < MServer.hlc) {
				MServer.vv.set(MServer.dcn, MServer.hlc);
			}
		}
	}

	private void sendReplicates(Record newVersion, Record preVersion, HashMap<Integer, Long> dv) {
		if (MServer.numberOfDcs < 2) return; //there is no need to send replicate messages. 
		MServer.lastReplicateTime = MServer.getCurrentTime();
		StringBuffer repMessage = new StringBuffer( "REP" + MServer.mainDelimiter + newVersion.sr + MServer.mainDelimiter + newVersion.ut
				+ MServer.mainDelimiter);
		//We don;t use LRT in the code. We control it with timer instead. So I comment it here...
		/*
		 * synchronized (MServer.lastReplcateTimeLock) {
		 * MServer.lastReplicateTime = newVersion.ut; }
		 */

		try {
			if (preVersion != null) {
				repMessage.append(preVersion.sr + MServer.mainDelimiter + preVersion.ut + MServer.mainDelimiter);

			} else {
				repMessage.append(MServer.emptyElement + MServer.mainDelimiter + MServer.emptyElement
						+ MServer.mainDelimiter);
			}
			if (dv.size() > 0) {
				for (Map.Entry<Integer, Long> dvItem : dv.entrySet()) {
					repMessage.append(dvItem.getKey() + MServer.intraDepItemDelimiter + dvItem.getValue()
							+ MServer.interDepItemDelimiter);
				}
			} else
				repMessage.append(MServer.emptyElement);
			repMessage.append(MServer.mainDelimiter + newVersion.key + MServer.mainDelimiter
					+ System.currentTimeMillis()); //Test
					//Test: + new String(newVersion.value, "UTF-8"));
			for (int i = 0; i < MServer.numberOfDcs; i++) {
				if (i == MServer.dcn)
					continue;
				System.out.println("Sending REP to server dcn= " + i + " pn= " + MServer.pn + ": " + repMessage);
				MServer.sendToPartition(i, MServer.pn, repMessage.toString());
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
