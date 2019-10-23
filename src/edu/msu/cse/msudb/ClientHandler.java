package edu.msu.cse.msudb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class ClientHandler implements Runnable {

	Socket connection;
	String message;

	public ClientHandler(Socket c) {
		this.connection = c;

	}

	public void run() {
		// System.out.println("A Client message received!:" + (String)msg);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			PrintWriter pw = new PrintWriter(connection.getOutputStream(), true);

			while (true) {
				message = br.readLine();
				Message mes = extractMessageString(message);
				if (mes.type.equals("GET")) {
					getMessageHandler((GetMessage) mes, pw);
				} else if (mes.type.equals("PUT")) {
					if (MServer.verbose)
						System.out.println("Received a PUT request, key= " + ((PutMessage) mes).key);
					putMessageHandler((PutMessage) mes, pw);
				} else if (mes.type.equals("ROTX")) {
					rotxMessageHandler((ROTXMessage) mes, pw);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			if (MServer.verbose)
				System.out.println("Client Disconnected");
		}
		/*
		 * try { br = new BufferedReader(new
		 * InputStreamReader(connection.getInputStream())); message = br.readLine(); }
		 * catch (IOException e2) { // TODO Auto-generated catch block
		 * e2.printStackTrace(); }
		 * 
		 * System.out.println(message);
		 * 
		 * Message mes = extractMessageString(message); try { if
		 * (mes.type.equals("GET")) { getMessageHandler((GetMessage) mes, new
		 * PrintWriter(connection.getOutputStream(), true)); } else if
		 * (mes.type.equals("PUT")) { putMessageHandler((PutMessage) mes, new
		 * PrintWriter(connection.getOutputStream(), true)); } } catch (Exception e) {
		 * e.printStackTrace(); }
		 */

	}

	private void rotxMessageHandler(ROTXMessage mes, PrintWriter printWriter) {
		// TODO Auto-generated method stub
		// taking max of dsv and ds
		if (mes.dsv.size() > 0) {
			for (int i = 0; i < MServer.DSV.size(); i++) {
				if (MServer.DSV.get(i) < mes.dsv.get(i))
					MServer.DSV.set(i, mes.dsv.get(i));
			}
		}
		for (Map.Entry<Integer, Long> dsEntry : mes.ds.entrySet()) {
			if (MServer.DSV.get(dsEntry.getKey()) < dsEntry.getValue())
				MServer.DSV.set(dsEntry.getKey(), dsEntry.getValue());
		}

		ArrayList<Long> sv = new ArrayList<Long>(MServer.DSV);

		// Add mes to rotxs to keep track of it
		int rotxId;
		synchronized (MServer.rotxs) {
			rotxId = MServer.currentrotxId++;
			MServer.rotxs.put(rotxId, mes);
		}

		// send requests
		for (String key : mes.keys) {
			// find the server to send
			try {
				int p = MServer.findPartition(key);
				StringBuffer SlcReqStr = new StringBuffer("");
				SlcReqStr.append("SREQ").append(MServer.mainDelimiter).append(MServer.pn).append(MServer.mainDelimiter)
						.append(rotxId).append(MServer.mainDelimiter).append(key).append(MServer.mainDelimiter);
				for (int i = 0; i < sv.size(); i++) {
					SlcReqStr.append(i).append(MServer.intraDepItemDelimiter).append(sv.get(i))
							.append(MServer.interDepItemDelimiter);
				}
				String SlcReqString = SlcReqStr.substring(0, SlcReqStr.length() - 1);

				MServer.sendToPartition(MServer.dcn, p, SlcReqString);

			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// wait for ROTX to be completed
		synchronized (mes) {
			try {
				if (MServer.verbose)
					System.out.println("waiting for all values of the keys" + mes.keys);
				if (mes.values.size() < mes.keys.size())
					mes.wait();
				if (MServer.verbose)
					System.out.println("all values received");
				// dsv
				StringBuffer DSVStr = new StringBuffer("");
				for (int i = 0; i < MServer.DSV.size(); i++) {
					DSVStr.append(i).append(MServer.intraDepItemDelimiter).append(MServer.DSV.get(i))
							.append(MServer.interDepItemDelimiter);
				}
				String DSVString = DSVStr.substring(0, DSVStr.length() - 1);

				// ds
				String DSString = MServer.emptyElement;
				StringBuffer DSStr = new StringBuffer("");
				if (mes.ds.size() > 0) {
					for (Map.Entry<Integer, Long> dvEntry : mes.ds.entrySet()) {
						DSStr.append(dvEntry.getKey()).append(MServer.intraDepItemDelimiter).append(dvEntry.getValue())
								.append(MServer.interDepItemDelimiter);
					}
					DSString = DSStr.substring(0, DSStr.length() - 1);
				}

				// values
				String valuString = MServer.emptyElement;
				StringBuffer valueStr = new StringBuffer("");
				if (mes.values.size() > 0) {
					for (Map.Entry<String, String> valueEntry : mes.values.entrySet()) {
						valueStr.append(valueEntry.getKey()).append(MServer.intraDepItemDelimiter)
								.append(valueEntry.getValue()).append(MServer.interDepItemDelimiter);
					}
					valuString = valueStr.substring(0, valueStr.length() - 1);
				}

				StringBuffer responseForClient = new StringBuffer("");
				responseForClient.append(DSVString).append(MServer.mainDelimiter).append(DSString)
						.append(MServer.mainDelimiter).append(valuString);

				
				// simulating slowdown
				try {
					// only sending to dc=0 is affect, and only if the current dc is not dc0;
					Thread.sleep(MServer.networkDelay);
					// if (GServer.verbose)
					// System.out.println("Sleeping to simulate network delay");

				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				printWriter.println(responseForClient.toString());
				synchronized (MServer.rotxs) {
					MServer.rotxs.remove(rotxId);
				}

			} catch (InterruptedException e) {

			}
		}
	}

	private Message extractMessageString(String message) throws UnsupportedEncodingException {
		Message mes;
		String type = message.substring(0, message.indexOf(MServer.mainDelimiter));
		if (type != null && type.equals("GET")) {
			mes = new GetMessage(message);
		} else if (type != null && type.equals("PUT")) {
			mes = new PutMessage(message);
		} else {
			mes = new ROTXMessage(message);
		}
		return mes;
	}

	/*
	 * A client sends a GET request with an item key and its GSTc to the server that
	 * serves the partition containing the item.
	 * 
	 * The server - first updates its GST if it is smaller than the client’s. - The
	 * server then obtains the latest version in the version chain of the requested
	 * item, which is either created by clients attached to the local datacenter or
	 * has an update timestamp no greater than the partition’s GST. Hence, a client
	 * always reads local updates without any delay and replicated updates from
	 * other datacenters once they are globally stable.
	 * 
	 * 
	 * 
	 * The partition returns the item value, its update timestamp, and its GST back
	 * to the client.
	 */

	private void getMessageHandler(GetMessage mes, PrintWriter pw) throws IOException {
		// taking max of the DSV of the server and the DSV of the client.
		if (mes.dsv.size() > 0) {
			for (int i = 0; i < MServer.DSV.size(); i++) {
				if (MServer.DSV.get(i) < mes.dsv.get(i))
					MServer.DSV.set(i, mes.dsv.get(i));
			}
		}
		Record record = MServer.findTheNewestStableVersion(mes.key);
		if (record == null) {
			pw.println("~:~:~:Not found");
			// connection.close();
			// Test.....
			if (MServer.collect) {
				MServer.numberOfOperations++;
			}
			// .........
		} else {

			StringBuffer DSVStr = new StringBuffer("0").append(MServer.intraDepItemDelimiter)
					.append(MServer.DSV.get(0));
			for (int i = 1; i < MServer.DSV.size(); i++) {
				DSVStr.append(MServer.interDepItemDelimiter).append(i).append(MServer.intraDepItemDelimiter)
						.append(MServer.DSV.get(i));
			}

			// rotx
			if (record.dv.containsKey(record.sr))
				record.dv.put(record.sr, Math.max(record.ut, record.dv.get(record.sr)));
			else
				record.dv.put(record.sr, record.ut);

			// rotx
			StringBuffer DSStr = new StringBuffer("");
			for (Map.Entry<Byte, Long> dvEntry : record.dv.entrySet()) {
				DSStr.append(dvEntry.getKey()).append(MServer.intraDepItemDelimiter).append(dvEntry.getValue())
						.append(MServer.interDepItemDelimiter);
			}
			String DSString = (!DSStr.equals("")) ? DSStr.substring(0, DSStr.length() - 1) : MServer.emptyElement;

			try {
				/*
				 * responseForClient.append(ByteUtil.longToString(record.ut)).append(MServer.
				 * mainDelimiter)
				 * .append(ByteUtil.byteToString(record.sr)).append(MServer.mainDelimiter).
				 * append(DSVStr).append(MServer.mainDelimiter)
				 * .append(record.key).append(MServer.mainDelimiter).append(new
				 * String(record.value, "UTF-8"));
				 */

				// rotx
				StringBuffer responseForClient = new StringBuffer("");
				responseForClient.append(DSVStr).append(MServer.mainDelimiter).append(DSString)
						.append(MServer.mainDelimiter).append(record.key).append(MServer.mainDelimiter)
						.append(new String(record.value, "UTF-8"));
				String response = responseForClient.toString();
				
				// simulating slowdown
				try {
					// only sending to dc=0 is affect, and only if the current dc is not dc0;
					Thread.sleep(MServer.networkDelay);
					// if (GServer.verbose)
					// System.out.println("Sleeping to simulate network delay");

				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				pw.println(response);
				// Test.....
				if (MServer.collect) {
					MServer.numberOfOperations++;
				}
				// .........
				// connection.close();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/*
	 * A client sends a PUT request, h PUTREQ k,v,DTc i , which includes the item
	 * key, the update value, and the client’s dependency time, to the server that
	 * manages the item.
	 * 
	 * The server then checks that the client’s dependency time is smaller than its
	 * physical clock time. -If it, it waits until the condition becomes true.
	 * 
	 * The server then updates the local element of its version vector with its
	 * physical clock time.
	 * 
	 * It creates a new version of the item by assigning it a tuple that consists of
	 * the key, value, update time, and its replica id, and inserts the newly
	 * created item version in the version chain of the item.
	 * 
	 * The server sends a reply with the update time of the newly created item
	 * version to the client.
	 */
	private void putMessageHandler(PutMessage mes, PrintWriter pw) throws IOException {

		// We don't need to wait when we use HCL
		/*
		 * 
		 * long currentTime = MServer.getCurrentTime(); //waiting if (currentTime <
		 * mes.dt) { try { Thread.sleep(mes.dt - currentTime); } catch
		 * (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * } currentTime = MServer.getCurrentTime();
		 */

		// rotx, updating the DSV
		for (Map.Entry<Byte, Long> dvEntry : mes.dv.entrySet()) {
			MServer.DSV.set(dvEntry.getKey(), Math.max(MServer.DSV.get(dvEntry.getKey()), dvEntry.getValue()));
		}

		// creating the new version
		Record newVersion = new Record();
		newVersion.key = mes.key;
		newVersion.sr = MServer.dcn;
		// HLC
		// rotx
		newVersion.ut = MServer.creatNewTimestamp(Math.max(mes.dt, MServer.DSV.get(MServer.dcn)));
		newVersion.dv = mes.dv;

		try {
			newVersion.value = mes.value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Retrieving the previous key (the key with the highest timestamp) for
		// conflict detection:
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

		// writing the new version to the DB
		DatabaseEntry theData = new DatabaseEntry();
		MServer.rb.objectToEntry(newVersion, theData);

		// updating the vv
		// I think it is not necessary, as in the gst compuation I use the
		// actual clock not vv entry for this node.
		synchronized (MServer.vvLock) {
			if (MServer.vv.get(MServer.dcn) < MServer.hlc) {
				MServer.vv.set(MServer.dcn, MServer.hlc);
			}
		}
		
		
		// simulating slowdown
		try {
			// only sending to dc=0 is affect, and only if the current dc is not dc0;
			Thread.sleep(MServer.networkDelay);
			// if (GServer.verbose)
			// System.out.println("Sleeping to simulate network delay");

		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		synchronized (MServer.heartbeatTimer) {

			MServer.db.put(null, theKey, theData);
			// Sending reply to the client
			String response = ByteUtil.longToString(newVersion.ut) + MServer.mainDelimiter
					+ ByteUtil.byteToString(newVersion.sr) + MServer.mainDelimiter + newVersion.key;
			pw.println(response);

			// connection.close();
			// send replicate messages....
			// note that sending replicate message should be before put to
			// guarantee that the replicate message
			// arives sooner than other possilbe replicate.
			// We use heartbeadTimer lock to serialize all writes to the channel
			// We also shutdown the heartbeat and restart it to send after a
			// heatbeatInterval.
			// so we do this:
			// First we lock the hearbeatTime, so other threads cannot send
			// replicate (it serializes)
			// Then, we shutdown the timer.
			// Send the replicate.
			// One the reolicates are sent to the channel, we restart the time.
			// release the lock.

			// MServer.heartbeatTimer.shutdown();
			sendReplicates(newVersion, preVersion, mes.dv);
			// MServer.heartbeatTimer = Executors.newScheduledThreadPool(1);
			// MServer.heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(),
			// MServer.heartbeatInterval,
			// MServer.heartbeatInterval, TimeUnit.MILLISECONDS);
		}


	}

	private void sendReplicates(Record newVersion, Record preVersion, HashMap<Byte, Long> dv)
			throws UnsupportedEncodingException {
		if (MServer.numberOfDcs < 2)
			return; // there is no need to send replicate messages.
		MServer.lastReplicateTime = MServer.getCurrentTime();
		StringBuffer repMessage = new StringBuffer("REP" + MServer.mainDelimiter + ByteUtil.byteToString(newVersion.sr)
				+ MServer.mainDelimiter + ByteUtil.longToString(newVersion.ut) + MServer.mainDelimiter);
		// We don;t use LRT in the code. We control it with timer instead. So I
		// comment it here...
		/*
		 * synchronized (MServer.lastReplcateTimeLock) { MServer.lastReplicateTime =
		 * newVersion.ut; }
		 */

		try {
			if (preVersion != null) {
				repMessage.append(ByteUtil.byteToString(preVersion.sr)).append(MServer.mainDelimiter)
						.append(ByteUtil.longToString(preVersion.ut)).append(MServer.mainDelimiter);

			} else {
				repMessage.append(MServer.emptyElement).append(MServer.mainDelimiter).append(MServer.emptyElement)
						.append(MServer.mainDelimiter);
			}
			if (dv.size() > 0) {
				for (Map.Entry<Byte, Long> dvItem : dv.entrySet()) {
					repMessage.append(ByteUtil.byteToString(dvItem.getKey())).append(MServer.intraDepItemDelimiter)
							.append(ByteUtil.longToString(dvItem.getValue())).append(MServer.interDepItemDelimiter);
				}
			} else
				repMessage.append(MServer.emptyElement);
			repMessage.append(MServer.mainDelimiter).append(newVersion.key).append(MServer.mainDelimiter)
					.append(new String(newVersion.value, "UTF-8"));
			for (int i = 0; i < MServer.numberOfDcs; i++) {
				if (i == MServer.dcn)
					continue;
				if (MServer.verbose)
					System.out.println("Sending REP to server dcn= " + i + " pn= " + MServer.pn + ": " + repMessage);
				MServer.sendToPartition(i, MServer.pn, repMessage.toString());
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
