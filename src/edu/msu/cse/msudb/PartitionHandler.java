package edu.msu.cse.msudb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CollectionCertStoreParameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Currency;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class PartitionHandler extends ChannelInboundHandlerAdapter {

	private RecordBinder rb;

	public PartitionHandler() {
		this.rb = new RecordBinder();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	public void channelActive(ChannelHandlerContext ctx) {
		System.out.println("New coordination channel");
	}

	public void channelRead(ChannelHandlerContext ctx, Object msg) {

		Message mes = extractMessageString((String) msg);
		if (mes.type.equals("ID")) {
			System.out.println("New Coordination message: " + (String) msg);
			idMessageHandler((IdMessage) mes, ctx);
		} else if (mes.type.equals("REP")) {
			System.out.println("New Coordination message: " + (String) msg);
			repMessageHandler((RepMessage) mes, ctx);

		} else if (mes.type.equals("HB")) {
			//System.out.println("New Coordination message: " + (String) msg);
			heartbeatMessageHandler((HeartbeatMessage) mes, ctx);
		} else if (mes.type.equals("VV")) {
			//System.out.println("New Coordination message: " + (String) msg);
			vvMessageHandler((VVMessage) mes, ctx);
		} else if (mes.type.equals("DSV")) {
			//System.out.println("New Coordination message: " + (String) msg);
			dsvMessageHandler((DSVMessage) mes, ctx);
		}
	}

	private Message extractMessageString(String str) {

		Message mes = null;
		String type = str.substring(0, str.indexOf(MServer.mainDelimiter));
		if (type.equals("ID")) {
			mes = new IdMessage(str);
		} else if (type.equals("REP")) {
			mes = new RepMessage(str);
		} else if (type.equals("HB")) {
			mes = new HeartbeatMessage(str);
		} else if (type.equals("VV")) {
			mes = new VVMessage(str);
		} else {
			mes = new DSVMessage(str);
		}
		return mes;
	}

	//It is used when two partitions start communication for the first time. 
	private void idMessageHandler(IdMessage mes, ChannelHandlerContext ctx) {
		System.out.println("New ID message: " + mes.id);
		MServer.partitionChannels.put(mes.id, ctx.channel()); //id = "dcn,pn"

	}
	/*
	 * When a server receives such an update replication message, it inserts the
	 * received item version in the corresponding version chain. However, this
	 * update is not visible to local clients until the partition’s GST becomes
	 * larger than its update timestamp.
	 */

	private void repMessageHandler(RepMessage mes, ChannelHandlerContext ctx) {

		//Updating HLC
		MServer.creatNewTimestamp(mes.ut);

		//first we update vv using the ut
		synchronized (MServer.vvLock) {
			if (MServer.vv.get(mes.dcn) < mes.ut) {
				MServer.vv.set(mes.dcn, mes.ut);
				//System.out.println("Inside paritionHandler, updating vv for " + mes.dcn + " to " + mes.ut);
			}
		}
		Record newVersion = new Record();
		newVersion.key = mes.key;
		newVersion.sr = mes.dcn;
		newVersion.ut = mes.ut;
		newVersion.dv = mes.dv;

		//System.out.println("Inside PartitionHandler, replicated message key " + newVersion.key);

		try {
			//Test section..............

			if (MServer.firstReplicate) {
				MServer.firstReplicate = false;
			} else {
				MServer.updateTimeOfCreation = new Long(mes.value);
				MServer.numberOfReplicatedMessage++;

				long currenTime = MServer.getCurrentTime();
				long updateVisiblityLatency = currenTime - MServer.updateTimeOfCreation;
				System.out.println("NumerOfReplicatedMessages= " + MServer.numberOfReplicatedMessage + " latency = "
						+ updateVisiblityLatency);
				MServer.avargeUVL = (new Float(MServer.avargeUVL * (MServer.numberOfReplicatedMessage - 1))
						+ updateVisiblityLatency) / MServer.numberOfReplicatedMessage;
				if (MServer.numberOfReplicatedMessage == MServer.maxNumberOfRepliateMessages) {
					System.out.println("Average update visibity latency= " + MServer.avargeUVL);
					MServer.avargeUVL = 0;
					MServer.numberOfReplicatedMessage = 0;
				}
			}

			//..........................

			newVersion.value = mes.value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Conflicti detection: 
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
		Record newestVersionOfThisNode = null;
		if (MServer.db.get(null, theKey, preData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			newestVersionOfThisNode = (Record) MServer.rb.entryToObject(preData);
		}

		if ((newestVersionOfThisNode == null && mes.preDc != -1) || (newestVersionOfThisNode != null
				&& (newestVersionOfThisNode.sr != mes.preDc || newestVersionOfThisNode.ut != mes.preUt))) {
			//handle conflict ....
			//It should be implement for desired behavior. 
			//However, if we do nothing still we will have convergence, BECAUSE OF THE COMPARATOR, as this node will consider the version with 
			//higher timestamp as winner. If timestamp are equal the version with higher dc number is winner. 
		}
		//writing the new version to the DB
		DatabaseEntry theData = new DatabaseEntry();
		MServer.rb.objectToEntry(newVersion, theData);
		//System.out.println("Inside PartitionHandler, replicated message key " + newVersion.key);
		//System.out.println("Inside repHandler: I am going to put the replicated message");
		MServer.db.put(null, theKey, theData);
	}

	//we update vv with the hearbeat message for the sender of the heartbeat message
	private void heartbeatMessageHandler(HeartbeatMessage mes, ChannelHandlerContext ctx) {
		synchronized (MServer.vvLock) {
			if (MServer.vv.get(mes.dcn) < mes.ct) {
				MServer.vv.set(mes.dcn, mes.ct);
			}
		}
	}

	//only parent nodes in the tree receive this message. 
	//upon receiving this message, it updates childrenLsts
	private void vvMessageHandler(VVMessage mes, ChannelHandlerContext ctx) {
		MServer.childrenVVs.put(mes.pn, mes.vv);
	}

	private void dsvMessageHandler(DSVMessage mes, ChannelHandlerContext ctx) {
		MServer.DSV = mes.dsv;
	}
}
