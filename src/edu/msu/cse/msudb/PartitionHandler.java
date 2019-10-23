package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class PartitionHandler extends ChannelInboundHandlerAdapter {

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	public void channelActive(ChannelHandlerContext ctx) {
		if (MServer.verbose)
			System.out.println("New coordination channel");
	}

	public void channelRead(ChannelHandlerContext ctx, Object msg) {

		try {
			Message mes = extractMessageString((String) msg);
			if (mes != null) {
				if (mes.type.equals("ID")) {
					if (MServer.verbose)
						System.out.println("New Coordination message: " + (String) msg);
					idMessageHandler((IdMessage) mes, ctx);
				} else if (mes.type.equals("REP")) {
					if (MServer.verbose)
						System.out.println("New Coordination message: " + (String) msg);
					repMessageHandler((RepMessage) mes, ctx);

				} else if (mes.type.equals("HB")) {
					// System.out.println("New Coordination message: " + (String) msg);
					heartbeatMessageHandler((HeartbeatMessage) mes, ctx);
				} else if (mes.type.equals("VV")) {
					// System.out.println("New Coordination message: " + (String) msg);
					vvMessageHandler((VVMessage) mes, ctx);
				} else if (mes.type.equals("DSV")) {
					// System.out.println("New Coordination message: " + (String) msg);
					dsvMessageHandler((DSVMessage) mes, ctx);
				} else if (mes.type.equals("SREQ")) {
					sreqMessageHandler((SliceReqMessage) mes, ctx);
				} else if (mes.type.equals("SREP")) {
					srepMessageHandler((SliceRepMessage) mes, ctx);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void srepMessageHandler(SliceRepMessage mes, ChannelHandlerContext ctx) {
		// debug
		if (MServer.verbose)
			System.out.println("received value for key " + mes.key);
		synchronized (MServer.rotxs) {
			// debug
			if (MServer.verbose)
				System.out.println("got lock for rotxs");
			ROTXMessage rotx = MServer.rotxs.get(mes.rotxId);

			synchronized (rotx) {
				rotx.values.put(mes.key, mes.value);
				if (rotx.values.size() == rotx.keys.size()) {
					rotx.notifyAll();
					if (MServer.verbose)
						System.out.println("mes notified");
				}
			}
		}
	}

	private void sreqMessageHandler(SliceReqMessage mes, ChannelHandlerContext ctx) {
		// find the version
		Record record = MServer.findTheStableVersionForSv(mes.key, mes.sv);

		// send back to the server
		StringBuffer SlcRepStr = new StringBuffer("");
		try {
			String value = "Not found";
			if (record != null)
				value = new String(record.value, "UTF-8");
			SlcRepStr.append("SREP").append(MServer.mainDelimiter).append(mes.rotxId).append(MServer.mainDelimiter)
					.append(mes.key).append(MServer.mainDelimiter).append(value).append(MServer.mainDelimiter);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String dsString = MServer.emptyElement;
		if (record != null) {
			if (record.dv.size() > 0) {
				StringBuffer dsStr = new StringBuffer("");
				for (Map.Entry<Byte, Long> dvElement : record.dv.entrySet()) {
					dsStr.append(dvElement.getKey()).append(MServer.intraDepItemDelimiter).append(dvElement.getValue())
							.append(MServer.interDepItemDelimiter);
				}
				dsString = dsStr.substring(0, dsStr.length() - 1);
			}
		}

		SlcRepStr.append(dsString);
		MServer.sendToPartition(MServer.dcn, mes.requestingPartition, SlcRepStr.toString());
	}

	private Message extractMessageString(String str) throws UnsupportedEncodingException {

		Message mes = null;
		if (str != null && str.indexOf(MServer.mainDelimiter) != -1) {
			String type = str.substring(0, str.indexOf(MServer.mainDelimiter));
			if (type.equals("ID")) {
				mes = new IdMessage(str);
			} else if (type.equals("REP")) {
				mes = new RepMessage(str);
			} else if (type.equals("HB")) {
				mes = new HeartbeatMessage(str);
			} else if (type.equals("VV")) {
				mes = new VVMessage(str);
			} else if (type.equals("DSV")) {
				mes = new DSVMessage(str);
			} else if (type.equals("SREQ")) {
				mes = new SliceReqMessage(str);
			} else if (type.equals("SREP")) {
				mes = new SliceRepMessage(str);
			}
		}
		return mes;
	}

	// It is used when two partitions start communication for the first time.
	private void idMessageHandler(IdMessage mes, ChannelHandlerContext ctx) {
		if (MServer.verbose)
			System.out.println("New ID message: " + mes.id);
		MServer.partitionChannels.put(mes.id, ctx.channel()); // id = "dcn,pn"

	}
	/*
	 * When a server receives such an update replication message, it inserts the
	 * received item version in the corresponding version chain. However, this
	 * update is not visible to local clients until the partition’s GST becomes
	 * larger than its update timestamp.
	 */

	private void repMessageHandler(RepMessage mes, ChannelHandlerContext ctx) {

		System.out.println("Received replicate message for " + mes.key );
		
		// Updating HLC
		MServer.creatNewTimestamp(mes.ut);

		// first we update vv using the ut
		synchronized (MServer.vvLock) {
			if (MServer.vv.get(mes.dcn) < mes.ut) {
				MServer.vv.set(mes.dcn, mes.ut);
				// System.out.println("Inside paritionHandler, updating vv for " + mes.dcn + "
				// to " + mes.ut);
			}
		}
		Record newVersion = new Record();
		newVersion.key = mes.key;
		newVersion.sr = mes.dcn;
		newVersion.ut = mes.ut;
		newVersion.dv = mes.dv;

		// System.out.println("Inside PartitionHandler, replicated message key " +
		// newVersion.key);

		try {
			/*
			 * //Test section..............
			 * 
			 * if (MServer.firstReplicate) { MServer.firstReplicate = false; } else {
			 * MServer.updateTimeOfCreation = new Long(mes.value);
			 * MServer.numberOfReplicatedMessage++;
			 * 
			 * long currenTime = MServer.getCurrentTime(); long updateVisiblityLatency =
			 * currenTime - MServer.updateTimeOfCreation;
			 * System.out.println("NumerOfReplicatedMessages= " +
			 * MServer.numberOfReplicatedMessage + " latency = " + updateVisiblityLatency);
			 * MServer.avargeUVL = (new Float(MServer.avargeUVL *
			 * (MServer.numberOfReplicatedMessage - 1)) + updateVisiblityLatency) /
			 * MServer.numberOfReplicatedMessage; if (MServer.numberOfReplicatedMessage ==
			 * MServer.maxNumberOfRepliateMessages) {
			 * System.out.println("Average update visibity latency= " + MServer.avargeUVL);
			 * MServer.avargeUVL = 0; MServer.numberOfReplicatedMessage = 0; } }
			 * 
			 * //..........................
			 */

			newVersion.value = mes.value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Conflicti detection:
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
			// handle conflict ....
			// It should be implement for desired behavior.
			// However, if we do nothing still we will have convergence, BECAUSE OF THE
			// COMPARATOR, as this node will consider the version with
			// higher timestamp as winner. If timestamp are equal the version with higher dc
			// number is winner.
		}
		// writing the new version to the DB
		DatabaseEntry theData = new DatabaseEntry();
		MServer.rb.objectToEntry(newVersion, theData);
		// System.out.println("Inside PartitionHandler, replicated message key " +
		// newVersion.key);
		// System.out.println("Inside repHandler: I am going to put the replicated
		// message");
		MServer.db.put(null, theKey, theData);
	}

	// we update vv with the hearbeat message for the sender of the heartbeat
	// message
	private void heartbeatMessageHandler(HeartbeatMessage mes, ChannelHandlerContext ctx) {
		synchronized (MServer.vvLock) {
			if (MServer.vv.get(mes.dcn) < mes.ct) {
				MServer.vv.set(mes.dcn, mes.ct);
			}
		}
	}

	// only parent nodes in the tree receive this message.
	// upon receiving this message, it updates childrenLsts
	private void vvMessageHandler(VVMessage mes, ChannelHandlerContext ctx) {
		MServer.childrenVVs.put(mes.pn, mes.vv);
	}

	private void dsvMessageHandler(DSVMessage mes, ChannelHandlerContext ctx) {
		MServer.DSV = mes.dsv;
	}
}
