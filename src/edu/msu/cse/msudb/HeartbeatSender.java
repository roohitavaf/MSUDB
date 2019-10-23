package edu.msu.cse.msudb;

public class HeartbeatSender implements Runnable {

	public void run() {
		// System.out.println("Heart beats!");
		try {
			if (MServer.getCurrentTime() < MServer.lastReplicateTime + MServer.heartbeatInterval)
				return;
			// long time = MServer.lastReplicateTime;
			MServer.lastReplcateTimeLock = MServer.getCurrentTime();
			String heartbeatMessage = "HB" + MServer.mainDelimiter + ByteUtil.byteToString(MServer.dcn)
					+ MServer.mainDelimiter + ByteUtil.longToString(MServer.creatNewTimestamp());

			for (int i = 0; i < MServer.numberOfDcs; i++) {
				// if (i == MServer.dcn)
				// continue;
				MServer.sendToPartition(i, MServer.pn, heartbeatMessage);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
