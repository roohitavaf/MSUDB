package edu.msu.cse.msudb;

public class HeartbeatSender implements Runnable {

	@Override
	public void run() {
		//System.out.println("Heart beats!");
		if (MServer.getCurrentTime() < MServer.lastReplicateTime + MServer.heartbeatInterval) return;
		//long time = MServer.lastReplicateTime;
		MServer.lastReplcateTimeLock = MServer.getCurrentTime();
		String heartbeatMessage = "HB" + MServer.mainDelimiter + MServer.dcn + MServer.mainDelimiter
				+ MServer.creatNewTimestamp();

		for (int i = 0; i < MServer.numberOfDcs; i++) {
			//if (i == MServer.dcn)
			//	continue;
			MServer.sendToPartition(i, MServer.pn, heartbeatMessage);
		}
		
	}
}
