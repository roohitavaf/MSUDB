package edu.msu.cse.msudb;

public class HeartbeatMessage extends Message {
	public int dcn;
	public long ct;
	
	
	public HeartbeatMessage(String mes) {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		this.dcn = new Integer(reqParts[1]);
		this.ct = new Long(reqParts[2]);
	}

}
