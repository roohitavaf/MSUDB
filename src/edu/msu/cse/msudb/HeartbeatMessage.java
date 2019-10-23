package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;

public class HeartbeatMessage extends Message {
	public byte dcn;
	public long ct;
	
	
	public HeartbeatMessage(String mes) throws UnsupportedEncodingException {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		this.dcn = ByteUtil.stringToByte(reqParts[1]);
		this.ct = ByteUtil.stringToLong(reqParts[2]);
	}

}
