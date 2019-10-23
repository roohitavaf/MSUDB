package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class SliceReqMessage extends Message {
	public String key; 
	public ArrayList<Long> sv; 
	public int requestingPartition; 
	public int rotxId;
	
	
	public SliceReqMessage(String mes) throws UnsupportedEncodingException {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		
		//debug 
				System.out.println("SliceReq: " + mes);
				
		this.type = reqParts[0];
		this.requestingPartition = new Integer(reqParts[1]);
		this.rotxId = new Integer (reqParts[2]);
		this.key = reqParts[3]; 
		this.sv = readSv(reqParts[4]);
	}

	public ArrayList<Long> readSv (String dsvStr){
		ArrayList<Long>  d = new ArrayList<>();
		if (!dsvStr.equals(MServer.emptyElement)) {
			String[] depsItem = dsvStr.split(MServer.interDepItemDelimiter);
			for (String depItem : depsItem) {
				long depValue = ByteUtil.stringToLong(depItem.substring(depItem.indexOf(MServer.intraDepItemDelimiter) + 1));
				//int dcId = new Integer (depItem.substring(0, depItem.indexOf(MServer.intraDepItemDelimiter))); 
				d.add(depValue);
			}
		}
		return d;
	}
	
}
