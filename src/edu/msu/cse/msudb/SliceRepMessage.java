package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class SliceRepMessage extends Message {
	public String value;
	public String key;
	public HashMap<Byte, Long> ds;
	public int rotxId; 
	
	
	public SliceRepMessage(String mes) throws UnsupportedEncodingException {
		String[] repParts = mes.split(MServer.mainDelimiter);
		//debug 
		System.out.println("SliceRep: " + mes);
		
		this.type = repParts[0];
		this.rotxId = new Integer(repParts[1]);
		this.key = repParts[2];
		this.value = repParts[3]; 
		this.ds = readDSV(repParts[4]);
	}

	public HashMap<Byte, Long> readDSV (String dsvStr){
		HashMap<Byte, Long>  d = new HashMap<Byte, Long>();
		if (!dsvStr.equals(MServer.emptyElement)) {
			String[] depsItem = dsvStr.split(MServer.interDepItemDelimiter);
			for (String depItem : depsItem) {
				long depValue = ByteUtil.stringToLong(depItem.substring(depItem.indexOf(MServer.intraDepItemDelimiter) + 1));
				d.put(ByteUtil.stringToByte(depItem.substring(0, depItem.indexOf(MServer.intraDepItemDelimiter))),
						depValue);
			}
		}
		return d;
	}
	
}
