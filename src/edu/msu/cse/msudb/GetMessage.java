package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class GetMessage extends Message
{
	public HashMap<Integer, Long>  dsv = new HashMap<Integer, Long>();
	public String key;
	
	public GetMessage(String mes) {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		this.dsv = readDSV(reqParts[1]);
		this.key = reqParts[2];
	}
	
	public HashMap<Integer, Long> readDSV (String dsvStr){
		HashMap<Integer, Long>  d = new HashMap<Integer, Long>();
		if (!dsvStr.equals(MServer.emptyElement)) {
			String[] depsItem = dsvStr.split(MServer.interDepItemDelimiter);
			for (String depItem : depsItem) {
				long depValue = ByteUtil.stringToLong(depItem.substring(depItem.indexOf(MServer.intraDepItemDelimiter) + 1));
				d.put(new Integer(depItem.substring(0, depItem.indexOf(MServer.intraDepItemDelimiter))),
						depValue);
			}
		}
		return d;
	}
}