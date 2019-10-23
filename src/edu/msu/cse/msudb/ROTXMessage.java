package edu.msu.cse.msudb;

import java.util.ArrayList;
import java.util.HashMap;

public class ROTXMessage extends Message
{
	public HashMap<Integer, Long>  dsv = new HashMap<Integer, Long>();
	public HashMap<Integer, Long>  ds = new HashMap<Integer, Long>();
	public ArrayList<String> keys =  new ArrayList<>();
	public HashMap<String, String> values = new HashMap<>();
	//public String key;
	
	public ROTXMessage(String mes) {		
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		this.dsv = readDSV(reqParts[1]);
		this.ds = readDSV(reqParts[2]); 
		this.keys = readKeys(reqParts[3]);
	}
	
	public ArrayList<String> readKeys (String keysStr){
		ArrayList<String> result = new ArrayList<>();
		if (!keysStr.equals(MServer.emptyElement)) {
			String[] keyItems = keysStr.split(MServer.interDepItemDelimiter);
			for (String keyItem : keyItems) {
				result.add(keyItem);
			}
		}
		return result; 
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