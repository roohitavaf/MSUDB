package edu.msu.cse.msudb;

import java.util.ArrayList;

public class VVMessage extends Message{
	public int pn;
	public ArrayList<Long> vv = new ArrayList<Long>();
	
	
	public VVMessage(String mes) {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		this.pn = new Integer(reqParts[1]);
		String vvs = reqParts[2];
		String[] vvsItem = vvs.split(MServer.interDepItemDelimiter);
		for (String vvItem : vvsItem)
		{
			vv.add(new Long(vvItem));
		}
	}
}
