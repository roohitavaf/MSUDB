package edu.msu.cse.msudb;

import java.util.ArrayList;

public class DSVMessage  extends Message{
	public ArrayList<Long> dsv = new ArrayList<Long>();
	
	
	public DSVMessage(String mes) {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		String dsvS = reqParts[1];
		String[] dsvItems = dsvS.split(MServer.interDepItemDelimiter);
		for (int i=0; i < MServer.numberOfDcs; i++)
		{
			dsv.add(new Long(dsvItems[i]));
		}
	}
}
