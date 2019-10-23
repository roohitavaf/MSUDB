package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class DSVMessage  extends Message{
	public ArrayList<Long> dsv = new ArrayList<Long>();
	
	
	public DSVMessage(String mes) throws UnsupportedEncodingException {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		String dsvS = reqParts[1];
		String[] dsvItems = dsvS.split(MServer.interDepItemDelimiter);
		for (int i=0; i < MServer.numberOfDcs; i++)
		{
			dsv.add(ByteUtil.stringToLong(dsvItems[i]));
		}
	}
}
