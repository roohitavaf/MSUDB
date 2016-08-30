package edu.msu.cse.msudb;

public class IdMessage extends Message{
	
	public String id;
	
	public IdMessage(String mes) {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		this.id = reqParts[1];
	}

}
