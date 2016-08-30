package edu.msu.cse.msudb;

public class GetMessage extends Message
{
	//public Long gst;
	public String key;
	
	public GetMessage(String mes) {
		String[] reqParts = mes.split(MServer.mainDelimiter);
		this.type = reqParts[0];
		//this.gst = new Long(reqParts[1]);
		this.key = reqParts[1];
	}
}