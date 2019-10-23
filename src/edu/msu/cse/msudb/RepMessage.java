package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;

public class RepMessage extends Message {
	public byte dcn;
	public long ut;
	public byte preDc;
	public long preUt;
	public HashMap<Byte, Long> dv = new HashMap<Byte, Long>();
	public String key;
	public String value;
	
	

	public RepMessage(String mes) throws UnsupportedEncodingException {

		int firstColon = mes.indexOf(MServer.mainDelimiter);
		int secondColon = mes.indexOf(MServer.mainDelimiter, firstColon + 1);
		int thirdColon = mes.indexOf(MServer.mainDelimiter, secondColon + 1);
		int forthColon = mes.indexOf(MServer.mainDelimiter, thirdColon + 1);
		int fifthColon = mes.indexOf(MServer.mainDelimiter, forthColon + 1);
		int sixthColon = mes.indexOf(MServer.mainDelimiter, fifthColon + 1);
		int seventhColon = mes.indexOf(MServer.mainDelimiter, sixthColon + 1);

		this.type = mes.substring(0, firstColon);
		this.dcn = ByteUtil.stringToByte(mes.substring(firstColon + 1, secondColon));
		this.ut =  ByteUtil.stringToLong(mes.substring(secondColon + 1, thirdColon));
		String pre = mes.substring(thirdColon + 1, forthColon);
		if (!pre.equals(MServer.emptyElement))
		{
			this.preDc = ByteUtil.stringToByte(pre);
			this.preUt = ByteUtil.stringToLong(mes.substring(forthColon + 1, fifthColon));
		}
		else 
		{
			this.preDc = -1;
			this.preUt = new Long(-1);
			
		}

		String deps = mes.substring(fifthColon + 1, sixthColon);
		if (!deps.equals(MServer.emptyElement)) {
			String[] depsItem = deps.split(MServer.interDepItemDelimiter);
			for (String depItem : depsItem) {
				this.dv.put(ByteUtil.stringToByte(depItem.substring(0, depItem.indexOf(MServer.intraDepItemDelimiter))),
						ByteUtil.stringToLong(depItem.substring(depItem.indexOf(MServer.intraDepItemDelimiter) + 1)));
			}
		}

		this.key = mes.substring(sixthColon + 1, seventhColon);
		this.value = mes.substring(seventhColon + 1);
	}
}
