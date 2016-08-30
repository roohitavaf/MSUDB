package edu.msu.cse.msudb;

import java.util.HashMap;

public class PutMessage extends Message {
	public HashMap<Integer, Long> dv = new HashMap<Integer, Long>();
	public long dt = 0;
	public String key;
	public String value;

	public PutMessage(String mes) {

		int firstColon = mes.indexOf(MServer.mainDelimiter);
		int secondColon = mes.indexOf(MServer.mainDelimiter, firstColon + 1);
		int thirdColon = mes.indexOf(MServer.mainDelimiter, secondColon + 1);

		this.type = mes.substring(0, firstColon);
		String deps = mes.substring(firstColon + 1, secondColon);
		if (!deps.equals(MServer.emptyElement)) {
			String[] depsItem = deps.split(MServer.interDepItemDelimiter);
			for (String depItem : depsItem) {
				long depValue = new Long(depItem.substring(depItem.indexOf(MServer.intraDepItemDelimiter) + 1));
				this.dv.put(new Integer(depItem.substring(0, depItem.indexOf(MServer.intraDepItemDelimiter))),
						depValue);
				if (depValue > dt)
					dt = depValue;
			}
		}
		this.key = mes.substring(secondColon + 1, thirdColon);
		this.value = mes.substring(thirdColon + 1);
	}
}