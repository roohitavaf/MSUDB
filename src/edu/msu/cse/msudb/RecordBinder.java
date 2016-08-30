package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.swing.plaf.ToolBarUI;
import javax.swing.tree.ExpandVetoException;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.*;

public class RecordBinder extends TupleBinding {

	@Override
	public Object entryToObject(TupleInput ti) {
		Record r = new Record();
		r.sr = ti.readInt();
		r.ut = ti.readLong();
		/*
		 * Test commented for test purposes int sizeOfDep = ti.readInt(); r.dv =
		 * new HashMap<Integer, Long>(); for (int i=0; i < sizeOfDep ; i++) {
		 * int dc= ti.readInt(); long v = ti.readLong(); r.dv.put(dc, v); }
		 */
		
		String deps = ti.readString();
		r.dv = new HashMap<Integer, Long>();
		if (!deps.equals(MServer.emptyElement)) {
			try {
				String[] depsPart = deps.split(MServer.interDepItemDelimiter);
				for (String depItem : depsPart) {
					r.dv.put(new Integer(depItem.substring(0, depItem.indexOf(MServer.intraDepItemDelimiter))),
							new Long((depItem.substring(depItem.indexOf(MServer.intraDepItemDelimiter + 1)))));
				}
			} catch (Exception e) {
				System.out.println(deps);
			}
		}
		r.key = ti.readString();
		//value....
		String value = ti.readString();
		try {
			r.value = value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}

	@Override
	public void objectToEntry(Object o, TupleOutput to) {
		// TODO Auto-generated method stub
		Record r = (Record) o;
		to.writeInt(r.sr);
		to.writeLong(r.ut);
		/*
		 * Test comment for test. to.writeInt(r.dv.size()); for
		 * (Map.Entry<Integer, Long> dvItem : r.dv.entrySet()) { int dc =
		 * dvItem.getKey(); long v = dvItem.getValue(); to.writeInt(dc);
		 * to.writeLong(v); }
		 */
		
		if (r.dv.size() > 0) {
			StringBuffer sb = new StringBuffer();
			for (Map.Entry<Integer, Long> dvItem : r.dv.entrySet()) {
				int dc = dvItem.getKey();
				long v = dvItem.getValue();
				sb.append(dc + MServer.intraDepItemDelimiter + v + MServer.interDepItemDelimiter);
			}
			to.writeString(sb.toString());
		} else
			to.writeString(MServer.emptyElement);
		to.writeString(r.key);
		try {
			to.writeString((new String(r.value, "UTF-8")));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
