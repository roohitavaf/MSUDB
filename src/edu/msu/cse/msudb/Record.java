package edu.msu.cse.msudb;

import java.util.ArrayList;
import java.util.HashMap;

public class Record {
	public byte sr; 
	public Long ut;
	public HashMap<Byte, Long> dv;
	public String key;
	public byte[] value;
}
