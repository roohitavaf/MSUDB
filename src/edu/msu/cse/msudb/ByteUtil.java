package edu.msu.cse.msudb;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class ByteUtil {
    private static ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
    private static ByteBuffer shortBuffer = ByteBuffer.allocate(Short.BYTES);  
    private static ByteBuffer byteBuffer = ByteBuffer.allocate(Byte.BYTES);  
    
    public static byte[] longToBytes(long x) {
        longBuffer.putLong(0, x);
        return longBuffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }
    
    public static byte[] shortToBytes(short x) {
        shortBuffer.putShort(0, x);
        return shortBuffer.array();
    }

    public static short bytesToShort(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }
    
    public static byte[] byteToBytes(Byte x) {
    	byteBuffer.put(0, x);
        return byteBuffer.array();
    }

    public static Byte bytesToByte(byte[] bytes) {
        return bytes[0];
    }
    
    
    public static String longToString (long x) throws UnsupportedEncodingException{
    	//return new String (longToBytes(x), "ISO-8859-1");
    	return x + "";
    }
    
    public static long stringToLong (String s){
    	//return bytesToLong(s.getBytes("ISO-8859-1"));
    	return new Long (s);
    }
    
    public static String byteToString (byte x) {
    	//return new String (byteToBytes(x), "ISO-8859-1");
    	return x + "";
    }
    
    public static byte stringToByte (String s) {
    	// return bytesToByte(s.getBytes("ISO-8859-1"));
    	return new Byte (s);
    	
    }
    
}