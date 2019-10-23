package edu.msu.cse.msudb;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {

	public static long getMd5HashLong (String strToHash) throws NoSuchAlgorithmException{
		long h = 1125899906842597L; // prime
		int len = strToHash.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + strToHash.charAt(i);
		}
		return h;
		/*
		byte[] bytesOfMessage = strToHash.getBytes();
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] md5 = md.digest(bytesOfMessage);
		//we only use 8 bytes of the generated md5. 
		long l = ((md5[0] & 0xFFL) << 56) |
		         ((md5[1] & 0xFFL) << 48) |
		         ((md5[2] & 0xFFL) << 40) |
		         ((md5[3] & 0xFFL) << 32) |
		         ((md5[4] & 0xFFL) << 24) |
		         ((md5[5] & 0xFFL) << 16) |
		         ((md5[6] & 0xFFL) <<  8) |
		         ((md5[7] & 0xFFL) <<  0) ;
		
		return  Math.abs(l);
		*/
	}
}
