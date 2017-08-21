package com.bohaisoft.dal.client.support.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtils {

	/*
	 * 返回16位的散列
	 */
	public static String toMD5(String str){  
	    try {  
	        MessageDigest md = MessageDigest.getInstance("MD5");  
	        md.update(str.getBytes());  
	        byte[]byteDigest = md.digest();  
	        int i;  
	        StringBuffer buf = new StringBuffer("");  
	        for (int offset = 0; offset < byteDigest.length; offset++) {  
	            i = byteDigest[offset];  
	            if (i < 0)  
	                i += 256;  
	            if (i < 16)  
	                buf.append("0");  
	            buf.append(Integer.toHexString(i));  
	        }  
	        return buf.toString().substring(8, 24);   
	    } catch (NoSuchAlgorithmException e) {  
	        e.printStackTrace();  
	        return null;  
	    }  
	      
	}  
}
