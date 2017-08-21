package com.bohaisoft.dal.client.support.utils;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class AppUtils {

	/**
	 * 获取应用名称
	 * @return
	 */
	public static String getAppName() {
		String appName = "";
		String codeLocation = AppUtils.class.getProtectionDomain().getCodeSource().getLocation().getFile();
		File codeDir = new File(codeLocation);
		if(codeDir.isFile()) {
			codeDir = codeDir.getParentFile();
		}
		if(codeLocation.indexOf("WEB-INF") != -1) {
			File parent = codeDir.getParentFile();
			if(parent.exists()) {
				File appDir = parent.getParentFile();
				if(appDir.exists()) {
					appName = appDir.getName();
				}
			}
		} else {
			File appDir = codeDir.getParentFile();
			if(appDir.exists()) {
				appName = appDir.getName();
			}
		}
		
		return appName;
	}
	
	/**
	 * 获得本机IP
	 * @return
	 */
	public static String getHostAddress() {
		String address = "127.0.0.1";
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while (en.hasMoreElements()) {
				NetworkInterface ni = en.nextElement();
				Enumeration<InetAddress> ads = ni.getInetAddresses();
				while (ads.hasMoreElements()) {
					InetAddress ip = ads.nextElement();
					if (!ip.isLoopbackAddress() && ip.isSiteLocalAddress()) {
						return ip.getHostAddress();
					}
				}
			}
		} catch (Exception e) {
			// logger.info("getHostIp error");
		}
		
		return address;
	}
	
}  
	

