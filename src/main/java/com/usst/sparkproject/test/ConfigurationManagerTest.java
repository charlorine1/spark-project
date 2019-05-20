package com.usst.sparkproject.test;

import org.junit.Test;

import com.usst.sparkproject.conf.ConfigurationManager;

public class ConfigurationManagerTest {
	
	@Test
	public void test() {
		String value1 = ConfigurationManager.getProperty("key1");
		String value2 = ConfigurationManager.getProperty("key2");
		System.out.println("key1--- value="+value1);
		System.out.println("key2--- value="+value2);
	}

}
