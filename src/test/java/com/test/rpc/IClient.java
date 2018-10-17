package com.test.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class IClient {
	public static void main(String[] args) throws IOException {
		IProxyProtocol proxy=RPC.getProxy(IProxyProtocol.class, 
				IProxyProtocol.versionID,
				new InetSocketAddress("127.0.0.1", 8888) , 
				new Configuration());
		String result=proxy.dosomething(1, "test");
		System.out.println(result);
		RPC.stopProxy(proxy);
	}

}
