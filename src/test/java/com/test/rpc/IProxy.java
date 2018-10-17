package com.test.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class IProxy implements IProxyProtocol {

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		System.out.println("server version:"+IProxyProtocol.versionID);
		return IProxyProtocol.versionID;
	}


	@Override
	public String dosomething(int num, String str) {
		int a=num++;
		System.out.println("server : num="+num+" String="+str);
		return str;
	}


	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
