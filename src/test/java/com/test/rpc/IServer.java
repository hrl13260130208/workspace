package com.test.rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.ipc.RPC.Server;





public class IServer implements IProxyProtocol {
	public static final int PORT=8888;
	
	public static final String ADDRESS="127.0.0.1";
	
	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		System.out.println("server version:"+IProxyProtocol.versionID);
		return IProxyProtocol.versionID;
	}


	@Override
	public String dosomething(int num, String str) {
		System.out.println("server : num="+num+" String="+str);
		return str+" server";
	}


	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		IProxy proxy=new IProxy();
		Server server=new RPC.Builder(new Configuration())
				.setProtocol(IProxyProtocol.class)
				.setInstance(new IServer())
				.setBindAddress(ADDRESS)
				.setPort(PORT)
				.build();
		server.start();
		
	}
	

}
