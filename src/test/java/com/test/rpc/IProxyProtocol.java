package com.test.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;


public interface IProxyProtocol extends VersionedProtocol{
	long versionID=0001;
	String dosomething(int num,String str);

}
