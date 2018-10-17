package hadoop_hello;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Hello {
	
	
	public static void main(String[] args) throws IOException {
		/*
		//ʹ��URL���hadoop�ж�ȡ�ļ�
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());//�趨URL��ʹ��hdfsЭ�飬Ĭ��ΪhttpЭ��
		URL url =new URL("hdfs://192.168.56.150:9000/wc_input/file1.txt");
		InputStream in =url.openStream();
		IOUtils.copyBytes(in, System.out, 4096,true);
		*/
		
		
		Configuration conf =new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.56.150:9000");
		FileSystem filesystem= FileSystem.get(conf);
//		boolean success=filesystem.exists(new Path("/wc_input"));
//		System.out.println(success);
		
		//�������ļ�����hadoop��
//		FSDataOutputStream out=filesystem.create(new Path("/novel.txt"));
//		FileInputStream fis=new FileInputStream("D:/tensorflow/dataset/novel/novel.txt");
//		IOUtils.copyBytes(fis, out, 4096,true);
		
		
		/*
		FileStatus[] status=filesystem.listStatus(new Path("/"));
		
		for (FileStatus filestatus:status){
			System.out.println(filestatus.getPath());
			System.out.println(filestatus.getBlockSize());
		}
		*/
		filesystem.copyToLocalFile(new Path("/out/000000_0"), new Path("C:/temp"));
	}

}
