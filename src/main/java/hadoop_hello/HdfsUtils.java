package hadoop_hello;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsUtils {
	private String URLDefault="hdfs://192.168.56.150:9000";
	private FileSystem fileSystem;
	public HdfsUtils(String url) {
		HdfsUtils(url);
	}
	public HdfsUtils() {
		HdfsUtils(URLDefault);
	}
	
	private void HdfsUtils(String url) {
		Configuration conf =new Configuration();
		conf.set("fs.defaultFS", url);
		try {
			fileSystem= FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public List list(String fileName){
		 List lists=new ArrayList<>();
		try {
			FileStatus[] status=fileSystem.listStatus(new Path(fileName));
			for (FileStatus filestatus:status){
				String str=filestatus.getPath().toString();
				lists.add(str.substring(str.lastIndexOf(fileName)+fileName.length()));
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		return lists;
		
	}
	
	public void text(String fileName){
		FSDataInputStream fsInput=null;
		try {
			fsInput=fileSystem.open(new Path(fileName));
			IOUtils.copyBytes(fsInput, System.out,2046,true);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				fsInput.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void put(String fsPath,String localPath){
		try {
			fileSystem.copyFromLocalFile(new Path(localPath), new Path(fsPath));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean delete(String fileName){
		boolean isSuccess=false;
		try {
			isSuccess=fileSystem.delete(new Path(fileName), true);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccess;
		
	}
	
	public boolean mkdir(String fileName){
		boolean isSuccess=false;
		try {
			isSuccess=fileSystem.mkdirs(new Path(fileName));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccess;
	}
	
	public static void main(String[] args) {
		HdfsUtils hu=new HdfsUtils();
		/*
		//list方法测试
		String path="/";
		List list=hu.list(path);
		for(int i=0;i<list.size();i++){
			System.out.println(list.get(i));
		}
		*/
		
		/*
		//text方法测试
		String path="/wc_input/file1.txt";
		hu.text(path);
		*/
		
		/*
		//put方法测试
		String fspath="/wc_input/file3.txt";
		String localpath="C:/Users/herenli/Desktop/文件/logs/applogs.txt";
		hu.put(fspath, localpath);
		*/
		
		/*
		//delete方法测试
		String path="/wc_input/file3.txt";
		System.out.println(hu.delete(path));
		*/
		
		String path="/wc_input/logs";
		System.out.println(hu.mkdir(path));
		
	}

}
