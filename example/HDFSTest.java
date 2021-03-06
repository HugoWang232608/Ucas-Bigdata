import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;        

/**
 *complie HDFSTest.java
 *
 * javac HDFSTest.java 
 *
 *execute HDFSTest.java
 *
 * java HDFSTest  
 * 
 */

public class HDFSTest {

    public static void main(String[] args) throws IOException, URISyntaxException{
		if (args.length <= 0) {
			System.out.println("Usage: HDFSTest <hdfs-file-path>");
			System.exit(1);
		}

		String file = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        while ((s=in.readLine())!=null) {
             System.out.println(s);
        }

        in.close();

        fs.close();
    }
}
