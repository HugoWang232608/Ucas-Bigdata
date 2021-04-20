import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.Iterator;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.log4j.*;


public class Hw1Grp2{
    public static void main(String[] args) throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException{
        if (args.length <= 0) {
			System.out.println("Usage example: java Hw1Grp2 R=<file> group:R2 'res:count,avg(R5),max(R0)'");
			System.exit(1);
		}
		
		// extract commandline information
        String file = args[0].substring(2);
        int group = Integer.parseInt(args[1].substring(9));
        String rep = args[2].substring(1, args[2].length()-1);
		String[] method = rep.split(":");
        String[] res = method[1].split(",");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);
		
		// judge what the commandline contains
        boolean isCount = false;
        boolean isAvg = false;
        boolean isMax = false;
        int avgKey = -1;
        int maxKey = -1;

        for (int i=0; i<res.length; i++){
            switch (res[i].substring(0,3)) {
                case "cou":
                    isCount = true;
                    break;
                case "avg":
                    isAvg = true;
                    avgKey = Integer.parseInt(res[i].substring(5, res[i].length() - 1));
                    break;
                case "max":
                    isMax = true;
                    maxKey = Integer.parseInt(res[i].substring(5, res[i].length() - 1));
					break;
                default:
                    break;
            }
        }

		// open and read the file
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        Hashtable<String, Value> hashTable = new Hashtable<>(); 
        while ((s=in.readLine())!=null) {
             String[] contents = s.split("\\|");// split the contents
             String key = contents[group];
             if(hashTable.containsKey(key)){
                 Value value = hashTable.get(key);
                 value.count = value.count + 1;
                 if(isAvg == true){
                     value.avg = value.avg + Double.parseDouble(contents[avgKey]);
                 if(isMax == true){
                     double max_now = Double.parseDouble(contents[maxKey]);
                     if(max_now > value.max){
                         value.max = max_now;
                     }
                 }
                 hashTable.put(key, value);
             }
            }else hashTable.put(key,new Value(1, (isAvg ? Double.parseDouble(contents[avgKey]) : 0), (isMax ? Double.parseDouble(contents[maxKey]) : 0)));
        }
        System.out.println("finished!");

    Logger.getRootLogger().setLevel(Level.WARN);

    // create table descriptor
    String tableName= "Result";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

    // create column descriptor
    HColumnDescriptor cf = new HColumnDescriptor("res");
    htd.addFamily(cf);

    // configure HBase
    Configuration configuration = HBaseConfiguration.create();
    HBaseAdmin hAdmin = new HBaseAdmin(configuration);

    if (hAdmin.tableExists(tableName)) {
        System.out.println("Table already exists");
    }
    else {
        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully");
    }
    hAdmin.close();

    HTable table = new HTable(configuration,tableName);
    Iterator<String> it = hashTable.keySet().iterator();
	
	// write the hashTable into Hbase
    while(it.hasNext()){
        String key = it.next();
        Value value0 = hashTable.get(key);
        value0.avg = value0.avg/value0.count;
		BigDecimal b = new  BigDecimal(value0.avg);
        value0.avg = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        
        Put put = new Put(key.getBytes());
		if(isCount == true){
        	put.add("res".getBytes(),"count".getBytes(),String.valueOf(value0.count).getBytes());
		}
        if(isAvg == true){
            put.add("res".getBytes(),("avg(R"+avgKey+")").toString().getBytes(),String.valueOf(value0.avg).getBytes());
        }
        if(isMax == true){
            put.add("res".getBytes(),("max(R"+maxKey+")").toString().getBytes(),String.valueOf(value0.max).getBytes());
        }
        table.put(put);
    }
    table.close();
    System.out.println("put successfully");
}
}
// the class Value
class Value{
    public int count = 0;
    public double avg = 0.00;
    public double max = -1;

    public Value(int count, double avg, double max){
        this.count = count;
        this.avg = avg;
        this.max = max;
    }
}
