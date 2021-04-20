--------------------------------------------------------------------------------
                README 
--------------------------------------------------------------------------------

PLEASE save your code and data to your drive!
WARNING: this VM will be cleaned without notice after you log out.  
         Your code and data on the VM will be lost!!!

## Directory Layout

   * example:  example codes for HDFS and HBase
   * input:    input test data for homework 1



Please enter example, in order to follow the guide.

   $ cd example


## HDFS Usage: 

### Start and Stop
     
   $ start-dfs.sh

   then, run 'jps' to check whether following processes have been started:

    * NameNode
    * DataNode
    * SecondaryNameNode

  
   To stop HDFS, run 

   $ start-dfs.sh


### HDFS Command List

   $ hadoop fs

   hdfs directory layout:

   $ hadoop fs -ls /


###. Run Example
Description: 
  put a file into HDFS by HDFS commands, and then write a Java program to 
read the file from HDFS

1. put file to HDFS

   $ hadoop fs -mkdir /hw1-input
   $ hadoop fs -put README.md /hw1-input
   $ hadoop fs -ls -R /hw1-input

2. write a Java program  @see ./HDFSTest.java

3. compile and run Java program

   $ javac HDFSTest.java
 
   $ java HDFSTest hdfs://localhost:9000/hw1-input/README.md



## HBase Usage: 

### Start and Stop

Start HDFS at first, then HBase.
   $ start-dfs.sh
   $ start-hbase.sh

   then, run 'jps' to check whether following processes have been started:

   * NameNode
   * DataNode
   * SecondaryNameNode
   * HMaster
   * HRegionServer
   * HQuorumPeer

   To stop HDFS, run 

   $ stop-hbase.sh
   $ start-dfs.sh


###. Run Example
Description: 
   put records into HBase 

1. write a Java program  @see ./HBaseTest.java

2. compile and run Java program

   $ javac HBaseTest.java
 
   $ java HBaseTest 

3. check

    $ hbase shell
    
    hbase(main):001:0> scan 'mytable'
    ROW                                                  COLUMN+CELL                                                                                                                                             
     abc                                                 column=mycf:a, timestamp=1428459927307, value=789                                                                                                       
    1 row(s) in 1.8950 seconds
    
    hbase(main):002:0> disable 'mytable'
    0 row(s) in 1.9050 seconds
    
    hbase(main):003:0> drop 'mytable'
    0 row(s) in 1.2320 seconds
    
    hbase(main):004:0> exit

--------------------------------------------------------------------------------
version: 2019-spring
