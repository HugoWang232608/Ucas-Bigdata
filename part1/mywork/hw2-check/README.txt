## Part1

0. set language to POSIX
   $ export LC_ALL="POSIX"

1. make sure ssh is running
   $ service ssh status

   if not, then run sshd (note that this is necessary in a docker container)
   $ service ssh start


2. make sure Hadoop is successfully started
   $ start-dfs.sh

   check if hadoop is running correctly
   $ jps

   14161 Jps
   11993 NameNode
   12396 SecondaryNameNode
   12173 DataNode

3. put input files into HDFS
   $ ./myprepare

4. run test
   $ ./run-test-part1.pl ./score <your-java-file>

Your score will be in ./score.  The run-test-part1.pl tests 2 input cases, you will
get one score for each case.  So the output full score for part1 is 2.









## Part2

0. set language to POSIX
   $ export LC_ALL="POSIX"

1. set env for GraphLite
   $ source <Home-of-GraphLite-0.20>/bin/setenv
  
2. set up for GraphColor check program
  
   enter the directory of hw2-check at first, then run

   $ ./setup-test-part2.sh
   

3. make sure ssh is running
   $ service ssh status

   if not, then run sshd (note that this is necessary in a docker container)
   $ service ssh start

4. run test

   $ ./run-test-part2.pl ./score  <your-cc-file>

Your score will be in ./score.  The run-test-part2.pl tests 2 input cases, you will
get one score for each case.  So the output full score for part2 is 2.
