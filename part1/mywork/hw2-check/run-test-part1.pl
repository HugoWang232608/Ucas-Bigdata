#!/usr/bin/perl

#
# Written by Shimin Chen
#
# Check hw2 part1 
#

use POSIX ":sys_wait_h";

my $script_path=`dirname $0`; chomp($script_path);

if ($#ARGV < 1) {
   print "Usage: $0 <score output> <file...>\n";
   exit (0);
}

$| = 1; # set auto flush

if ( -f $ARGV[0] ) {
  print "$ARGV[0] already exists and cannot be score output!\n";
  exit (0);
}

my $num_groups=4;
my %result_cnt; 
my %result_avg;

my $num_input_cases=2;

my @total;

for (my $i=0; $i<$num_input_cases; $i++) {
   $total[$i] = &hash_standard_results($i, "$script_path/part1-result/result_$i");
   print "$script_path/part1-result/result_$i has $total[$i] lines\n";
}


open OUT, ">$ARGV[0]" or die "can't open $ARGV[0] for writing!\n";

for (my $i=1; $i<=$#ARGV; $i++) {
   my $file_name= $ARGV[$i];
   my $mydate=`date`; chomp($mydate);
   print "\n\n\n";
   print "------------------------------------------------------------\n";
   print "[$mydate] $i $file_name\n";
   print "------------------------------------------------------------\n";

   my $chk_grp_result= &checkGroup($file_name);
   print OUT $file_name, " group: ", $chk_grp_result, "\n";
   print $file_name, " group: ", $chk_grp_result, "\n";
   my $score= &grading($file_name);
   print OUT $file_name, " raw score: ", $score, "\n";
   print $file_name, " raw score: ", $score, "\n";
}

close(OUT);

# ---
# check if the group is computed correctly
# ---
sub checkGroup($)
{
   my ($file_name)= @_;

   if ($file_name =~ /(\d)_(\w+)_hw2.java/) {
      my $group= $1;
      my $student_id= $2;

      my $last_6digits= 0;
      if ($file_name =~ /(\d\d\d\d\d\d)_hw2.java/) {
            $last_6digits= $1;
      }
      else {
            return "last 6 char of ID not digits?";
      }
      my $compute = $last_6digits % $num_groups;
      if ($compute != $group) {
         return "should be group $compute";
      }

      return "good";
   }
   else {
      return "bad file name format";
   }
}

# ---
# get the group and student id
# ---
sub getGroupID($)
{
   my ($file_name)= @_;

   if ($file_name =~ /(\d)_(\w+)_hw2.java/) {
      my $group= $1;
      my $student_id= $2;

      return ($group, $student_id);
   }
   else {
      print "Error: Bad file name $file_name\n";
      return (-1, -1);
   }
}

# ---
# print the command and run it
# ---
sub mysystem($)
{
  my ($command) = @_;
  print $command, "\n";
  return system($command);
}

# ---
# set up a single test
# &setupTest($source, $main_class);
# ---
sub setupTest($$)
{
   my ($source, $main_class)= @_;

   # 1. create a sandbox directory
   &mysystem("rm -rf sandbox; mkdir sandbox");
   &mysystem("cp $source sandbox/$main_class.java");
   &mysystem("cp $script_path/$main_class-manifest.txt sandbox/");

   # 2. compile
   print "------------------------------------------------------------\n";
   print "Compile\n";
   print "------------------------------------------------------------\n";
   &mysystem("cd sandbox; javac $main_class.java 2>&1; jar cfm $main_class.jar $main_class-manifest.txt *.class; cd ..");

   # 3. check jar file
   my $jarfile= "sandbox/$main_class.jar";
   if ( -f $jarfile ) {
      return 0;
   }
   else {
      print "Error compiling the source java file!\n";
      return -1;
   }
}

# ---
# hash the standard results into a hash table for later checking
#
# $total = &hash_standard_results($id, $standard);
# ---
sub hash_standard_results($$)
{
  my ($which, $standard) = @_;

  my $total= 0;
  my ($src1, $dest1, $cnt1, $avg1, $sum1);

  open IN1, "$standard" or die "can't open $standard!\n";
  while (<IN1>) {
    my $line1= $_; chomp($line1);

    if ($line1 =~ /^\s*([\w\.]+)\s+([\w\.]+)\s+([\w\.]+)\s+([\w\.]+)\s*$/) {
      #print "src1: $src1, dest1: $dest1, cnt1: $cnt1, sum1: $sum1, avg1: $avg1\n";

      $src1= $1; $dest1= $2; $cnt1= $3; $sum1= $4;
      $avg1= $sum1 / $cnt1;

      $total ++;

      my $key= $which . '-' . $src1 . '-' . $dest1;
      $result_cnt{$key} = $cnt1;
      $result_avg{$key} = $avg1;
    }
    else {
      die "Error reading $standard at line $line1!\n";
    }

  }
  close(IN1);

  return $total;
}

# ---
# use the hash table to check the correctness of the results
#
# $correct = &check_part1($id, $hw2_output);
# ---
sub check_part1($$)
{
  my ($which, $hw2) = @_;

  my $correct = 0;
  my ($src2, $dest2, $cnt2, $avg2);

  open IN2, "$hw2" or return 0;
  while (<IN2>) {
    my $line2= $_; chomp($line2);

    if ($line2 =~ /^\s*([\w\.]+)\s+([\w\.]+)\s+([\w\.]+)\s+([\w\.]+)\s*$/) {
      # print "src2: $src2, dest2: $dest2, cnt2: $cnt2, avg2: $avg2\n";
      $src2= $1; $dest2= $2; $cnt2= $3; $avg2= $4;

      my $key= $which . '-' . $src2 . '-' . $dest2;
      my $cnt1= $result_cnt{$key};
      my $avg1= $result_avg{$key};

      if (($cnt1 == $cnt2) &&
          (($avg1-0.002 <= $avg2) && ($avg2 <= $avg1+0.002))) {
          $correct ++;
      }
    }
  }
  close(IN2);

  return $correct;
}

# ---
# run a single test
# &runTest($main_class, $input, $result);
# ---
sub runTest($$$)
{
   my ($main_class, $input, $result)= @_;

   print "------------------------------------------------------------\n";
   print "Run with input $input\n";
   print "------------------------------------------------------------\n";

   # 1. remove output directory
   &mysystem("hdfs dfs -rm -f -r /hw2/output 2>&1");

   # 2. run
   &mysystem("cd sandbox; hadoop jar ./$main_class.jar $input /hw2/output >out 2>err; cd ..");

   # 3. obtain result
   &mysystem("hdfs dfs -cat /hw2/output/part-* > $result");
}

# ---
# grading homework 1
# &grading($source)
# ---
sub grading($)
{
   my ($source)= @_;
   my ($group, $student_id)= &getGroupID($source);

   if ($group == -1) {return 0;}
   
   my $main_class= "Hw2Part1";

   # 1. set up test
   if (&setupTest($source, $main_class) < 0) {return 0;}

   # 2. run test
   for (my $i=0; $i<$num_input_cases; $i++) {
      &runTest($main_class, "/hw2/part1-input/input_$i", "sandbox/output_$i");
   }
   
   # 3. check result
   my $score= 0;
   for (my $i=0; $i<$num_input_cases; $i++) {

      my $correct= &check_part1($i, "sandbox/output_$i");
      my $error= $total[$i] - $correct;

      if ($error == 0) {
        $score += 1;
      }
   }

   # 4. preserve the sandbox
   my $pos= index($source, ".java");
   if ($pos >= 0) {
       my $d= substr($source, 0, $pos);
       &mysystem("rm -rf $d; mv sandbox $d");
   }

   return $score;
}
