#!/usr/bin/perl

#
# Written by Shimin Chen
#
# Check hw2 part2 
#
use POSIX ":sys_wait_h";
use strict;

my $checking = 1;

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

my $num_input_case=2;

# ---
# Preparation
# ---

my %result_path; 

my @sssp_total;
my @kcore_total;
my @triangle_total;

if ($checking == 1) {

  # sssp
  for (my $i=0; $i<$num_input_case; $i++) {
   $sssp_total[$i] = &hash_sssp_results("sssp$i", "$script_path/part2-result/SSSP-result$i");
   print "$script_path/part2-result/SSSP-result$i has $sssp_total[$i] lines\n";
  }

  # kcore
  for (my $i=0; $i<$num_input_case; $i++) {
   $kcore_total[$i] = &hash_kcore_results("kcore$i", "$script_path/part2-result/KCore-result$i");
   print "$script_path/part2-result/KCore-result$i has $kcore_total[$i] lines\n";
  }

  # triangle
  for (my $i=0; $i<$num_input_case; $i++) {
   $triangle_total[$i] = &hash_triangle_results("triangle$i", "$script_path/part2-result/Triangle-result$i");
   print "$script_path/part2-result/Triangle-result$i has $triangle_total[$i] lines\n";
  }
}


# ---
# Run
# ---

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

   my ($group, $student_id)= &getGroupID($file_name);
   my $score= 0;
   if ($group == 0) {
     $score= &grading_sssp($file_name);
   }
   elsif ($group == 1) {
     $score= &grading_kcore($file_name);
   }
   elsif ($group == 2) {
     $score= &grading_color($file_name);
   }
   elsif ($group == 3) {
     $score= &grading_triangle($file_name);
   }

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

   if ($file_name =~ /(\d)_(\w+)_hw2.cc/) {
      my $group= $1;
      my $student_id= $2;

      my $last_6digits= 0;
      if ($file_name =~ /(\d\d\d\d\d\d)_hw2.cc/) {
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

   if ($file_name =~ /(\d)_(\w+)_hw2.cc/) {
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
   &mysystem("cp $source sandbox/$main_class.cc");

   # 2. compile
   print "------------------------------------------------------------\n";
   print "Compile\n";
   print "------------------------------------------------------------\n";
   &mysystem("cd sandbox;" .
             'g++ -std=c++0x -g -O2 -I${HADOOP_HOME}/include -I${GRAPHLITE_HOME}/include'." $main_class.cc -fPIC -shared -o $main_class.so;" .
             "cd ..");

   # 3. check so file
   my $sofile= "sandbox/$main_class.so";
   if ( -f $sofile ) {
      return 0;
   }
   else {
      print "Error compiling the source file!\n";
      return -1;
   }
}

# ---
# run a single test
# &runTest($main_class, $input, $result, $args);
# ---
sub runTest($$$$)
{
   my ($main_class, $input, $result, $args)= @_;

   print "------------------------------------------------------------\n";
   print "Run with input $input\n";
   print "------------------------------------------------------------\n";

   # 1. clean up
   &mysystem("killall -9 graphlite 2>&1 >/dev/null");

   my $curdir= `pwd`; chomp($curdir);

   # 2. run
   my $pid;
   do { $pid= fork; } while ($pid < 0);

   if ($pid == 0) {
      # child
      &mysystem("cd sandbox; ". 
             "start-graphlite ./$main_class.so ".
             '../' . "$input $curdir/sandbox/orig-$result $args >out 2>err; cd ..");
      exit 0;
   } else {
      # parent

      my $kid= -1;
      my $num_waits= 0;
      my $max_waits= 300;
      do {
         sleep (1);
         $num_waits ++;
         $kid= waitpid($pid, WNOHANG);
      } while (($kid <= 0)&&($num_waits<$max_waits));

      if (($kid<=0) && ($num_waits >= $max_waits)) {
          print "graphlite has run at least $max_waits seconds\n";
          &mysystem("killall -9 graphlite 2>&1 >/dev/null");
      }
   }

   sleep(2);

   # 3. obtain result
   &mysystem("cd sandbox; cat orig-$result* > $result; cd ..");
}

# --------------------------------------------------------------------------------
# SSSP
# --------------------------------------------------------------------------------

# ---
# hash the standard results into a hash table for later checking
#
# $total = &hash_sssp_results($id, $standard);
# ---
sub hash_sssp_results($$)
{
  my ($which, $standard) = @_;

  my $total= 0;
  my ($vid, $path);

  open IN1, "$standard" or die "can't open $standard!\n";
  while (<IN1>) {
    my $line1= $_; chomp($line1);

    if ($line1 =~ /^\s*([\w\.]+):\s+([\w\.]+)\s*$/) {

      $vid= $1; $path= $2;
      $total ++;

      my $key= $which . '-' . $vid ;
      $result_path{$key} = $path;
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
# $correct = &check_part2_sssp($id, $hw2_output);
# ---
sub check_part2_sssp($$)
{
  my ($which, $hw2) = @_;

  my $correct = 0;
  my ($vid2, $path2);

  my $error = 0;

  open IN2, "$hw2" or return 0;
  while (<IN2>) {
    my $line2= $_; chomp($line2);

    if ($line2 =~ /^\s*([\w\.]+):\s+([\w\.]+)\s*$/) {
      $vid2= $1; $path2= $2;

      my $key= $which . '-' . $vid2;
      my $path1= $result_path{$key};

      if ((($path1-0.002 <= $path2) && ($path2 <= $path1+0.002))) {
          $correct ++;
      }
      else {
        if ($error < 10) {
          print "Error: $line2, should be $path1\n";
        }
        $error ++;
      }
    }
  }
  close(IN2);

  if ($error > 0) {$correct -= $error;}

  return $correct;
}

# ---
# grading homework 2 part 2
# &grading($source)
# ---
sub grading_sssp($)
{
   my ($source)= @_;
   my ($group, $student_id)= &getGroupID($source);

   if ($group == -1) {return 0;}
   
   my $main_class= "SSSPVertex";

   # 1. set up test
   if (&setupTest($source, $main_class) < 0) {return 0;}

   # 2. run test
   for (my $i=0; $i<$num_input_case; $i++) {
      &runTest($main_class, "part2-input/SSSP-graph$i" . "_4w", "output_$i", '0');
   }

   # 3. check result
   my $score= 0;

   if ($checking == 1) {
     for (my $i=0; $i<$num_input_case; $i++) {

        print "check sssp $i\n";
        my $correct= &check_part2_sssp("sssp$i", "sandbox/output_$i");
        my $error= $sssp_total[$i] - $correct;

        if ($error == 0) {
          $score += 1;
        }
        else {
          print "$error vertices are wrong!\n";
        }
     }
   }

   # 4. preserve the sandbox
   my $pos= index($source, ".cc");
   if ($pos >= 0) {
       my $d= substr($source, 0, $pos);
       &mysystem("rm -rf $d; mv sandbox $d");
   }

   return $score;
}

# --------------------------------------------------------------------------------
# KCORE
# --------------------------------------------------------------------------------

# ---
# hash the standard results into a hash table for later checking
#
# $total = &hash_kcore_results($id, $standard);
# ---
sub hash_kcore_results($$)
{
  my ($which, $standard) = @_;

  my $total= 0;
  my ($vid);

  open IN1, "$standard" or die "can't open $standard!\n";
  while (<IN1>) {
    my $line1= $_; chomp($line1);

    if ($line1 =~ /^\s*([\w\.]+)\s*$/) {

      $vid= $1; 
      $total ++;

      my $key= $which . '-' . $vid ;
      $result_path{$key} = 1;
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
# $correct = &check_part2_kcore($id, $hw2_output);
# ---
sub check_part2_kcore($$)
{
  my ($which, $hw2) = @_;

  my $correct = 0;
  my ($vid2);

  my $error = 0;

  open IN2, "$hw2" or return 0;
  while (<IN2>) {
    my $line2= $_; chomp($line2);

    if ($line2 =~ /^\s*([\w\.]+)\s*$/) {
      $vid2= $1;

      my $key= $which . '-' . $vid2;
      my $v= $result_path{$key};

      if ($v == 1) {
          $correct ++;
      }
      else {
        if ($error < 10) {
          print "Error: $line2\n";
        }
        $error ++;
      }
    }
  }
  close(IN2);

  if ($error > 0) {$correct -= $error;}

  return $correct;
}


# ---
# grading homework 2 part 2
# &grading($source)
# ---
sub grading_kcore($)
{
   my ($source)= @_;
   my ($group, $student_id)= &getGroupID($source);

   if ($group == -1) {return 0;}
   
   my $main_class= "KCoreVertex";

   # 1. set up test
   if (&setupTest($source, $main_class) < 0) {return 0;}

   # 2. run test
   &runTest($main_class, "part2-input/KCore-graph0" . "_4w", "output_0", "6");
   &runTest($main_class, "part2-input/KCore-graph1" . "_4w", "output_1", "7");

   # 3. check result
   my $score= 0;
   if ($checking == 1) {
     for (my $i=0; $i<$num_input_case; $i++) {

        print "check kcore $i\n";
        my $correct= &check_part2_kcore("kcore$i", "sandbox/output_$i");
        my $error= $kcore_total[$i] - $correct;

        if ($error == 0) {
          $score += 1;
        }
        else {
          print "$error vertices are wrong!\n";
        }
     }
   }

   # 4. preserve the sandbox
   my $pos= index($source, ".cc");
   if ($pos >= 0) {
       my $d= substr($source, 0, $pos);
       &mysystem("rm -rf $d; mv sandbox $d");
   }

   return $score;
}

# --------------------------------------------------------------------------------
# Color
# --------------------------------------------------------------------------------
#
# check_color($hw2_input, $hw2_output)
#
sub check_color($$)
{
   my ($input, $output)= @_;

   print "------------------------------------------------------------\n";
   print "check $input and $output\n";
   print "------------------------------------------------------------\n";

   # 1. generate a mixed edge + vertex input

   # 1.1 get number of lines
   my $line= `wc -l $input`;
   my $numlines = 0;
   if ($line =~ /^(\d+)\s+/) {
      $numlines= $1;
   }
   else {
      die "Error processing $input!\n";
   }

   # 1.2 cut the file into two
   my $num_edges = $numlines - 2;
   &mysystem("head -n 2 $input > sandbox/head");
   &mysystem("tail -n $num_edges $input > sandbox/tail");

   my $num_vertices= 0;
   $line= `head -n 1 sandbox/head`;
   if ($line =~ /(\d+)/) {
      $num_vertices= $1;
   }

   # 1.3 generate a vertex file
   my $count= 0;
   open IN3, "sandbox/$output" or die "can't open sandbox/$output!\n";
   open OUT3, ">sandbox/vertex" or die "can't create sandbox/vertex!\n";
   while (<IN3>) {
     if (/^\s*(\d+):\s*(\d+)\s*$/) {
        print OUT3 "$1 -1 $2\n";
        $count ++;
     }
   }
   close(OUT3);
   close(IN3);

   if ($count != $num_vertices) {
      print "$output contains $count lines, expecting $num_vertices lines!\n";
      return 0;
   }

   # 1.4 mix all together
   &mysystem("cat sandbox/vertex sandbox/tail > sandbox/vertex-edge");
   &mysystem("sort -n sandbox/vertex-edge > sandbox/vertex-edge-sorted");
   &mysystem("cat sandbox/head sandbox/vertex-edge-sorted > sandbox/check-input_1");

   # 2. run checking
   my $curdir= `pwd`; chomp($curdir);
   &mysystem("cd sandbox; ". 
             "start-graphlite ../CheckGraphColor/CheckGraphColor.so ".
             "$curdir/sandbox/check-input $curdir/sandbox/check-output -v >out 2>err; cd ..");

   # 3. get checking result
   &mysystem("tail -n 1 sandbox/check-output_1");
   $line= `cat sandbox/check-output_1`;
   if ($line =~ /good/) {
      return 1;
   }
   else {
      return 0;
   }
}

# ---
# grading homework 2 part 2
# &grading($source)
# ---
sub grading_color($)
{
   my ($source)= @_;
   my ($group, $student_id)= &getGroupID($source);

   if ($group == -1) {return 0;}
   
   my $main_class= "ColorVertex";

   # 1. set up test
   if (&setupTest($source, $main_class) < 0) {return 0;}

   # 2. run test
   &runTest($main_class, "part2-input/Color-graph0" . "_4w", "output_0", "0 5");
   &runTest($main_class, "part2-input/Color-graph1" . "_4w", "output_1", "0 30");

   # 3. check result
   my $score= 0;
   for (my $i=0; $i<$num_input_case; $i++) {
      my $r= &check_color("part2-input/Color-graph$i", "output_$i");
      if ($r == 1) {
         $score += 1;
      }
   }

   # 4. preserve the sandbox
   my $pos= index($source, ".cc");
   if ($pos >= 0) {
       my $d= substr($source, 0, $pos);
       &mysystem("rm -rf $d; mv sandbox $d");
   }

   return $score;
}

# --------------------------------------------------------------------------------
# Triangle
# --------------------------------------------------------------------------------
# ---
# hash the standard results into a hash table for later checking
#
# $total = &hash_triangle_results($id, $standard);
# ---
sub hash_triangle_results($$)
{
  my ($which, $standard) = @_;

  my $total= 0;
  my ($vid, $path);

  open IN1, "$standard" or die "can't open $standard!\n";
  while (<IN1>) {
    my $line1= $_; chomp($line1);

    if ($line1 =~ /^\s*([\w\.]+):*\s+([\w\.]+)\s*$/) {

      $vid= $1; $path= $2;
      $total ++;

      my $key= $which . '-' . $vid ;
      $result_path{$key} = $path;
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
# $correct = &check_part2($id, $hw2_output);
# ---
sub check_part2_triangle($$)
{
  my ($which, $hw2) = @_;

  my $correct = 0;
  my ($vid2, $path2);

  my %count;
  foreach my $i (keys %result_path) {
    $count{$i}= 0;
  }

  my $error = 0;

  open IN2, "$hw2" or return 0;
  while (<IN2>) {
    my $line2= $_; chomp($line2);

    if ($line2 =~ /^\s*([\w\.]+):*\s+([\w\.]+)\s*$/) {
      $vid2= $1; $path2= $2;

      my $key= $which . '-' . $vid2;
      my $path1= -9999;
      if ($result_path{$key} ne '') {
        $path1= $result_path{$key};
      }
      $count{$key} = $count{$key} + 1;

      if ($path1 == $path2) {
        if ($count{$key} == 1) {
          $correct ++;
        }
      }
      else {
        if ($error < 10) {
          print "Error: $line2\n";
        }
        $error ++;
      }
    }
  }
  close(IN2);

  if ($error > 0) {$correct -= $error;}

  return $correct;
}

# ---
# grading homework 2 part 2
# &grading($source)
# ---
sub grading_triangle($)
{
   my ($source)= @_;
   my ($group, $student_id)= &getGroupID($source);

   if ($group == -1) {return 0;}
   
   my $main_class= "TriangleVertex";

   # 1. set up test
   if (&setupTest($source, $main_class) < 0) {return 0;}

   # 2. run test
   for (my $i=0; $i<$num_input_case; $i++) {
      &runTest($main_class, "part2-input/Triangle-graph$i" . "_4w", "output_$i", "");
   }

   # 3. check result
   my $score= 0;
   if ($checking == 1) {
     for (my $i=0; $i<$num_input_case; $i++) {

        my $correct= &check_part2_triangle("triangle$i", "sandbox/output_$i");
        my $error= $triangle_total[$i] - $correct;

        if ($error == 0) {
          $score += 1;
        }
        else {
          print "$error vertices are wrong!\n";
        }
     }
   }

   # 4. preserve the sandbox
   my $pos= index($source, ".cc");
   if ($pos >= 0) {
       my $d= substr($source, 0, $pos);
       &mysystem("rm -rf $d; mv sandbox $d");
   }

   return $score;
}
