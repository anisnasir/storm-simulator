#!/bin/bash
JAR="build/libs/StreamLoadBalancing-0.0.5-SNAPSHOT-jar-with-dependencies.jar"
indir="/Users/Anis/Datasets/"
inFileName="wiki"
NumberOfServers="10"
NumberOfSources="1"
NumberOfReplicas="10"
epsilon="0.01"
#epsilon="0.005 0.01 0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0 1.2 1.5 1.8 2.0 2.2 2.5 2.8 3"
lbname="cg"
maxprocs="10"
command="java -jar ${JAR}"
datasets="wiki"
#datasets="twitter"
#initialTimestamp["twitter"]=1341791969 
initialTimestamp["wiki"]=1199195421

for data in $datasets; do
 input="${indir}${inFileName}"
 for ns in $NumberOfServers ; do
  for ((y = 1; y <= 1; y++)) ;do
   for ((z = 2; z <= 2; z++)) ; do
    output="output_logs/hetero/WP/${lbname}/${ns}/output"   
    for nr in $NumberOfSources ; do
     for e in $epsilon; do
      echo "$command 9 ${input} ${output}_${ns}_${nr}_${lbname}_${e}_${y}_${z} ${ns} ${initialTimestamp[$data]} ${nr} ${NumberOfReplicas} ${e} ${y} ${z} " >> ${output}_${ns}_${nr}_${lbname}_${e}_${y}_${z}.log
      cmdlines="$cmdlines $command 9 ${input} ${output}_${ns}_${nr}_${lbname}_${e}_${y}_${z} ${ns} ${initialTimestamp[$data]} ${nr} ${NumberOfReplicas} ${e} ${y} ${z} >> ${output}_${ns}_${nr}_${lbname}_${e}_${y}_${z}.log;"
      done
     done
    done
   done
  done
done
#echo $cmdlinesi
echo -e $cmdlines | parallel --max-procs $maxprocs
echo "Done"
