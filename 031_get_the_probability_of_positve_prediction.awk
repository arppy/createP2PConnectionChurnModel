{
 split(FILENAME,array,"[-.]");
 positives[array[2]]+=$2;
 alls[array[2]]+=$1;
 all+=$1;
 positive+=$2;
}
END{
 print(all,positive);
 for(hour in alls){
  print(hour,alls[hour],positives[hour],positives[hour]/alls[hour])
 }
}