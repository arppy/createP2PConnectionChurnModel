BEGIN {
MAX_VALUE[1]=22
MAX_VALUE[2]=10
MAX_VALUE[3]=10
MAX_VALUE[4]=85
MAX_VALUE[5]=85
MAX_VALUE[6]=12
MAX_VALUE[7]=12
MAX_VALUE[8]=0
MAX_VALUE[9]=0
MAX_VALUE[10]=6
MAX_VALUE[11]=6
MAX_VALUE[12]=0
MAX_VALUE[13]=0
MAX_VALUE[14]=111
MAX_VALUE[15]=111
MAX_VALUE[16]=712
MAX_VALUE[17]=712
for (i=1; i<=17; i++)  {
  sum+=MAX_VALUE[i]+1
}
print(sum)
sum=0
for (i=1; i<=17; i+=2)  {
  sum+=MAX_VALUE[i]+2
}
print(sum)
FS=";"
}
FNR==1 {
 split(FILENAME,farray,"[x.]")
 prefix=farray[2]
}
{
  svmString = "0"
  startPoz = 0
  for (i=1;i<=NF;i++) {
     if($i >= 0) {
        svmString = svmString" "(startPoz+$i)":1"
     }
     startPoz += (MAX_VALUE[i]+1)
  }
  print(svmString) >> "1010rr1101rr10network100000x"prefix".svmlight"
  #allHour2
  startPoz = 0
  csvString = ""($1+1)
  startPoz += MAX_VALUE[1]+2
  for (i=2;i<=NF;i+=2) {
     csvString=csvString","(startPoz+$i+1)
     csvString=csvString","(startPoz+$(i+1)+1)
     startPoz += MAX_VALUE[i]+2
  }
  print(csvString) >> "1010rr1101rr10network100000x"prefix".csv"
}