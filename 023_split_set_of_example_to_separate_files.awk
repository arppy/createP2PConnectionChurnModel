FNR==1{
  split(FILENAME,fname,"[.x]")
  baseOutFileName = fname[1]"x"fname[2]
  prefix = fname[2]
  p=0
  outFileName = "partOfSvmLight_"prefix"/"baseOutFileName"_p"p".svmlight"
}
{
  if(FNR%100000==0) {
    p++
    outFileName = "partOfSvmLight_"prefix"/"baseOutFileName"_p"p".svmlight"
  }
  print($0) >> outFileName
}