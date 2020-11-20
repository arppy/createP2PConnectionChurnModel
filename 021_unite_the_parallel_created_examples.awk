#! /usr/bin/awk -f
{
  if (!($0 in traceEncodeDict)) {
    traceEncodeDict[$0] = 1
    print($0)
  }
}