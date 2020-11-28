import sys
import pandas as pd

def main(argv):
  outnameread = argv[1] + '-read.csv'
  outnamewrite = argv[1] + '-write.csv'
  with open(str(argv[1])+'.log') as f:
    txt = f.readlines()
    valueRead = []
    valueWrite = []
    avgvalueRead = []
    avgvalueWrite = []
    timeRead = []
    timeWrite = []
    keyRead = []
    keyWrite = []
    keySet = set()
    cachehit = 0
    cachemiss = 0

  for i in range(len(txt)):
    line = txt[i].strip()
    if 'RunTime for RocksDB Value State Read' in line:
      valueRead.append(line.split(' ')[16])
      timeRead.append(line[0:23])
      keyRead.append(line.split(' ')[14])
      keySet.add(line.split(' ')[14])
    elif 'RunTime for RocksDB Value State Write' in line:
      valueWrite.append(line.split(' ')[16])
      timeWrite.append(line[0:23])
      keyWrite.append(line.split(' ')[14])
    elif 'Cache miss for' in line:
      cachemiss += 1
    elif 'Cache hit for' in line:
      cachehit += 1
#     elif 'RunTime for NDB Value State Write' in line:
#       valueWrite.append(line.split(' ')[13])
#     elif 'AverageTime for NDB Value State Read' in line:
#       avgvalueRead.append(line.split(' ')[13])
#     elif 'AverageTime for NDB Value State Write' in line:
#       avgvalueWrite.append(line.split(' ')[13])


#   maxlen = max([len(valueRead), len(valueWrite), len(avgvalueRead), len(avgvalueWrite)])
#   df = pd.DataFrame([(range(maxlen)), valueRead, valueWrite, avgvalueRead, avgvalueWrite])
  dfRead = pd.DataFrame([timeRead, valueRead, keyRead])
  dfWrite = pd.DataFrame([timeWrite, valueWrite, keyWrite])
  dfRead = dfRead.T
  dfWrite = dfWrite.T
  print("Total Keys %s", len(keySet))
  print("Cache Hit %s", cachehit)
  print("Cache Miss %s", cachemiss)
#   df.columns=['Counter','Read','Write', 'Read Average(Rolling)', 'Write Average(Rolling)']
  dfRead.columns=['Time','Read', 'Key']
  dfWrite.columns=['Time','Write', 'Key']
  dfRead.to_csv(outnameread, index=False)
  dfWrite.to_csv(outnamewrite, index=False)

if __name__ == "__main__":
   main(sys.argv)
