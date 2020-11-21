import sys
import pandas as pd

def main(argv):
  outname = argv[1] + '.csv'
  with open(str(argv[1])+'.log') as f:
    txt = f.readlines()
    valueRead = []
    valueWrite = []
    avgvalueRead = []
    avgvalueWrite = []

  for i in range(len(txt)):
    line = txt[i].strip()
    if 'RunTime for NDB Value State Read' in line:
      valueRead.append(line.split(' ')[13])
    elif 'RunTime for NDB Value State Write' in line:
      valueWrite.append(line.split(' ')[13])
    elif 'AverageTime for NDB Value State Read' in line:
      avgvalueRead.append(line.split(' ')[13])
    elif 'AverageTime for NDB Value State Write' in line:
      avgvalueWrite.append(line.split(' ')[13])

  maxlen = max([len(valueRead), len(valueWrite), len(avgvalueRead), len(avgvalueWrite)])
  df = pd.DataFrame([(range(maxlen)), valueRead, valueWrite, avgvalueRead, avgvalueWrite])
  df = df.T
  df.columns=['Counter','Read','Write', 'Read Average(Rolling)', 'Write Average(Rolling)']
  df.to_csv(outname, index=False)


if __name__ == "__main__":
   main(sys.argv)
