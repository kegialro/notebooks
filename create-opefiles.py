import pandas as pd

def generate_ope():

  #nombres_columnas = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18']
                      
  df = pd.read_csv('table1.ope', encoding='ISO-8859-1', delimiter='|', nrows=1000, header=None)
  df.to_csv('part-0000.csv', sep='|', encoding='ISO-8859-1', index=False, header=False)
  df.to_csv('part-0000.ope', sep='|', encoding='ISO-8859-1', index=False, header=False)

if __name__ == "__main__":
  generate_ope()

