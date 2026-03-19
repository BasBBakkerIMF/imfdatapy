from imfdatapy import IMFData

connection = IMFData(True)
#print(connection.datasets)
print(connection.listDataStructures('DSD_CPI'))
print('-'*10)
print('-'*10)
print(connection.getDataStructures('DSD_CPI'))
