from imfdatapy import IMFData

connection = IMFData(False)
#print(connection.datasets)
print(connection.get_data('CPI', key = 'USA.CPI._T.IX.A'))
#print(connection.listDataStructures('DSD_CPI'))
#print('-'*10)
#print('-'*10)
#print(connection.getDataStructures('DSD_CPI'))
