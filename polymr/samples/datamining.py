"""
Purpose : Predict if passenger will survive to the Titanic based on machine learning techniques
"""

from polymr.inout.file import CsvFileInput


#Load the Titanic train dataset
titanic = CsvFileInput('titanic.csv')

#Profile the dataset feature
titanic.print_summary()

#Extract the fields    
fields = {'survived': 0, 'pclass': 1, 'sex':5, 'age':6}
titanic =  titanic.select(fields)

titanic.print_explain("survived")


