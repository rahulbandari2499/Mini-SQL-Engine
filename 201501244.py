import sys
import re

##TODO
#handle quotations for csv files.
#handle upper,lower cases for Query.
#use a function for execption handling.

DEBUG=True

class Parser():
    def __init__(self):
         self.query=[]
         self.tables={}

    def parse_meta_data(self):
        fs=open('metadata.txt','r')
        try:
            for line in fs:
                temp=line.strip()
                if temp=='<begin_table>':
                    table_begin=1
                    continue
                if table_begin==1:
                    new_table=temp
                    self.tables[new_table]=[]
                    table_begin=0
                    continue
                if temp !='<end_table>':
                    self.tables[new_table].append(temp)
            return self.tables
        except:
            print "Cannot Open the file metadata.txt"

    def parse_given_query(self):
            i1 = (re.sub(' +',' ',str(sys.argv[1]))).strip()
            if DEBUG:
                print "i1",i1
            if "from" in i1.lower():
                ind=i1.lower().find('from')
                a=i1[ind:ind+4]
                i2=i1.split(a)
                if DEBUG:
                    print "i2",i2
            else:
                sys.exit("Incorrect Syntax")

            i2[0]= (re.sub(' +',' ',i2[0])).strip()
            if "select" in i2[0].lower():
                ind=i2[0].lower().find('select')
                temp_columns=i2[0][ind+6:]
                temp_columns= (re.sub(' +',' ',temp_columns)).strip()
                if DEBUG:
                    print "temp_columns",temp_columns
                self.query.append('select')
            else:
                sys.exit("Incorrect Syntax")










class Query:
    def __init__(self,tables):
        self.tables=tables #table_name::column names




if __name__=="__main__":
    p=Parser()
    tables=p.parse_meta_data()
    p.parse_given_query()
    #q=Query(tables)
