import sys
import re
import copy
import csv
from texttable import Texttable

##TODO
#handle quotations for csv files.
#handle upper,lower cases for Query.
#use a function for execption handling.
# see for NOT, ==

DEBUG=True
FUNCS=['max','min','avg','distinct','sum']

def print_error(err):
    sys.stderr.write(err+"\n")
    quit(-1)

def condition_error_check(col, t, temp_table,col_list):
    if temp_table != t:
        print_error('Given table \'' + temp_table + '\' is not present ')
    elif col not in col_list:
        print_error('Column \'' + col + '\' is not found in \'' +table_here + '\'')



class Parser():
    def __init__(self):
         self.query=[]
         self.tables={}
         self.query_cols=[]
         self.query_tables=[]
         self.condition=[]

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
            print_error("Error:Cannot Open the file metadata.txt")

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
                sys.exit("Error:Invalid Syntax")

            i2[0]= (re.sub(' +',' ',i2[0])).strip()
            if "select" in i2[0].lower():
                ind=i2[0].lower().find('select')
                temp_columns=i2[0][ind+6:]
                temp_columns= (re.sub(' +',' ',temp_columns)).strip()
                if DEBUG:
                    print "temp_columns",temp_columns
                self.query.append('select')
            else:
                sys.exit("Error:Invalid Syntax")

            # if "distinct(" not in temp_columns.lower() and "distinct" in temp_columns.lower() :
                # ind=temp_columns.lower().find('distinct')
                # temp_columns=temp_columns[8:]

            temp_columns=(re.sub(' +',' ',temp_columns)).strip()
            temp_columns=temp_columns.split(',')
            for col in temp_columns:
                temp_columns[temp_columns.index(col)] = (re.sub(' +',' ',col)).strip()
            if DEBUG:
                print "temp_columns",temp_columns
            self.query_cols=copy.deepcopy(temp_columns)

            i2[1] = (re.sub(' +',' ',i2[1])).strip()
            i3 = i2[1].split('where')
            if len(i3)>1:
                for i in range(0,2):
                    i3[i]=(re.sub(' +',' ',i3[i])).strip()
            self.condition=copy.deepcopy(i3)
            temp_tables=(re.sub(' +',' ',i3[0])).strip()
            temp_tables=temp_tables.split(',')
            for table in temp_tables:
                temp_tables[temp_tables.index(table)] = (re.sub(' +',' ',table)).strip()
            if DEBUG:
                print "temp_tables",temp_tables
            for table in temp_tables:
                if table not in self.tables.keys():
                    sys.exit("Error:Table "+str(table)+" Not Found")
            self.query_tables=copy.deepcopy(temp_tables)


class Query(Parser):
    def __init__(self,parser_class):
        self.tables=parser_class.tables #table_name::column names
        self.query_tables=parser_class.query_tables
        self.query_cols=parser_class.query_cols
        self.condition=parser_class.condition
        if DEBUG:
            print "in query class",self.tables,self.query_tables,self.query_cols,self.condition

    def diff_cols(self):
        norm_cols=[]
        dis_cols=[]
        func_cols=[]
        for col in self.query_cols:
            col_taken=0
            for func in FUNCS:
                if func + '(' in col.lower():
                    col_taken=1
                    temp_name=col.strip(')').split(func+'(')
                    temp_name=temp_name[1]
                    if func=="distinct":
                        dis_cols.append(temp_name)
                    else:
                        func_cols.append([func,temp_name])
                    break
            if col_taken==0:
                if col !='':
                    norm_cols.append(col.strip('()'))
        return norm_cols,dis_cols,func_cols

    def query_process(self):
        norm_cols,dis_cols,func_cols=self.diff_cols()
        if DEBUG:
            print "norm_cols,dis_cols,func_cols",norm_cols,dis_cols,func_cols

        ## select <something> from table1 where <some_condition>
        if len(self.condition) > 1 and len(self.query_tables) == 1:
            self.one_table_one_where(norm_cols,dis_cols,func_cols)
        elif len(dis_cols) !=0 :
            self.distinct_cols_query(dis_cols)

    def distinct_cols_query(self,dis_cols):
        col_data={}
        header_row=[]
        max_len=0
        for col in dis_cols:
            temp_table,column=self.resolve_alias(col,self.query_tables)
            t_name=str(temp_table)+'.csv'
            file_data=[]
            self.read_from_file(t_name,file_data)
            header_row.append(str(temp_table)+"."+str(col))
            row_data=[]
            for data in file_data:
                val=data[self.tables[temp_table].index(column)]
                if val not in row_data:
                    row_data.append(val)
            col_data[column]=row_data
            max_len = max(max_len, len(col_data[column]))
        print col_data
        final_output=[]
        final_output.append(header_row)
        i=0
        while(i<max_len):
            row=[]
            for col in dis_cols:
                if(i<len(col_data[col])):
                    row.append(col_data[col][i])
                else:
                    row.append('NULL')
            i=i+1
            final_output.append(row)
        print "final_output", final_output
        self.print_output(final_output)

    def one_table_one_where(self,norm_cols,dis_cols,func_cols):
        if len(self.query_cols) == 1 and self.query_cols[0] == '*':
            if DEBUG:
                print "yes im in star loop"
            norm_cols = self.tables[self.query_tables[0]]
        t_name=str(self.query_tables[0])+'.csv'
        file_data=[]
        self.read_from_file(t_name,file_data)
        #if DEBUG:
        #    print "file_data",file_data
        final_output=[]
        s=[]
        for col in norm_cols:
            s.append(self.query_tables[0]+'.'+col)
        final_output.append(s)
        for data in file_data:
            final_string=self.convert_string(data,self.query_tables[0])
            s=[]
            #print final_string
            if eval(final_string):
                for col in norm_cols:
                    s.append(data[self.tables[str(self.query_tables[0])].index(col)])
                final_output.append(s)
        if DEBUG:
            print final_output
        self.print_output(final_output)

    def convert_string(self,data,t):
        s=""
        #print "sp",self.condition[1].split()
        for q in self.condition[1].split():
            q=(re.sub(' +',' ',q)).strip()
            if q == '=':
                s+='=='
            elif q.lower() == 'and' or q.lower() == 'or':
                s+=' ' + q.lower() + ' '
            elif q in self.tables[t]:
                s+=data[self.tables[t].index(q)]
            elif '.' in q:
                temp_table,column = self.resolve_alias(q, [t])
                condition_error_check(column, t, temp_table,self.tables[t])
                #print "temp_table,column",temp_table,column

                s+=data[self.tables[temp_table].index(column)]
            else:
                s+=q
        return s

    def resolve_alias(self,query,tabs):
        if '.' in query:
            query=query.split('.')
            query[0]= (re.sub(' +',' ',query[0])).strip()
            query[1]= (re.sub(' +',' ',query[1])).strip()
            if query[0] not in tabs:
                print_error('Table \''+query[0]+"\'  doesn't exist in  metadata")
            return query[0],query[1]
        temp_table=''
        repeat_count=0
        for tab in tabs:
            if query in self.tables[tab]:
                repeat_count+=1
                temp_table=tab
        if repeat_count > 1:
            print_error("Given column \'" + query + "\' is ambiguos")
        elif repeat_count == 0 :
            print_error(" column \'"+ query + "\'doesn't exist")
        return temp_table,query

    def read_from_file(self,t_name,file_data):
        with open(t_name,'rb') as fs:
            rows = csv.reader(fs)
            for row in rows:
                file_data.append(row)

    def print_output(self,final_output):
        t = Texttable()
        t.add_rows(final_output)
        print t.draw()


if __name__=="__main__":
    p=Parser()
    tables=p.parse_meta_data()
    p.parse_given_query()
    q=Query(p)
    q.query_process()
