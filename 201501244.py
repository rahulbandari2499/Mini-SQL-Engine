import sys
import re
import copy
import csv
from texttable import Texttable

##TODO:
#Semicolons and multiple queries
#handle quotations for csv files.
#handle upper,lower cases for Query.
#OPERATOR SPLITTING
#use a function for execption handling.
# see for NOT, ==
#no repetation of the join column.


DEBUG=False
FUNCS=['max','min','avg','distinct','sum']
OPERATORS=['>=','<=','==','>','<','=']

def print_error(err):
    sys.stderr.write(err+"\n")
    quit(-1)

def condition_error_check(col, t, temp_table,col_list):
    if temp_table != t:
        print_error('Given table \'' + temp_table + '\' is not present ')
    elif col not in col_list:
        print_error('Column \'' + col + '\' is not found in \'' +temp_table + '\'')

def column_error_check(col,col_list,tab):
    if col not in col_list:
        print_error('Given column \'' + col + '\' in table \'' + tab + '\'')

def where_clause_error_check(list):
    if len(list) != 2:
        print_error('Error:syntax error in where condition')
def check_for_string(string, lis):
    for i in range(len(lis)):
        lis[i]=(re.sub(' +',' ',str(lis[i]))).strip()
    return string in lis

def select_error_check(q):
    temp = q.split('from')
    if len(temp)>=3:
        print_error('Error: given more than one \"from\" statement')
    temp[0]=(re.sub(' +',' ',str(temp[0]))).strip()
    if not check_for_string('select', (temp[0].lower().split())):
        print_error('Error: no select statement given')
    elif q.lower().count('select')>=2:
        print_error('More than one select statement given')

def clause_error_check(temp_cond,norm_cols,func_cols,dis_cols):
    if   len(dis_cols)+len(norm_cols)+len(func_cols) == 0:
        print_error('Nothing given to select')
    if  (len(func_cols) != 0 or len(dis_cols) != 0) and len(temp_cond) > 1 :
        print_error('Error: where Condition can only be given to project columns')
    elif len(dis_cols) != 0 and len(func_cols) != 0:
        print_error('distinct and aggregate queries cannot be given at the time')

class Parser():
    def __init__(self,que):
         self.query=[]
         self.tables={}
         self.query_cols=[]
         self.query_tables=[]
         self.condition=[]
         self.given_query=que
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
            self.given_query=self.given_query.strip('"')
            i1 = (re.sub(' +',' ',self.given_query)).strip()
            if DEBUG:
                print "i1",i1
            select_error_check(self.given_query)
            if "from" in i1.lower():
                ind=i1.lower().find('from')
                a=i1[ind:ind+4]
                i2=i1.split(a)
                if DEBUG:
                    print "i2",i2
            else:
                print_error("Error:Invalid Syntax")

            i2[0]= (re.sub(' +',' ',i2[0])).strip()
            if "select" in i2[0].lower():
                ind=i2[0].lower().find('select')
                temp_columns=i2[0][ind+6:]
                temp_columns= (re.sub(' +',' ',temp_columns)).strip()
                if DEBUG:
                    print "temp_columns",temp_columns
                self.query.append('select')

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
        clause_error_check(self.condition,norm_cols,func_cols,dis_cols)
        if DEBUG:
            print "norm_cols,dis_cols,func_cols",norm_cols,dis_cols,func_cols

        ## select <something> from table1 where <some_condition>
        if len(self.condition) > 1 and len(self.query_tables) == 1:
            #print "case1"
            self.one_table_one_where(norm_cols,dis_cols,func_cols)
        elif len(self.condition) > 1 and len(self.query_tables) > 1:
            #print "case2"
            self.more_tables_one_where(norm_cols)
        elif len(func_cols)!=0:
            #print "case3"
            self.func_cols_query(func_cols)
        elif len(dis_cols) !=0 :
            #print "case4"
            self.distinct_cols_query(dis_cols)
        elif len(self.query_tables) >=2 :
            #print "case5"
            self.join_cols_query(norm_cols)
        else:
            #print "case6"
            self.single_table_many_cols_query(norm_cols)

    def single_table_many_cols_query(self,norm_cols):
        temp_tab=self.query_tables[0]
        if len(norm_cols)==1 and norm_cols[0]=='*':
            norm_cols = self.tables[temp_tab]
        for col in norm_cols:
            if col not in self.tables[temp_tab]:
                print_error('The given column \'' + column + '\' is not found in the given table \'' + table + '\' ')
        final_output=[]
        s=[]
        for col in norm_cols:
            s.append(temp_tab+'.'+col)
        final_output.append(s)
        t1=(re.sub(' +',' ',temp_tab)).strip()
        t_name=str(t1)+'.csv'
        file_data1=[]
        self.read_from_file(t_name,file_data1)
        for data in file_data1:
            s=[]
            for col in norm_cols:
                s.append(data[self.tables[temp_tab].index(col)])
            final_output.append(s)
        self.print_output(final_output)

    def join_cols_query(self,norm_cols):
        temp_cols,temp_tabs=self.get_query_tabs_cols(norm_cols)
        print temp_cols,temp_tabs
        good_data=[]
        final_output=[]
        if len(temp_tabs) ==2:
            t1=temp_tabs[0]
            t2=temp_tabs[1]
            t1=(re.sub(' +',' ',t1)).strip()
            t_name=str(t1)+'.csv'
            file_data1=[]
            self.read_from_file(t_name,file_data1)
            t2=(re.sub(' +',' ',t2)).strip()
            t_name=str(t2)+'.csv'
            file_data2=[]
            self.read_from_file(t_name,file_data2)
            for i2 in file_data2:
                for i1 in file_data1:
                    good_data.append(i1+i2)
            self.format_output(temp_tabs,temp_cols,good_data)

        else:
            for t in temp_tabs:
                s=''
                for col in temp_cols[t]:
                    s+=t+'.'+col +','
                print s.strip(',')
                t=(re.sub(' +',' ',t)).strip()
                t_name=str(t)+'.csv'
                file_data=[]
                self.read_from_file(t_name,file_data)
                # print file_data

                for d in file_data:
                    s=''
                    for col in temp_cols[t]:
                        s+=d[self.tables[t].index(col)]+','
                    print s.strip(',')
                print
            #self.print_output(final_output)
####
    def func_cols_query(self,func_cols):
        final_output=[]
        s=[]
        row_data=[]
        for col in func_cols:
            func_name=col[0]
            col_name=col[1]
            temp_table=''
            temp_col=''
            if '.' in col_name:
                temp_table,temp_col=col_name.split('.')
            else:
                repeat_count=0
                for tab in self.query_tables:
                    if col_name in self.tables[tab]:
                        repeat_count+=1
                        temp_table=tab
                        temp_col=col_name
                if repeat_count > 1:
                    print_error("Given column \'" + col_name + "\' is ambiguos")
                elif repeat_count == 0 :
                    print_error(" column \'"+ col_name + "\'doesn't exist")
            s.append(func_name+'('+temp_table+'.'+temp_col+')')
            t_name=str(temp_table)+'.csv'
            file_data=[]
            self.read_from_file(t_name,file_data)
            temp_data=[]
            for data in file_data:
                temp_data.append(int(data[self.tables[temp_table].index(temp_col)]))

            if func_name.lower()=='max':
                row_data.append(str(max(temp_data)))
            elif func_name.lower()=='min':
                row_data.append(str(min(temp_data)))
            elif func_name.lower()=='sum':
                row_data.append(str(sum(temp_data)))
            elif func_name.lower()=='avg':
                row_data.append(str(float(sum(temp_data))/len(temp_data)))
        final_output.append(s)
        final_output.append(row_data)
        self.print_output(final_output)

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
        #print col_data
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
        #print "final_output", final_output
        self.print_output(final_output)

    def more_tables_one_where(self,norm_cols):
        where_query=self.condition[1]
        param=where_query
        oper=''
        if 'and' in where_query.lower():
            ind=where_query.lower().find('and')
            a=where_query[ind:ind+3]
            where_query=where_query.split(a)
            param=where_query[:]
            oper='and'
        elif 'or' in where_query.lower():
            ind=where_query.lower().find('or')
            a=where_query[ind:ind+2]
            where_query=where_query.split(a)
            param=where_query[:]
            oper='or'
        else:
            where_query=[where_query]
            param=where_query[:]
        if len(where_query) >=3 :
            print_error("Only one type of AND/OR clause should be used")
        if DEBUG:
            print "where_query",where_query

        temp_q=where_query[0]
        for op in OPERATORS:
            if op in temp_q:
                temp_q = temp_q.split(op)
                break
        if DEBUG:
            print "temp_q",temp_q
        if len(temp_q) == 2 and '.' in temp_q[1]:
            self.normal_two_tables_join([where_query,oper],norm_cols)
        else:
            self.special_join_with_clauses(param,oper,norm_cols)

    def special_join_with_clauses(self,query,oper,norm_cols):

        good_data={}
        join_tables=[]
        temp_cols,temp_tabs=self.get_query_tabs_cols(norm_cols)
        if DEBUG:
            print temp_tabs,temp_cols
        for q in query:
            final_exp=""
            op=""
            for op in OPERATORS:
                if op in q:
                    #temp=q.split(op)
                    op=op
                    break
            temp=q.split(op)
            final_exp+=temp[0].strip()+" "+op+" "+temp[1]
            temp=final_exp.split()
            t1,c1=self.resolve_alias(temp[0],self.query_tables)
            join_tables.append(t1)
            if DEBUG:
                print "t1,c1",t1,c1
            good_data[t1]=[]
            q=q.replace(temp[0],' '+ c1 + ' ')
            t_name=str(t1)+'.csv'
            file_data1=[]
            self.read_from_file(t_name,file_data1)
            for d in file_data1:
                exp=self.convert_string(d,t1,temp)
                if eval(exp):
                    good_data[t1].append(d)
            if DEBUG:
                print "gd",good_data[t1]
        #print good_data
        output_data = self.join_clause_data(oper,join_tables,good_data)
        self.format_output(temp_tabs,temp_cols,output_data)

    def join_clause_data(self,oper,temp_tabs,data):
        if(len(temp_tabs)!=2):
            print_error("Error:Should be Two conditions in where Statement")
        t1=temp_tabs[0]
        t2=temp_tabs[1]
        output_data=[]
        if oper == 'or':
            t1=(re.sub(' +',' ',t1)).strip()
            t_name=str(t1)+'.csv'
            file_data1=[]
            self.read_from_file(t_name,file_data1)
            t2=(re.sub(' +',' ',t2)).strip()
            t_name=str(t2)+'.csv'
            file_data2=[]
            self.read_from_file(t_name,file_data2)
            for i1 in data[t1]:
                for i2 in file_data2:
                    if i2 not in data[t2]:
                        if i1+i2 not in output_data:
                            output_data.append(i1+i2)
            for i1 in data[t2]:
                for i2 in file_data1:
                    if i2 not in data[t1]:
                        if i2+i1 not in output_data:
                            output_data.append(i2+i1)
            for i1 in data[t1]:
                for i2 in data[t2]:
                    if i1+i2 not in output_data:
                        output_data.append(i1+i2)

            return output_data
        elif oper == 'and':
            for i1 in data[t1]:
                for i2 in data[t2]:
                    if i1+i2 not in output_data:
                        output_data.append(i1+i2)
            return output_data
        else:
            output_data=[]
            t1=data.keys()[0]
            flag=False
            t2=temp_tabs[1]
            if t1==temp_tabs[1]:
                t2=temp_tabs[0]
                flag=True
            for i1 in data[t1]:
                for i2 in data[t2]:
                    if not flag:
                        output_data.append(i2+i1)
                        continue
                    output_data.append(i1+i2)
            return output_data

    def normal_two_tables_join(self,query_list,norm_cols):
        if DEBUG:
            print "func:normal_two_tables_join"
        bad_data = {}
        good_data = {}
        # print  "query_list",query_list
        for query in query_list[0]:
            query=(re.sub(' +',' ',query)).strip()
            oper=''
            temp_query=[]
            for op in OPERATORS:
                if op in query:
                    temp_query = query.split(op)
                    oper = op
                    if oper == '=':
                        oper *=2
                    break
            if len(temp_query) > 2:
                print_error("Error: use correct expression in the where condition")
            cols_in_where,tables_in_where = self.get_query_tabs_cols(temp_query)
            if DEBUG:
                print "cols_in_where,tables_in_where",cols_in_where,tables_in_where
            t1=self.query_tables[0]
            t2=self.query_tables[1]
            column_error_check(cols_in_where[t1][0],self.tables[t1],t1)
            column_error_check(cols_in_where[t2][0],self.tables[t2],t2)
            c1=self.tables[t1].index(cols_in_where[t1][0])
            c2=self.tables[t2].index(cols_in_where[t2][0])
            bad_data[query]=[]
            good_data[query]=[]
            t_name=str(t1)+'.csv'
            file_data1=[]
            self.read_from_file(t_name,file_data1)
            t_name=str(t2)+'.csv'
            file_data2=[]
            self.read_from_file(t_name,file_data2)
            for data1 in file_data1:
                for data2 in file_data2:
                    combined = data1[c1] + oper + data2[c2]
                    if eval(combined):
                        good_data[query].append(data1 + data2)
                    else:
                        bad_data[query].append(data1 + data2)
            if query_list[1] == '':
                s=[]
                for k in good_data.keys():
                    for d in good_data[k]:
                        s.append(d)
            else:
                print "UPDATE:multiple statements in join query arrived"
                #s = mix_data()
            if DEBUG:
                print "s",s
            temp_col,temp_tab=self.get_query_tabs_cols(norm_cols)
            for col in temp_col[temp_tab[1]]:
                if col in temp_col[temp_tab[0]]:
                    index=temp_col[temp_tab[0]].index(col)
                    del temp_col[temp_tab[0]][int(index)]

            if DEBUG:
                print temp_col,temp_tab
            self.format_output(temp_tab,temp_col,s)

    def format_output(self,temp_tabs,temp_cols,data):
        final_output=[]
        s=[]
        for i in range(len(temp_tabs)):
            temp=temp_tabs[i]
            for col in temp_cols[temp]:
                s.append(temp+'.'+col)
        final_output.append(s)
        for d in data:
            s=[]
            sum=0
            for i in range(len(temp_tabs)):
                temp=temp_tabs[i]
                for col in temp_cols[temp]:
                    s.append(d[self.tables[temp].index(col)+sum])
                sum+=len(self.tables[temp])
            final_output.append(s)
        if DEBUG:
            print "final_output",final_output
        self.print_output(final_output)

    def get_query_tabs_cols(self,query_list,from_special_case=1):
        query_cols={}
        query_tabs=[]
        if len(query_list) ==1 and query_list[0]=="*":
            for tab in self.query_tables:
                query_cols[tab]=[]
                for col in self.tables[tab]:
                    query_cols[tab].append(col)
            return query_cols,self.query_tables

        for q in query_list:
            t,c=self.resolve_alias(q,self.query_tables,from_special_case)
            if t not in query_cols.keys():
                query_cols[t]=[]
                query_tabs.append(t)
            query_cols[t].append(c)
        return query_cols,query_tabs

    def one_table_one_where(self,norm_cols,dis_cols,func_cols):
        if len(self.query_cols) == 1 and self.query_cols[0] == '*':
            if DEBUG:
                print "yes im in star loop"
            norm_cols = self.tables[self.query_tables[0]]
        where_query=self.condition[1]
        param=where_query
        oper=''
        if 'and' in where_query.lower():
            ind=where_query.lower().find('and')
            a=where_query[ind:ind+3]
            where_query=where_query.split(a)
            param=where_query[:]
            oper='and'
        elif 'or' in where_query.lower():
            ind=where_query.lower().find('or')
            a=where_query[ind:ind+2]
            where_query=where_query.split(a)
            param=where_query[:]
            oper='or'
        else:
            where_query=[where_query]
            param=where_query[:]
        if len(where_query) >=3 :
            print_error("Only one type of AND/OR clause should be used")
        if DEBUG:
            print "where_query",where_query

        final_exp=""
        op=""
        for q in param:
            for op in OPERATORS:
                if op in self.condition[1]:
                    op=op
                    break
            temp=q.split(op)
            final_exp+=temp[0].strip()+" "+op+" "+temp[1]
            if(param.index(q)!=len(param)-1):
               final_exp+=" "+oper+" "
        temp=final_exp.split()
        t_name=str(self.query_tables[0])+'.csv'
        file_data=[]
        self.read_from_file(t_name,file_data)
        final_output=[]
        s=[]
        for col in norm_cols:
            if '.' not in col:
                s.append(self.query_tables[0]+'.'+col)
            else :
                s.append(col)
        final_output.append(s)
        for data in file_data:
            final_string=self.convert_string(data,self.query_tables[0],temp)
            s=[]
            if eval(final_string):
                for col in norm_cols:
                        temp_table=''
                        temp_col=''
                        if '.' in col:
                            temp_table,temp_col=col.split('.')
                        else:
                            repeat_count=0
                            for tab in self.query_tables:
                                if col  in self.tables[tab]:
                                    repeat_count+=1
                                    temp_table=tab
                                    temp_col=col
                            if repeat_count > 1:
                                print_error("Given column \'" + col + "\' is ambiguos")
                            elif repeat_count == 0 :
                                print_error(" column \'"+ col + "\'doesn't exist")
                        s.append(data[self.tables[str(self.query_tables[0])].index(temp_col)])
                final_output.append(s)
            if DEBUG:
                print final_output
        self.print_output(final_output)

    def convert_string(self,data,t,cond=[]):
        s=""
        if(len(cond)==0):
            cond=self.condition[1].split()
        else:
            cond=cond
        for q in cond:
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

                s+=data[self.tables[temp_table].index(column)]
            else:
                s+=q
        return s

    def resolve_alias(self,query,tabs,from_special_case=1):
        if '.' in query:
            query=query.split('.')
            query[0]= (re.sub(' +',' ',query[0])).strip()
            query[1]= (re.sub(' +',' ',query[1])).strip()
            if DEBUG:
                print "tabs",tabs
            if query[0] not in tabs:
                print_error('Table \''+query[0]+"\'  doesn't exist in given query")
            return query[0],query[1]
        temp_table=''
        repeat_count=0
        for tab in tabs:
            if query in self.tables[tab]:
                repeat_count+=1
                temp_table=tab
        if repeat_count > 1 and from_special_case:
            print_error("Given column \'" + query + "\' is ambiguos")
        elif repeat_count == 0 and from_special_case:
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
    sql_queries=str(sys.argv[1]).split(';')
    for query in sql_queries:
        if query !='':
            print "Given_Query:",query
            print "Output:"
            p=Parser(query.strip())
            tables=p.parse_meta_data()
            p.parse_given_query()
            q=Query(p)
            q.query_process()
