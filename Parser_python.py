import pandas as pd
class Parser:
    def __init__(self):
        pass

    def email__parsing__(self):
        import os
        import pandas as pd
        import regex as re
        def specific_folder():
            in1=str(input("Enter the drive you want to clean data for : ", ))
            folder_names=[i1 for i1 in os.listdir(f"{in1}://") if '.' not in i1]
            folder_name=input(f'select a folder from this list {folder_names} : ',)
            return in1,folder_name
        csvs_dictionary={}
        drive_name,folder_name=specific_folder()
        file_direc=f'{drive_name}://{folder_name}'
        all_csv=[i2 for i2 in os.listdir(f'{drive_name}://{folder_name}') if '.csv' in i2]
        correct_format_files,not_correct_formatted,some_columns_present=[],[],[]
        dataframe_list=[]
        for file_name_csv in all_csv:
            try:
                with open(f'{file_direc}//{file_name_csv}','r+',encoding='cp437') as f:
                    column_names=[x1.lower() for x1 in (f.readlines()[0]).split(",")]
                    print(f'checking format for the file : {file_name_csv}')
                    check_header_present=[i1 for i1 in re.findall(r'[Ff]*[A-Za-z]*[Nn][Aa][Mm][Ee]|[Aa]+[dD][A-Za-z]+[sS]',','.join(column_names))]
                    # to check whether first row has header or not.
                    if len(check_header_present)>0:
                        correct_format_files.append(file_name_csv)
                        #column_names_extract=re.findall(r'[fF]*[A-Za-z]*ame|[Aa]*d[A-Za-z]+s|[Ll]*[A-Za-z]*ame|(P[A-Za-z]|phone) \
                        #([ number]| )|D[ A-Za-z]*o[A-Za-z ]*[Bb]*[A-Za-z]*|',','.join(column_names))
                        names_columns=[i1 for i1 in re.findall(r'[FLfl]+[irstast_]*[Nn][Aa][Mm][Ee]',','.join(column_names)) ]
                        if names_columns==[]:
                            names_columns=re.findall(r'[Nn][Aa][Mm][Ee]',','.join(column_names))
                        address_columns=list(set([i1 for i1 in re.findall(r'[A-Za-z_]*[Aa]+[dD][A-Za-z]+[Ss][012]*',','.join(column_names)) if len(i1)>=6]))
                        if len(address_columns)>=2:
                            address_columns=[i4 for i4 in [i3 for i3 in address_columns if 'ip' not in i3]if 'email' not in i4]
                            if address_columns==['address1','address2']:
                                address_columns=['address1']
                            elif address_columns==['address','address2'] or address_columns==['address2','address']:
                                address_columns=['address']
                            else :
                                 address_columns=[address_columns[0]]
                            #add=list(filter(sorted(list(map(lambda x:(x,len(x)),address_columns)),key=lambda x: x[1])))
                            #print(add)
                        #if len(address_columns)>=3:
                         #   address_columns=re.findall(r'(^m|^[Aa])[ Aa]+[Dd]+[A-Za-z]+[012]+',','.join([i1 for i1 in column_names if 'address' in i1]))
                        email_columns=[i1 for i1 in re.findall(r'[A-Za-z]+@.[com]+|[Ee]+mail[address]*',','.join(column_names)) if len(i1)>4]                        
                        cell_phone=list(set([i1 for i1 in re.findall(r'[Cc]+[eE][Ll][Ll]|[Pp]+[honeHONE]+',','.join(column_names)) if len(i1)>=4 ]))
                        cell_=[]
                        if len(cell_phone)>=2:
                            for i1 in column_names:
                                for i2 in cell_phone:
                                    if i1==i2:
                                        cell_.append(i1)
                            if cell_==[]:
                                cell_=[i1 for i1 in column_names if 'cell' in i1]
                            cell_phone=cell_
                        dob_=[i1 for i1 in re.findall(r'[dD][A-Za-z _]*[oO][ _A-Za-z]*[ bB][A-Za-z]*',','.join(column_names))]
                        if dob_==[]:
                            dob_=[x1 for x1 in column_names if 'birth' in x1]
                        columns_string=f'{names_columns},{address_columns},{email_columns},{cell_phone},{dob_}'
                        columns_extracted=[i1.strip()[1:-1] for i1 in re.sub('\'','.',re.sub('\[','',re.sub(r'\]','',columns_string))).split(',')]
                        columns_extracted=[i1 for i1 in columns_extracted if i1!='']
                        #columns_string=','.join(list(filter(lambda x : '',columns_extracted)))
                        print('------------------------------------------------------------------\n')
                        print('                                                                    ')
                        print(f'Original columns were {column_names}')
                        print(f'Current csv {file_name_csv} extracted columns {columns_extracted}')
                        csvs_dictionary_p={}
                        if dob_==[] or len(columns_extracted)!=6:#len(re.sub('\'','',re.sub('\[','',re.sub(r'\]','',columns_string))).split(','))!=6:
                            print(f'The {file_name_csv} CSV file has different a correct column.Missing a key column..')
                            #some_columns_present.append(file_name_csv)
                            #string_columns=re.sub('\'','',re.sub('\[','',re.sub(r'\]','',columns_string))).split(',')
                            with open(f'G://file_records_diff_formatted {folder_name}.txt','a+') as f:
                                f.write(f"{file_name_csv} : {columns_extracted}\n")
                            csvs_dictionary_p[file_name_csv]=columns_extracted
                            continue
                        #where the number of columns are equal to 6
                        elif dob_!=[] and len(columns_extracted)==6:#len(re.sub('\'','',re.sub('\[','',re.sub(r'\]','',columns_string))).split(','))==6:

                            print(f"Correct format for {file_name_csv}")
                            string_columns_ideal=[i1.strip() for i1 in re.sub('\'','',re.sub('\[','',re.sub(r'\]','',columns_string))).split(',')]
                            #print(f'original Columns are {column_names}')
                            #print(f'Extracted columns are {string_columns_ideal}')
                            csvs_dictionary[f'{file_name_csv}']=columns_extracted
                            with open(f'G://file_records_{folder_name}.txt','a+') as f:
                                f.write(f"{file_name_csv} : {columns_extracted} \n")
                            '''dataframe=pd.read_csv(f'{file_direc}//{file_name_csv}',names=string_columns_ideal,encoding='cp437',dtype=[str,str,str,str,str,str])
                            dataframe_list.extend(dataframe.values.tolist())'''
                            
                    elif check_header_present<=0:
                        not_correct_formatted.append(file_name_csv)
                print('------------------------------------------------------------------\n')
                print('                                                                    ')            
            except:  
                pass    

        print("---Found all Formatted Files")
        return csvs_dictionary,file_direc
    '''def format_files_now(self,data_dictionary,directory):
        print("Now Converting to One")
        col_names=list(data_dictionary.values())[0]
        data_=[]
        temporary_dataframe=pd.DataFrame(columns=col_names)
        for key,value in data_dictionary.items():
            df1=pd.read_csv(f'{directory}//{key}',usecols=value,encoding='cp437',on_bad_lines='skip')
            data_.append(df1)
        temporary_dataframe=pd.concat(data_)
        temporary_dataframe.to_csv(f'{directory}//result_parser1.csv')'''

email_parser=Parser()
data_dictionary,directory_information=email_parser.email__parsing__()
#email_parser.format_files_now(data_dictionary,directory_information)
