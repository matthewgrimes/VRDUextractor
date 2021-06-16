
# coding: utf-8

# In[5]:

import pandas as pd

import glob

import os

def parse_directories(parent_directory):
    os.chdir(parent_directory)
    directories = [directory for directory in os.listdir() if os.path.isdir(directory)]
    for directory in directories:
        vbox_raw_path = glob.glob(os.path.join(directory,'*VRDU*csv'))
        vbo_raw_path = glob.glob(os.path.join(directory,'*Trimmed.VBO'))
        if vbox_raw_path == [] or vbo_raw_path == []:
            print(f'Empty directory {directory}')
            continue
        vbox_raw_path = vbox_raw_path[0]
        vbo_raw_path = vbo_raw_path[0]
        writer = pd.ExcelWriter(vbox_raw_path.replace('csv','xlsx'))
        parse_vrdu_files(vbox_raw_path,vbo_raw_path,writer)

def parse_vrdu_files(vbox_name,vbo_name,writer):
    vbox_raw = open(vbox_name).readlines()
    vbo_raw = open(vbo_name,'rb').readlines()
    # Get last line that has 'Line' in it -- this is the start of the actual data
    start_of_data=[index for index,line in enumerate(vbox_raw) if 'Line' in line][-1]

    parsed_data = pd.DataFrame([row.split(',') for row in vbox_raw[start_of_data+1:]])
    # Append src_id to CCVS1 only
    parsed_data.loc[parsed_data[6]=='CCVS1',6]=            parsed_data.loc[parsed_data[6]=='CCVS1',6]+'-Src='+parsed_data.loc[parsed_data[6]=='CCVS1',9].str[-2:].astype(int).astype(str)
    pgn_column_lookup={
        'EEC1':[],
        'VD':[],
        'EBC5':[],
        'VDC2':[],
        'VBOX3i_0x301':[24,25],
        'VBOX3i_0x302':[],
        'VBOX3i_0x303':[],
        'EEC2':[],
        'CCVS1':[],
        'XBR':[],
        'CCVS1-Src=0':[60,61],
        'ACC1':[22,23,26,27,34,35,36,37],
        'HOURS':[]
     }
    # check that lookup keys match data values
    if set(pgn_column_lookup.keys()) != set(parsed_data[6].values.tolist()):
        print("The lookup table doesn't match the values in the data!")
        print('In lookup but not data:')
        print(set(pgn_column_lookup.keys())-set(parsed_data[6].values.tolist()))
        print('In data but not lookup:')
        print(set(parsed_data[6].values.tolist())-set(pgn_column_lookup.keys()))
    
    columns_to_keep = ['time','velocity','Range-tg1','LngRsv-tg1','LatRsv-tg1','RelSpd-tg1','Spd-tg1']

    pgn_data_tables = {}

    for key in pgn_column_lookup.keys():
        if key not in parsed_data[6].values.tolist():
            continue
        pgn_data_tables[key] = parsed_data.loc[parsed_data[6]==key,:]                                .apply(lambda x: x[[1,6]+pgn_column_lookup[x[6]][1::2]],axis=1)
        pgn_data_tables[key].columns = ['time','pgn']+[parsed_data.loc[parsed_data[6]==key,column].values.tolist()[0] for column in  pgn_column_lookup[key][0::2]]

    def safe_numeric(x):
        try:
            return float(x)
        except:
            return x

    # Position the dataframes in the worksheet.
    i = 0
    for key, value in pgn_data_tables.items():
        for column in value.columns:
            value[column] = value[column].apply(safe_numeric)
        value.drop('pgn',axis=1).to_excel(writer, sheet_name='CAN', startrow=7, startcol = i,index=False)
        value['pgn'].head(1).to_excel(writer, sheet_name='CAN', startrow=6, startcol = i,index=False,header=False)
        i = i + len(value.columns)
    can_width = i

    column_names_index =[index for index,line in enumerate(vbo_raw) if '[column names]' in str(line)][0]+1
    data_index =[index for index,line in enumerate(vbo_raw) if '[data]' in str(line)][0]+1

    vbo_parsed = pd.DataFrame([str(row).split(' ') for row in vbo_raw[data_index:]], columns=str(vbo_raw[column_names_index]).split(' ')[:-1])

    vbo_final = vbo_parsed[columns_to_keep].astype(float)

    for column in vbo_final.columns:
        vbo_final[column] = vbo_final[column].apply(safe_numeric)

    vbo_final['time']=(vbo_final['time']//10000)*60*60 + (vbo_final['time']//100)%100*60 + vbo_final['time']%100

    vbo_final.to_excel(writer,sheet_name='VBOX',index=False)

#     worksheet = writer.sheets['CAN']
#     for i in range(can_width+1):
#         worksheet.set_column(i,i,12)
#     worksheet = writer.sheets['VBOX']
#     for i in range(len(vbo_final.columns)+1):
#         worksheet.set_column(i,i,12)

    writer.save()

