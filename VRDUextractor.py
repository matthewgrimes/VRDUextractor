import pandas as pd

import glob

import os

def parse_directories(parent_directory):
    os.chdir(parent_directory)
    # Iterate through folders in the parent directory
    directories = [directory for directory in os.listdir() if os.path.isdir(directory)]
    for directory in directories:
        # Try to find the csv and VBO file we want to operate on--note the
        # string matching
        vbox_raw_path = glob.glob(os.path.join(directory,'*VRDU*csv'))
        vbo_raw_path = glob.glob(os.path.join(directory,'*Trimmed.VBO'))
        # If we can't find both files, print the directory and continue
        if vbox_raw_path == [] or vbo_raw_path == []:
            print(f'Empty directory {directory}')
            continue
        # The paths should be lists of one element, so grab the first element
        vbox_raw_path = vbox_raw_path[0]
        vbo_raw_path = vbo_raw_path[0]
        # Initialize the writer to go to an excel file with the same name as
        # the csv, just different extenstion
        writer = pd.ExcelWriter(vbox_raw_path.replace('csv','xlsx'))
        # Call the main parser
        parse_vrdu_files(vbox_raw_path,vbo_raw_path,writer)

def parse_vrdu_files(vbox_name,vbo_name,writer):
    vbox_raw = open(vbox_name).readlines()
    vbo_raw = open(vbo_name,'rb').readlines()
    # Get last line that has 'Line' in it -- this is the start of the actual data
    start_of_data=[index for index,line in enumerate(vbox_raw) if 'Line' in line][-1]

    parsed_data = pd.DataFrame([row.split(',') for row in vbox_raw[start_of_data+1:]])
    # Append src_id to CCVS1 only
    parsed_data.loc[parsed_data[6]=='CCVS1',6]=parsed_data.loc[parsed_data[6]=='CCVS1',6]+'-Src='+parsed_data.loc[parsed_data[6]=='CCVS1',9].str[-2:].astype(int).astype(str)
    # This dictionary is how we organize which columns are wanted for each pgn
    # if columns are missing, or a pgn is missing, this is where you'd add it
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

    # These are the columns we'll grab for the VBO file
    columns_to_keep = ['time','velocity','Range-tg1','LngRsv-tg1','LatRsv-tg1','RelSpd-tg1','Spd-tg1']

    pgn_data_tables = {}

    for key in pgn_column_lookup.keys():
        # Skip keys that aren't in the data
        if key not in parsed_data[6].values.tolist():
            continue
        # To each key, assign data where
        # the pgn = key, and the columns are
        # 1 and 6 for time and pgn, then the *odd* columns defined above
        # (the even columns are the names)
        pgn_data_tables[key] = parsed_data.loc[parsed_data[6]==key,:].apply(lambda x: x[[1,6]+pgn_column_lookup[x[6]][1::2]],axis=1)
        # Assign column names based on values in the *even* columns defined in the lookup above
        pgn_data_tables[key].columns = ['time','pgn']+[parsed_data.loc[parsed_data[6]==key,column].values.tolist()[0] for column in  pgn_column_lookup[key][0::2]]

    # This is a small helper function to operate on columns of data
    # If the value in the column can be turned into a float, do that
    # if you get an error, then just leave it as is
    # Helpful for columns with strings and floats
    def safe_numeric(x):
        try:
            return float(x)
        except:
            return x
    # Now we write out pgn_data_tables to the excel worksheet
    # Under the tab CAN
    # Position the dataframes in the worksheet.
    i = 0
    for key, value in pgn_data_tables.items():
        # apply safe_numeric above to make the data numeric where possible
        for column in value.columns:
            value[column] = value[column].apply(safe_numeric)
        # Drop pgn from the data before writing it out
        value.drop('pgn',axis=1).to_excel(writer, sheet_name='CAN', startrow=7, startcol = i,index=False)
        # Add the value of the pgn above the table as a title
        value['pgn'].head(1).to_excel(writer, sheet_name='CAN', startrow=6, startcol = i,index=False,header=False)
        # Keep track of where to write the next table
        i = i + len(value.columns)
    # This is the final number of columns written out -- it's mainly used to
    # adjust the column width in the notebook, but that's broken at the moment
    can_width = i

    # Now we can work on the vbo file
    # Get the column names and data by looking for [column names] and [data] in
    # the raw file
    column_names_index =[index for index,line in enumerate(vbo_raw) if '[column names]' in str(line)][0]+1
    data_index =[index for index,line in enumerate(vbo_raw) if '[data]' in str(line)][0]+1
    # build a dataframe using the data and column names
    vbo_parsed = pd.DataFrame([str(row).split(' ') for row in vbo_raw[data_index:]], columns=str(vbo_raw[column_names_index]).split(' ')[:-1])

    # Grab only the columns we want to keep for the final data
    vbo_final = vbo_parsed[columns_to_keep]

    # Convert to numeric where possible
    for column in vbo_final.columns:
        vbo_final[column] = vbo_final[column].apply(safe_numeric)
    # the time column is in the format HHMMSS as a number -- this will not stand
    # so we grab HH and multiply by 60*60, add MM*60, then add SS to get seconds
    # since midnight
    vbo_final['time']=(vbo_final['time']//10000)*60*60 + (vbo_final['time']//100)%100*60 + vbo_final['time']%100

    # write out the final dataset to the VBOX tab with a little bit of wiggle
    # room above for notes
    vbo_final.to_excel(writer,sheet_name='VBOX', startrow=5, index=False)
    # leaving this in for now, but appears to not work with newer versions of
    # openpyxl
    # what the code *should* do is adjust the excel column widths to be a little
    # more readable
#     worksheet = writer.sheets['CAN']
#     for i in range(can_width+1):
#         worksheet.set_column(i,i,12)
#     worksheet = writer.sheets['VBOX']
#     for i in range(len(vbo_final.columns)+1):
#         worksheet.set_column(i,i,12)

    writer.save()

