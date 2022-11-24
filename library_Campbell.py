# -*- coding: utf-8 -*-
"""
@author: Eric Larmanou, Aarhus university, Denmark
various tools for manuiplating data from Campbell dataloggers
"""

import os
from glob import glob
from datetime import datetime
import pandas as pd
import numpy as np

Delimiter = ','
Settings = {}
nan = float('nan')

def GetInfo(File):
    #read header only
    
    #read the 1st row
    DF_info = pd.read_csv(File, sep=Delimiter, decimal='.', skiprows=0, header=None, nrows=1)
    if DF_info.loc[0,0] == 'TOA5': #full header
        #read the header
        DF_header = pd.read_csv(File, sep=Delimiter, decimal='.', skiprows=[0], header=0, nrows=2)
        return DF_info, DF_header, 4
    elif sum(DF_info.apply(lambda s: pd.to_numeric(s, errors='coerce').notnull().all()))/len(DF_info.columns)<0.3: # less than 30% of numeric data then we assume a 1 row header
        return None, DF_info, 1
    else: # no header
        return None, None, 0

def LoadFile(File):
    #read 1 file
    
    #read 5 rows (up to 4 header + 1 data row)
    DF_info = pd.read_csv(File, sep=Delimiter, decimal='.', skiprows=0, header=None, nrows=1, quoting=1)
    if DF_info.loc[0,0] == 'TOA5': #full header
        #read the header
        DF_header = pd.read_csv(File, sep=Delimiter, decimal='.', skiprows=[0], header=0, nrows=3)
        Field = str(DF_header.iloc[2,0])
        DF_header = DF_header.head(2)
        header = 0
        skiprows = [0,2,3]
    else:
        #list of numeric columns
        ListNumeric = DF_info.apply(lambda s: pd.to_numeric(s, errors='coerce').notnull().all())
        if (sum(ListNumeric) / len(DF_info.columns)) < 0.3: # less than 30% of numeric data then we assume a 1 row header
            DF_header = pd.read_csv(File, sep=Delimiter, decimal='.', skiprows=None, header=0, nrows=1)
            Field = str(DF_header.iloc[0,0])
            DF_header = DF_header.head(0)
            DF_info = None
            header = 0
            skiprows = 0
        else: # no header
            Field = str(DF_info.iloc[0,0])
            DF_header = None
            DF_info = None
            header = None
            skiprows = None
    
    parse_dates = [0] #could be ['TIMESTAMP'] when there is a header, but we assume it's always column 0
    index_col = 0 # same as above with 'TIMESTAMP'
    if (DF_header is not None) and (not 'TIMESTAMP' in DF_header) and (not 'timestamps' in DF_header):
        print('there is a header without TIMESTAMP, weird...')
    
    #read data from file
    dateparse = lambda x: datetime.strptime(x, FindDateFormat(Field)) # function to convert string into date
    DF_data = pd.read_csv(File, sep=Delimiter, decimal='.', skiprows=skiprows, header=header, parse_dates=parse_dates, date_parser=dateparse, index_col=index_col, na_values = ['NAN', -9999], keep_default_na = False)
    
    return DF_data, DF_info, DF_header

def FindDateFormat(Field):
    import re
    
    # we try to guess the date fromat
    if re.search("\A20\d\d\d\d\d\d\d\d\d\d\d\d\Z", Field):
        DateFromat = '%Y%m%d%H%M%S'
    elif re.search("\A20\d\d-\d\d-\d\d \d\d:\d\d:\d\d\Z", Field):
        DateFromat = '%Y-%m-%d %H:%M:%S'
    elif re.search("\A20\d\d-\d\d-\d\d \d\d:\d\d:\d\d.\d\d\d\Z", Field):
        DateFromat = '%Y-%m-%d %H:%M:%S.%f'
    else:
        DateFromat = None
        print('unrecognised date format, adapt your code buddy.')
    return DateFromat

def LoadFolder(Folder):
    #read all files matching variable Folder ('C:\' or 'C:\*.dat'...)
    #read the header of the first file
    
    Files = sorted(glob(Folder))
    return LoadFiles(Files)

def LoadFiles(Files):
    # reading in every file in the given directory - first trial: X:\Zackenberg\GeoBasis_2019\MM1\CR1000\OriginalData
    DF_data = pd.DataFrame()                   # empty dataframe for appended data
    NoHeaderYet = True
    for File in Files:
        DF_tmp, DF_info, DF_header = LoadFile(File)
        
        #handle header differences between 2 files
        if not DF_data.empty: # if not first file
            if NoHeaderYet: #no specific header in the old dataframe
                if DF_header is not None: #specific header in the new dataframe
                    DF_data.columns = DF_tmp.columns # we use the the new dataframe header
            else: #specific header in the old dataframe
                if DF_header is None: #no specific header in the new dataframe
                    DF_tmp.columns = DF_data.columns # we use the the old dataframe header
                elif not (DF_data.columns.difference(DF_tmp.columns).empty and DF_tmp.columns.difference(DF_data.columns).empty): #2 headers are different
                    #dataframes will be merged based on column names
                    print('Different headers found from file ' + File)
                    print(DF_data.columns.difference(DF_tmp.columns).to_list() + DF_tmp.columns.difference(DF_data.columns).to_list())
        # concatenate the old and new dataframes
        DF_data = pd.concat([DF_data,DF_tmp])
        if DF_header is not None:
            NoHeaderYet = False
        
    if not Files:
        DF_info = None
        DF_header = None
    
    return DF_data, DF_info, DF_header

def GetBounds(Folder):
    #return the first and last date of each data files matching variable Folder ('C:\' or 'C:\*.dat'...)
    
    import io
    Files = sorted(glob(Folder))
    
    # create DF
    DFBounds = pd.DataFrame({'DateStart':np.datetime64(),'DateEnd':np.datetime64(),'DateFile':np.datetime64(),'HeaderSize':int(),'NbColumns':int()}, index=[os.path.basename(File) for File in Files])
    #DFBounds = pd.DataFrame(index = Files, columns = ['DateStart','DateEnd','DateFile'], dtype = 'datetime64[ns]')
    DFBounds.index.name = 'File'
    dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    
    for iFile, File in enumerate(Files):
        #print(File)
        _,_,HeaderSize = GetInfo(Files[iFile])
        fid = open(File, 'rb')
        #fid = open(r'C:\MyDoc\data\backupZac\ICOS\DATA\GL-ZaF\AC_Backup\toto.dat', 'rb')
        
        Lines = ''
        #read the header + 1 data row
        for NoHeader in range(0,HeaderSize+1):
            tline = fid.readline().decode()
            Lines += tline
        # seek to end of file
        try:  # catch OSError in case of a one line file 
            fid.seek(-2, os.SEEK_END)
            while fid.read(1) != b'\n':
                fid.seek(-2, os.SEEK_CUR)
        except OSError:
            fid.seek(0)
        for tline in fid:
            pass
        Lines += tline.decode()
        fid.close()
        
        # build the function to convert string into date, using the first column of the last row to guess the format
        Field = Lines.split('\n')[-2].split(Delimiter)[0].strip('"')
        dateparse = lambda x: datetime.strptime(x, FindDateFormat(Field))
        
        # convert strings with first and last row into a temporary dataframe
        DF_tmp = pd.read_csv(io.StringIO(Lines), sep=Delimiter, decimal='.', skiprows=HeaderSize, header=None, dayfirst=True, parse_dates=[0], date_parser=dateparse, index_col=0, na_values = ['NAN'], keep_default_na = False)
        
        # feed the DF
        #DFBounds.at[File,'DateStart'] = DF_tmp.index[0]
        #DFBounds.at[File,'DateEnd'] = DF_tmp.index[1]
        #DFBounds.at[File,'DateFile'] = datetime.fromtimestamp(os.path.getmtime(File))
        #DFBounds.at[File,'HeaderSize'] = HeaderSize
        #DFBounds.at[File,'NbColumns'] = len(DF_tmp.columns)
        DFBounds.iloc[iFile,:] = [DF_tmp.index[0], DF_tmp.index[1], datetime.fromtimestamp(os.path.getmtime(File)), HeaderSize, len(DF_tmp.columns)+1]
    
    return DFBounds

def LoadHeader(FileHeader):       
    #load the header (file with 3 rows, like campbel TOA5 format without the 1st row)

    #test that the header file exists
    if os.path.exists(FileHeader):
        #load the header file
        DF_header = pd.read_csv(FileHeader, sep=Delimiter, decimal='.', skiprows=None, header=0, nrows=2)
    else:
        print('Warning: header file "' + FileHeader + '" not found, but not necessary a problem.')
        DF_header = pd.DataFrame()
        
    return DF_header

class classTable():
    def __init__(self, Logger):
        self.Id = Logger.index[0]
        Logger = Logger.loc[self.Id]
        self.Site = Logger.at['Site']
        self.Logger = Logger.at['Logger']
        self.Table = Logger.at['Table']
        self.FolderMask = Logger.at['FolderMask']
        self.Folder = os.path.join(os.path.dirname(Logger.at['FolderMask']), '')
        self.FileInventory = Logger.at['FileInventory']
        self.FileHeader = Logger.at['FileHeader']
        self.Inventory = self.LoadInventory(self.FolderMask, self.FileInventory)
        self.Header = LoadHeader(self.FileHeader)
        
    def LoadPeriod(self, DateStart, DateEnd):
        self.DateStart = DateStart
        self.DateEnd = DateEnd
        if DateStart is None and DateEnd is None: #select all data
            Filter = pd.Series(np.full(len(self.Inventory), True), index = self.Inventory.index)
        elif DateStart is None:
            Filter = self.Inventory.DateStart < DateEnd
        elif DateEnd is None:
            Filter = DateStart < self.Inventory.DateEnd
        else:
            Filter = (DateStart < self.Inventory.DateEnd) & (self.Inventory.DateStart<DateEnd)
        
        Files = (self.Folder + self.Inventory.index[Filter]).tolist()
        self.DF_data, self.DF_info, self.DF_header = LoadFiles(Files)
        if self.DF_header is None and not self.DF_data.empty:
            self.DF_data.columns = self.Header.columns[1:] # skip the first column (timestamp)
    
    def SaveInventory(self, Folder, FileInventory):
        #save the file inventory with start and end dates
        #only the pickle files are used. csv files are generated to give a human readable file
        DFBounds = GetBounds(Folder)
        DFBounds.to_pickle(FileInventory)
        DFBounds.to_csv(os.path.splitext(FileInventory)[0] + '.csv')
        return DFBounds
            
    def LoadInventory(self, Folder, FileInventory):       
        #load the file inventory with start and end dates, update it if necessary
        Update = False
        
        #test that the inventory file exists
        if os.path.exists(FileInventory):
            #load the inventory pickle file
            DFBounds = pd.read_pickle(FileInventory)
            
            #compare inventory with exisiting file
            Files = sorted(glob(Folder))
            DFFiles = pd.DataFrame(index = [os.path.basename(File) for File in Files], columns = ['DateFile'], dtype = 'datetime64[ns]')
            DFFiles.loc[:,'DateFile'] = [datetime.fromtimestamp(os.path.getmtime(File)) for File in Files]
            Update = not DFBounds[['DateFile']].equals(DFFiles)
        else:
            Update = True
            
        if Update:
            print('Scan of the folder "' + Folder + '"')
            DFBounds = self.SaveInventory(Folder, FileInventory)
        
        return DFBounds

class SiteSet():
    def __init__(self, FileLoggers):
        #load logger list
        self.Loggers = pd.read_csv(FileLoggers, sep=',', header=0, keep_default_na=False, index_col='Id')
        
        #load file lists
        self.Tables = {}
        for Id in self.Loggers.index:
            self.Tables[Id] = classTable(self.Loggers.loc[[Id]])
    
    def LoadData(self, ListId, DateStart, DateEnd):
        #load data
        for Id in ListId:
            print('Loading data from logger "' + Id + '"')
            self.Tables[Id].LoadPeriod(DateStart, DateEnd)

def PlotMM1(DF):
    import matplotlib.pyplot as plt
    # Battery voltage
    plt.figure()
    plt.plot(DF.BattV_Min)
    plt.grid(True)
    plt.suptitle('Battery voltage (V)')
    
    # NDVI
    plt.figure()
    plt.plot(DF.NDVI_Avg)
    plt.grid(True)
    plt.ylim(-1,1)
    plt.suptitle('NDVI')
    
    # Short wave radiation
    plt.figure()
    plt.plot(DF.cal_NetRad_Pyrano_Up_Avg, marker='.', linestyle='None', label='Short wave in')
    plt.plot(DF.cal_NetRad_Pyrano_Lo_Avg, marker='.', linestyle='None', label='Short wave out')
    plt.grid(True)
    plt.legend()
    plt.suptitle('Short wave radiation (W/m2)')
    
    # Long wave radiation
    plt.figure()
    plt.plot(DF.Li_cor_Avg, marker='.', linestyle='None', label='Long wave in')
    plt.plot(DF.Lu_cor_Avg, marker='.', linestyle='None', label='Long wave out')
    plt.grid(True)
    plt.legend()
    plt.suptitle('Long wave radiation (W/m2)')
    
    # CNR4 temperature
    plt.figure()
    plt.plot(DF.cal_CNR4_Temp_Avg, marker='.', linestyle='None')
    plt.grid(True)
    plt.suptitle('CNR4 temperature (deg C)')
    
    # Air temperature
    plt.figure()
    plt.plot(DF.AirTC_Avg, marker='.', linestyle='None')
    plt.grid(True)
    plt.suptitle('Air temperature (deg C)')
    
    # Relative humidity
    plt.figure()
    plt.plot(DF.RH, marker='.', linestyle='None')
    plt.grid(True)
    plt.suptitle('Relative humidity (%)') 
    
    # Snow depth
    plt.figure()
    plt.plot(DF.cal_SR50_SnowDepth_Avg/100, marker='.', linestyle='None')
    plt.grid(True)
    plt.suptitle('Snow depth (m)') 
    
    # Snow temperature temperature I
    plt.figure()
    plt.plot(DF.SnowT_120cm_Avg, marker='.', ms=0.8, linestyle='None', label='120 cm')
    plt.plot(DF.SnowT_90cm_Avg, marker='.', ms=0.8, linestyle='None', label='90 cm')
    plt.plot(DF.SnowT_60cm_Avg, marker='.', ms=0.8, linestyle='None', label='60 cm')
    plt.plot(DF.SnowT_40cm_Avg, marker='.', ms=0.8, linestyle='None', label='40 cm')
    plt.plot(DF.SnowT_20cm_Avg, marker='.', ms=0.8, linestyle='None', label='20 cm')
    plt.plot(DF.SnowT_10cm_Avg, marker='.', ms=0.8, linestyle='None', label='10 cm')
    plt.grid(True)
    plt.legend()
    plt.suptitle('SnowTemperature I')
    
    # Soil temperature
    plt.figure()
    plt.plot(DF.SoilT_2Acm_Avg, marker='.', ms=4, linestyle='None', label='-2A cm (Unstable)')
    plt.plot(DF.SoilT_2Bcm_Avg, marker='.', ms=4, linestyle='None', label='-2B cm')
    plt.plot(DF.SoilT_10cm_Avg, marker='.', ms=4, linestyle='None', label='-10 cm')
    plt.plot(DF.SoilT_20cm_Avg, marker='.', ms=4, linestyle='None', label='-20 cm')
    plt.plot(DF.SoilT_40cm_Avg, marker='.', ms=4, linestyle='None', label='-40 cm')
    plt.plot(DF.SoilT_60cm_Avg, marker='.', ms=4, linestyle='None', label='-60 cm')
    plt.grid(True)
    plt.legend()
    plt.suptitle('Soil temperature (deg C)') 
    
    # Soil heat flux
    plt.figure()
    plt.plot(DF.raw_SoilHeatFlux_Avg, marker='.', linestyle='None')
    plt.grid(True)
    plt.suptitle('Raw soil heat flux (W/m2)') 
    
    # Soil heat flux calibrated
    plt.figure()
    plt.plot(DF.cal_SoilHeatFlux_Avg, marker='.', linestyle='None')
    plt.grid(True)
    plt.suptitle('Calibrated soil heat flux (W/m2)')
    

if __name__ == "__main__":
    #FileINI = GetInputArguments()
    #ghg2bin(FileINI)
    
    FileName = r"C:\MyDoc\data\backupZac\GeoBasis_Current\M2\Originaldata\CR1000_M2_M2_data_2022-07-25b.dat"
    FileName = r"C:\MyDoc\data\backupZac\ICOS\DATA\GL-ZaF\SOIL STATION NORTH\GL_Zaf_BM_20210704_L03_F01.dat"
    Folder = "C:\\MyDoc\\data\\backupZac\\ICOS\\DATA\\GL-ZaF\\SOIL STATION SOUTH\\*.dat"
    FileInventory = "C:\\MyDoc\\data\\backupZac\\ICOS\\DATA\\GL-ZaF\\SOIL STATION SOUTH\\inventory.pkl"
    
    #DateBounds = LoadCampbell_Bounds()
    #Load(FileName)
    #DateBounds = GetBounds_n(r'C:\MyDoc\data\backupZac\ICOS\DATA\GL-ZaF\AC_Backup\GL_Zaf_BM_20210709_L06_F01.dat')
    #LoadInventory(Folder, FileInventory)
    FileLoggers = 'C:\\MyDoc\\prog\\jupyter\\zac\\loggers.csv'
    ListId = ['MM1_Met', 'MM2_Met', 'LogMM1_Met']
    ListId = ['MM2_ETC_AC_Backup1']
    DateStart = datetime.strptime('2022-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    DateEnd =  datetime.strptime('2022-05-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    Zac = SiteSet(FileLoggers)
    Zac.LoadData(ListId, DateStart, DateEnd)
    
    #Data, Variables = Load(r'C:\MyDoc\data\backupZac\GeoBasis_Current\MM1\CR1000\OriginalData\*.dat')
    PlotMM1(Zac.Tables['MM1_Met'].DF_data)
    
    