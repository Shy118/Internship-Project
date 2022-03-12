"""
@author: Shy118
@IP: GlobalFoundries Singapore
"""

import warnings
warnings.filterwarnings("ignore")
import traceback, sys, os
from PyQt5 import QtWidgets as qtw
from PyQt5 import QtCore, QtGui
from PyQt5 import QtWebEngineWidgets
from PyQt5.QtGui import QColor, QIcon, QPixmap, QImage, QFont
from PyQt5.QtCore import QRect, Qt, QObject, pyqtSignal, QThread, QThreadPool, QRunnable, QTimer, QDate, QSize, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import (QApplication, QMainWindow, 
                             QLabel, QPushButton, QButtonGroup, QWidget, QTextEdit, QFileDialog, QFrame,
                             QStackedLayout, QListWidget, QCheckBox, QSizePolicy, QScrollArea,
                             QVBoxLayout, QHBoxLayout, QStackedWidget, QTableWidget, QTableWidgetItem,
                             QGridLayout, QComboBox, QProgressBar, QSlider, QToolButton, QStyle,
                             QFormLayout, QTabWidget, QLineEdit, QDateEdit)
from qtrangeslider import QLabeledDoubleRangeSlider


import pathlib
import shutil
from datetime import datetime
import time
from time import sleep
import random
import mysql.connector
import pandas as pd
from pandas import ExcelWriter
import numpy as np
import scipy as sp
import math
import csv
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import seaborn as sns
import plotly.offline
import plotly.graph_objs as go
import plotly.express as px
import plotly.tools as tls
from skimage import io

class MySQL_Query(QObject):
    # Class consisting of all SQL query related fucntions including 1) Updating UI 2) Acquring data for PCA + Data Query
    def __init__(self, query1, query2, system_log, progress):    
        super(MySQL_Query, self).__init__()
        self.query1 = query1
        self.query2 = query2
        self.system_log = system_log
        self.progress = progress

        # Insert database credential
        try:
            self.dbconfig = mysql.connector.connect(host = '',
                    user = '',
                    password = '',
                    database = '',                     
                    charset='',
                    use_unicode=True)
    
            if self.dbconfig.is_connected():
                self.db_Info = self.dbconfig.get_server_info()
                self.cursor = self.dbconfig.cursor(buffered= True)
                self.cursor.execute("select database();")
                self.record = self.cursor.fetchone()
            
        except:
            self.system_log.emit("Error while connecting to MySQL")      
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dbconfig.close()
        
    def query_all(self, x):
        self.cursor.execute(x)
        all_data = self.cursor.fetchall()
        return all_data
        
    def query_one(self, x):
        self.cursor.execute(x)
        one_data = self.cursor.fetchone()
        return one_data
    
    def query_headers(self, x):
        # Query function to acquire SQL data table column names
        self.cursor.execute(x)
        header_names = [i[0] for i in self.cursor.description]
        return header_names      
        
    def df_et_wmap_raw(self):
        # Query functiion to acquire ET data consisting of only 'LT' according to the product specified + Selected Lots
        self.system_log.emit(f"You're connected to database: {self.record}")
        self.Product_No = self.query1[0]
        self.Lot_No = self.query1[1]
        self.Test_Type = self.query1[2]
        self.Bin = self.query1[3]
        self.Class = self.query1[4]
        if type(self.Lot_No) == tuple:
            querying_et_wmap_raw = f" SELECT et_param_raw.Product, et_param_raw.Lot_No, et_param_raw.Wafer_Alias, et_param_raw.Scribe, \
                                    et_param_raw.Test_ID, et_param_spec.Param_Name, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                    AND et_param_raw.Product = et_param_spec.Product \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product_No}' \
                                    AND et_param_raw.Lot_No IN {self.Lot_No} AND et_param_spec.Test_Layer = 'LT' \
                                    ORDER BY Test_ID"                  
                
            querying_et_param = f" SELECT DISTINCT et_param_spec.Param_Name, count(et_param_spec.Param_Name) \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID  \
                                    AND et_param_raw.Product = et_param_spec.Product  \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product_No}' \
                                    AND et_param_raw.Lot_No IN {self.Lot_No} \
                                    AND et_param_spec.Test_Layer = 'LT' \
                                    GROUP BY et_param_spec.Param_Name \
                                    ORDER BY et_param_raw.Test_ID"
            
        else:
            querying_et_wmap_raw = f" SELECT et_param_raw.Product, et_param_raw.Lot_No, et_param_raw.Wafer_Alias, et_param_raw.Scribe, \
                                    et_param_raw.Test_ID, et_param_spec.Param_Name, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                    AND et_param_raw.Product = et_param_spec.Product \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product_No}' \
                                    AND et_param_raw.Lot_No = '{self.Lot_No}' AND et_param_spec.Test_Layer = 'LT' \
                                    ORDER BY Test_ID"                  
                
            querying_et_param = f" SELECT DISTINCT et_param_spec.Param_Name, count(et_param_spec.Param_Name) \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID  \
                                    AND et_param_raw.Product = et_param_spec.Product  \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product_No}' \
                                    AND et_param_raw.Lot_No = '{self.Lot_No}' \
                                    AND et_param_spec.Test_Layer = 'LT' \
                                    GROUP BY et_param_spec.Param_Name \
                                    ORDER BY et_param_raw.Test_ID"

        self.system_log.emit("Fetching data...")
        et_wmap_raw = self.query_all(querying_et_wmap_raw)
        df_et_wmap_raw_header_names = self.query_headers(querying_et_wmap_raw)
        self.df_et_wmap_raw = pd.DataFrame(et_wmap_raw, columns = df_et_wmap_raw_header_names)
        self.param_name = self.query_all(querying_et_param)
        return self.df_et_wmap_raw, self.param_name
    
    def wafer_bin(self):   
        # Query functiion to acquire Bin data according to product specified + Selected Lot + Test Type + Bin list
        if type(self.Bin) == tuple and type(self.Lot_No) == tuple:
            querying_bin_count = f" SELECT sort_wafer_bins.Product, sort_wafer_bins.Lot_No, sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, \
                                    sort_wafer_bins.Measure, (SUM(sort_wafer_bins.Bin_Count)/product_info.GDPW)*100 AS Bin_Percentage \
                                    FROM sort_wafer_bins \
                                    LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                    WHERE sort_wafer_bins.Product LIKE '{self.Product_No}' \
                                    AND sort_wafer_bins.Lot_No IN {self.Lot_No} \
                                    AND sort_wafer_bins.Test_Type = '{self.Test_Type}' \
                                    AND sort_wafer_bins.Bin IN {self.Bin}\
                                    GROUP BY sort_wafer_bins.Scribe"
        
        elif type(self.Lot_No) == tuple and type(self.Bin) != tuple:
            querying_bin_count = f" SELECT sort_wafer_bins.Product, sort_wafer_bins.Lot_No, sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, \
                                    sort_wafer_bins.Measure, (SUM(sort_wafer_bins.Bin_Count)/product_info.GDPW)*100 AS Bin_Percentage \
                                    FROM sort_wafer_bins \
                                    LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                    WHERE sort_wafer_bins.Product LIKE '{self.Product_No}' \
                                    AND sort_wafer_bins.Lot_No IN {self.Lot_No} \
                                    AND sort_wafer_bins.Test_Type = '{self.Test_Type}' \
                                    AND sort_wafer_bins.Bin = '{self.Bin}'\
                                    GROUP BY sort_wafer_bins.Scribe"
                                
        elif type(self.Bin) == tuple and type(self.Lot_No) != tuple:
            querying_bin_count = f" SELECT sort_wafer_bins.Product, sort_wafer_bins.Lot_No, sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, \
                                    sort_wafer_bins.Measure, (SUM(sort_wafer_bins.Bin_Count)/product_info.GDPW)*100 AS Bin_Percentage \
                                    FROM sort_wafer_bins \
                                    LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                    WHERE sort_wafer_bins.Product LIKE '{self.Product_No}' \
                                    AND sort_wafer_bins.Lot_No = '{self.Lot_No}' \
                                    AND sort_wafer_bins.Test_Type = '{self.Test_Type}' \
                                    AND sort_wafer_bins.Bin IN {self.Bin}\
                                    GROUP BY sort_wafer_bins.Scribe"
                                
        else:
            querying_bin_count = f" SELECT sort_wafer_bins.Product, sort_wafer_bins.Lot_No, sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, \
                                    sort_wafer_bins.Measure, (SUM(sort_wafer_bins.Bin_Count)/product_info.GDPW)*100 AS Bin_Percentage \
                                    FROM sort_wafer_bins \
                                    LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                    WHERE sort_wafer_bins.Product LIKE '{self.Product_No}' \
                                    AND sort_wafer_bins.Lot_No = '{self.Lot_No}' \
                                    AND sort_wafer_bins.Test_Type = '{self.Test_Type}' \
                                    AND sort_wafer_bins.Bin = '{self.Bin}'\
                                    GROUP BY sort_wafer_bins.Scribe"     
                                 
        wafer_classification = []
        wafer_bin = self.query_all(querying_bin_count)
        df_wafer_bin_header_names = self.query_headers(querying_bin_count)
        self.df_wafer_bin = pd.DataFrame(wafer_bin, columns = df_wafer_bin_header_names)
        wafer_bin_list = self.df_wafer_bin['Bin_Percentage'].tolist()
        for n in range(len(wafer_bin_list)):
            if wafer_bin_list[n] < self.Class[0]:
                wafer_classification.append('Good')
            elif wafer_bin_list[n] >= self.Class[0] and wafer_bin_list[n] <= self.Class[1]:
                wafer_classification.append('Mild')
            else:
                wafer_classification.append('Bad')
        self.df_wafer_bin['Classification'] = wafer_classification
        return self.df_wafer_bin
    
    def wafer_yield(self):
        # Query functiion to acquire wafer sort yield according to product specified + Selected Lots + Test Type
        # Queried wafer sort yield data is then classified into Good/Mild/Bad wafers defined by user
        if type(self.Lot_No) == tuple :
            querying_yield = f" SELECT Product, Lot_No, Wafer_Alias, Scribe, Measure, (Good_Die/Tested_Die)*100 AS Yield \
                                FROM sort_wafer_raw \
                                WHERE Product LIKE '{self.Product_No}' \
                                AND Lot_No IN {self.Lot_No} \
                                AND Test_Type = '{self.Test_Type}'\
                                ORDER BY Measure DESC"

        else:
            querying_yield = f" SELECT Product, Lot_No, Wafer_Alias, Scribe, Measure, (Good_Die/Tested_Die)*100 AS Yield FROM sort_wafer_raw \
                                WHERE Product LIKE '{self.Product_No}' \
                                AND Lot_No = '{self.Lot_No}' \
                                AND Test_Type = '{self.Test_Type}'\
                                ORDER BY Measure DESC"
                                
        wafer_classification = []
        wafer_yield = self.query_all(querying_yield)
        df_wafer_yield_header_names = self.query_headers(querying_yield)
        self.df_wafer_yield = pd.DataFrame(wafer_yield, columns = df_wafer_yield_header_names)
        self.df_wafer_yield.drop_duplicates(subset=['Product','Lot_No','Wafer_Alias','Scribe'], inplace = True)
        self.df_wafer_yield.reset_index(drop = True, inplace = True)
        
        wafer_yield_list = self.df_wafer_yield['Yield'].tolist()
        for n in range(len(wafer_yield_list)):
            if wafer_yield_list[n] < self.Class[0]:
                wafer_classification.append('Bad')
            elif wafer_yield_list[n] >= self.Class[0] and wafer_yield_list[n] <= self.Class[1]:
                wafer_classification.append('Mild')
            else:
                wafer_classification.append('Good')
        self.df_wafer_yield['Classification'] = wafer_classification
        return self.df_wafer_yield
    
    def df_et_wmap_raw_import(self):
        # Query functiion to acquire ET data consisting of only 'LT' according to imported list of wafer Scribe
        # Called only when the import option is checked
        self.system_log.emit(f"You're connected to database: {self.record}")
        self.Scribe = self.query1
        querying_et_wmap_raw = f" SELECT et_param_raw.Product, et_param_raw.Lot_No, et_param_raw.Wafer_Alias, et_param_raw.Scribe, \
                                et_param_raw.Test_ID, et_param_spec.Param_Name, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                FROM et_param_raw \
                                LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                AND et_param_raw.Product = et_param_spec.Product \
                                AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                WHERE et_param_raw.Scribe IN {self.Scribe} \
                                AND et_param_spec.Test_Layer = 'LT' \
                                ORDER BY et_param_raw.Test_ID"                  
            
        querying_et_param = f" SELECT DISTINCT et_param_spec.Param_Name, count(et_param_spec.Param_Name) \
                                FROM et_param_raw \
                                LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID  \
                                AND et_param_raw.Product = et_param_spec.Product  \
                                AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                WHERE et_param_raw.Scribe IN {self.Scribe} \
                                AND et_param_spec.Test_Layer = 'LT' \
                                GROUP BY et_param_spec.Param_Name \
                                ORDER BY et_param_raw.Test_ID"
            

        self.system_log.emit("Fetching data...")
        et_wmap_raw = self.query_all(querying_et_wmap_raw)
        df_et_wmap_raw_header_names = self.query_headers(querying_et_wmap_raw)
        self.df_et_wmap_raw = pd.DataFrame(et_wmap_raw, columns = df_et_wmap_raw_header_names)
        self.param_name = self.query_all(querying_et_param)
        return self.df_et_wmap_raw, self.param_name

    def data_specs(self, data_preparation):
        # Query functiion to specifically acquire parameter names of ET data consisting of only 'LT' according to the product specified
        self.system_log.emit("Assigning parameters...")
        self.data_specs_headers = data_preparation
        self.param_name = []
        for row in range(len(self.data_specs_headers)):
            querying_param_name = f" SELECT Param_Name FROM et_param_spec WHERE Product LIKE '{self.Product_No}' AND Test_ID = {self.data_specs_headers[row][0]}"
            param_name = self.query_one(querying_param_name)
            self.param_name.append(param_name[0])         
            progression = np.round((row/len(self.data_specs_headers))*100, decimals = 2)
            self.progress.emit(progression)
        return self.param_name
    
    def query_tech(self):
        # Query function to update 'Tech' dropbox on UI for both PCA & Data Query Selection Menu
        # Called automacially upon launching of application
        self.system_log.emit("Fetching tech...")
        querying_filter = "SELECT DISTINCT Tech FROM product_info ORDER BY Tech + 2"
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i][0] for i in range(len(filtered_result)))
        self.system_log.emit("Fetching tech completed")
        return filtered_result_list
   
    def query_process_tech_category(self):
        # Query function to update 'Process' dropbox on UI for both PCA & Data Query Selection Menu
        # Called automacially upon selection of 'Tech'
        self.system_log.emit("Fetching process tech category...")
        querying_filter = f" SELECT DISTINCT Tech2 FROM fab7ye.lot_info WHERE Tech = '{self.query1}' ORDER BY Tech2;"
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i][0] for i in range(len(filtered_result)))
        self.system_log.emit("Fetching process tech category completed")
        return filtered_result_list
        
    def query_product(self):
        # Query function to update 'Product' dropbox on UI for both PCA & Data Query Selection Menu
        # Called automacially upon selection of 'Process'
        self.system_log.emit("Fetching product...")
        querying_filter = f" SELECT DISTINCT Product FROM fab7ye.lot_info WHERE Tech2 = '{self.query1}' ORDER BY Product;"
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i][0] for i in range(len(filtered_result)))
        self.system_log.emit("Fetching product completed")
        return filtered_result_list
    
    def query_test(self):
        # Query function to update 'Test Type' dropbox on UI for both PCA & Data Query Selection Menu
        # Called automacially upon selection of 'Product'
        self.system_log.emit("Fetching test type...")
        querying_filter = f" SELECT DISTINCT Test_Type FROM sort_wafer_bins WHERE Product LIKE '{self.query1}' ORDER BY length(Test_Type), Test_Type"
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i][0] for i in range(len(filtered_result)))
        self.system_log.emit("Fetching test type completed")
        return filtered_result_list
    
    def query_bin(self):
        # Query function to acqurie the list of Bin information according to the product specified + Selected Test Type
        # Aquired information will be updated onto the 'Bin List' Selection of both the PCA & Data Query Selection Menu UI
        # Called automacially upon selection of 'Test Type' or by checking the 'Bin' Checkbox after Lot Selection
        self.system_log.emit("Fetching bin...")
        querying_filter = f" SELECT DISTINCT sort_wafer_bins.Bin, sort_bin_def_raw.Flag AS Type, sort_wafer_bins.Bin_Def AS Description \
                            FROM sort_bin_def_raw \
                            LEFT JOIN sort_wafer_bins ON sort_wafer_bins.Product = sort_bin_def_raw.Product \
                            AND sort_wafer_bins.Bin = sort_bin_def_raw.Bin \
                            AND sort_wafer_bins.Bin_Def = sort_bin_def_raw.Bin_Def \
                            WHERE sort_wafer_bins.Product LIKE '{self.query1}' \
                            AND sort_wafer_bins.Test_Type = '{self.query2[0]}' \
                            ORDER BY length(sort_wafer_bins.Bin), Bin ASC"
                            
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i] for i in range(len(filtered_result)))
        filtered_result_header_names = self.query_headers(querying_filter)
        df_filtered_result_list = pd.DataFrame(filtered_result_list, columns = filtered_result_header_names)
        self.system_log.emit("Fetching bin completed")
        return df_filtered_result_list
    
    def query_filterbin(self):
        # Query function to acqurie the specifc Bin information according to the product specified + Selected Test Type + filter by Bin No.
        # Aquired information will be updated onto the 'Bin List' Selection of both the PCA & Data Query Selection Menu UI
        # Called upon hitting enter after specifying Bin No. in the Checked Bin 'Filter By' Text Box
        self.system_log.emit("Fetching bin...")
        querying_filter = f" SELECT DISTINCT sort_wafer_bins.Bin, sort_bin_def_raw.Flag AS Type, sort_wafer_bins.Bin_Def AS Description \
                            FROM sort_bin_def_raw \
                            LEFT JOIN sort_wafer_bins ON sort_wafer_bins.Product = sort_bin_def_raw.Product \
                            AND sort_wafer_bins.Bin = sort_bin_def_raw.Bin \
                            AND sort_wafer_bins.Bin_Def = sort_bin_def_raw.Bin_Def \
                            WHERE sort_wafer_bins.Product LIKE '{self.query1}' \
                            AND sort_wafer_bins.Test_Type = '{self.query2[0]}' \
                            AND sort_wafer_bins.Bin LIKE '{self.query2[1]}' \
                            ORDER BY length(sort_wafer_bins.Bin), Bin ASC"
                            
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i] for i in range(len(filtered_result)))
        filtered_result_header_names = self.query_headers(querying_filter)
        df_filtered_result_list = pd.DataFrame(filtered_result_list, columns = filtered_result_header_names)
        self.system_log.emit("Fetching bin completed")
        return df_filtered_result_list
    
    def query_lot(self):
        # Query function to acqurie list of Lot information according to the product specified + Selected Test Type sorted by latest Measure Date
        # Aquired information will be updated onto the 'Lot List' Selection of both the PCA & Data Query Selection Menu UI
        # Called automacially upon selection of 'Test Type'
        self.system_log.emit("Fetching lot...")
        querying_filter = f" SELECT sort_lot_rp1.Lot_No, sort_lot_rp1.Lot_Grade, \
                            DATE(sort_lot_rp1.Measure) AS Measure, DATE(lot_fabout.Fabout) AS Fabout \
                            FROM sort_lot_rp1 LEFT JOIN lot_fabout \
                            ON sort_lot_rp1.Lot_No = lot_fabout.Lot_No \
                            WHERE sort_lot_rp1.Product LIKE '{self.query1}' \
                            AND sort_lot_rp1.Test_Type = '{self.query2[0]}' \
                            ORDER BY sort_lot_rp1.Measure DESC"
                            
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i] for i in range(len(filtered_result)))
        filtered_result_header_names = self.query_headers(querying_filter)
        df_filtered_result_list = pd.DataFrame(filtered_result_list, columns = filtered_result_header_names)
        self.system_log.emit("Fetching lot completed")
        return df_filtered_result_list
    
    def query_lot_time(self):
        # Query function to acqurie list of Lot information according to the product specified + Selected Test Type + Date Range sorted by latest Measure Date
        # Aquired information will be updated onto the 'Lot List' Selection of both the PCA & Data Query Selection Menu UI
        # Called automacially upon clicking onto the 'search' button under the 'Time Frame' selection
        self.system_log.emit("Fetching lot...")
        querying_filter = f" SELECT sort_lot_rp1.Lot_No, sort_lot_rp1.Lot_Grade, \
                            DATE(sort_lot_rp1.Measure) AS Measure, DATE(lot_fabout.Fabout) AS Fabout \
                            FROM sort_lot_rp1 LEFT JOIN lot_fabout \
                            ON sort_lot_rp1.Lot_No = lot_fabout.Lot_No\
                            WHERE sort_lot_rp1.Product LIKE '{self.query1[1]}' \
                            AND sort_lot_rp1.Test_Type = '{self.query1[0]}' \
                            AND sort_lot_rp1.Measure BETWEEN '{self.query2[0]}%' AND '{self.query2[1]}%' \
                            ORDER BY sort_lot_rp1.Measure DESC"
                            
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i] for i in range(len(filtered_result)))
        filtered_result_header_names = self.query_headers(querying_filter)
        df_filtered_result_list = pd.DataFrame(filtered_result_list, columns = filtered_result_header_names)
        self.system_log.emit("Fetching lot completed")
        return df_filtered_result_list
    
    def query_step(self):
        # Query function to acqurie list of Step data information according to the Selected Lot List
        # Aquired information will be updated onto the 'Tool' Selection under the 'Group By' section of PCA Selection Menu
        # Called automacially upon checking the 'Tool' Selection
        self.system_log.emit("Fetching step...")
        if type(self.query1) == tuple:
            querying_filter = f" SELECT DISTINCT wip_wc_proc_process_end.Step, wip_lot_ope_process.Step_Name \
                                FROM wip_wc_proc_process_end \
                                LEFT JOIN wip_lot_ope_process ON wip_wc_proc_process_end.Lot_No = wip_lot_ope_process.Lot_No \
                                AND wip_wc_proc_process_end.Step = wip_lot_ope_process.Step \
                                WHERE wip_wc_proc_process_end.Lot_No IN {self.query1} \
                                ORDER BY wip_wc_proc_process_end.Step"
        else:
            querying_filter = f" SELECT DISTINCT wip_wc_proc_process_end.Step, wip_lot_ope_process.Step_Name \
                                FROM wip_wc_proc_process_end \
                                LEFT JOIN wip_lot_ope_process ON wip_wc_proc_process_end.Lot_No = wip_lot_ope_process.Lot_No \
                                AND wip_wc_proc_process_end.Step = wip_lot_ope_process.Step \
                                WHERE wip_wc_proc_process_end.Lot_No = '{self.query1}' \
                                ORDER BY wip_wc_proc_process_end.Step"
                                
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i] for i in range(len(filtered_result)))
        filtered_result_header_names = self.query_headers(querying_filter)
        df_filtered_result_list = pd.DataFrame(filtered_result_list, columns = filtered_result_header_names)
        self.system_log.emit("Fetching step completed")
        return df_filtered_result_list
       
    def query_tool(self):
        # Query function to acqurie Tool & Chamber data according to the Selected Lot list + Step 
        # Aquired data will be used to classify wafers based on Tool & Chamber used to be shown on the PCA Biplot
        # Called automacially upon 'Step' Selection
        self.system_log.emit("Fetching tool...")
        if type(self.query1) == tuple:
            querying_filter = f" SELECT Scribe, Step, Equipment AS Tool, Chamber \
                                 FROM wip_wc_proc_process_end \
                                 WHERE Lot_No IN {self.query1} \
                                 AND Step = '{self.query2}'"
        else:
            querying_filter = f" SELECT Scribe, Step, Equipment AS Tool, Chamber \
                                 FROM wip_wc_proc_process_end \
                                 WHERE Lot_No = '{self.query1}' \
                                 AND Step = '{self.query2}'"
                                 
        filtered_result = self.query_all(querying_filter)
        filtered_result_list = list(filtered_result[i] for i in range(len(filtered_result)))
        filtered_result_header_names = self.query_headers(querying_filter)
        df_filtered_result_list = pd.DataFrame(filtered_result_list, columns = filtered_result_header_names)
        self.system_log.emit("Fetching tool completed")
        return df_filtered_result_list
    
    def query_ET(self):
        # Query function to acqurie list of ET datainformation according to the product specified + Selected Lot List
        # Aquired information will be updated onto the 'ET List' Selection  of Data Query Selection Menu UI
        # Called automacially upon clicking onto the 'search' button under the 'Time Frame' selection
        self.system_log.emit("Fetching ET parameter...")
        if type(self.query1) == tuple:
            querying_et = f" SELECT et_param_raw.Test_ID, et_param_spec.Param_Name \
                            FROM et_param_raw \
                            LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                            AND et_param_raw.Product = et_param_spec.Product \
                            AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                            WHERE et_param_raw.Product LIKE '{self.query2[0]}' \
                            AND et_param_raw.Lot_No IN {self.query1} \
                            GROUP BY et_param_spec.Param_Name \
                            ORDER BY et_param_raw.Test_ID"  
                                        
        else:
            querying_et = f" SELECT et_param_raw.Test_ID, et_param_spec.Param_Name \
                            FROM et_param_raw \
                            LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                            AND et_param_raw.Product = et_param_spec.Product \
                            AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                            WHERE et_param_raw.Product LIKE '{self.query2[0]}' \
                            AND et_param_raw.Lot_No = '{self.query1}' \
                            GROUP BY et_param_spec.Param_Name \
                            ORDER BY et_param_raw.Test_ID"  
                                        
        et = self.query_all(querying_et)
        et_header_names = self.query_headers(querying_et)
        df_et = pd.DataFrame(et, columns = et_header_names)
        self.system_log.emit("Fetching ET completed")
        return df_et
    
    def query_filterET(self):
        # Query function to acqurie list of ET data information according to the product specified + Selected Lot List + filter by Param Name
        # Aquired information will be updated onto the 'ET List' Selection  of Data Query Selection Menu UI
        # Called upon hitting enter after specifying keyword to filter in the Checked ET 'Filter By' Text Box
        self.system_log.emit("Filtering ET parameter...")
        if type(self.query1) == tuple:
            querying_et = f" SELECT et_param_raw.Test_ID, et_param_spec.Param_Name \
                            FROM et_param_raw \
                            LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                            AND et_param_raw.Product = et_param_spec.Product \
                            AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                            WHERE et_param_raw.Product LIKE '{self.query2[0]}' \
                            AND et_param_raw.Lot_No IN {self.query1} \
                            AND et_param_spec.Param_Name LIKE '{self.query2[1]}' \
                            GROUP BY et_param_spec.Param_Name \
                            ORDER BY et_param_raw.Test_ID"  
                                        
        else:
            querying_et = f" SELECT et_param_raw.Test_ID, et_param_spec.Param_Name \
                            FROM et_param_raw \
                            LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                            AND et_param_raw.Product = et_param_spec.Product \
                            AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                            WHERE et_param_raw.Product LIKE '{self.query2[0]}' \
                            AND et_param_raw.Lot_No = '{self.query1}' \
                            AND et_param_spec.Param_Name LIKE '{self.query2[1]}' \
                            GROUP BY et_param_spec.Param_Name \
                            ORDER BY et_param_raw.Test_ID"  
                                        
        et = self.query_all(querying_et)
        et_header_names = self.query_headers(querying_et)
        df_et = pd.DataFrame(et, columns = et_header_names)
        self.system_log.emit("Fetching filtered ET parameter completed")
        return df_et
    
    def query_filterinlinestepname(self):
        # Query function to acqurie list of Inline data information according to the product specified + Selected Lot List + filter by Step Name
        # Aquired information will be updated onto the 'Inline List' Selection of Data Query Selection Menu UI
        # Called upon hitting enter after specifying keyword to filter in the Checked Inline 'Filter By' Text Box & 'Step Name' Selected in the Dropbox
        self.system_log.emit("Fetching inline parameter filter by step name...")

        querying_inline = f" SELECT DISTINCT inline_param_raw.Route, inline_param_raw.Step, wip_proc_def.Step_Name, inline_param_raw.Param_Name \
                            FROM inline_param_raw \
                            LEFT JOIN wip_proc_def \
                            ON inline_param_raw.Route = wip_proc_def.Route\
                            AND inline_param_raw.Step = wip_proc_def.Step\
                            WHERE inline_param_raw.Product LIKE '{self.query1}' \
                            AND wip_proc_def.Step_Name LIKE '{self.query2}' \
                            ORDER BY inline_param_raw.Step ASC"
                                
        inline = self.query_all(querying_inline)
        inline_header_names = self.query_headers(querying_inline)
        df_inline = pd.DataFrame(inline, columns = inline_header_names)
        self.system_log.emit("Fetching inline parameter completed")
        return df_inline
    
    def query_filterinlinestep(self):
        # Query function to acqurie list of Inline data information according to the product specified + Selected Lot List + filter by Step No.
        # Aquired information will be updated onto the 'Inline List' Selection of Data Query Selection Menu UI
        # Called upon hitting enter after specifying keyword to filter in the Checked Inline 'Filter By' Text Box & 'Step' Selected in the Dropbox
        self.system_log.emit("Fetching inline parameter filter by step...")

        querying_inline = f" SELECT DISTINCT inline_param_raw.Route, inline_param_raw.Step, wip_proc_def.Step_Name, inline_param_raw.Param_Name \
                            FROM inline_param_raw \
                            LEFT JOIN wip_proc_def \
                            ON inline_param_raw.Route = wip_proc_def.Route\
                            AND inline_param_raw.Step = wip_proc_def.Step\
                            WHERE inline_param_raw.Product LIKE '{self.query1}' \
                            AND wip_proc_def.Step LIKE '{self.query2}' \
                            ORDER BY inline_param_raw.Step ASC"
                           
        inline = self.query_all(querying_inline)
        inline_header_names = self.query_headers(querying_inline)
        df_inline = pd.DataFrame(inline, columns = inline_header_names)
        self.system_log.emit("Fetching inline parameter completed")
        return df_inline
    
    def query_filterwipstepname(self):
        # Query function to acqurie list of Wip data information according to the product specified + Selected Lot List + filter by Step Name
        # Aquired information will be updated onto the 'Wip List' Selection of Data Query Selection Menu UI
        # Called upon hitting enter after specifying keyword to filter in the Checked Inline 'Filter By' Text Box & 'Step Name' Selected in the Dropbox

        self.system_log.emit("Fetching wip filter by step name...")
        querying_inline = f" SELECT DISTINCT wip_lot_ope_process.Step, wip_lot_ope_process.Step_Name \
                            FROM wip_lot_ope_process \
                            LEFT JOIN wip_proc_def \
                            ON wip_lot_ope_process.Route = wip_proc_def.Route \
                            AND wip_lot_ope_process.Step = wip_proc_def.Step \
                            WHERE wip_lot_ope_process.Product LIKE '{self.query1}' \
                            AND wip_lot_ope_process.Step_Name LIKE '{self.query2}' \
                            ORDER BY wip_lot_ope_process.Step ASC"

        inline = self.query_all(querying_inline)
        inline_header_names = self.query_headers(querying_inline)
        df_inline = pd.DataFrame(inline, columns = inline_header_names)
        self.system_log.emit("Fetching wip parameter completed")
        return df_inline
    
    def query_filterwipstep(self):
        # Query function to acqurie list of Wip data information according to the product specified + Selected Lot List + filter by Step No.
        # Aquired information will be updated onto the 'Wip List' Selection of Data Query Selection Menu UI
        # Called upon hitting enter after specifying keyword to filter in the Checked Wip 'Filter By' Text Box & 'Step' Selected in the Dropbox
        self.system_log.emit("Fetching wip filter by step...")

        querying_inline = f" SELECT DISTINCT wip_lot_ope_process.Step, wip_lot_ope_process.Step_Name \
                            FROM wip_lot_ope_process \
                            LEFT JOIN wip_proc_def \
                            ON wip_lot_ope_process.Route = wip_proc_def.Route \
                            AND wip_lot_ope_process.Step = wip_proc_def.Step \
                            WHERE wip_lot_ope_process.Product LIKE '{self.query1}' \
                            AND wip_lot_ope_process.Step LIKE '{self.query2}' \
                            ORDER BY wip_lot_ope_process.Step ASC"
                                
        inline = self.query_all(querying_inline)
        inline_header_names = self.query_headers(querying_inline)
        df_inline = pd.DataFrame(inline, columns = inline_header_names)
        self.system_log.emit("Fetching wip completed")
        return df_inline
    
    def query_data(self):
        # Query function consisting of multiple if-else statement to query various data type indicated by user
        # Data Queired are been merged subsequently into correlated data (by wafer) in the sequence Bin, ET, Inline followed by Wip
        # Executed upon clicking on the 'Query' Button after selecting save directory
        # ET & Inline Spec Book will be included into separate sheets (Excel) or files (CSV)
        self.system_log.emit("Querying Data...")
        self.Node = self.query1[0]
        self.Process = self.query1[1]
        self.Product = self.query1[2]
        self.TestType = self.query1[3]
        self.ETStat = self.query1[4] 
        self.Lot_No = self.query1[5]
        self.Bin = self.query1[6]
        self.BinType = self.query1[7]
        self.ET = self.query1[8]
        self.Inline_collated = self.query1[9]
        self.InlineStat = self.query1[10]
        self.Wip_collated = self.query1[11]
        
        if self.Inline_collated != 'None':
            if len(self.Inline_collated) == 1:
                self.Inline_Step_List = self.Inline_collated[0][0]
                self.Inline_Route_List = self.Inline_collated[0][1]
                self.Inline_Param_List = self.Inline_collated[0][2]
            
                if len(self.Inline_Route_List) and len(self.Inline_Step_List) and len(self.Inline_Param_List) == 1:
                    self.Inline_Step = self.Inline_Step_List[0]
                    self.Inline_Route = self.Inline_Route_List[0]
                    self.Inline_Param = self.Inline_Param_List[0]
                    
                else:
                    self.Inline_Step = [self.Inline_Step_List[i] for i in range(len(self.Inline_Step_List))]
                    self.Inline_Route = [self.Inline_Route_List[i] for i in range(len(self.Inline_Route_List))]
                    self.Inline_Param = [self.Inline_Param_List[i] for i in range(len(self.Inline_Param_List))]
                    self.Inline_Step = tuple(self.Inline_Step)
                    self.Inline_Route = tuple(self.Inline_Route)
                    self.Inline_Param = tuple(self.Inline_Param)
            else:
                self.Inline_Step = [n for i in self.Inline_collated for n in i[0]]
                self.Inline_Route = [n for i in self.Inline_collated for n in i[1]]
                self.Inline_Param = [n for i in self.Inline_collated for n in i[2]]
                self.Inline_Step  = tuple(self.Step)
                self.Inline_Route = tuple(self.Inline_Route)
                self.Inline_Param = tuple(self.Inline_Param)
      
        if self.Wip_collated != 'None':
            if len(self.Wip_collated) == 1:
                self.Wip_Step_List = self.Wip_collated[0][0]
                self.Wip_Name_List = self.Wip_collated[0][1]
    
                if len(self.Wip_Step_List) and len(self.Wip_Name_List) == 1:
                    self.Wip_Step = self.Wip_Step_List[0]
                    self.Wip_Name = self.Wip_Name_List[0]
                    
                else:
                    self.Wip_Step = [self.Wip_Step_List[i] for i in range(len(self.Wip_Step_List))]
                    self.Wip_Name = [self.Wip_Name_List[i] for i in range(len(self.Wip_Name_List))]
                    self.Wip_Step = tuple(self.Wip_Step)
                    self.Wip_Name = tuple(self.Wip_Name)
            else:
                self.Wip_Step = [n for i in self.Wip_collated for n in i[0]]
                self.Wip_Name = [n for i in self.Wip_collated for n in i[1]]
                self.Wip_Step  = tuple(self.Wip_Step)
                self.Wip_Name = tuple(self.Wip_Name)
        
        if type(self.Lot_No) == tuple:
            querying_lot = f" SELECT sort_lot_rp1.Product, sort_lot_rp1.Lot_No, lot_fabout.Fabout AS Fabout \
                            FROM sort_lot_rp1 LEFT JOIN lot_fabout \
                            ON sort_lot_rp1.Lot_No = lot_fabout.Lot_No \
                            WHERE sort_lot_rp1.Product LIKE '{self.Product}' \
                            AND sort_lot_rp1.Lot_No IN {self.Lot_No} \
                            AND sort_lot_rp1.Test_Type = '{self.TestType}' \
                            ORDER BY sort_lot_rp1.Lot_No"
                            
            querying_wafer = f" SELECT sort_wafer_raw.Product, sort_wafer_raw.Lot_No, sort_wafer_raw.Wafer_Alias, sort_wafer_raw.Scribe, \
                                sort_wafer_raw.Measure, sort_wafer_raw.Good_Die, sort_wafer_raw.Tested_Die, \
                                product_info.GDPW, (sort_wafer_raw.Good_Die/sort_wafer_raw.Tested_Die)*100 AS Yield \
                                FROM sort_wafer_raw \
                                LEFT JOIN product_info ON sort_wafer_raw.Product = product_info.Product \
                                WHERE sort_wafer_raw.Product LIKE '{self.Product}' \
                                AND sort_wafer_raw.Lot_No IN {self.Lot_No} \
                                ORDER BY sort_wafer_raw.Measure DESC"
            
            if self.BinType == 'Percentage':
                if type(self.Bin) == tuple:                    
                    querying_bin = f" SELECT sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, sort_wafer_bins.Bin_Def, \
                                        sort_wafer_bins.Bin,(sort_wafer_bins.Bin_Count/product_info.GDPW)*100 AS Bin_Percentage \
                                        FROM sort_wafer_bins \
                                        LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                        WHERE sort_wafer_bins.Product = '{self.Product}' \
                                        AND sort_wafer_bins.Lot_No IN {self.Lot_No} \
                                        AND fab7ye.sort_wafer_bins.Test_Type = '{self.TestType}' \
                                        AND fab7ye.sort_wafer_bins.Bin IN {self.Bin} \
                                        ORDER BY Measure DESC"
                                    
                elif self.Bin != 'None':
                    querying_bin = f" SELECT sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, sort_wafer_bins.Bin, sort_wafer_bins.Bin_Def, \
                                        (sort_wafer_bins.Bin_Count/product_info.GDPW)*100 AS Bin_Percentage \
                                        FROM sort_wafer_bins \
                                        LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                        WHERE sort_wafer_bins.Product = '{self.Product}' \
                                        AND sort_wafer_bins.Lot_No IN {self.Lot_No} \
                                        AND fab7ye.sort_wafer_bins.Test_Type = '{self.TestType}' \
                                        AND fab7ye.sort_wafer_bins.Bin = '{self.Bin}' \
                                        ORDER BY Measure DESC"  
            
            elif self.BinType == 'Count':
                if type(self.Bin) == tuple:                    
                    querying_bin = f" SELECT Wafer_Alias, Scribe, Bin_Def, Bin, Bin_Count \
                                        FROM sort_wafer_bins \
                                        WHERE Product = '{self.Product}' \
                                        AND Lot_No IN {self.Lot_No} \
                                        AND Test_Type = '{self.TestType}' \
                                        AND Bin IN {self.Bin} \
                                        ORDER BY Measure DESC"  
                                    
                elif self.Bin != 'None':
                    querying_bin = f" SELECT Wafer_Alias, Scribe, Bin_Def, Bin, Bin_Count \
                                        FROM sort_wafer_bins \
                                        WHERE Product = '{self.Product}' \
                                        AND Lot_No IN {self.Lot_No} \
                                        AND Test_Type = '{self.TestType}' \
                                        AND Bin = '{self.Bin}' \
                                        ORDER BY Measure DESC"  
                                        
            if self.ET != 'None':
                if type(self.ET) == tuple:
                    querying_et = f" SELECT et_param_raw.Wafer_Alias, et_param_raw.Scribe, et_param_raw.Test_Prog, et_param_raw.Test_Spec, \
                                    et_param_raw.Test_ID, et_param_spec.Param_Name, et_param_spec.Param_Unit, et_param_raw.Spec_Low, et_param_raw.Spec_High, \
                                    et_param_spec.Key_Test, et_param_spec.Min_Valid_Site, et_param_spec.Dispo_Limit, et_param_spec.Critical_ET, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                    AND et_param_raw.Product = et_param_spec.Product \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product}' \
                                    AND et_param_raw.Lot_No IN {self.Lot_No} \
                                    AND et_param_spec.Test_ID IN {self.ET} \
                                    ORDER BY et_param_raw.Test_ID"     
                                        
                else:
                    querying_et = f" SELECT et_param_raw.Wafer_Alias, et_param_raw.Scribe, et_param_raw.Test_Prog, et_param_raw.Test_Spec,\
                                    et_param_raw.Test_ID, et_param_spec.Param_Name, et_param_spec.Param_Unit, et_param_raw.Spec_Low, et_param_raw.Spec_High, \
                                    et_param_spec.Key_Test, et_param_spec.Min_Valid_Site, et_param_spec.Dispo_Limit, et_param_spec.Critical_ET, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                    AND et_param_raw.Product = et_param_spec.Product \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product}' \
                                    AND et_param_raw.Lot_No IN {self.Lot_No} \
                                    AND et_param_spec.Test_ID = '{self.ET}' \
                                    ORDER BY et_param_raw.Test_ID"  
            
            if self.Inline_collated != 'None':                  
                if type(self.Inline_Step) == tuple and type(self.Inline_Route) == tuple and type(self.Inline_Param) == tuple:
                    querying_inline = f" SELECT inline_param_raw.Wafer_Alias, inline_param_raw.Scribe, inline_param_raw.Route, inline_param_raw.Step, wip_proc_def.Step_Name, \
                                        inline_param_raw.Param_Name, inline_param_raw.Spec_Low, inline_param_raw.Target, inline_param_raw.Spec_High, inline_param_raw.Unit, inline_param_raw.SMap_Raw AS smap \
                                        FROM inline_param_raw \
                                        LEFT JOIN wip_proc_def \
                                        ON inline_param_raw.Route = wip_proc_def.Route \
                                        AND inline_param_raw.Step = wip_proc_def.Step \
                                        WHERE inline_param_raw.Product = '{self.Product}' \
                                        AND inline_param_raw.Lot_No IN {self.Lot_No} \
                                        AND inline_param_raw.Step IN {self.Inline_Step} \
                                        AND inline_param_raw.Route IN {self.Inline_Route} \
                                        AND inline_param_raw.Param_Name IN {self.Inline_Param} \
                                        ORDER BY inline_param_raw.Param_Name"
                                            
                else:
                    querying_inline = f" SELECT inline_param_raw.Wafer_Alias, inline_param_raw.Scribe, inline_param_raw.Route, inline_param_raw.Step, wip_proc_def.Step_Name, \
                                        inline_param_raw.Param_Name, inline_param_raw.Spec_Low, inline_param_raw.Target, inline_param_raw.Spec_High, inline_param_raw.Unit, inline_param_raw.SMap_Raw AS smap \
                                        FROM inline_param_raw \
                                        LEFT JOIN wip_proc_def \
                                        ON inline_param_raw.Route = wip_proc_def.Route \
                                        AND inline_param_raw.Step = wip_proc_def.Step \
                                        WHERE inline_param_raw.Product = '{self.Product}' \
                                        AND inline_param_raw.Lot_No IN {self.Lot_No} \
                                        AND inline_param_raw.Step = '{self.Inline_Step}' \
                                        AND inline_param_raw.Route = '{self.Inline_Route}' \
                                        AND inline_param_raw.Param_Name = '{self.Inline_Param}' \
                                        ORDER BY inline_param_raw.Param_Name"

            if self.Wip_collated != 'None':
                if type(self.Wip_Step) == tuple and type(self.Wip_Name) == tuple:
                    querying_wip = f" SELECT wip_wc_proc_process_end.Scribe, wip_wc_proc_process_end.Step, wip_proc_def.Step_Name, \
                                    wip_wc_proc_process_end.Equipment, wip_wc_proc_process_end.Chamber \
                                    FROM wip_wc_proc_process_end \
                                    LEFT JOIN wip_proc_def \
                                    ON wip_wc_proc_process_end.Route = wip_proc_def.Route \
                                    AND wip_wc_proc_process_end.Step = wip_proc_def.Step \
                                    WHERE wip_wc_proc_process_end.Lot_No IN {self.Lot_No} \
                                    AND wip_proc_def.Step IN {self.Wip_Step} \
                                    ORDER BY fab7ye.wip_wc_proc_process_end.Proc_Time DESC"
                                            
                else:
                    querying_wip = f" SELECT wip_wc_proc_process_end.Scribe, wip_wc_proc_process_end.Step, wip_proc_def.Step_Name, \
                                    wip_wc_proc_process_end.Equipment, wip_wc_proc_process_end.Chamber \
                                    FROM wip_wc_proc_process_end \
                                    LEFT JOIN wip_proc_def \
                                    ON wip_wc_proc_process_end.Route = wip_proc_def.Route \
                                    AND wip_wc_proc_process_end.Step = wip_proc_def.Step\
                                    WHERE wip_wc_proc_process_end.Lot_No IN {self.Lot_No} \
                                    AND wip_proc_def.Step = '{self.Wip_Step}' \
                                    ORDER BY fab7ye.wip_wc_proc_process_end.Proc_Time DESC" 
                
        else:
            querying_lot = f" SELECT sort_lot_rp1.Product, sort_lot_rp1.Lot_No, lot_fabout.Fabout \
                            FROM sort_lot_rp1 LEFT JOIN lot_fabout \
                            ON sort_lot_rp1.Lot_No = lot_fabout.Lot_No \
                            WHERE sort_lot_rp1.Product LIKE '{self.Product}' \
                            AND sort_lot_rp1.Lot_No = '{self.Lot_No}' \
                            AND sort_lot_rp1.Test_Type = '{self.TestType}' \
                            ORDER BY sort_lot_rp1.Lot_No"
                            
            querying_wafer = f" SELECT sort_wafer_raw.Product, sort_wafer_raw.Lot_No, sort_wafer_raw.Wafer_Alias, sort_wafer_raw.Scribe, \
                                sort_wafer_raw.Measure, sort_wafer_raw.Good_Die, sort_wafer_raw.Tested_Die, \
                                product_info.GDPW, (sort_wafer_raw.Good_Die/sort_wafer_raw.Tested_Die)*100 AS Yield \
                                FROM sort_wafer_raw \
                                LEFT JOIN product_info ON sort_wafer_raw.Product = product_info.Product \
                                WHERE sort_wafer_raw.Product LIKE '{self.Product}' \
                                AND sort_wafer_raw.Lot_No = '{self.Lot_No}' \
                                ORDER BY sort_wafer_raw.Measure DESC"
            
            if self.BinType == 'Percentage':
                if type(self.Bin) == tuple:                    
                    querying_bin = f" SELECT sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, sort_wafer_bins.Bin_Def, \
                                        sort_wafer_bins.Bin,(sort_wafer_bins.Bin_Count/product_info.GDPW)*100 AS Bin_Percentage \
                                        FROM sort_wafer_bins \
                                        LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                        WHERE sort_wafer_bins.Product = '{self.Product}' \
                                        AND sort_wafer_bins.Lot_No = '{self.Lot_No}' \
                                        AND fab7ye.sort_wafer_bins.Test_Type = '{self.TestType}' \
                                        AND fab7ye.sort_wafer_bins.Bin IN {self.Bin} \
                                        ORDER BY Measure DESC"
                                    
                else:
                    querying_bin = f" SELECT sort_wafer_bins.Wafer_Alias, sort_wafer_bins.Scribe, sort_wafer_bins.Bin, sort_wafer_bins.Bin_Def, \
                                        (sort_wafer_bins.Bin_Count/product_info.GDPW)*100 AS Bin_Percentage \
                                        FROM sort_wafer_bins \
                                        LEFT JOIN product_info ON sort_wafer_bins.Product = product_info.Product \
                                        WHERE sort_wafer_bins.Product = '{self.Product}' \
                                        AND sort_wafer_bins.Lot_No = '{self.Lot_No}' \
                                        AND fab7ye.sort_wafer_bins.Test_Type = '{self.TestType}' \
                                        AND fab7ye.sort_wafer_bins.Bin = '{self.Bin}' \
                                        ORDER BY Measure DESC"  
            
            elif self.BinType == 'Count':
                if type(self.Bin) == tuple:                    
                    querying_bin = f" SELECT Wafer_Alias, Scribe, Bin_Def, Bin, Bin_Count \
                                        FROM sort_wafer_bins \
                                        WHERE Product = '{self.Product}' \
                                        AND Lot_No = '{self.Lot_No}' \
                                        AND Test_Type = '{self.TestType}' \
                                        AND Bin IN {self.Bin} \
                                        ORDER BY Measure DESC"  
                                    
                else:
                    querying_bin = f" SELECT Wafer_Alias, Scribe, Bin_Def, Bin, Bin_Count \
                                        FROM sort_wafer_bins \
                                        WHERE Product = '{self.Product}' \
                                        AND Lot_No = '{self.Lot_No}' \
                                        AND Test_Type = '{self.TestType}' \
                                        AND Bin = '{self.Bin}' \
                                        ORDER BY Measure DESC"  
            
            if self.ET != 'None':
                if type(self.ET) == tuple:
                    querying_et = f" SELECT et_param_raw.Wafer_Alias, et_param_raw.Scribe, et_param_raw.Test_Prog, et_param_raw.Test_Spec, \
                                    et_param_raw.Test_ID, et_param_spec.Param_Name, et_param_spec.Param_Unit, et_param_raw.Spec_Low, et_param_raw.Spec_High, \
                                    et_param_spec.Key_Test, et_param_spec.Min_Valid_Site, et_param_spec.Dispo_Limit, et_param_spec.Critical_ET, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                    AND et_param_raw.Product = et_param_spec.Product \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product}' \
                                    AND et_param_raw.Lot_No = '{self.Lot_No}' \
                                    AND et_param_spec.Test_ID IN {self.ET} \
                                    ORDER BY et_param_raw.Test_ID"    
                                    
                else:
                    querying_et = f" SELECT et_param_raw.Wafer_Alias, et_param_raw.Scribe, et_param_raw.Test_Prog, et_param_raw.Test_Spec, \
                                    et_param_raw.Test_ID, et_param_spec.Param_Name, et_param_spec.Param_Unit, et_param_raw.Spec_Low, et_param_raw.Spec_High, \
                                    et_param_spec.Key_Test, et_param_spec.Min_Valid_Site, et_param_spec.Dispo_Limit, et_param_spec.Critical_ET, convert(uncompress(WMap_Raw) using utf8) as wmap \
                                    FROM et_param_raw \
                                    LEFT JOIN et_param_spec ON et_param_raw.Test_ID = et_param_spec.Test_ID \
                                    AND et_param_raw.Product = et_param_spec.Product \
                                    AND et_param_raw.Test_Spec = et_param_spec.Test_Spec \
                                    WHERE et_param_raw.Product LIKE '{self.Product}' \
                                    AND et_param_raw.Lot_No = '{self.Lot_No}' \
                                    AND et_param_spec.Test_ID = '{self.ET}' \
                                    ORDER BY et_param_raw.Test_ID"
            
            if self.Inline_collated != 'None':
                if type(self.Inline_Step) == tuple and type(self.Inline_Route) == tuple and type(self.Inline_Param) == tuple:
                    querying_inline = f" SELECT inline_param_raw.Wafer_Alias, inline_param_raw.Scribe, inline_param_raw.Route, inline_param_raw.Step, wip_proc_def.Step_Name, \
                                        inline_param_raw.Param_Name, inline_param_raw.Spec_Low, inline_param_raw.Target, inline_param_raw.Spec_High, inline_param_raw.Unit, inline_param_raw.SMap_Raw AS smap \
                                        FROM inline_param_raw \
                                        LEFT JOIN wip_proc_def \
                                        ON inline_param_raw.Route = wip_proc_def.Route \
                                        AND inline_param_raw.Step = wip_proc_def.Step \
                                        WHERE inline_param_raw.Product = '{self.Product}' \
                                        AND inline_param_raw.Lot_No = '{self.Lot_No}' \
                                        AND inline_param_raw.Step IN {self.Inline_Step} \
                                        AND inline_param_raw.Route IN {self.Inline_Route} \
                                        AND inline_param_raw.Param_Name IN {self.Inline_Param} \
                                        ORDER BY inline_param_raw.Param_Name"
                                            
                else:
                    querying_inline = f" SELECT inline_param_raw.Wafer_Alias, inline_param_raw.Scribe, inline_param_raw.Route, inline_param_raw.Step, wip_proc_def.Step_Name, \
                                        inline_param_raw.Param_Name, inline_param_raw.Spec_Low, inline_param_raw.Target, inline_param_raw.Spec_High, inline_param_raw.Unit, inline_param_raw.SMap_Raw AS smap \
                                        FROM inline_param_raw \
                                        LEFT JOIN wip_proc_def \
                                        ON inline_param_raw.Route = wip_proc_def.Route \
                                        AND inline_param_raw.Step = wip_proc_def.Step \
                                        WHERE inline_param_raw.Product = '{self.Product}' \
                                        AND inline_param_raw.Lot_No = '{self.Lot_No}' \
                                        AND inline_param_raw.Step = '{self.Inline_Step}' \
                                        AND inline_param_raw.Route = '{self.Inline_Route}' \
                                        AND inline_param_raw.Param_Name = '{self.Inline_Param}' \
                                        ORDER BY inline_param_raw.Param_Name"
             
            if self.Wip_collated != 'None':
                if type(self.Wip_Step) == tuple and type(self.Wip_Name) == tuple:
                    querying_wip = f" SELECT wip_wc_proc_process_end.Scribe, wip_wc_proc_process_end.Step, wip_proc_def.Step_Name, \
                                    wip_wc_proc_process_end.Equipment, wip_wc_proc_process_end.Chamber \
                                    FROM wip_wc_proc_process_end \
                                    LEFT JOIN wip_proc_def \
                                    ON wip_wc_proc_process_end.Route = wip_proc_def.Route \
                                    AND wip_wc_proc_process_end.Step = wip_proc_def.Step \
                                    WHERE wip_wc_proc_process_end.Lot_No = '{self.Lot_No}' \
                                    AND wip_proc_def.Step IN {self.Wip_Step} \
                                    ORDER BY fab7ye.wip_wc_proc_process_end.Proc_Time DESC"
                                            
                else:
                    querying_wip = f" SELECT wip_wc_proc_process_end.Scribe, wip_wc_proc_process_end.Step, wip_proc_def.Step_Name, \
                                    wip_wc_proc_process_end.Equipment, wip_wc_proc_process_end.Chamber \
                                    FROM wip_wc_proc_process_end \
                                    LEFT JOIN wip_proc_def \
                                    ON wip_wc_proc_process_end.Route = wip_proc_def.Route \
                                    AND wip_wc_proc_process_end.Step = wip_proc_def.Step\
                                    WHERE wip_wc_proc_process_end.Lot_No = '{self.Lot_No}' \
                                    AND wip_proc_def.Step = '{self.Wip_Step}' \
                                    ORDER BY fab7ye.wip_wc_proc_process_end.Proc_Time DESC"  
        
        data = self.query_all(querying_lot)
        data_header_names = self.query_headers(querying_lot)
        df_data = pd.DataFrame(data, columns = data_header_names)
        
        wafer_yield = self.query_all(querying_wafer)
        wafer_yield_header_names = self.query_headers(querying_wafer)
        df_wafer_yield = pd.DataFrame(wafer_yield, columns = wafer_yield_header_names)
        df_wafer_yield['Yield'] = [float(df_wafer_yield['Yield'][i]) for i in range(len(df_wafer_yield.index))]
        df_wafer_yield.drop_duplicates(subset = ['Wafer_Alias','Scribe'], inplace = True)
        df_wafer_yield.reset_index(drop = True, inplace = True)
        df_data = pd.merge(df_data, df_wafer_yield, how = 'left', on = ['Product','Lot_No'])
        
        if self.Bin != 'None':
            wafer_bin = self.query_all(querying_bin)
            wafer_bin_header_names = self.query_headers(querying_bin)
            df_wafer_bin = pd.DataFrame(wafer_bin, columns = wafer_bin_header_names)
            if self.BinType == 'Percentage':
                df_wafer_bin['Bin_Percentage'] = [float(df_wafer_bin['Bin_Percentage'][i]) for i in range(len(df_wafer_bin.index))]
                df_wafer_bin['Bin'] = [df_wafer_bin['Bin'][i].decode('utf-8') for i in range(len(df_wafer_bin.index))]
                df_wafer_bin['Bin_w_Def'] = [self.TestType + '_' + 'Bin' + df_wafer_bin['Bin'][i] + '_' + df_wafer_bin['Bin_Def'][i] + '_' + 'Percentage' for i in range(len(df_wafer_bin.index))]
                df_wafer_bin.drop(['Bin','Bin_Def'], axis=1, inplace = True)
                df_wafer_bin = df_wafer_bin.pivot(index = ['Wafer_Alias','Scribe'], columns='Bin_w_Def', values='Bin_Percentage').reset_index()
                df_wafer_bin.replace('', np.nan, inplace=True)
                df_wafer_bin.fillna(0, inplace = True)
                
            elif self.BinType == 'Count':
                df_wafer_bin['Bin'] = [df_wafer_bin['Bin'][i].decode('utf-8') for i in range(len(df_wafer_bin.index))]
                df_wafer_bin['Bin_w_Def'] = [self.TestType + '_' + 'Bin' + df_wafer_bin['Bin'][i] + '_' + df_wafer_bin['Bin_Def'][i] + '_' + 'Count' for i in range(len(df_wafer_bin.index))]
                df_wafer_bin.drop(['Bin','Bin_Def'], axis=1, inplace = True)
                df_wafer_bin = df_wafer_bin.pivot(index = ['Wafer_Alias','Scribe'], columns='Bin_w_Def', values='Bin_Count').reset_index()
                df_wafer_bin.replace('', np.nan, inplace=True)
                df_wafer_bin.fillna(0, inplace = True)
                
            else:
                self.system_log.emit("Error! Unknown data type selected for Bin")
                
            df_data = pd.merge(df_data, df_wafer_bin, how = 'left', on = ['Wafer_Alias','Scribe'])
        
        if self.ET != 'None':
            et = self.query_all(querying_et)
            et_header_names = self.query_headers(querying_et)
            df_et = pd.DataFrame(et, columns = et_header_names)
            df_et.drop_duplicates(subset = ['Wafer_Alias','Scribe','Test_Spec','Test_ID','Param_Name'], inplace = True)
            df_et.reset_index(drop = True, inplace = True)
            
            wmap_raw_value_index = df_et.columns.get_loc('wmap')
            stored_wmap_raw_data = []
            stored_data_section = []
            expanded_data_column = []
            expanded_data_wafer = []     
            expanded_data_et = []
            
            # Unpack ET site level data from wmap
            for row in range(len(df_et)):
                wmap_raw_value_str = df_et.loc[row][wmap_raw_value_index]
                wmap_raw_value = wmap_raw_value_str.replace(';', '')
                wmap_raw_value = wmap_raw_value.split(',')
                wmap_raw_value = list(filter(None, wmap_raw_value))
                wmap_raw_value_filtered = []
                
                for element in wmap_raw_value:
                    element1 = element.split(':')
                    element1[0] = int(element1[0])
                    element1[1] = float(element1[1])
                    element1 = tuple(element1)
                    wmap_raw_value_filtered.append(element1)
                    
                stored_wmap_raw_data.append(tuple(wmap_raw_value_filtered))
                stored_data_section.append(len(wmap_raw_value_filtered))
                
            # Consolidate measurements for each parameter as a column to perform data cleaning by IQR
            # Afterwhich the wafer averaged ET values are calculated

            if self.ETStat == 'Site':
                for i in range(len(df_et)):  
                    for j in range(len(stored_wmap_raw_data[i])):
                        expanded_data_column.append(stored_wmap_raw_data[i][j][1])
                        expanded_data_wafer.append(df_et['Scribe'][i])
                        expanded_data_et.append(df_et['Param_Name'][i] + '_' + self.ETStat + '#' + str(stored_wmap_raw_data[i][j][0]))
                
                data = pd.DataFrame(expanded_data_column, columns = ['Data'])
                data.insert(0, 'ET', expanded_data_et)
                data.insert(0, 'Scribe', expanded_data_wafer)
                
                df_et_processed = data.pivot_table(index = 'Scribe', columns='ET', values='Data').reset_index()

            else:
                count = 0
                for i in range(len(df_et)):  
                    for j in range(len(stored_wmap_raw_data[i])):
                        expanded_data_column.append(stored_wmap_raw_data[i][j][1])
                        expanded_data_wafer.append(df_et['Scribe'][i])
                        expanded_data_et.append(df_et['Param_Name'][i] + '_' + self.ETStat)             
                        
                data = pd.DataFrame(expanded_data_column, columns = ['Data'])
                data.insert(0, 'ET', expanded_data_et)
                data.insert(0, 'Scribe', expanded_data_wafer)
                
                if self.ETStat == 'Mean':
                    data_processed = data.groupby(['Scribe','ET'], sort = False).mean().reset_index()  
                elif self.ETStat == 'Median':
                    data_processed = data.groupby(['Scribe','ET'], sort = False).median().reset_index()
                elif self.ETStat == 'Max':
                    data_processed = data.groupby(['Scribe','ET'], sort = False).max().reset_index()
                elif self.ETStat == 'Min':
                    data_processed = data.groupby(['Scribe','ET'], sort = False).min().reset_index()
                else:
                    self.system_log.emit("Error! Unknown data type selected for ET paramter")
                
                # Adding processed/cleaned ET parameters column by column for every iteration
                df_et_processed = data_processed.pivot(index = 'Scribe', columns='ET', values='Data').reset_index()
          
            df_data = pd.merge(df_data, df_et_processed, how='left',on=['Scribe'])
            df_data.drop_duplicates(inplace = True)
            df_data.reset_index(drop = True, inplace = True)
            
            df_et_spec = df_et[['Test_ID','Test_Spec','Test_Prog','Param_Name', 'Param_Unit','Spec_Low', 'Spec_High', 'Key_Test', 'Min_Valid_Site', 'Dispo_Limit','Critical_ET']]
            df_et_spec.drop_duplicates(inplace = True)
            df_et_spec.reset_index(drop = True, inplace = True)
        
        if self.Inline_collated != 'None':
            inline = self.query_all(querying_inline)
            inline_header_names = self.query_headers(querying_inline)
            df_inline = pd.DataFrame(inline, columns = inline_header_names)
            df_inline.drop_duplicates(subset = ['Wafer_Alias','Scribe','Route','Step','Param_Name'], inplace = True)
            df_inline.reset_index(drop = True, inplace = True)
            
            smap_raw_value_index = df_inline.columns.get_loc('smap')
            stored_smap_raw_data = []
            stored_data_section = []
            expanded_data_column = []
            expanded_data_wafer = []
            expanded_inline = []
                
            for row in range(len(df_inline)):
                smap_raw_value_str = df_inline.loc[row][smap_raw_value_index]
                smap_raw_value = smap_raw_value_str.split(',')[:-1]
                smap_raw_value_filtered = []
                
                for element in smap_raw_value:
                    element1 = element.split(':')
                    if type(element1[0]) == float:
                        element1[0] = int(float(element1[0]))
                    else:
                        element1[0] = element1[0]
                    element1[1] = float(element1[1])
                    element1 = tuple(element1)
                    smap_raw_value_filtered.append(element1)
                    
                stored_smap_raw_data.append(tuple(smap_raw_value_filtered))
                stored_data_section.append(len(smap_raw_value_filtered))
                
            # Consolidate measurements for each parameter as a column to perform data cleaning by IQR
            # Afterwhich the wafer averaged ET values are calculated  
            
            if self.InlineStat == 'Site':
                for i in range(len(df_inline)):  
                    for j in range(len(stored_smap_raw_data[i])):
                        expanded_data_column.append(stored_smap_raw_data[i][j][1])
                        expanded_data_wafer.append(df_inline['Scribe'][i])
                        expanded_inline.append(df_inline['Route'][i] + '_' + df_inline['Step'][i] + '_' + df_inline['Param_Name'][i] + '_' + self.InlineStat + '#' + str(stored_smap_raw_data[i][j][0]))
      
                data = pd.DataFrame(expanded_data_column, columns = ['Data'])
                data.insert(0, 'Inline', expanded_inline)
                data.insert(0, 'Scribe', expanded_data_wafer)
                df_inline_processed = data.pivot_table(index = 'Scribe', columns='Inline', values='Data').reset_index()

            else:    
                count = 0
                for i in range(len(df_inline)):  
                    for j in range(len(stored_smap_raw_data[i])):
                        expanded_data_column.append(stored_smap_raw_data[i][j][1])
                        expanded_data_wafer.append(df_inline['Scribe'][i])
                        expanded_inline.append(df_inline['Route'][i] + '_' + df_inline['Step'][i] + '_' + df_inline['Param_Name'][i] + '_' + self.InlineStat)                   
                    count += 1
                        
                data = pd.DataFrame(expanded_data_column, columns = ['Data'] )
                data.insert(0, 'Inline', expanded_inline)
                data.insert(0, 'Scribe', expanded_data_wafer)
                    
                if self.InlineStat == 'Mean':
                    data_processed = data.groupby(['Scribe','Inline'], sort = False).mean().reset_index()  
                elif self.InlineStat == 'Median':
                    data_processed = data.groupby(['Scribe','Inline'], sort = False).median().reset_index()
                elif self.InlineStat == 'Max':
                    data_processed = data.groupby(['Scribe','Inline'], sort = False).max().reset_index()
                elif self.InlineStat == 'Min':
                    data_processed = data.groupby(['Scribe','Inline'], sort = False).min().reset_index()
                else:
                    self.system_log.emit("Error! Unknown data type selected for Inline paramter")
            
                df_inline_processed = data_processed.pivot(index = 'Scribe', columns='Inline', values='Data').reset_index()
            
            df_data = pd.merge(df_data, df_inline_processed, how='left',on=['Scribe'])
            df_inline_spec = df_inline[['Step','Step_Name','Param_Name', 'Spec_Low', 'Target','Spec_High','Unit']]
            df_inline_spec['Param'] = [df_inline_spec['Step'][i] + '_' + df_inline_spec['Step_Name'][i] + '_' + df_inline_spec['Param_Name'][i] for i in range(len(df_inline_spec.index))]
            df_inline_spec.drop(['Step','Step_Name','Param_Name'], axis=1, inplace = True)
            df_inline_spec = df_inline_spec[['Param','Spec_Low', 'Target','Spec_High','Unit']]
            df_inline_spec.drop_duplicates(inplace = True)
            df_inline_spec.reset_index(drop = True, inplace = True)
        
        if self.Wip_collated != 'None':
            wip = self.query_all(querying_wip)
            wip_header_names = self.query_headers(querying_wip)
            df_wip = pd.DataFrame(wip, columns = wip_header_names)
            
            if len(df_wip.index) != 0:
                df_wip.drop_duplicates(inplace = True)
                df_wip.reset_index(drop = True, inplace = True)
                df_wip = df_wip.groupby(['Scribe','Step','Step_Name','Equipment'])['Chamber'].apply(lambda x: ','.join(x.astype(str))).reset_index()
                df_wip = pd.merge(df_wip.groupby(['Scribe','Step','Step_Name'])['Equipment'].first().reset_index(), df_wip, how='left', on=['Scribe','Step','Step_Name','Equipment'])
                
                df_wip['Step_w_Name_Eq'] = [df_wip['Step'][i] + '_' + df_wip['Step_Name'][i] + '_' + 'Eqp' for i in range(len(df_wip.index))]
                df_wip['Step_w_Name_Chmb'] = [df_wip['Step'][i] + '_' + df_wip['Step_Name'][i] + '_' + 'Chmb' for i in range(len(df_wip.index))]
                df_wip.drop(['Step','Step_Name'], axis=1, inplace = True)
                
                df_wip = pd.merge(df_wip[['Scribe','Step_w_Name_Eq','Equipment']].pivot(index='Scribe', columns='Step_w_Name_Eq', values='Equipment').reset_index(), \
                df_wip[['Scribe','Step_w_Name_Chmb','Chamber']].pivot(index='Scribe', columns='Step_w_Name_Chmb', values='Chamber').reset_index(), how='left', on='Scribe')
                df_wip_headers_reorder = [name for pair in zip(df_wip.columns[1:int((len(df_wip.columns)-1)/2)+1],df_wip.columns[int((len(df_wip.columns)-1)/2)+1:]) for name in pair]
                df_wip_headers_reorder.append('Scribe')
                df_data = pd.merge(df_data, df_wip[df_wip_headers_reorder], how = 'left', on = ['Scribe'])
        
        df_data.insert(0,'Process', self.Process)
        df_data.insert(0,'Tech', self.Node)
        
        self.system_log.emit("Querying Data completed")
        
        if self.ET == 'None' and self.Inline_collated == 'None':
            return df_data
        elif self.ET != 'None' and self.Inline_collated == 'None':
            return df_data, df_et_spec
        elif self.ET == 'None' and self.Inline_collated != 'None':
            return df_data, df_inline_spec
        else:
            return df_data, df_et_spec, df_inline_spec

class Ui(QWidget):
    # Class consisting of all UI related functions
    # UI displaying functions includes mainMenuUi, PCAUi (ProductLotUi,TimeFrameUi), DataQueryUi
    # Other functions are called upon interacting with UI objects
    # Ui Objects with functionailty includes:
        # 1) QComboBox (Dropbox)
            # -> QComboBox.activated.connect(function()) # Do something e.g Query
        # 2) QCheckBox (Checkbox)
            # -> QCheckBox.clicked.connect(function())  # Do something e.g Query, update Ui
        # 3) QPushButton (Button)
            # -> QPushButton.clicked.connect(function()) # Do Something e.g Execute PCA Analysis, Query Data
        # 4) QTabWiget (Tabs)
            # -> QTabWidget.stabBarClicked.connect(functino()) # Do something e.g Switch tabs
    def setupUi(self, Main):
        # Setup function to call all the other Ui function
        # called upon running the application
        Main.setObjectName("Main")
        Main.resize(1280, 720)
        Main.closeEvent = self.closeEvent
        self.width = 1280
        self.height = 720

        self.frames = QStackedLayout()
        self.mainMenu_layout = QGridLayout()
        self.mainMenu = QWidget()
        self.SelectionMenu = QWidget()
        self.DataQuerySelectionMenu = QWidget()

        self.mainMenuUi()
        self.PCAUi()
        self.DataQueryUi()
        
        self.frames.addWidget(self.mainMenu)
        self.frames.addWidget(self.SelectionMenu)
        self.frames.addWidget(self.DataQuerySelectionMenu)
        
        self.path = pathlib.Path().resolve()
        self.outputfolder = os.path.join(self.path,"Output temp")
        self.foldercreated = 'False'
        self.analysis_count = 1
        
    def mainMenuUi(self):
        # UI function display the Main Menu
        # Called from SetupUi()
        self.mainMenu.setWindowTitle('GlobalFoundries')
        self.mainMenu.closeEvent = self.closeEvent
        self.mainMenu.resize(self.width, self.height)
        self.mainMenuText = QLabel(self.mainMenu)
        self.mainMenu_layout.setSpacing(6)
        self.mainMenuText.setStyleSheet("font: 14pt Century Gothic")
        self.mainMenuText.setAlignment(Qt.AlignCenter|Qt.AlignBottom)
        self.mainMenuText.setText("Welcome to GlobalFoundries:\nBig Data Mining Project")

        self.PCAButton = QPushButton("PCA", self.mainMenu)
        self.DataQueryButton = QPushButton("Data Query", self.mainMenu)
        #self.Version = QLabel("v1.1")
        #self.Version.setStyleSheet("font: 10pt Century Gothic")
        #self.Version.setAlignment(Qt.AlignBottom|Qt.AlignRight)
        self.mainMenu_layout.addWidget(self.mainMenuText,0,3,2,1)
        self.mainMenu_layout.addWidget(self.PCAButton,2,3,1,1)
        self.mainMenu_layout.addWidget(self.DataQueryButton,3,3,1,1)
        self.mainMenu_layout.addWidget(QLabel(""),1,0,2,2)
        self.mainMenu_layout.addWidget(QLabel(""),1,4,2,2)
        self.mainMenu_layout.addWidget(QLabel(""),4,3,2,1)
        #self.mainMenu_layout.addWidget(self.Version,5,5,1,1)
        self.mainMenu.setLayout(self.mainMenu_layout)
        
    def PCAUi(self):
        # UI function display the PCA Selection Menu
        # Called from SetupUi
        self.SelectionMenu_layout = QVBoxLayout()
        
        self.SelectionMenu.setWindowTitle('Selection Menu')
        self.SelectionMenu.closeEvent = self.closeEvent
        self.SelectionMenu.resize(960, 960)

        self.menuButton1 = QPushButton("Back to main menu")
        self.Output_container = QHBoxLayout()
        self.OutputWindow = QPushButton("Output Window")
        self.ExportResult = QPushButton("Export Result")
        self.OutputWindow.clicked.connect(self.PCA_Output)
        self.ExportResult.clicked.connect(self.ExportData)
        self.Output_container.addWidget(self.OutputWindow)
        self.Output_container.addWidget(self.ExportResult)
        
        self.Title = QLabel()
        self.Title.setStyleSheet("font: 14pt Century Gothic; text-decoration: underline")
        self.Title.setText("PCA Analysis")
                
        self.DataSource_layout = QGridLayout()
        self.DataSource_layout.setSpacing(8)
        self.DataSource_label = QLabel()
        self.DataSource_label.setText("Data Source:")

        self.DataImportSelection = QCheckBox("Import")
        self.DataImportSelection.setChecked(False)
        self.DataImportSelection.clicked.connect(self.ShowDataSource)
        self.DataSource_import_button = QPushButton("Import")
        self.DataSource_import_button.clicked.connect(self.ImportDataSource)
        self.DataSource_import_button.hide()
        self.DataSource_filelabel = QLabel("")
        self.DataSource_layout.addWidget(self.DataSource_label,0,0)
        self.DataSource_layout.addWidget(self.DataImportSelection,0,1,1,1)
        self.DataSource_layout.addWidget(self.DataSource_import_button,0,2,1,1)
        self.DataSource_layout.addWidget(self.DataSource_filelabel,0,3,1,5)
        
        self.QueryTech()
        
        self.Node_layout = QGridLayout()
        self.Node_layout.setSpacing(8)
        self.Nodelabel = QLabel()
        self.Nodelabel.setText("Tech:")
        self.Node_Box = QComboBox()
        self.Node_Box.setEditable(True)
        self.Node_Box.activated.connect(self.QueryProcessTechCategory)
        self.Node_layout.addWidget(self.Nodelabel,0,0)
        self.Node_layout.addWidget(self.Node_Box,0,1,1,1)
        self.Node_layout.addWidget(QLabel(""),0,3,1,6)

        self.Process_tech_category_layout = QGridLayout()
        self.Process_tech_category_layout.setSpacing(8)
        self.Process_tech_categorylabel = QLabel()
        self.Process_tech_categorylabel.setText("Process:")
        self.Process_tech_category_Box = QComboBox()
        self.Process_tech_category_Box.setEditable(True)
        self.Process_tech_category_Box.activated.connect(self.QueryProduct)
        self.Process_tech_category_layout.addWidget(self.Process_tech_categorylabel,0,0)
        self.Process_tech_category_layout.addWidget(self.Process_tech_category_Box,0,1,1,1)
        self.Process_tech_category_layout.addWidget(QLabel(""),0,3,1,6)
        
        self.Product_layout = QGridLayout()
        self.Product_layout.setSpacing(8)
        self.Product_Nolabel = QLabel()
        self.Product_Nolabel.setText("Product:")
        self.Product_No = QComboBox()
        self.Product_No.setEditable(True)
        self.Product_No.activated.connect(self.QueryTest)
        self.Product_layout.addWidget(self.Product_Nolabel,0,0)
        self.Product_layout.addWidget(self.Product_No,0,1,1,2)
        self.Product_layout.addWidget(QLabel(""),0,3,1,5)
        
        self.TestSelection = QGridLayout()
        self.TestSelection.setSpacing(8)
        self.TestTypelabel = QLabel()
        self.TestTypelabel.setText("Test Type:")
        self.TestTypeBox = QComboBox()
        self.TestTypeBox.setEditable(True)
        self.TestTypeBox.activated.connect(self.QueryLotBin)
        self.TestSelection.addWidget(self.TestTypelabel,0,0)
        self.TestSelection.addWidget(self.TestTypeBox,0,1,1,2)
        self.TestSelection.addWidget(QLabel(""),0,3,1,5)

        self.TargetSelection = QGridLayout()
        self.TargetSelection.setSpacing(8)
        self.TargetTypeLabel = QLabel()
        self.TargetTypeLabel.setText("Target:") 
        self.BinSelection = QCheckBox("Bin")
        self.BinSelection.setChecked(True)
        self.BinSelection.clicked.connect(self.SwitchClassification2)
        self.YieldSelection = QCheckBox("Yield")
        self.YieldSelection.setChecked(False)
        self.YieldSelection.clicked.connect(self.SwitchClassification1)
        self.TargetSelection.addWidget(self.TargetTypeLabel,0,0)
        self.TargetSelection.addWidget(self.BinSelection,0,1,1,1)
        self.TargetSelection.addWidget(self.YieldSelection,0,2,1,1)
        self.TargetSelection.addWidget(QLabel(""),0,3,1,5)
    
        self.Classification = QGridLayout()
        self.Classification.setSpacing(8)
        self.Classificationlabel = QLabel()
        self.Classificationlabel.setAlignment(Qt.AlignTop)
        self.Classificationlabel.setText("Classification (%):")
        
        self.Classification_Bar_Stacked = QStackedWidget()
        self.Classification_Bar_Bin = QLabeledDoubleRangeSlider(Qt.Horizontal)
        self.Classification_Bar_Bin.setRange(0,10)
        self.Classification_Bar_Bin.setValue([2,8])
        self.Classification_Bar_Bin.valueChanged.connect(self.classvaluebin)
        
        self.Classification_Bar_Yield = QLabeledDoubleRangeSlider(Qt.Horizontal)
        self.Classification_Bar_Yield.setDecimals(1)
        self.Classification_Bar_Yield.setRange(0,100)
        self.Classification_Bar_Yield.setValue([85,95])
        self.Classification_Bar_Yield.valueChanged.connect(self.classvalueyield)
        
        self.Classification_Bar_Stacked.addWidget(self.Classification_Bar_Bin)
        self.Classification_Bar_Stacked.addWidget(self.Classification_Bar_Yield)
        
        self.Classification.addWidget(self.Classificationlabel,1,0,2,1)
        self.Classification.addWidget(self.Classification_Bar_Stacked,0,1,2,7)
        self.Classification_Bar_Stacked.setCurrentIndex(0)
        
        self.Groupby_layout = QGridLayout()
        self.Groupby_layout.setSpacing(8)
        self.Groupbylabel = QLabel()
        self.Groupbylabel.setText("Group by:")
        self.LotGroupSelection = QCheckBox("Lot")
        self.LotGroupSelection.setChecked(True)
        self.ToolGroupSelection = QCheckBox("Tool")
        self.Toollabel = QLabel('Step:')
        self.Toollabel.setAlignment(Qt.AlignRight|Qt.AlignCenter)
        self.Toollabel.hide()
        self.StepSelection = QComboBox()
        self.StepSelection.activated.connect(self.QueryTool)
        self.StepSelection.setEditable(True)
        self.StepSelection.hide()
        self.ToggleGroupSelection = QPushButton("Switch Group")
        self.ToggleGroupSelection.clicked.connect(self.ToggleBiplot)
        self.LotGroupSelection.clicked.connect(self.Group_traverse1)
        self.ToolGroupSelection.clicked.connect(self.Group_traverse2)
        self.Groupby_layout.addWidget(self.Groupbylabel,0,0)
        self.Groupby_layout.addWidget(self.LotGroupSelection,0,1,1,1)
        self.Groupby_layout.addWidget(self.ToolGroupSelection,0,2,1,1)
        self.Groupby_layout.addWidget(self.Toollabel,0,3,1,1)
        self.Groupby_layout.addWidget(self.StepSelection,0,4,1,3)
        self.Groupby_layout.addWidget(self.ToggleGroupSelection,0,7,1,1)
        
        self.Selection = QGridLayout()
        self.Selection.setSpacing(8)
        self.Selectionlabel = QLabel()
        self.Selectionlabel.setText("Selection:")
        self.LotSelection = QCheckBox("Lot")
        self.TimeFrameSelection = QCheckBox("Time Frame")
        self.LotSelection.setChecked(True)
        self.Selection.addWidget(self.Selectionlabel,0,0)
        self.Selection.addWidget(self.LotSelection,0,1,1,1)
        self.Selection.addWidget(self.TimeFrameSelection,0,2,1,1)
        self.Selection.addWidget(QLabel(""),0,3,1,5)
        self.TimeFrameSelection.clicked.connect(self.display_traverse1)
        self.LotSelection.clicked.connect(self.display_traverse2)
    
        self.SelectionList_layout = QGridLayout()
        self.SelectionList_layout.setAlignment(Qt.AlignTop)
        self.SelectionList_layout.setSpacing(13)
        self.LotListInput = QTableWidget()
        self.LotListInput.setFont(QFont('Arial', 8))
        self.LotListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.LotListInput.setSortingEnabled(True)
        self.BinListInput = QTableWidget()
        self.BinListInput.setFont(QFont('Arial', 8))
        self.BinListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.BinListInput.setSortingEnabled(True)
        self.SelectionInputListTab = QTabWidget()
        self.SelectionInputListTab.addTab(self.LotListInput, "Lot List")
        self.SelectionInputListTab.addTab(self.BinListInput,"Bin List")
        self.SelectionInputListTab.tabBarClicked.connect(self.SwitchSelectionList)
        
        self.LotListOutput = QTableWidget()
        self.LotListOutput.setFont(QFont('Arial', 8))
        self.LotListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.LotListOutput.setSortingEnabled(True)
        self.BinListOutput = QTableWidget()
        self.BinListOutput.setFont(QFont('Arial', 8))
        self.BinListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.BinListOutput.setSortingEnabled(True)
        self.SelectionOutputListTab = QTabWidget()
        self.SelectionOutputListTab.addTab(self.LotListOutput, "Lot List")
        self.SelectionOutputListTab.addTab(self.BinListOutput,"Bin List")
        self.SelectionOutputListTab.tabBarClicked.connect(self.SwitchSelectionList)
        self.LotListOutputTrack = 0
        self.BinListOutputTrack = 0
        
        self.Forward_Button = QToolButton()
        self.Forward_Button.setArrowType(Qt.RightArrow) 
        self.Forward_Button.clicked.connect(self.Forward)
        
        self.Full_Forward_Button = QToolButton()
        self.Full_Forward_Button.setIcon(self.Full_Forward_Button.style().standardIcon(QStyle.SP_MediaSeekForward))
        self.Full_Forward_Button.clicked.connect(self.FullForward)
        
        self.Backward_Button = QToolButton()
        self.Backward_Button.setArrowType(Qt.LeftArrow)
        self.Backward_Button.clicked.connect(self.Backward)
        
        self.Full_Backward_Button = QToolButton()
        self.Full_Backward_Button.setIcon(self.Full_Backward_Button.style().standardIcon(QStyle.SP_MediaSeekBackward))
        self.Full_Backward_Button.clicked.connect(self.FullBackward)

        self.SelectionList_layout.addWidget(self.SelectionInputListTab,0,0,15,6)
        self.SelectionList_layout.addWidget(self.SelectionOutputListTab,0,8,15,6)
        self.SelectionList_layout.addWidget(self.Forward_Button,6,7,1,1)
        self.SelectionList_layout.addWidget(self.Full_Forward_Button,7,7,1,1)
        self.SelectionList_layout.addWidget(self.Backward_Button,8,7,1,1)
        self.SelectionList_layout.addWidget(self.Full_Backward_Button,9,7,1,1)
        
        self.Bin_layout = QFormLayout()
        self.Bin_layout.setContentsMargins(0,0,0,0)
        self.Bin_No = QLineEdit()
        self.Bin_layout.addRow("Bin:", self.Bin_No)
        
        self.Analyze_layout = QGridLayout()
        self.Analyze_layout.setSpacing(11)
        self.Lot_button = QPushButton('Analyze')
        self.Lot_button.clicked.connect(self.RunScript)
        self.Analyze_layout.addWidget(self.Lot_button,0,1,1,7)
        
        self.SystemLog = QLabel()
        self.SystemLog.setText("System log:")
        
        self.SystemLog_Dialog = QTextEdit()
        
        self.ProgressBarlabel = QLabel()
        self.ProgressBarlabel.setText("Progress:")
        self.ProgressBar = QProgressBar()
        
        self.Lot_container = QWidget()     
        self.TimeFrame_container = QWidget()

        self.ProductLotUi()
        self.TimeFrameUi()
        
        self.pca_menu = QStackedWidget()
        self.pca_menu.addWidget(self.Lot_container) 
        self.pca_menu.addWidget(self.TimeFrame_container) 
        self.pca_menu.setCurrentIndex(0)

        self.Menu_left_layout = QVBoxLayout()
        self.Menu_left_layout.addWidget(self.menuButton1)
        self.Menu_left_layout.addLayout(self.Output_container)
        self.Menu_left_layout.addWidget(self.Title)
        self.Menu_left_layout.addLayout(self.DataSource_layout)
        self.Menu_left_layout.addLayout(self.Node_layout)
        self.Menu_left_layout.addLayout(self.Process_tech_category_layout)
        self.Menu_left_layout.addLayout(self.Product_layout)
        self.Menu_left_layout.addLayout(self.TestSelection)         
        self.Menu_left_layout.addLayout(self.TargetSelection)
        self.Menu_left_layout.addLayout(self.Classification)
        self.Menu_left_layout.addLayout(self.Groupby_layout)
        self.Menu_left_layout.addLayout(self.Selection)
        self.Menu_left_layout.addWidget(self.pca_menu)
        self.Menu_left_layout.addLayout(self.Bin_layout)
        self.Menu_left_layout.addLayout(self.Analyze_layout)
        self.Menu_left_layout.addLayout(self.SelectionList_layout)
        self.Menu_left_layout.addWidget(self.SystemLog)
        self.Menu_left_layout.addWidget(self.SystemLog_Dialog)
        self.Menu_left_layout.addWidget(self.ProgressBarlabel)
        self.Menu_left_layout.addWidget(self.ProgressBar)
        
        self.Biplot_Scroll = QScrollArea()
        self.Biplot_Scroll.setWidgetResizable(True)
        self.Biplot_container = QWidget()
        self.Biplot_layout = QGridLayout()
        self.Biplot_container.setLayout(self.Biplot_layout)
        self.Biplot_layout.setContentsMargins(0,0,0,0)
        self.Biplot_layout.setSpacing(11)
        
        self.imageLabel = QLabel("")
        self.imageLabel.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        
        self.Biplot_left_column_dummy_button = QPushButton("")
        self.Biplot_right_column_dummy_button = QPushButton("")
        self.Biplot_top_row_dummy_button = QPushButton("")
        self.Biplot_bottom_row_dummy_button = QPushButton("")
        self.Biplot_button12 = QPushButton("")
        self.Biplot_button13 = QPushButton("")
        self.Biplot_button23 = QPushButton("")
        self.Biplot_button14 = QPushButton("")
        self.Biplot_button24 = QPushButton("")
        self.Biplot_button34 = QPushButton("")
        self.Biplot_button15 = QPushButton("")
        self.Biplot_button25 = QPushButton("")
        self.Biplot_button35 = QPushButton("")
        self.Biplot_button45 = QPushButton("")
        self.Biplot_button16 = QPushButton("")
        self.Biplot_button26 = QPushButton("")
        self.Biplot_button36 = QPushButton("")
        self.Biplot_button46 = QPushButton("")
        self.Biplot_button56 = QPushButton("")
        self.Biplot_button17 = QPushButton("")
        self.Biplot_button27 = QPushButton("")
        self.Biplot_button37 = QPushButton("")
        self.Biplot_button47 = QPushButton("")
        self.Biplot_button57 = QPushButton("")
        self.Biplot_button67 = QPushButton("")
        self.Biplot_button18 = QPushButton("")
        self.Biplot_button28 = QPushButton("")
        self.Biplot_button38 = QPushButton("")
        self.Biplot_button48 = QPushButton("")
        self.Biplot_button58 = QPushButton("")
        self.Biplot_button68 = QPushButton("")
        self.Biplot_button78 = QPushButton("")
        self.Biplot_button19 = QPushButton("")
        self.Biplot_button29 = QPushButton("")
        self.Biplot_button39 = QPushButton("")
        self.Biplot_button49 = QPushButton("")
        self.Biplot_button59 = QPushButton("")
        self.Biplot_button69 = QPushButton("")
        self.Biplot_button79 = QPushButton("")
        self.Biplot_button89 = QPushButton("")
        self.Biplot_button110 = QPushButton("")
        self.Biplot_button210 = QPushButton("")
        self.Biplot_button310 = QPushButton("")
        self.Biplot_button410 = QPushButton("")
        self.Biplot_button510 = QPushButton("")        
        self.Biplot_button610 = QPushButton("")
        self.Biplot_button710 = QPushButton("")
        self.Biplot_button810 = QPushButton("")
        self.Biplot_button910 = QPushButton("")
        
        self.Biplot_button_size = QSize(275,275)
        self.Biplot_left_column_dummy_button.setFixedSize(65,2500)
        self.Biplot_right_column_dummy_button.setFixedSize(440,2500)
        self.Biplot_top_row_dummy_button.setFixedSize(250,275)
        self.Biplot_bottom_row_dummy_button.setFixedSize(250,50)
        
        self.Biplot_button_group = QButtonGroup()
        self.Biplot_button_group.addButton(self.Biplot_button12)
        self.Biplot_button_group.addButton(self.Biplot_button13)
        self.Biplot_button_group.addButton(self.Biplot_button23)
        self.Biplot_button_group.addButton(self.Biplot_button14)
        self.Biplot_button_group.addButton(self.Biplot_button24)
        self.Biplot_button_group.addButton(self.Biplot_button34) 
        self.Biplot_button_group.addButton(self.Biplot_button15)
        self.Biplot_button_group.addButton(self.Biplot_button25)
        self.Biplot_button_group.addButton(self.Biplot_button35)
        self.Biplot_button_group.addButton(self.Biplot_button45)
        self.Biplot_button_group.addButton(self.Biplot_button16)
        self.Biplot_button_group.addButton(self.Biplot_button26)
        self.Biplot_button_group.addButton(self.Biplot_button36)
        self.Biplot_button_group.addButton(self.Biplot_button46)
        self.Biplot_button_group.addButton(self.Biplot_button56)
        self.Biplot_button_group.addButton(self.Biplot_button17)
        self.Biplot_button_group.addButton(self.Biplot_button27)
        self.Biplot_button_group.addButton(self.Biplot_button37)
        self.Biplot_button_group.addButton(self.Biplot_button47)
        self.Biplot_button_group.addButton(self.Biplot_button57)
        self.Biplot_button_group.addButton(self.Biplot_button67)
        self.Biplot_button_group.addButton(self.Biplot_button18)
        self.Biplot_button_group.addButton(self.Biplot_button28)
        self.Biplot_button_group.addButton(self.Biplot_button38)
        self.Biplot_button_group.addButton(self.Biplot_button48)
        self.Biplot_button_group.addButton(self.Biplot_button58)
        self.Biplot_button_group.addButton(self.Biplot_button68)
        self.Biplot_button_group.addButton(self.Biplot_button78)                               
        self.Biplot_button_group.addButton(self.Biplot_button19)
        self.Biplot_button_group.addButton(self.Biplot_button29)
        self.Biplot_button_group.addButton(self.Biplot_button39)
        self.Biplot_button_group.addButton(self.Biplot_button49)
        self.Biplot_button_group.addButton(self.Biplot_button59)
        self.Biplot_button_group.addButton(self.Biplot_button69)
        self.Biplot_button_group.addButton(self.Biplot_button79)
        self.Biplot_button_group.addButton(self.Biplot_button89)
        self.Biplot_button_group.addButton(self.Biplot_button110)
        self.Biplot_button_group.addButton(self.Biplot_button210)
        self.Biplot_button_group.addButton(self.Biplot_button310)
        self.Biplot_button_group.addButton(self.Biplot_button410)
        self.Biplot_button_group.addButton(self.Biplot_button510)
        self.Biplot_button_group.addButton(self.Biplot_button610)
        self.Biplot_button_group.addButton(self.Biplot_button710)
        self.Biplot_button_group.addButton(self.Biplot_button810)
        self.Biplot_button_group.addButton(self.Biplot_button910)
        
        for button in self.Biplot_button_group.buttons():
            button.setFlat(True)
            button.setFixedSize(self.Biplot_button_size)
        
        self.Biplot_button_group.buttonClicked[int].connect(self.Plot_Biplot)
        
        self.Biplot_left_column_dummy_button.setFlat(True)
        self.Biplot_right_column_dummy_button.setFlat(True)
        self.Biplot_top_row_dummy_button.setFlat(True)
        self.Biplot_bottom_row_dummy_button.setFlat(True)

        self.Biplot_layout.addWidget(self.imageLabel,0,0,11,11)
        
        self.Biplot_layout.addWidget(self.Biplot_left_column_dummy_button,1,0,10,1)
        self.Biplot_layout.addWidget(self.Biplot_right_column_dummy_button,1,10,10,1)
        self.Biplot_layout.addWidget(self.Biplot_top_row_dummy_button,0,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_bottom_row_dummy_button,10,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button12,1,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button13,2,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button14,3,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button15,4,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button16,5,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button17,6,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button18,7,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button19,8,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button110,9,1,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button23,2,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button24,3,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button25,4,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button26,5,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button27,6,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button28,7,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button29,8,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button210,9,2,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button34,3,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button35,4,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button36,5,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button37,6,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button38,7,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button39,8,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button310,9,3,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button45,4,4,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button46,5,4,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button47,6,4,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button48,7,4,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button49,8,4,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button410,9,4,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button56,5,5,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button57,6,5,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button58,7,5,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button59,8,5,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button510,9,5,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button67,6,6,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button68,7,6,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button69,8,6,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button610,9,6,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button78,7,7,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button79,8,7,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button710,9,7,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button89,8,8,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button810,9,8,1,1)
        self.Biplot_layout.addWidget(self.Biplot_button910,9,9,1,1)
        
        self.Biplot_Scroll.setWidget(self.Biplot_container)
        
        self.PC1 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC1 = NavigationToolbar(self.PC1, self)
        self.biplot_containerPC1 = QWidget()
        self.biplot_layoutPC1 = QVBoxLayout(self.biplot_containerPC1)  
        self.biplot_layoutPC1.addWidget(self.toolbarPC1)
        self.biplot_layoutPC1.addWidget(self.PC1)  
        
        self.PC1 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC1 = NavigationToolbar(self.PC1, self)
        self.biplot_containerPC1 = QWidget()
        self.biplot_layoutPC1 = QVBoxLayout(self.biplot_containerPC1)  
        self.biplot_layoutPC1.addWidget(self.toolbarPC1)
        self.biplot_layoutPC1.addWidget(self.PC1)  
        
        self.PC2 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC2 = NavigationToolbar(self.PC2, self)
        self.biplot_containerPC2 = QWidget()
        self.biplot_layoutPC2 = QVBoxLayout(self.biplot_containerPC2)  
        self.biplot_layoutPC2.addWidget(self.toolbarPC2)
        self.biplot_layoutPC2.addWidget(self.PC2)  
        
        self.PC3 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC3 = NavigationToolbar(self.PC3, self)
        self.biplot_containerPC3 = QWidget()
        self.biplot_layoutPC3 = QVBoxLayout(self.biplot_containerPC3)  
        self.biplot_layoutPC3.addWidget(self.toolbarPC3)
        self.biplot_layoutPC3.addWidget(self.PC3)  
        
        self.PC4 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC4 = NavigationToolbar(self.PC4, self)
        self.biplot_containerPC4 = QWidget()
        self.biplot_layoutPC4 = QVBoxLayout(self.biplot_containerPC4)  
        self.biplot_layoutPC4.addWidget(self.toolbarPC4)
        self.biplot_layoutPC4.addWidget(self.PC4)  
        
        self.PC5 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC5 = NavigationToolbar(self.PC5, self)
        self.biplot_containerPC5 = QWidget()
        self.biplot_layoutPC5 = QVBoxLayout(self.biplot_containerPC5)  
        self.biplot_layoutPC5.addWidget(self.toolbarPC5)
        self.biplot_layoutPC5.addWidget(self.PC5)  
        
        self.PC6 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC6 = NavigationToolbar(self.PC6, self)
        self.biplot_containerPC6 = QWidget()
        self.biplot_layoutPC6 = QVBoxLayout(self.biplot_containerPC6)  
        self.biplot_layoutPC6.addWidget(self.toolbarPC6)
        self.biplot_layoutPC6.addWidget(self.PC6)  
        
        self.PC7 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC7 = NavigationToolbar(self.PC7, self)
        self.biplot_containerPC7 = QWidget()
        self.biplot_layoutPC7 = QVBoxLayout(self.biplot_containerPC7)  
        self.biplot_layoutPC7.addWidget(self.toolbarPC7)
        self.biplot_layoutPC7.addWidget(self.PC7)  
        
        self.PC8 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC8 = NavigationToolbar(self.PC8, self)
        self.biplot_containerPC8 = QWidget()
        self.biplot_layoutPC8 = QVBoxLayout(self.biplot_containerPC8)  
        self.biplot_layoutPC8.addWidget(self.toolbarPC8)
        self.biplot_layoutPC8.addWidget(self.PC8)  
        
        self.PC9 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC9 = NavigationToolbar(self.PC9, self)
        self.biplot_containerPC9 = QWidget()
        self.biplot_layoutPC9 = QVBoxLayout(self.biplot_containerPC9)  
        self.biplot_layoutPC9.addWidget(self.toolbarPC9)
        self.biplot_layoutPC9.addWidget(self.PC9)  
        
        self.PC10 = MplCanvasPlot(10.5, 6, 100)
        self.toolbarPC10 = NavigationToolbar(self.PC10, self)
        self.biplot_containerPC10 = QWidget()
        self.biplot_layoutPC10 = QVBoxLayout(self.biplot_containerPC10)  
        self.biplot_layoutPC10.addWidget(self.toolbarPC10)
        self.biplot_layoutPC10.addWidget(self.PC10)  
        
        self.screeplot = MplCanvasPlot(10.5, 6, 100)
        self.screeplot_toolbar = NavigationToolbar(self.screeplot, self)
        self.screeplot_container = QWidget()
        self.screeplot_layout = QVBoxLayout(self.screeplot_container)  
        self.screeplot_layout.addWidget(self.screeplot_toolbar)
        self.screeplot_layout.addWidget(self.screeplot)
        self.setLayout(self.screeplot_layout)
        
        self.HeatMapplot = MplCanvasPlot(25, 80, 100)
        self.HeatMapplot_toolbar = NavigationToolbar(self.HeatMapplot, self)
        self.HeatMapplot_container = QWidget()
        self.HeatMapplot_layout = QVBoxLayout(self.HeatMapplot_container)  
        self.HeatMapplot_layout.addWidget(self.HeatMapplot_toolbar)
        self.HeatMapplot_layout.addWidget(self.HeatMapplot)
        self.setLayout(self.HeatMapplot_layout)
        
        self.HeatMapScroll = QScrollArea()
        self.HeatMapScroll.setWidget(self.HeatMapplot_container)   
     
        self.PCTable = QTableWidget()
        self.PCTab = QTabWidget()
        self.PCTab.addTab(self.PCTable, 'Table')
        self.PCTab.addTab(self.HeatMapScroll, 'Correlation Heat Map')
        self.PCTab.addTab(self.biplot_containerPC1, 'PC1')
        self.PCTab.addTab(self.biplot_containerPC2, 'PC2')
        self.PCTab.addTab(self.biplot_containerPC3, 'PC3')
        self.PCTab.addTab(self.biplot_containerPC4, 'PC4')
        self.PCTab.addTab(self.biplot_containerPC5, 'PC5')
        self.PCTab.addTab(self.biplot_containerPC6, 'PC6')
        self.PCTab.addTab(self.biplot_containerPC7, 'PC7')
        self.PCTab.addTab(self.biplot_containerPC8, 'PC8')
        self.PCTab.addTab(self.biplot_containerPC9, 'PC9')
        self.PCTab.addTab(self.biplot_containerPC10, 'PC10')
        
        self.Output = QTabWidget()
        self.Output.addTab(self.Biplot_Scroll, "Biplot")
        self.Output.addTab(self.screeplot_container,"Scree-plot")
        self.Output.addTab(self.PCTab,"Principal Components")
        
        self.Biplot_buttons = []
        
        self.PC_list = [self.PC1,self.PC2,self.PC3,self.PC4,self.PC5,self.PC6,self.PC7,self.PC8,self.PC9,self.PC10]
        
        self.Menu_right_container = QWidget()
        self.Menu_right_layout = QVBoxLayout()
        self.Menu_right_layout.addWidget(self.Output)

        self.SelectionMenu_layout.addLayout(self.Menu_left_layout)
        self.SelectionMenu.setLayout(self.SelectionMenu_layout)

    def ProductLotUi(self):
        self.Lot_layout = QFormLayout()
        self.Lot_layout.setContentsMargins(0,0,0,0)
        self.Lot_No = QLineEdit()
        self.Lot_No.textChanged.connect(self.QueryStep)
        self.Lot_layout.addRow("Lot No.:", self.Lot_No)
        self.Lot_container.setLayout(self.Lot_layout)
        
    def TimeFrameUi(self):
        self.TimeFrame_container_layout = QGridLayout()
        self.TimeFrame_container_layout.setSpacing(10)  
        self.TimeFrame_container_layout.setContentsMargins(0,0,0,0)
        
        self.Datelabel = QLabel()
        self.Datelabel.setText("Date Range: From")
        self.DateTolabel = QLabel()
        self.DateTolabel.setText("To")
        self.DateTolabel.setAlignment(Qt.AlignCenter)
        
        self.Earlier_Date = QDateEdit()
        self.Later_Date = QDateEdit()
        self.Earlier_Date.setDisplayFormat("yyyy-MM-dd")
        self.Later_Date.setDisplayFormat("yyyy-MM-dd")
        self.Earlier_Date.setDate(QDate(2021,1,1))
        self.Later_Date.setDate(QDate.currentDate())
        self.Earlier_Date.setCalendarPopup(True)
        self.Later_Date.setCalendarPopup(True)
        
        self.TimeFrame_searchbutton = QPushButton('Search')
        self.TimeFrame_searchbutton.clicked.connect(self.SearchUp)
        
        self.TimeFrame_container_layout.addWidget(self.Datelabel,0,0,1,1)
        self.TimeFrame_container_layout.addWidget(self.Earlier_Date,0,1,1,3)
        self.TimeFrame_container_layout.addWidget(self.DateTolabel,0,4,1,1)
        self.TimeFrame_container_layout.addWidget(self.Later_Date,0,5,1,3)
        self.TimeFrame_container_layout.addWidget(self.TimeFrame_searchbutton,0,8,1,2)
        self.TimeFrame_container.setLayout(self.TimeFrame_container_layout)    
     
    def Plot_Biplot(self, id):
        # Display function to show various Biplot upon clicking on the respestive buttons
        for button in self.Biplot_button_group.buttons():
            if button is self.Biplot_button_group.button(id):
                self.biplot_container = QWidget()
                self.web = QWebEngineView()
                self.biplot_layout = QVBoxLayout(self.biplot_container) 
                self.biplot_layout.addWidget(self.web)     
                plotly.offline.plot(self.Biplot_list[self.Biplot_loc[abs(id) - 2]], filename=f'{self.outputfoldercount}/{self.Biplot_loc[abs(id) - 2]}.html', auto_open=False)
                self.web.load(QUrl.fromLocalFile(f'{self.outputfoldercount}/{self.Biplot_loc[abs(id) - 2]}.html'))
                self.web.show()
                self.biplot_container.setWindowTitle(f'PC{self.Biplot_loc[abs(id) - 2][6]} vs PC{self.Biplot_loc[abs(id) - 2][7]}')
                self.biplot_container.resize(self.width, self.height)
                self.biplot_container.setLayout(self.biplot_layout)
                self.biplot_container.show()
        
    def PCA_Output(self):
        # Ui functions showing the Output Window (Biplot, PCA Correlation table, Barplot)
        self.Menu_right_container.setWindowTitle('PCA Output')
        self.Menu_right_container.resize(self.width, self.height)
        self.Menu_right_container.setLayout(self.Menu_right_layout)
        self.Menu_right_container.show()
    
    def ImportDataSource(self):          
        folderpath = QFileDialog.getOpenFileName(self, 'Select data to import'," ", "Excel (*.xlsx);;Comma Separated Values (*.csv)",)
        self.SystemMessage('Importing Data...')
        
        if self.SelectionInputListTab.count() == 3 and self.SelectionOutputListTab.count() == 3:
            self.SelectionInputListTab.removeTab(3)
            self.SelectionOutputListTab.removeTab(3)
            
        else:
            self.ImportDataHeadersInput = QTableWidget()
            self.ImportDataHeadersInput.setFont(QFont('Arial', 8))
            self.ImportDataHeadersInput.setSelectionBehavior(QTableWidget.SelectRows)
            self.ImportDataHeadersInput.setSortingEnabled(True)
            self.SelectionInputListTab.addTab(self.ImportDataHeadersInput, "Import Data Column")
            
            self.ImportDataHeadersOutput = QTableWidget()
            self.ImportDataHeadersOutput.setFont(QFont('Arial', 8))
            self.ImportDataHeadersOutput.setSelectionBehavior(QTableWidget.SelectRows)
            self.ImportDataHeadersOutput.setSortingEnabled(True)
            self.SelectionOutputListTab.addTab(self.ImportDataHeadersOutput, "Import Data Column")
        
        if folderpath[1] == 'Excel (*.xlsx)':
            self.ImportedData_name = folderpath[0].split('/')[-1].split('.')[0]
            self.ImportedData = pd.ExcelFile(folderpath[0])
            self.ImportedSheets_name = self.ImportedData.sheet_names
            self.ImportedSheets = {sheet:self.ImportedData.parse(sheet) for sheet in self.ImportedData.sheet_names}
            self.ImportDataExcelHeaders()
        else:
            self.ImportedData_name = folderpath[0].split('/')[-1].split('.')[0]
            self.ImportedSheets_name = [self.ImportedData_name]
            self.Importedcsv = pd.read_csv(folderpath[0])
            self.ImportDatacsvHeaders()
            
        self.DataSource_filelabel.setText(f"{self.ImportedData_name} (Use wafer level data in Imported file only)")
            
    def GetData(self, data):
        self.full_data = data[0]
        self.biplot_data = data[1]
        
    def ExportData(self):
        folderpath = QFileDialog.getExistingDirectory(self, 'Select directory to save data')
        WriteData = pd.ExcelWriter(f"{folderpath}/PCA {self.Product_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.xlsx")

        self.full_data.to_excel(WriteData, sheet_name = 'Data')
        self.PC_Screeplot.to_excel(WriteData, sheet_name = 'PC Importance')
        self.df_principal_components.to_excel(WriteData, sheet_name = 'Principal Components')
        self.biplot_data.to_excel(WriteData, sheet_name = 'Biplot Data')
        
        if self.ToolGroupSelection.isChecked():
            self.StepTool.to_excel(WriteData, sheet_name = 'Tool Data')
            
        WriteData.save()

    def QueryTech(self):
        # Function to Query Tech to update Tech DropBox in PCA Selection Menu UI
        with QueryThread('None','None','None') as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateTech)
            
    def UpdateTech(self, query):
        # Update Tech DropBox in PCA Selection Menu UI Queried from QueryTech()
        self.Node_Box.addItems(query)
    
    def QueryProcessTechCategory(self):
        # Function to Query Process to update Process DropBox in PCA Selection Menu UI
        with QueryThread('Process_Tech_Category', self.Node_Box.currentText(), 'None') as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateProcessTechCategory)
        
    def UpdateProcessTechCategory(self, query):
        # Update Process DropBox in PCA Selection Menu UI Queried from QueryTech()
        self.Process_tech_category_Box.clear()
        self.Process_tech_category_Box.addItems(query)
        
    def QueryProduct(self):
        # Function to Query Product to update Product DropBox in PCA Selection Menu UI
        with QueryThread('Product', self.Process_tech_category_Box.currentText(), 'None') as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateProduct)
        
    def UpdateProduct(self, query):
        # Update Product DropBox in PCA Selection Menu UI Queried from QueryTech()
        self.Product_No.clear()
        self.Product_No.addItems(query)
        
    def QueryTest(self):
        # Function to Query Test Type to update Test Type DropBox in PCA Selection Menu UI
        with QueryThread('Test', self.Product_No.currentText(), 'None') as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateTest)
      
    def UpdateTest(self, query):
        # Update Test Type DropBox in PCA Selection Menu UI Queried from QueryTech()
        self.TestTypeBox.clear()
        self.TestTypeBox.addItems(query)
    
    def QueryLotBin(self):
        # Function to Query Bin to update Bin List in PCA Selection Menu UI
        with QueryThread('Lot Bin', self.Product_No.currentText(), [self.TestTypeBox.currentText(), 'None']) as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateLot)
            self.QueryThread.query_result2.connect(self.UpdateBin)
            
    def UpdateBin(self, query):     
        # Function to update Bin List in PCA Selection Menu UI Queried from QueryLotBin()
        self.BinListOutputTrack = 0
        if len(query) == 0:
            self.SystemMessage('Error! No Bin Found')
            return
        self.BinListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.BinListInput.setRowCount(rows)
        self.BinListInput.setColumnCount(cols)
        self.BinListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.BinListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                if j == 0:
                    item = QTableWidgetItem(str(query.iloc[i,j].decode('utf-8')))
                    self.BinListInput.setItem(i, j, item)
                else:    
                    item = QTableWidgetItem(str(query.iloc[i,j]))
                    self.BinListInput.setItem(i, j, item)

        self.BinListInput.resizeColumnsToContents()
        self.BinListInput.resizeRowsToContents()
        
        self.BinListOutput.setColumnCount(cols)
        self.BinListOutput.setHorizontalHeaderLabels(query.columns.tolist())
        
    def UpdateLot(self, query):
        # Function to update Lot List in PCA Selection Menu UI Queried from QueryLotBin()
        self.LotListOutputTrack = 0
        if len(query) == 0:
            self.SystemMessage('Error! No Lot found')
            return
        self.LotListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.LotListInput.setRowCount(rows)
        self.LotListInput.setColumnCount(cols)
        self.LotListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.LotListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                item = QTableWidgetItem(str(query.iloc[i,j]))
                self.LotListInput.setItem(i, j, item)
                
        self.LotListInput.resizeColumnsToContents()
        self.LotListInput.resizeRowsToContents()
        
        self.LotListOutput.setColumnCount(cols)
        self.LotListOutput.setHorizontalHeaderLabels(query.columns.tolist())
        
    def ImportDataExcelHeaders(self):
        # Function to acquire data/info headers from imported Excel
        # Called upon checking the 'Import' Chechbox selecting targeted Excel to read from
        self.ImportDataHeadersListOutputTrack = 0
        self.ImportDataHeaders_List_count = 0
        
        self.ImportDataHeadersInput.setRowCount(sum([len(self.ImportedSheets[self.ImportedSheets_name[i]].columns) for i in range(len(self.ImportedSheets))]))
        self.ImportDataHeadersInput.setColumnCount(2)
        self.ImportDataHeadersInput.setHorizontalHeaderLabels(['Sheet Name','Data Headers'])
        self.ImportDataHeadersInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)  
        
        count = 0
        for i in range(len(self.ImportedSheets_name)):
            for j in range(len(self.ImportedSheets[self.ImportedSheets_name[i]].columns)):
                sheet = QTableWidgetItem(self.ImportedSheets_name[i])
                header = QTableWidgetItem(self.ImportedSheets[self.ImportedSheets_name[i]].columns[j])
                self.ImportDataHeadersInput.setItem(count, 0, sheet)
                self.ImportDataHeadersInput.setItem(count, 1, header)
                count += 1
        self.ImportDataHeadersInput.resizeColumnsToContents()
        self.ImportDataHeadersInput.resizeRowsToContents()
        
        self.ImportDataHeadersOutput.setRowCount(0)
        self.ImportDataHeadersOutput.setColumnCount(2)
        self.ImportDataHeadersOutput.setHorizontalHeaderLabels(['Sheet Name','Data Headers'])
    
    def ImportDatacsvHeaders(self):
        # Function to acquire data/info headers from imported CSV
        # Called upon checking the 'Import' Chechbox and selecting targeted CSV to read from
        self.ImportDataHeadersListOutputTrack = 0
        self.ImportDataHeaders_List_count = 0
        
        self.ImportDataHeadersInput.setRowCount(len(self.Importedcsv))
        self.ImportDataHeadersInput.setColumnCount(2)
        self.ImportDataHeadersInput.setHorizontalHeaderLabels(['Sheet Name','Data Headers'])
        self.ImportDataHeadersInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)  
        
        for n in range(len(self.Importedcsv.columns)):
            sheet = QTableWidgetItem(self.ImportedSheets_name[0])
            header = QTableWidgetItem(self.Importedcsv.columns[n])
            self.ImportDataHeadersInput.setItem(n, 0, sheet)
            self.ImportDataHeadersInput.setItem(n, 1, header)
            
        self.ImportDataHeadersInput.resizeColumnsToContents()
        self.ImportDataHeadersInput.resizeRowsToContents()
        
        self.ImportDataHeadersOutput.setRowCount(0)
        self.ImportDataHeadersOutput.setColumnCount(2)
        self.ImportDataHeadersOutput.setHorizontalHeaderLabels(['Sheet Name','Data Headers'])
    
    def QueryStep(self):   
        # Function to Query Step AND Step Name to update Step DropBox in PCA Selection Menu UI
        # Called upon checking on 'Tool' Checkbox in the 'Group By' option
        if self.ToolGroupSelection.isChecked():
            self.Lot_No_text = tuple(self.Lot_No.text().split(' '))
            if len(self.Lot_No.text().split(' ')) == 1:    
                self.Lot_No_text = self.Lot_No_text[0]
                
            with QueryThread('Step',self.Lot_No_text, 'None') as self.QueryThread:
                self.QueryThread.start()
                self.QueryThread.system_log.connect(self.SystemMessage)
                self.QueryThread.query_result.connect(self.UpdateStep)  
        
    def UpdateStep(self, query):
        # Update Step DropBox under 'Tool' in PCA Selection Menu UI Queried from QueryStep()
        self.StepSelection.clear()
        Step_list = []
        for n in range(len(query)):
            Step_list.append(f"{query.iloc[n][0]}:{query.iloc[n][1]}")
        self.StepSelection.addItems(Step_list)
        
    def QueryTool(self):
        # Function to Query Tool data to be used in performing PCA 
        # Called upon selcting a Step from the 'Step' Dropbox in PCA Selection Menu UI
        self.StepSelected = self.StepSelection.currentText().split(':')[0]
        with QueryThread('Tool',self.Lot_No_text, self.StepSelected) as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateTool)
            
    def UpdateTool(self, query):
        # Function to acquire Tool data Queried from QueryTool()
        self.StepTool = query

    def Forward(self):
        # Function to include selected Lot/Bin to Selected List in PCA Selection Menu UI 
        # Indicated as right arrow
        if self.SelectionInputListTab.currentIndex() == 0 and self.SelectionOutputListTab.currentIndex() == 0:
            item = self.LotListInput.selectedIndexes()
            item_list = [item[i:i+self.LotListInput.columnCount()] for i in range(0, len(item), self.LotListInput.columnCount())]
            for rows in range(len(item_list)):
                self.LotListOutput.insertRow(self.LotListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.LotListOutputTrack):
                        if self.LotListOutputTrack != 0 and cols == 0 :
                            if item_list[rows][cols].data(Qt.DisplayRole) == self.LotListOutput.item(n,cols).data(Qt.DisplayRole):
                                self.SystemMessage("Error! Attempting to add lot already exist in selection list")
                                self.LotListOutput.removeRow(self.LotListOutputTrack) 
                                return
                    self.LotListOutput.setItem(self.LotListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.LotListOutputTrack += 1   
            self.LotListOutput.resizeRowsToContents()
            self.LotListOutput.resizeColumnsToContents()
            self.CompileLotList()   
            
        elif self.SelectionInputListTab.currentIndex() == 1 and self.SelectionOutputListTab.currentIndex() == 1:
            item = self.BinListInput.selectedIndexes()
            item_list = [item[i:i+self.BinListInput.columnCount()] for i in range(0, len(item), self.BinListInput.columnCount())]
            for rows in range(len(item_list)):
                self.BinListOutput.insertRow(self.BinListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.BinListOutputTrack):
                        if self.BinListOutputTrack != 0 and item_list[rows][0].data(Qt.DisplayRole) == self.BinListOutput.item(n,0).data(Qt.DisplayRole) \
                            and item_list[rows][2].data(Qt.DisplayRole) == self.BinListOutput.item(n,2).data(Qt.DisplayRole):
                                self.SystemMessage("Error! Attempting to add bin already exist in selection list")
                                self.BinListOutput.removeRow(self.BinListOutputTrack)
                                return
                    self.BinListOutput.setItem(self.BinListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.BinListOutputTrack += 1    
            self.BinListOutput.resizeRowsToContents()
            self.BinListOutput.resizeColumnsToContents()
            self.CompileBinList() 
        
        elif self.SelectionInputListTab.currentIndex() == 2 and self.SelectionOutputListTab.currentIndex() == 2:
            item = self.ImportDataHeadersInput.selectedIndexes()
            item_list = [item[i:i+self.ImportDataHeadersInput.columnCount()] for i in range(0, len(item), self.ImportDataHeadersInput.columnCount())]
            for rows in range(len(item_list)):
                self.ImportDataHeadersOutput.insertRow(self.ImportDataHeadersListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.ImportDataHeadersListOutputTrack):
                        if self.ImportDataHeadersListOutputTrack != 0 and item_list[rows][0].data(Qt.DisplayRole) == self.ImportDataHeadersOutput.item(n,0).data(Qt.DisplayRole) \
                            and item_list[rows][1].data(Qt.DisplayRole) == self.ImportDataHeadersOutput.item(n,1).data(Qt.DisplayRole):
                                self.SystemMessage("Error! Attempting to add Column already exist in selection list")
                                return
                    self.ImportDataHeadersOutput.setItem(self.ImportDataHeadersListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.ImportDataHeadersListOutputTrack += 1    
            self.ImportDataHeadersOutput.resizeRowsToContents()
            self.ImportDataHeadersOutput.resizeColumnsToContents()
            self.CompileImportDataList()
            
        else:
            self.SystemMessage("Error! Displaying Available List does not match Selected list")
        
    def Backward(self):
        # Function to exclude selected Lot/Bin from Selected List in PCA Selection Menu UI 
        # Indicated as left arrow
        if self.SelectionInputListTab.currentIndex() == 0  and self.SelectionOutputListTab.currentIndex() == 0 and self.LotListOutputTrack !=0:
            item = self.LotListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.LotListOutput.removeRow(row_remove)
                self.LotListOutputTrack -= 1
            
            self.LotListOutput.resizeRowsToContents()
            self.CompileLotList()
            
        elif self.SelectionInputListTab.currentIndex() == 1 and self.SelectionOutputListTab.currentIndex() == 1 and self.BinListOutputTrack !=0:
            item = self.BinListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.BinListOutput.removeRow(row_remove)
                self.BinListOutputTrack -= 1
           
            self.BinListOutput.resizeRowsToContents()
            self.CompileBinList()
            
        elif self.SelectionInputListTab.currentIndex() == 2 and self.SelectionOutputListTab.currentIndex() == 2 and self.ImportDataHeadersListOutputTrack !=0:
            item = self.ImportDataHeadersOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.ImportDataHeadersOutput.removeRow(row_remove)
                self.ImportDataHeadersListOutputTrack -= 1
           
            self.ImportDataHeadersOutput.resizeRowsToContents()
            self.CompileImportDataList()
            
        else:
            self.SystemMessage("Error! Displaying Available List does not match Selected list")
        
    def FullForward(self):
        # Function to include all of Lot/Bin to Selected List in PCA Selection Menu UI 
        # Indicated as double right arrow
        if self.SelectionInputListTab.currentIndex() == 0 and self.SelectionOutputListTab.currentIndex() == 0:
            output_rows = self.LotListOutput.rowCount()
            rows = self.LotListInput.rowCount()
            cols = self.LotListInput.columnCount()
            self.LotListOutput.setRowCount(self.LotListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.LotListInput.item(i,j))
                    self.LotListOutput.setItem(output_rows + i,j,item)
            
            self.LotListOutputTrack = rows + output_rows
            self.LotListOutput.resizeRowsToContents()
            self.CompileLotList() 
            
        elif self.SelectionInputListTab.currentIndex() == 1 and self.SelectionOutputListTab.currentIndex() == 1:
            output_rows = self.BinListOutput.rowCount()
            rows = self.BinListInput.rowCount()
            cols = self.BinListInput.columnCount()
            self.BinListOutput.setRowCount(self.BinListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.BinListInput.item(i,j))
                    self.BinListOutput.setItem(output_rows + i,j,item)
                    
            self.BinListOutputTrack = rows + output_rows     
            self.BinListOutput.resizeRowsToContents()
            self.CompileBinList()
            
        elif self.SelectionInputListTab.currentIndex() == 2 and self.SelectionOutputListTab.currentIndex() == 2:
            output_rows = self.ImportDataHeadersOutput.rowCount()
            rows = self.ImportDataHeadersInput.rowCount()
            cols = self.ImportDataHeadersInput.columnCount()
            self.ImportDataHeadersOutput.setRowCount(self.ImportDataHeadersOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.ImportDataHeadersInput.item(i,j))
                    self.ImportDataHeadersOutput.setItem(output_rows + i,j,item)
                    
            self.ImportDataHeadersListOutputTrack = rows + output_rows     
            self.ImportDataHeadersOutput.resizeRowsToContents()
            self.CompileImportDataList()
            
        else:
            self.SystemMessage("Error! Displaying Available List does not match Selected list")
        
    def FullBackward(self):
        # Function to exclude all of Lot/Bin from PCA Selection Menu UI 
        # Indicated as double left arrow
        if self.SelectionInputListTab.currentIndex() == 0 and self.SelectionOutputListTab.currentIndex() == 0:
            self.LotListOutput.setRowCount(0)
            self.LotListOutputTrack = 0
            self.CompileLotList()
            
        elif self.SelectionInputListTab.currentIndex() == 1 and self.SelectionOutputListTab.currentIndex() == 1:
            self.BinListOutput.setRowCount(0)
            self.BinListOutputTrack = 0
            self.CompileBinList()
            
        elif self.SelectionInputListTab.currentIndex() == 2 and self.SelectionOutputListTab.currentIndex() == 2:
            self.ImportDataHeadersOutput.setRowCount(0)
            self.ImportDataHeadersListOutputTrack = 0
            self.CompileImportDataList()
            
        else:
            self.SystemMessage("Error! Displaying Available List does not match Selected list")

    def CompileLotList(self):
        # Function to consolidate selected Lot list to be used for PCA
        # Called upon making any changes on the Lot Selected list
        Lot_List = []
        for n in range(self.LotListOutputTrack):
            item = self.LotListOutput.item(n,0).data(Qt.DisplayRole)
            Lot_List.append(item)
        Lot_List = ' '.join(Lot_List)
        self.Lot_No.setText(Lot_List)
    
    def CompileBinList(self):
        # Function to consolidate selected Bin list to be used for PCA
        # Called upon making any changes on the Bin Selected list
        Bin_List = []
        for n in range(self.BinListOutputTrack):
            item = self.BinListOutput.item(n,0).data(Qt.DisplayRole)
            Bin_List.append(item)
        Bin_List = ' '.join(Bin_List)    
        self.Bin_No.setText(Bin_List)
    
    def CompileImportDataList(self):
        # Function to consolidate imported data to be used for PCA
        # # Called making any changes on the 'Import Data Column' Selected list
        self.ImportData_headers_selected = {}
        for i in range(len(self.ImportedSheets_name)):
            ImportData_List = []
            for j in range(self.ImportDataHeadersListOutputTrack):
                sheet = self.ImportDataHeadersOutput.item(j,0).data(Qt.DisplayRole)
                header = self.ImportDataHeadersOutput.item(j,1).data(Qt.DisplayRole)
                if self.ImportedSheets_name[i] == sheet:
                    ImportData_List.append(header.strip())
            self.ImportData_headers_selected[self.ImportedSheets_name[i]] = ImportData_List
                       
    def ShowDataSource(self):
        # Function to show 'Import' Button
        # Called upon checking on the 'Import' Checkbox
        if self.DataImportSelection.isChecked():
            self.Node_Box.clear()
            self.Process_tech_category_Box.clear()
            self.Product_No.clear()
            self.TestTypeBox.clear()
            self.Lot_No.clear()
            self.Bin_No.clear()
            self.LotListInput.setRowCount(0)
            self.LotListOutput.setRowCount(0)
            self.BinListInput.setRowCount(0)
            self.BinListOutput.setRowCount(0)
            self.DataSource_import_button.show()
            self.DataSource_filelabel.show()
            self.SelectionInputListTab.setTabVisible(2, True)
            self.SelectionOutputListTab.setTabVisible(2, True)
            self.Node_Box.setEnabled(False)
            self.Process_tech_category_Box.setEnabled(False)
            self.Product_No.setEnabled(False)
            self.TestTypeBox.setEnabled(False)
            self.BinSelection.setEnabled(False)
            self.BinSelection.setChecked(False)
            self.YieldSelection.setChecked(True)
            self.Lot_No.setEnabled(False)
            self.Bin_No.setEnabled(False)
            self.LotGroupSelection.setChecked(True)
            self.ToolGroupSelection.setEnabled(False)
            self.ToolGroupSelection.setChecked(False)
            self.ToggleGroupSelection.setEnabled(False)
            self.LotSelection.setEnabled(False)
            self.TimeFrameSelection.setEnabled(False)
        else:
            self.QueryTech()
            self.DataSource_import_button.hide()
            self.DataSource_filelabel.hide()
            self.SelectionInputListTab.setTabVisible(2, False)
            self.SelectionOutputListTab.setTabVisible(2, False)
            self.Node_Box.setEnabled(True)
            self.Process_tech_category_Box.setEnabled(True)
            self.Product_No.setEnabled(True)
            self.TestTypeBox.setEnabled(True)
            self.BinSelection.setEnabled(True)
            self.Lot_No.setEnabled(True)
            self.Bin_No.setEnabled(True)
            self.ToolGroupSelection.setEnabled(True)
            self.ToggleGroupSelection.setEnabled(True)
            self.LotSelection.setEnabled(True)
            self.TimeFrameSelection.setEnabled(True)
        
    def ShowInline(self):
        # Function to show 'Step' Dropbox
        # Called upon checking on the 'Tool' Checkbox under 'Group by:' section
        if self.InlineSelection.isChecked():
            self.InlineImportbutton.show()
        else:
            self.InlineImportbutton.hide()
            
    def classvalueyield(self):
        # Function to consolidate classification defined by user to be used for PCA
        # Called upon making any changes on the 'Yield' Selection 'Classification (%)' Slider Bar
        value = self.Classification_Bar_Yield.value()
        value1 = np.round(value[0], decimals = 1)
        value2 = np.round(value[1], decimals = 1)
        difference = np.round(value2 - value1, decimals = 1)
        if difference <= 0.1:
            value1 = value2
            self.Classification_Bar_Yield.setValue([value1,value2])
        value = self.Classification_Bar_Yield.value()
        value1 = np.round(value[0], decimals = 1)
        value2 = np.round(value[1], decimals = 1)   
        self.SystemMessage(f"\nChoice of Classification:\nGood (> {value2}%)\nMild ({value1}% - {value2}%)\nBad (0 - {value1}%)")
     
    def classvaluebin(self):
        # Function to consolidate classification defined by user to be used for PCA
        # Called upon making any changes on the 'Bin' Selection 'Classification (%)' Slider Bar
        value = self.Classification_Bar_Bin.value()
        value1 = np.round(value[0], decimals = 2)
        value2 = np.round(value[1], decimals = 2)
        difference = np.round(value2 - value1, decimals = 2)
        if difference <= 0.01:
            value1 = value2
            self.Classification_Bar_Bin.setValue([value1,value2])
        value = self.Classification_Bar_Bin.value()
        value1 = np.round(value[0], decimals = 2)
        value2 = np.round(value[1], decimals = 2)            
        self.SystemMessage(f"\nChoice of Classification:\nGood (0 - {value1}%)\nMild ({value1}% - {value2}%)\nBad(> {value2}%)")
        
    def SwitchClassification1(self, index):
        # Function to switch between 'Yield' OR 'Bin' Classification
        # Called upon Checking on either of the 'Yield' OR 'Bin' Checkboxes under the 'Target' section
        if self.BinSelection.isChecked():
            self.BinSelection.setChecked(False)
            self.Classification_Bar_Stacked.setCurrentIndex(index)
        else:
            self.BinSelection.setChecked(True)
            self.Classification_Bar_Stacked.setCurrentIndex(index)
            
    def SwitchClassification2(self, index):
        # Function to switch between 'Yield' OR 'Bin' Classification
        # Called upon Checking on either of the 'Yield' OR 'Bin' Checkboxes under the 'Target' section
        if self.YieldSelection.isChecked():
            self.YieldSelection.setChecked(False)
            self.Classification_Bar_Stacked.setCurrentIndex(index-1)   
        else:
            self.YieldSelection.setChecked(True)
            self.Classification_Bar_Stacked.setCurrentIndex(index+1) 
            
    def SwitchSelectionList(self, index):
        # Function to switch between Lot/Bin Available & Selection List, ensure that they are synchronized
        self.SelectionInputListTab.setCurrentIndex(index)
        self.SelectionOutputListTab.setCurrentIndex(index)
        
    def display_traverse1(self, index):
        # Function to switch between Lot/Time Frame Selection
        # Called upon Checking on either of the 'Lot' OR 'Time Frame' Checkboxes under the 'Selection' section
        self.pca_menu.setCurrentIndex(index)
        if self.LotSelection.isChecked():
            self.LotSelection.setChecked(False)
        else:
            self.LotSelection.setChecked(True)
            
    def display_traverse2(self, index):
        # Function to switch between Lot/Time Frame Selection
        # Called upon Checking on either of the 'Lot' OR 'Time Frame' Checkboxes under the 'Selection' section
        if index == 0:
            self.pca_menu.setCurrentIndex(index+1)
        else:
            self.pca_menu.setCurrentIndex(index-1)
            
        if self.TimeFrameSelection.isChecked():
            self.TimeFrameSelection.setChecked(False)
        else:
            self.TimeFrameSelection.setChecked(True)          
            
    def Group_traverse1(self):
        # Function to switch between Lot/Tool 'Group by:' Selection
        # Called upon Checking on either of the 'Lot' OR 'Tool' Checkboxes under the 'Group by' section
        if self.ToolGroupSelection.isChecked():
            self.StepSelection.hide()
            self.Toollabel.hide()
            self.ToolGroupSelection.setChecked(False)
        else:
            self.StepSelection.show()
            self.Toollabel.show()
            self.ToolGroupSelection.setChecked(True)
            
    def Group_traverse2(self):
        # Function to switch between Lot/Tool 'Group by:' Selection
        # Called upon Checking on either of the 'Lot' OR 'Tool' Checkboxes under the 'Group by' section
        if self.ToolGroupSelection.isChecked():
            self.StepSelection.show()
            self.Toollabel.show()
            self.LotGroupSelection.setChecked(False)
            self.QueryStep()
        else:
            self.StepSelection.hide()
            self.Toollabel.hide()
            self.LotGroupSelection.setChecked(True)
    
    def RunScript(self):  
        # Execute PCA upon clicking the 'Analyze' Button
        self.PCA_Output()
        if self.foldercreated == 'False':
            os.mkdir(self.outputfolder)
            self.outputfolder = self.outputfolder.replace(os.sep, '/')   
            self.foldercreated = 'True'
        
        if self.DataImportSelection.isChecked():
            self.DataSource = ['Import',[self.ImportedSheets,self.ImportedSheets_name, self.ImportData_headers_selected]]
            self.Product_No_text = []
            self.Lot_No_text = []
            
        else:
            self.DataSource = ['MySQL'] 
            self.Product_No_text = self.Product_No.currentText()
            if self.Lot_No.text() == '':
                self.SystemMessage('Error! Lot entry not found')
                return
            
            self.Lot_No_text = tuple(self.Lot_No.text().split(' '))
            
            if len(self.Lot_No.text().split(' ')) == 1:    
                self.Lot_No_text = self.Lot_No_text[0]
            
                    
        if self.Bin_No.text() == '' and self.BinSelection.isChecked():
            self.SystemMessage('Error! Bin entry not found')
            return
        
        self.Bin_text = tuple(self.Bin_No.text().split(' '))
        if len(self.Bin_No.text().split(' ')) == 1:    
            self.Bin_text = self.Bin_text[0]
    
        if self.YieldSelection.isChecked():
            self.Classification_value = self.Classification_Bar_Yield.value()
            self.Classification_Type = 'Yield'
        else:
            self.Classification_value = self.Classification_Bar_Bin.value()
            self.Classification_Type = 'Bin'
            
        if self.ToolGroupSelection.isChecked() == False:
            self.StepTool = 'None'  
    
        with Thread([self.DataSource, self.Product_No.currentText(), self.Lot_No_text, self.TestTypeBox.currentText(), self.Bin_text, self.Classification_value, self.Classification_Type, self.StepTool]) as self.ScriptThread:
            self.ScriptThread.start()
            self.ScriptThread.progress.connect(self.UpdateProgress)
            self.ScriptThread.system_log.connect(self.SystemMessage)
            self.ScriptThread.data.connect(self.GetData)
            self.ScriptThread.principal_components.connect(self.PCA_Components)
            self.ScriptThread.PC_barplot.connect(self.PCBarPlot)
            self.ScriptThread.screeplot.connect(self.Screeplot)
            self.ScriptThread.biplot.connect(self.Biplot)
                
    def SearchUp(self):
        # Function to Query available Lot List based on user defined Time Frame selection
        # Called upon clicking onto the 'Search' Button under 'Time Frame' Selection
        self.LotListOutputTrack = 0
        self.BinListOutputTrack = 0
        self.Earlier_Date2_text = self.Earlier_Date.text()
        self.Later_Date2_text = self.Later_Date.text()
        with QueryThread('Time',[self.TestTypeBox.currentText(),self.Product_No.currentText()],[self.Earlier_Date2_text,self.Later_Date2_text]) as self.QueryThread:
            self.QueryThread.start()
            self.QueryThread.system_log.connect(self.SystemMessage)
            self.QueryThread.query_result.connect(self.UpdateLot)
            
    def SystemMessage(self, log):
        # Function to update the status of the application as shown in the 'System log:' Dialog Box in PCA UI 
        self.SystemLog_Dialog.append(f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: {log}")        
        
    def UpdateProgress(self, value):
        # Function to update progression of the PCA as shown in the 'Progress:' Progress Bar from 0 - 100 %
        # Progression shown are for Data Preparation/Outlier Removal before analysis
        self.ProgressBar.setMaximum(100)
        self.ProgressBar.setValue(value)
        self.ProgressBar.setFormat("%.02f %%" % value)
        
    def PCA_Components(self, df_principal_components):
        # Function to update the 'Principal Components' -> 'Table' tab as shown in the 'PCA Output' window
        # Function to also plot 'Principal Components' -> 'Correlation Heat Map' tab as shown in the 'PCA Output' window
        self.PCTable.setRowCount(0)
        self.df_principal_components = df_principal_components
        PC = df_principal_components.reset_index()
        PC = PC.rename(columns = {'index':'Variables'}, inplace = False)
        rows = len(PC)
        cols = len(PC.columns)
        self.PCTable.setRowCount(rows)
        self.PCTable.setColumnCount(cols)
        self.PCTable.setHorizontalHeaderLabels(PC.columns.tolist())
        self.PCTable.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                item = QTableWidgetItem(str(PC.iloc[i , j]))
                self.PCTable.setItem(i, j, item)
        self.PCTable.setSortingEnabled(True)
        self.PCTable.resizeColumnsToContents()
        
        self.HeatMapplot.axes.clear()
        self.HeatMapplot.axes.figure.subplots_adjust(left = 0.3)
        sns.heatmap(df_principal_components, annot=True,cmap="coolwarm", linecolor="white", linewidths=0.1, cbar_kws={"orientation": "vertical", 'shrink': 0.8, "label": "Importance"}, ax = self.HeatMapplot.axes)
        self.HeatMapplot.axes.xaxis.set_label_position('top')
        self.HeatMapplot.axes.xaxis.set_ticks_position('top')
        
        self.HeatMapplot.axes.tick_params(color='w')
        self.HeatMapplot.axes.set_xlabel('Principal Components', fontsize=24)
        self.HeatMapplot.axes.set_ylabel('Variables', fontsize=24)
        self.HeatMapplot.axes.set_title('Corrleation Heat Map', fontsize=32)
        self.HeatMapplot.draw()
    
    def BiplotWindow(self, fig):
        # Function to display each of the corresponding Biplots in a separate window
        # Called upon clicking on any of the Biplots shwon in the 'PCA Output' window
        plotly.offline.plot(fig, filename='name.html', auto_open=False)
        app = QApplication(sys.argv)
        web = QWebEngineView()
        file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "name.html"))
        web.load(QUrl.fromLocalFile(file_path))
        web.show()
        sys.exit(app.exec_())
    
    def Screeplot(self, df_PC_explained_variance):
        # Function to also plot 'Scree-plot' tab as shown in the 'PCA Output' window
        self.screeplot.axes.clear()                              
        self.PC_Screeplot = df_PC_explained_variance.reset_index(inplace = False)
        self.PC_Screeplot = self.PC_Screeplot.rename(columns = {'index':'Principal Components'}, inplace = False)
        sns.barplot(x = 'Principal Components', y = 'Variance Explained (%)', data = self.PC_Screeplot, palette="flare", ax = self.screeplot.axes)
        for n in range(len(self.PC_Screeplot.index)):
           self.screeplot.axes.text(n-0.15, self.PC_Screeplot['Variance Explained (%)'][n]+0.25, f"{self.PC_Screeplot['Variance Explained (%)'][n]}")
        
        self.screeplot.axes.set_xlabel('Principal Components', fontsize=20)
        self.screeplot.axes.set_ylabel('Variance Explained (%)', fontsize=20)
        self.screeplot.axes.set_title('Screeplot', fontsize=20)
        self.screeplot.draw()

    def PCBarPlot(self, df_principal_components):
        # Function to plot 'Principal Components' barplot -> 'PC1','PC2', ... ,'PC10' tab as shown in the 'PCA Output' window
        # Showing the top 50 most correlated parameters for each of the Principal Components
        self.df_principal_components = df_principal_components
        self.PC_BarPlot = self.df_principal_components.reset_index(inplace = False)
        self.PC_BarPlot = self.PC_BarPlot.rename(columns = {'index':'Variables'}, inplace = False)
        for n in range(len(self.df_principal_components.columns)):
            self.PC_list[n].axes.clear()                              
            self.PC_list[n].axes.figure.subplots_adjust(bottom = 0.45)
            df_ChosenPC = self.PC_BarPlot.sort_values(by=[f'PC{n+1}'], key=abs, ascending = False)
            sns.barplot(x = 'Variables', y = f'PC{n+1}', data = df_ChosenPC.iloc[0:50,], palette="flare", ax = self.PC_list[n].axes)
            self.PC_list[n].axes.set_xticklabels(self.PC_list[n].axes.get_xticklabels(),rotation = 70, ha = 'right')
            self.PC_list[n].axes.set_xlabel('Variables', fontsize=20)
            self.PC_list[n].axes.set_ylabel('Correlation Factor', fontsize=20)
            self.PC_list[n].axes.set_title(f'Principal Component {n+1} Correlation', fontsize=20)
            self.PC_list[n].draw()
        
    def Biplot(self, biplot):
        # Function to plot 'Biplot' tab as shown in the 'PCA Output' window
        self.loadings = biplot[0]
        self.df_biplot = biplot[1]
        self.Biplot_list = {}  
        self.Biplot_loc = []
        
        self.outputfoldercount = os.path.join(self.outputfolder, f"Analysis {self.analysis_count}")
        os.mkdir(self.outputfoldercount)
        self.outputfoldercountslash = self.outputfoldercount.replace(os.sep, '/')
        
        if len(self.loadings.columns) >= 10:
            NumberOfPC = 11
        else:
            NumberOfPC = len(self.loadings.columns)          
        
        pairplot = sns.pairplot(self.df_biplot[['PC1','PC2','PC3','PC4','PC5','PC6','PC7','PC8','PC9','PC10','Classification']], hue='Classification', palette = {"Bad": "red","Good": "green","Mild": "orange"}, corner=True)
        pairplot.fig.suptitle("Biplot Overview")
        pairplot.savefig(f"{self.outputfoldercount}/Pairplot Overview.jpg",dpi = 300)
        pixMap = QPixmap(f"{self.outputfoldercount}/Pairplot Overview.jpg")
        pixMap = pixMap.scaled(3200,3000)
        self.imageLabel.setPixmap(pixMap)
        
        n = 0
        for y in range(2,NumberOfPC):
            for x in range(1,NumberOfPC - 1):
                if x != y and x < y and n < 45:
                    self.Biplot_button_group.button(-n-2).setStyleSheet("")
                    l = len(self.loadings)   
                    fig = go.Figure()
                    
                    if self.ToolGroupSelection.isChecked():
                        fig = px.scatter(self.df_biplot, x=f'PC{x}', y=f'PC{y}', symbol='Tool & Chamber', color='Classification', color_discrete_map={"Bad": "red","Good": "green","Mild": "orange"},\
                                         hover_data={'Product':True,'Lot_No':True,'Wafer_Alias':True,'Scribe':False,'Measure':True,'Step':True,'Tool & Chamber':True,f'PC{x}':False,f'PC{y}':False})
                    
                    else:
                        fig = px.scatter(self.df_biplot, x=f'PC{x}', y=f'PC{y}', symbol='Lot_No', color='Classification', color_discrete_map={"Bad": "red","Good": "green","Mild": "orange"},\
                                         hover_data={'Product':True,'Lot_No':True,'Wafer_Alias':True,'Scribe':False,'Measure':True,f'PC{x}':False,f'PC{y}':False})            
                    
                    fig.update_traces(marker_size=10)   
                    fig.update_xaxes(range=[-1, 1])
                    fig.update_yaxes(range=[-1, 1])
                    fig.update_layout(title=f"Biplot PC{x} vs PC{y}", title_x=0.5)
                   
                    magnitude_principalsx = sorted(list(self.loadings[f'PC{x}'][i] for i in range(l)), key = abs, reverse = True)
                    magnitude_principalsy = sorted(list(self.loadings[f'PC{y}'][i] for i in range(l)), key = abs, reverse = True)
                    for i in range(l):     
                        for count in range(5):
                            if self.loadings[f'PC{x}'][i] == magnitude_principalsx[count]:
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i]*1.5, y=self.loadings[f'PC{y}'][i], ax=0, ay=0, text=self.loadings.index[i], font=dict(color='magenta',size=12))
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i], y=self.loadings[f'PC{y}'][i], ax=0, ay=0, xref = 'x', yref = 'y', axref = 'x', ayref = 'y', showarrow=True, arrowsize=1, arrowhead=1, arrowwidth = 2, arrowcolor='mediumblue')
                            if self.loadings[f'PC{y}'][i] == magnitude_principalsy[count]:
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i], y=self.loadings[f'PC{y}'][i]*1.2, ax=0, ay=0, text=self.loadings.index[i], font=dict(color='magenta',size=12))
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i], y=self.loadings[f'PC{y}'][i], ax=0, ay=0, xref = 'x', yref = 'y', axref = 'x', ayref = 'y', showarrow=True, arrowsize=1, arrowhead=1, arrowwidth = 2, arrowcolor='mediumblue')
                  
                    self.Biplot_list[f'Biplot{x}{y}'] = fig         
                    self.Biplot_loc.append(f'Biplot{x}{y}')
                    n += 1
                    
        self.analysis_count = self.analysis_count + 1
        
    def ToggleBiplot(self):
        # Function to re-plot 'Biplot' to swich between 'Lot' OR ' Tool' after making changes in the 'Group by:' section
        # Called upon clicking on the 'Switch Group' Button under the 'Group by:' section
        if self.ToolGroupSelection.isChecked() and len(self.df_biplot.columns) > 17:    
            self.df_biplot = self.df_biplot.drop(['Tool','Chamber','Step','Tool & Chamber'], axis = 1)
            self.StepTool['Tool & Chamber'] = self.StepTool['Tool'] + " " + self.StepTool['Chamber']
            self.StepTool.drop(['Tool','Chamber'], axis=1, inplace = True)
            self.df_biplot = pd.merge(self.StepTool, self.df_biplot, how = "left", on = 'Scribe')
            self.df_biplot.drop_duplicates(inplace = True)
            self.df_biplot.reset_index(drop = True, inplace = True)
            self.df_biplot[['Classification','Tool & Chamber']].replace('', np.nan, inplace=True)
            self.df_biplot.dropna(subset=['Classification','Tool & Chamber'], inplace=True)  
                    
        elif self.ToolGroupSelection.isChecked():
            self.StepTool['Tool & Chamber'] = self.StepTool['Tool'] + " " + self.StepTool['Chamber']
            self.StepTool.drop(['Tool','Chamber'], axis=1, inplace = True)
            self.df_biplot = pd.merge(self.StepTool, self.df_biplot, how = "left", on = 'Scribe')
            self.df_biplot.drop_duplicates(inplace = True)
            self.df_biplot.reset_index(drop = True, inplace = True)
            self.df_biplot[['Classification','Tool & Chamber']].replace('', np.nan, inplace=True) 
            self.df_biplot.dropna(subset=['Classification','Tool & Chamber'], inplace=True)
            
        else:      
            pass
        
        self.biplot_data = self.df_biplot
        
        if self.analysis_count == 1:
            print("Error! Perform analysis before switching group")
            return
        
        if self.analysis_count == 0:
            self.outputfoldercount = os.path.join(self.outputfolder, f"Analysis {self.analysis_count}")
            os.mkdir(self.outputfoldercount)
            self.outputfoldercountslash = self.outputfoldercount.replace(os.sep, '/')
            
        self.Biplot_list = {}
        self.Biplot_loc = []
        
        if len(self.loadings.columns) >= 10:
            NumberOfPC = 11
        else:
            NumberOfPC = len(self.loadings.columns)

        pairplot = sns.pairplot(self.df_biplot[['PC1','PC2','PC3','PC4','PC5','PC6','PC7','PC8','PC9','PC10','Classification']], hue='Classification', palette = {"Bad": "red","Good": "green","Mild": "orange"}, corner=True)
        pairplot.fig.suptitle("Biplot Overview")
        pairplot.savefig(f"{self.outputfoldercount}/Pairplot Overview.jpg",dpi = 300)
        pixMap = QPixmap(f"{self.outputfoldercount}/Pairplot Overview.jpg")
        pixMap = pixMap.scaled(3200,3000)
        self.imageLabel.setPixmap(pixMap)
        
        n = 0
        for y in range(2,NumberOfPC):
            for x in range(1,NumberOfPC - 1):
                if x != y and x < y and n < 45:
                    self.Biplot_button_group.button(-n-2).setStyleSheet("")
                    l = len(self.loadings)   
                    fig = go.Figure()
                    
                    if self.ToolGroupSelection.isChecked():
                        fig = px.scatter(self.df_biplot, x=f'PC{x}', y=f'PC{y}', symbol='Tool & Chamber', color='Classification', color_discrete_map={"Bad": "red","Good": "green","Mild": "orange"},\
                                         hover_data={'Product':False,'Lot_No':True,'Wafer_Alias':True,'Scribe':False,'Measure':True,'Step':True,'Tool & Chamber':True,f'PC{x}':False,f'PC{y}':False})
                    
                    else:
                        fig = px.scatter(self.df_biplot, x=f'PC{x}', y=f'PC{y}', symbol='Lot_No', color='Classification', color_discrete_map={"Bad": "red","Good": "green","Mild": "orange"},\
                                         hover_data={'Product':False,'Lot_No':True,'Wafer_Alias':True,'Scribe':False,'Measure':True,f'PC{x}':False,f'PC{y}':False})            
                    
                    fig.update_traces(marker_size=10)   
                    fig.update_xaxes(range=[-1, 1])
                    fig.update_yaxes(range=[-1, 1])
                    fig.update_layout(title=f"Biplot PC{x} vs PC{y}", title_x=0.5)
                   
                    magnitude_principalsx = sorted(list(self.loadings[f'PC{x}'][i] for i in range(l)), key = abs, reverse = True)
                    magnitude_principalsy = sorted(list(self.loadings[f'PC{y}'][i] for i in range(l)), key = abs, reverse = True)
                    for i in range(l):     
                        for count in range(5):
                            if self.loadings[f'PC{x}'][i] == magnitude_principalsx[count]:
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i]*1.5, y=self.loadings[f'PC{y}'][i], ax=0, ay=0, text=self.loadings.index[i], font=dict(color='magenta',size=12))
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i], y=self.loadings[f'PC{y}'][i], ax=0, ay=0, xref = 'x', yref = 'y', axref = 'x', ayref = 'y', showarrow=True, arrowsize=1, arrowhead=1, arrowwidth = 2, arrowcolor='mediumblue')
                            if self.loadings[f'PC{y}'][i] == magnitude_principalsy[count]:
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i], y=self.loadings[f'PC{y}'][i]*1.2, ax=0, ay=0, text=self.loadings.index[i], font=dict(color='magenta',size=12))
                                fig.add_annotation(x=self.loadings[f'PC{x}'][i], y=self.loadings[f'PC{y}'][i], ax=0, ay=0, xref = 'x', yref = 'y', axref = 'x', ayref = 'y', showarrow=True, arrowsize=1, arrowhead=1, arrowwidth = 2, arrowcolor='mediumblue')
                   
                    self.Biplot_list[f'Biplot{x}{y}'] = fig
                    self.Biplot_loc.append(f'Biplot{x}{y}')
                    n += 1
        self.analysis_count = self.analysis_count + 1
           
    def DataQueryUi(self):
        # UI function display the Data Query Selection Menu
        # Called from SetupUi()
        self.DataQuerySelectionMenu_layout = QVBoxLayout()
        
        self.DataQuerySelectionMenu.setWindowTitle('Selection Menu')
        self.DataQuerySelectionMenu.closeEvent = self.closeEvent
        self.DataQuerySelectionMenu.resize(960, 960)

        self.DataQuerymenuButton = QPushButton("Back to main menu")
        
        self.DataQueryTitle = QLabel()
        self.DataQueryTitle.setStyleSheet("font: 14pt Century Gothic; text-decoration: underline")
        self.DataQueryTitle.setText("Data Query")
        self.DataQueryNode_layout = QGridLayout()
        self.DataQueryNode_layout.setSpacing(8)
        self.DataQueryNodelabel = QLabel()
        self.DataQueryNodelabel.setText("Tech:")
        self.DataQueryNode_Box = QComboBox()
        self.DataQueryNode_Box.setEditable(True)
        self.DataQueryNode_Box.activated.connect(self.DataQuery_QueryProcessTechCategory)
        self.DataQueryNode_layout.addWidget(self.DataQueryNodelabel,0,0)
        self.DataQueryNode_layout.addWidget(self.DataQueryNode_Box,0,1,1,1)
        self.DataQueryNode_layout.addWidget(QLabel(""),0,3,1,6)
        
        self.DataQuery_QueryTech()

        self.DataQueryProcess_tech_category_layout = QGridLayout()
        self.DataQueryProcess_tech_category_layout.setSpacing(8)
        self.DataQueryProcess_tech_categorylabel = QLabel()
        self.DataQueryProcess_tech_categorylabel.setText("Process:")
        self.DataQueryProcess_tech_category_Box = QComboBox()
        self.DataQueryProcess_tech_category_Box.setEditable(True)
        self.DataQueryProcess_tech_category_Box.activated.connect(self.DataQuery_QueryProduct)
        self.DataQueryProcess_tech_category_layout.addWidget(self.DataQueryProcess_tech_categorylabel,0,0)
        self.DataQueryProcess_tech_category_layout.addWidget(self.DataQueryProcess_tech_category_Box,0,1,1,1)
        self.DataQueryProcess_tech_category_layout.addWidget(QLabel(""),0,3,1,6)
        
        self.DataQueryProduct_layout = QGridLayout()
        self.DataQueryProduct_layout.setSpacing(8)
        self.DataQueryProduct_Nolabel = QLabel()
        self.DataQueryProduct_Nolabel.setText("Product:")
        self.DataQueryProduct_No = QComboBox()
        self.DataQueryProduct_No.setEditable(True)
        self.DataQueryProduct_No.activated.connect(self.DataQuery_QueryTest)
        self.DataQueryProduct_layout.addWidget(self.DataQueryProduct_Nolabel,0,0)
        self.DataQueryProduct_layout.addWidget(self.DataQueryProduct_No,0,1,1,2)
        self.DataQueryProduct_layout.addWidget(QLabel(""),0,3,1,5)
        
        self.DataQueryTestSelection = QGridLayout()
        self.DataQueryTestSelection.setSpacing(8)
        self.DataQueryTestTypelabel = QLabel()
        self.DataQueryTestTypelabel.setText("Test Type:")
        self.DataQueryTestTypeBox = QComboBox()
        self.DataQueryTestTypeBox.setEditable(True)
        self.DataQueryTestTypeBox.activated.connect(self.DataQuery_QueryLot)
        self.DataQueryTestSelection.addWidget(self.DataQueryTestTypelabel,0,0)
        self.DataQueryTestSelection.addWidget(self.DataQueryTestTypeBox,0,1,1,2)
        self.DataQueryTestSelection.addWidget(QLabel(""),0,3,1,5)
        
        self.DataQueryTimeFrame_container = QWidget()
        self.DataQueryTimeFrame_container_layout = QGridLayout()
        self.DataQueryTimeFrame_container_layout.setSpacing(10)  
        self.DataQueryTimeFrame_container_layout.setContentsMargins(0,0,0,0)
        
        self.DataQueryDatelabel = QLabel()
        self.DataQueryDatelabel.setText("Date Range: From")
        self.DataQueryDateTolabel = QLabel()
        self.DataQueryDateTolabel.setText("To")
        self.DataQueryDateTolabel.setAlignment(Qt.AlignCenter)
        
        self.DataQueryEarlier_Date = QDateEdit()
        self.DataQueryLater_Date = QDateEdit()
        self.DataQueryEarlier_Date.setDisplayFormat("yyyy-MM-dd")
        self.DataQueryLater_Date.setDisplayFormat("yyyy-MM-dd")
        self.DataQueryEarlier_Date.setDate(QDate(2021,1,1))
        self.DataQueryLater_Date.setDate(QDate.currentDate())
        self.DataQueryEarlier_Date.setCalendarPopup(True)
        self.DataQueryLater_Date.setCalendarPopup(True)
        
        self.DataQueryTimeFrame_searchbutton = QPushButton('Search')
        self.DataQueryTimeFrame_searchbutton.clicked.connect(self.DataQuerySearchUp)
        
        self.DataQueryTimeFrame_container_layout.addWidget(self.DataQueryDatelabel,0,0,1,1)
        self.DataQueryTimeFrame_container_layout.addWidget(self.DataQueryEarlier_Date,0,1,1,3)
        self.DataQueryTimeFrame_container_layout.addWidget(self.DataQueryDateTolabel,0,4,1,1)
        self.DataQueryTimeFrame_container_layout.addWidget(self.DataQueryLater_Date,0,5,1,3)
        self.DataQueryTimeFrame_container_layout.addWidget(self.DataQueryTimeFrame_searchbutton,0,8,1,2)
        self.DataQueryTimeFrame_container.setLayout(self.DataQueryTimeFrame_container_layout)  
        
        self.DataQueryDataSelection = QGridLayout()
        self.DataQueryDataSelection.setSpacing(8)
        
        self.DataQueryET_container = QWidget()
        self.DataQueryET_layout = QGridLayout()
        self.DataQueryET_layout.setContentsMargins(0,0,0,0)
        self.DataQueryET_layout.setSpacing(2)
        self.DataQueryET_container_frame = QFrame()
        self.DataQueryET_container_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
        self.DataQueryET_container_frame.setStyleSheet("QFrame {border-width: 1;"
                                "border-style: solid}")

        self.DataQueryDataTypeLabel = QLabel()
        self.DataQueryDataTypeLabel.setText("Data Type:")
        self.DataQueryETSelection = QCheckBox("ET")
        self.DataQueryETSelection.clicked.connect(self.DataQueryShowET) 
        self.DataQueryETStatSelection = QComboBox()
        self.DataQueryETStatSelection.setEditable(True)
        self.DataQueryETStatSelection.addItems(['Mean','Median','Max','Min','Site'])
        self.DataQueryETFilterSelection = QCheckBox("Filter By:")
        self.DataQueryETFilterSelected = QLineEdit()
        self.DataQueryETFilterSelected.returnPressed.connect(self.DataQuery_QueryET)
        self.DataQueryETFilterSelected.setFixedWidth(120)
        
        self.DataQueryET_container_frame.hide()
        self.DataQueryETStatSelection.hide()
        self.DataQueryETStatSelection.hide()
        self.DataQueryETFilterSelection.hide()
        self.DataQueryETFilterSelected.hide()
        
        self.DataQueryET_layout.addWidget(self.DataQueryET_container_frame,0,0,2,2)
        self.DataQueryET_layout.addWidget(self.DataQueryETSelection,0,0,1,1)
        self.DataQueryET_layout.addWidget(self.DataQueryETStatSelection,0,1,1,1)
        self.DataQueryET_layout.addWidget(self.DataQueryETFilterSelection,1,0,1,1)
        self.DataQueryET_layout.addWidget(self.DataQueryETFilterSelected,1,1,1,1)
        self.DataQueryET_container.setLayout(self.DataQueryET_layout)
        
        self.DataQueryBin_container = QWidget()
        self.DataQueryBin_layout = QGridLayout()
        self.DataQueryBin_layout.setContentsMargins(0,0,0,0)
        self.DataQueryBin_layout.setSpacing(2)
        self.DataQueryBin_container_frame = QFrame()
        self.DataQueryBin_container_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
        self.DataQueryBin_container_frame.setStyleSheet("QFrame {border-width: 1;"
                                "border-style: solid}")
        
        self.DataQueryBinSelection = QCheckBox("Bin")
        self.DataQueryBinSelection.clicked.connect(self.DataQueryShowBin) 
        self.DataQueryBinStatSelection = QComboBox()
        self.DataQueryBinStatSelection.setEditable(True)
        self.DataQueryBinStatSelection.addItems(['Percentage','Count'])
        self.DataQueryBinFilterSelection = QCheckBox("Filter by Bin No:")
        self.DataQueryBinFilterSelected = QLineEdit()
        self.DataQueryBinFilterSelected.returnPressed.connect(self.DataQuery_FilterBin)
        self.DataQueryBinFilterSelected.setFixedWidth(120)
        
        self.DataQueryBin_container_frame.hide()
        self.DataQueryBinStatSelection.hide()
        self.DataQueryBinStatSelection.hide()
        self.DataQueryBinFilterSelection.hide()
        self.DataQueryBinFilterSelected.hide()
        
        self.DataQueryBin_layout.addWidget(self.DataQueryBin_container_frame,0,0,2,2)
        self.DataQueryBin_layout.addWidget(self.DataQueryBinSelection,0,0,1,1)
        self.DataQueryBin_layout.addWidget(self.DataQueryBinStatSelection,0,1,1,1)
        self.DataQueryBin_layout.addWidget(self.DataQueryBinFilterSelection,1,0,1,1)
        self.DataQueryBin_layout.addWidget(self.DataQueryBinFilterSelected,1,1,1,1)
        self.DataQueryBin_container.setLayout(self.DataQueryBin_layout)
        
        self.DataQueryInline_container = QWidget()
        self.DataQueryInline_layout = QGridLayout()
        self.DataQueryInline_layout.setContentsMargins(0,0,0,0)
        self.DataQueryInline_layout.setSpacing(5)
        self.DataQueryInline_container_frame = QFrame()
        self.DataQueryInline_container_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
        self.DataQueryInline_container_frame.setStyleSheet("QFrame {border-width: 1;"
                                "border-style: solid}")
        
        
        self.DataQueryInlineSelection = QCheckBox("Inline")
        self.DataQueryInlineSelection.clicked.connect(self.DataQueryShowInline) 

        self.DataQueryInlineStatSelection = QComboBox()
        self.DataQueryInlineStatSelection.setEditable(True)
        self.DataQueryInlineFilterSelection = QLabel("Filter By:")
        self.DataQueryInlineFilterDescriptionSelection = QComboBox()
        self.DataQueryInlineFilterDescriptionSelection.setEditable(True)
        self.DataQueryInlineFilterDescriptionSelection.addItems(['Step','Step Name'])
        self.DataQueryInlineFilterSelected = QLineEdit()
        self.DataQueryInlineFilterSelected.returnPressed.connect(self.DataQuery_FilterInline)
        self.DataQueryInlineFilterSelected.setFixedWidth(220)
        self.DataQueryInlineStatSelection.addItems(['Mean','Median','Max','Min','Site'])
        
        self.DataQueryInline_container_frame.hide()
        self.DataQueryInlineFilterSelection.hide()
        self.DataQueryInlineFilterDescriptionSelection.hide()
        self.DataQueryInlineFilterSelected.hide()
        self.DataQueryInlineStatSelection.hide()
        
        self.DataQueryInline_layout.addWidget(self.DataQueryInline_container_frame,0,0,2,3)
        self.DataQueryInline_layout.addWidget(self.DataQueryInlineSelection,0,0,1,1)
        self.DataQueryInline_layout.addWidget(self.DataQueryInlineStatSelection,0,1,1,1)
        self.DataQueryInline_layout.addWidget(self.DataQueryInlineFilterSelection,1,0,1,1)
        self.DataQueryInline_layout.addWidget(self.DataQueryInlineFilterDescriptionSelection,1,1,1,1)
        self.DataQueryInline_layout.addWidget(self.DataQueryInlineFilterSelected,1,2,1,1)
        self.DataQueryInline_layout.addWidget(QLabel(""),0,4,2,1)
        self.DataQueryInline_container.setLayout(self.DataQueryInline_layout)
        
        self.DataQueryWip_container = QWidget()
        self.DataQueryWip_layout = QGridLayout()
        self.DataQueryWip_layout.setContentsMargins(0,0,0,0)
        self.DataQueryWip_layout.setSpacing(7)
        self.DataQueryWip_container_frame = QFrame()
        self.DataQueryWip_container_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
        self.DataQueryWip_container_frame.setStyleSheet("QFrame {border-width: 1;"
                                "border-style: solid}")
        
        self.DataQueryWipSelection = QCheckBox("Wip")
        self.DataQueryWipSelection.clicked.connect(self.DataQueryShowWip) 
        self.DataQueryWipFilterSelection = QLabel("Filter By:")
        self.DataQueryWipFilterDescriptionSelection = QComboBox()
        self.DataQueryWipFilterDescriptionSelection.setEditable(True)
        self.DataQueryWipFilterDescriptionSelection.addItems(['Step','Step Name'])
        self.DataQueryWipFilterSelected = QLineEdit()
        self.DataQueryWipFilterSelected.returnPressed.connect(self.DataQuery_FilterWip)
        self.DataQueryWipFilterSelected.setFixedWidth(220)
        
        self.DataQueryWip_container_frame.hide()
        self.DataQueryWipFilterSelection.hide()
        self.DataQueryWipFilterDescriptionSelection.hide()
        self.DataQueryWipFilterSelected.hide()
        
        self.DataQueryWip_layout.addWidget(self.DataQueryWip_container_frame,0,0,2,3)
        self.DataQueryWip_layout.addWidget(self.DataQueryWipSelection,0,0,1,1)
        self.DataQueryWip_layout.addWidget(self.DataQueryWipFilterSelection,1,0,1,1)
        self.DataQueryWip_layout.addWidget(self.DataQueryWipFilterDescriptionSelection,1,1,1,1)
        self.DataQueryWip_layout.addWidget(self.DataQueryWipFilterSelected,1,2,1,1)
        self.DataQueryWip_layout.addWidget(QLabel(""),0,4,2,1)
        self.DataQueryWip_container.setLayout(self.DataQueryWip_layout)
        
        self.DataQueryDataSelection.addWidget(self.DataQueryDataTypeLabel,0,0)
        self.DataQueryDataSelection.addWidget(self.DataQueryBin_container,0,1,2,2)
        self.DataQueryDataSelection.addWidget(self.DataQueryET_container,2,1,2,2)
        self.DataQueryDataSelection.addWidget(self.DataQueryInline_container,0,3,2,5)
        self.DataQueryDataSelection.addWidget(self.DataQueryWip_container,2,3,2,5)
        
        self.DataQuerySelectionList_layout = QGridLayout()
        self.DataQuerySelectionList_layout.setAlignment(Qt.AlignTop)
        self.DataQuerySelectionList_layout.setSpacing(19)
        
        self.DataQueryLotListInput = QTableWidget()
        self.DataQueryLotListInput.setFont(QFont('Arial', 8))
        self.DataQueryLotListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryLotListInput.setSortingEnabled(True)
        self.DataQueryBinListInput = QTableWidget()
        self.DataQueryBinListInput.setFont(QFont('Arial', 8))
        self.DataQueryBinListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryBinListInput.setSortingEnabled(True)
        self.DataQueryETListInput = QTableWidget()
        self.DataQueryETListInput.setFont(QFont('Arial', 8))
        self.DataQueryETListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryETListInput.setSortingEnabled(True)
        self.DataQueryInlineListInput = QTableWidget()
        self.DataQueryInlineListInput.setFont(QFont('Arial', 8))
        self.DataQueryInlineListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryInlineListInput.setSortingEnabled(True)
        self.DataQueryWipListInput = QTableWidget()
        self.DataQueryWipListInput.setFont(QFont('Arial', 8))
        self.DataQueryWipListInput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryWipListInput.setSortingEnabled(True)
        
        self.DataQuerySelectionInputListTab = QTabWidget()
        self.DataQuerySelectionInputListTab.addTab(self.DataQueryLotListInput, "Lot List")
        self.DataQuerySelectionInputListTab.addTab(self.DataQueryBinListInput,"Bin List")
        self.DataQuerySelectionInputListTab.addTab(self.DataQueryETListInput,"ET List")
        self.DataQuerySelectionInputListTab.addTab(self.DataQueryInlineListInput,"Inline List")
        self.DataQuerySelectionInputListTab.addTab(self.DataQueryWipListInput,"Wip List")
        self.DataQuerySelectionInputListTab.tabBarClicked.connect(self.DataQuerySwitchSelectionList)
        
        self.DataQueryLotListOutput = QTableWidget()
        self.DataQueryLotListOutput.setFont(QFont('Arial', 8))
        self.DataQueryLotListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryLotListOutput.setSortingEnabled(True)
        self.DataQueryBinListOutput = QTableWidget()
        self.DataQueryBinListOutput.setFont(QFont('Arial', 8))
        self.DataQueryBinListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryBinListOutput.setSortingEnabled(True)
        self.DataQueryETListOutput = QTableWidget()
        self.DataQueryETListOutput.setFont(QFont('Arial', 8))
        self.DataQueryETListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryETListOutput.setSortingEnabled(True)
        self.DataQueryInlineListOutput = QTableWidget()
        self.DataQueryInlineListOutput.setFont(QFont('Arial', 8))
        self.DataQueryInlineListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryInlineListOutput.setSortingEnabled(True)
        self.DataQueryWipListOutput = QTableWidget()
        self.DataQueryWipListOutput.setFont(QFont('Arial', 8))
        self.DataQueryWipListOutput.setSelectionBehavior(QTableWidget.SelectRows)
        self.DataQueryWipListOutput.setSortingEnabled(True)

        self.DataQuerySelectionOutputListTab = QTabWidget()
        self.DataQuerySelectionOutputListTab.addTab(self.DataQueryLotListOutput, "Lot List")
        self.DataQuerySelectionOutputListTab.addTab(self.DataQueryBinListOutput,"Bin List")
        self.DataQuerySelectionOutputListTab.addTab(self.DataQueryETListOutput,"ET List")
        self.DataQuerySelectionOutputListTab.addTab(self.DataQueryInlineListOutput,"Inline List")
        self.DataQuerySelectionOutputListTab.addTab(self.DataQueryWipListOutput,"Wip List")
        self.DataQuerySelectionOutputListTab.tabBarClicked.connect(self.DataQuerySwitchSelectionList)
        
        self.DataQueryLotListOutputTrack = 0
        self.DataQueryBinListOutputTrack = 0
        
        self.DataQueryForward_Button = QToolButton()
        self.DataQueryForward_Button.setArrowType(Qt.RightArrow) 
        self.DataQueryForward_Button.clicked.connect(self.DataQueryForward)
        
        self.DataQueryFull_Forward_Button = QToolButton()
        self.DataQueryFull_Forward_Button.setIcon(self.DataQueryFull_Forward_Button.style().standardIcon(QStyle.SP_MediaSeekForward))
        self.DataQueryFull_Forward_Button.clicked.connect(self.DataQueryFullForward)
        
        self.DataQueryBackward_Button = QToolButton()
        self.DataQueryBackward_Button.setArrowType(Qt.LeftArrow)
        self.DataQueryBackward_Button.clicked.connect(self.DataQueryBackward)
        
        self.DataQueryFull_Backward_Button = QToolButton()
        self.DataQueryFull_Backward_Button.setIcon(self.DataQueryFull_Backward_Button.style().standardIcon(QStyle.SP_MediaSeekBackward))
        self.DataQueryFull_Backward_Button.clicked.connect(self.DataQueryFullBackward)

        self.DataQuerySelectionList_layout.addWidget(self.DataQuerySelectionInputListTab,0,0,17,9)
        self.DataQuerySelectionList_layout.addWidget(self.DataQuerySelectionOutputListTab,0,10,17,9)
        self.DataQuerySelectionList_layout.addWidget(self.DataQueryForward_Button,7,9,1,1)
        self.DataQuerySelectionList_layout.addWidget(self.DataQueryFull_Forward_Button,8,9,1,1)
        self.DataQuerySelectionList_layout.addWidget(self.DataQueryBackward_Button,9,9,1,1)
        self.DataQuerySelectionList_layout.addWidget(self.DataQueryFull_Backward_Button,10,9,1,1)
        
        self.DataQuerySystem_layout = QGridLayout()
        self.DataQuerySystem_layout.setSpacing(17)
        self.DataQuerySystemLog = QLabel()
        self.DataQuerySystemLog.setText("System log:")
        
        self.DataQuerySystemLog_Dialog = QTextEdit()
        self.DataQuerySystemLog_Dialog.setFixedWidth(430)

        self.DataQuery_FileTypeSelection = QComboBox()
        self.DataQuery_FileTypeSelection.addItems(['Excel (.xlsx)','Comma Separated Values (.csv)'])
        
        self.DataQuery_button = QPushButton('Query')
        self.DataQuery_button.setFixedSize(440,150)
        self.DataQuery_button.setFont(QFont('Times', 24))
        self.DataQuery_button.clicked.connect(self.RunQueryScript)
        
        self.DataQuerySystem_layout.addWidget(self.DataQuerySystemLog,0,0,1,8)
        self.DataQuerySystem_layout.addWidget(self.DataQuerySystemLog_Dialog,1,0,2,8)
        self.DataQuerySystem_layout.addWidget(QLabel(""),0,9,2,1)
        self.DataQuerySystem_layout.addWidget(QLabel("Output format:"),1,10,1,1)
        self.DataQuerySystem_layout.addWidget(self.DataQuery_FileTypeSelection,1,11,1,7)
        self.DataQuerySystem_layout.addWidget(self.DataQuery_button,2,10,1,8)
        
        self.Menu_DataQuery_layout = QVBoxLayout()
        self.Menu_DataQuery_layout.addWidget(self.DataQuerymenuButton)
        self.Menu_DataQuery_layout.addWidget(self.DataQueryTitle)
        self.Menu_DataQuery_layout.addLayout(self.DataQueryNode_layout)
        self.Menu_DataQuery_layout.addLayout(self.DataQueryProcess_tech_category_layout)
        self.Menu_DataQuery_layout.addLayout(self.DataQueryProduct_layout)
        self.Menu_DataQuery_layout.addLayout(self.DataQueryTestSelection) 
        self.Menu_DataQuery_layout.addWidget(self.DataQueryTimeFrame_container)        
        self.Menu_DataQuery_layout.addLayout(self.DataQueryDataSelection)
        self.Menu_DataQuery_layout.addLayout(self.DataQuerySelectionList_layout)
        self.Menu_DataQuery_layout.addLayout(self.DataQuerySystem_layout)
        self.DataQuerySelectionMenu.setLayout(self.Menu_DataQuery_layout)
        
    def DataQueryExportData(self, data):
        if self.DataQuery_FileTypeSelection.currentText() == 'Excel (.xlsx)':   
            WriteData = pd.ExcelWriter(f"{self.folderpath}/Data Query {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.xlsx")

            if self.DataQueryETSelection.isChecked() and self.DataQueryInlineSelection.isChecked():
                query_data = data[0] 
                et_spec = data[1]
                inline_spec = data[2]
                
                query_data.to_excel(WriteData, sheet_name = 'Query Data', index=False)
                et_spec.to_excel(WriteData, sheet_name = 'ET Spec Book', index=False)
                inline_spec.to_excel(WriteData, sheet_name = 'Inline Spec Book', index=False)
            
            elif self.DataQueryETSelection.isChecked() and self.DataQueryInlineSelection.isChecked() == False:
                query_data = data[0] 
                et_spec = data[1]
                
                query_data.to_excel(WriteData, sheet_name = 'Query Data', index=False)
                et_spec.to_excel(WriteData, sheet_name = 'ET Spec Book', index=False)
                
            elif self.DataQueryETSelection.isChecked() == False and self.DataQueryInlineSelection.isChecked():
                query_data = data[0] 
                inline_spec = data[1]
            
                query_data.to_excel(WriteData, sheet_name = 'Query Data', index=False)
                inline_spec.to_excel(WriteData, sheet_name = 'Inline Spec Book', index=False)
                
            else:
                query_data = data
            
                query_data.to_excel(WriteData, sheet_name = 'Query Data', index=False)
                
            WriteData.save()
        
        elif self.DataQuery_FileTypeSelection.currentText() == 'Comma Separated Values (.csv)':
            
            if self.DataQueryETSelection.isChecked() and self.DataQueryInlineSelection.isChecked():
                query_data = data[0] 
                et_spec = data[1]
                inline_spec = data[2]
                
                query_data.to_csv(f"{self.folderpath}/Query Data {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
                et_spec.to_csv(f"{self.folderpath}/ET Spec Book {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
                inline_spec.to_csv(f"{self.folderpath}/Inline Spec Book {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
            
            elif self.DataQueryETSelection.isChecked() and self.DataQueryInlineSelection.isChecked() == False:
                query_data = data[0] 
                et_spec = data[1]
                
                query_data.to_csv(f"{self.folderpath}/Query Data {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
                et_spec.to_csv(f"{self.folderpath}/ET Spec Book {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
                
            elif self.DataQueryETSelection.isChecked() == False and self.DataQueryInlineSelection.isChecked():
                query_data = data[0] 
                inline_spec = data[1]
            
                query_data.to_csv(f"{self.folderpath}/Query Data {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
                inline_spec.to_csv(f"{self.folderpath}/Inline Spec Book {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
                
            else:
                query_data = data
                query_data.to_csv(f"{self.folderpath}/Query Data {self.DataQueryProduct_No.currentText()} {datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv", index=False)
            
    def DataQuery_QueryTech(self):
        # Function to Query Tech to update Tech DropBox in Data Query Selection Menu UI
        with QueryThread('None','None','None') as self.DataQuery_QueryThread:
            self.DataQuery_QueryThread.start()
            self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
            self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateTech)
            
    def DataQueryUpdateTech(self, query):
        # Update Tech DropBox in Data Query Selection Menu UI Queried from DataQuery_QueryTech()
        self.DataQueryNode_Box.addItems(query)
    
    def DataQuery_QueryProcessTechCategory(self):
        # Function to Query Product to update Process DropBox in Data Query Selection Menu UI
        with QueryThread('Process_Tech_Category', self.DataQueryNode_Box.currentText(), 'None') as self.DataQuery_QueryThread:
            self.DataQuery_QueryThread.start()
            self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
            self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateProcessTechCategory)
        
    def DataQueryUpdateProcessTechCategory(self, query):
        # Update Process DropBox in Data Query Selection Menu UI Queried from DataQuery_QueryProcessTechCategory()
        self.DataQueryProcess_tech_category_Box.clear()
        self.DataQueryProcess_tech_category_Box.addItems(query)
        
    def DataQuery_QueryProduct(self):
        # Function to Query Product to update Product DropBox in Data Query Selection Menu UI
        with QueryThread('Product', self.DataQueryProcess_tech_category_Box.currentText(), 'None') as self.DataQuery_QueryThread:
            self.DataQuery_QueryThread.start()
            self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
            self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateProduct)
        
    def DataQueryUpdateProduct(self, query):
        # Update Product DropBox in Data Query Selection Menu UI Queried from DataQueryUpdateProduct()
        self.DataQueryProduct_No.clear()
        self.DataQueryProduct_No.addItems(query)
        
    def DataQuery_QueryTest(self):
        # Function to Query Test Type to update Test Type DropBox in Data Query Selection Menu UI
        with QueryThread('Test', self.DataQueryProduct_No.currentText(), 'None') as self.DataQuery_QueryThread:
            self.DataQuery_QueryThread.start()
            self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
            self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateTest)
      
    def DataQueryUpdateTest(self, query):
        # Update Test Type DropBox in Data Query Selection Menu UI Queried from DataQuery_QueryTest()
        self.DataQueryTestTypeBox.clear()
        self.DataQueryTestTypeBox.addItems(query)
        
    def DataQuery_QueryLot(self):
        self.DataQueryLotListInput.setRowCount(0)
        self.DataQueryBinListInput.setRowCount(0)
        self.DataQueryETListInput.setRowCount(0)
        self.DataQueryInlineListInput.setRowCount(0)
        self.DataQueryWipListInput.setRowCount(0)
        self.DataQueryLotListOutput.setRowCount(0)
        self.DataQueryBinListOutput.setRowCount(0)
        self.DataQueryETListOutput.setRowCount(0)
        self.DataQueryInlineListOutput.setRowCount(0)
        self.DataQueryWipListOutput.setRowCount(0)
        # Function to Query Lot to update Lot List in Data Query Selection Menu UI
        with QueryThread('Lot', self.DataQueryProduct_No.currentText(), [self.DataQueryTestTypeBox.currentText(), 'None']) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateLot)
    
    def DataQuery_QueryBin(self):
        # Function to Query Bin to update Bin List in Data Query Selection Menu UI
        if self.DataQueryBinFilterSelection.isChecked() == False:
            with QueryThread('Bin', self.DataQueryProduct_No.currentText(), [self.DataQueryTestTypeBox.currentText(), 'None']) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateBin)
                
        elif self.DataQueryBinFilterSelection.isChecked():
            with QueryThread('Filter Bin', self.DataQueryProduct_No.currentText(), [self.DataQueryTestTypeBox.currentText(),self.DataQueryBinFilterSelected.text()]) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateBin)           
        else:
            self.DataQuerySystemMessage("Error! Unkown filtering option")
            
    def DataQuery_QueryET(self):  
        # Function to Query ET to update ET List in Data Query Selection Menu UI
        if self.DataQueryLotListOutput.rowCount() != 0:    
            if self.DataQueryETFilterSelection.isChecked() == False:
                with QueryThread('ET',self.Lot_List, [self.DataQueryProduct_No.currentText(),self.DataQueryETFilterSelected.text()]) as self.DataQuery_QueryThread:
                    self.DataQuery_QueryThread.start()
                    self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                    self.DataQuery_QueryThread.query_result2.connect(self.DataQueryUpdateET)
                    
            elif self.DataQueryETFilterSelection.isChecked():
                with QueryThread('Filter ET',self.Lot_List, [self.DataQueryProduct_No.currentText(),self.DataQueryETFilterSelected.text()]) as self.DataQuery_QueryThread:
                    self.DataQuery_QueryThread.start()
                    self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                    self.DataQuery_QueryThread.query_result2.connect(self.DataQueryUpdateET)
            
            else:
                self.DataQuerySystemMessage("Error! Unkown filtering option")
            
        else:
            self.DataQuerySystemMessage("Error! Lot selection not specified")
            return

    def DataQuery_FilterBin(self):     
        # Function to Query filtered Bin No. to update Bin List in Data Query Selection Menu UI
        if self.DataQueryBinFilterSelection.isChecked():
            with QueryThread('Filter Bin', self.DataQueryProduct_No.currentText(), [self.DataQueryTestTypeBox.currentText(),self.DataQueryBinFilterSelected.text()]) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateBin) 
            
    def DataQuery_FilterInline(self):
        # Function to Query filtered Inline to update Inline List in Data Query Selection Menu UI
        # Filered either by Step No. OR Step Name
        if self.DataQueryInlineFilterDescriptionSelection.currentText() == 'Step' :
            with QueryThread('Filter Inline Step', self.DataQueryProduct_No.currentText(), self.DataQueryInlineFilterSelected.text()) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateInline)
                
        elif self.DataQueryInlineFilterDescriptionSelection.currentText() == 'Step Name':
            with QueryThread('Filter Inline Step Name', self.DataQueryProduct_No.currentText(), self.DataQueryInlineFilterSelected.text()) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateInline)       
        else:
            self.DataQuerySystemMessage("Error! Unkown filtering option")   
            
    def DataQuery_FilterWip(self):
        # Function to Query filtered Wip to update Wip List in Data Query Selection Menu UI
        # Filered either by Step No. OR Step Name
        if self.DataQueryWipFilterDescriptionSelection.currentText() == 'Step':
            with QueryThread('Filter Wip Step', self.DataQueryProduct_No.currentText(), self.DataQueryWipFilterSelected.text()) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateWip)
                
        elif self.DataQueryWipFilterDescriptionSelection.currentText() == 'Step Name':
            with QueryThread('Filter Wip Step Name', self.DataQueryProduct_No.currentText(), self.DataQueryWipFilterSelected.text()) as self.DataQuery_QueryThread:
                self.DataQuery_QueryThread.start()
                self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
                self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateWip)                
        else:
            self.DataQuerySystemMessage("Error! Unkown filtering option")
        
    def DataQueryUpdateBin(self, query):     
        # Function to update Bin List in Data Query Selection Menu UI Queried from DataQuery_QueryBin() OR DataQuery_FilterBin()
        self.DataQueryBinListOutputTrack = 0
        self.DataQueryBin_List_count = 0
        if len(query) == 0:
            self.DataQuerySystemMessage('Error! No Bin Found')
            return
        self.DataQueryBinListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.DataQueryBinListInput.setRowCount(rows)
        self.DataQueryBinListInput.setColumnCount(cols)
        self.DataQueryBinListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.DataQueryBinListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                if j == 0:
                    item = QTableWidgetItem(str(query.iloc[i,j].decode('utf-8')))
                    self.DataQueryBinListInput.setItem(i, j, item)
                else:    
                    item = QTableWidgetItem(str(query.iloc[i,j]))
                    self.DataQueryBinListInput.setItem(i, j, item)
                    
        self.DataQueryBinListInput.resizeColumnsToContents()
        self.DataQueryBinListInput.resizeRowsToContents()
        
        self.DataQueryBinListOutput.setColumnCount(cols)
        self.DataQueryBinListOutput.setHorizontalHeaderLabels(query.columns.tolist())
        
    def DataQueryUpdateLot(self, query):
        # Function to update Lot List in Data Query Selection Menu UI Queried from DataQuery_QueryLot()
        self.DataQueryLotListOutputTrack = 0
        self.DataQueryLot_List_count = 0
        if len(query) == 0:
            self.DataQuerySystemMessage('Error! No Lot found')
            return
        self.DataQueryLotListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.DataQueryLotListInput.setRowCount(rows)
        self.DataQueryLotListInput.setColumnCount(cols)
        self.DataQueryLotListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.DataQueryLotListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                item = QTableWidgetItem(str(query.iloc[i,j]))
                self.DataQueryLotListInput.setItem(i, j, item)
        self.DataQueryLotListInput.resizeColumnsToContents()
        self.DataQueryLotListInput.resizeRowsToContents()
        
        self.DataQueryLotListOutput.setColumnCount(cols)
        self.DataQueryLotListOutput.setHorizontalHeaderLabels(query.columns.tolist())
        
    def DataQueryUpdateET(self, query):
        # Function to update ET List in Data Query Selection Menu UI Queried from DataQuery_QueryET()
        self.DataQueryETListOutputTrack = 0
        self.DataQueryET_List_count = 0
        if len(query) == 0:
            self.DataQuerySystemMessage('Error! No ET parameters found')
            return
        self.DataQueryETListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.DataQueryETListInput.setRowCount(rows)
        self.DataQueryETListInput.setColumnCount(cols)
        self.DataQueryETListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.DataQueryETListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                item = QTableWidgetItem(str(query.iloc[i,j]))
                self.DataQueryETListInput.setItem(i, j, item)
        self.DataQueryETListInput.resizeColumnsToContents()
        self.DataQueryETListInput.resizeRowsToContents()
        
        self.DataQueryETListOutput.setColumnCount(cols)
        self.DataQueryETListOutput.setHorizontalHeaderLabels(query.columns.tolist())
        
    def DataQueryUpdateInline(self, query):
        # Function to update Inline List in Data Query Selection Menu UI Queried from DataQuery_FilterInline()
        self.DataQueryInlineListOutputTrack = 0
        self.DataQueryInline_List_count = 0
        if len(query) == 0:
            self.DataQuerySystemMessage('Error! No Inline parameter found')
            return
        self.DataQueryInlineListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.DataQueryInlineListInput.setRowCount(rows)
        self.DataQueryInlineListInput.setColumnCount(cols)
        self.DataQueryInlineListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.DataQueryInlineListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                item = QTableWidgetItem(str(query.iloc[i,j]))
                self.DataQueryInlineListInput.setItem(i, j, item)
        self.DataQueryInlineListInput.resizeColumnsToContents()
        self.DataQueryInlineListInput.resizeRowsToContents()
        
        self.DataQueryInlineListOutput.setColumnCount(cols)
        self.DataQueryInlineListOutput.setHorizontalHeaderLabels(query.columns.tolist())
        
    def DataQueryUpdateWip(self, query):
        # Function to update Inline List in Data Query Selection Menu UI Queried from DataQuery_FilterWip()
        self.DataQueryWipListOutputTrack = 0
        self.DataQueryWip_List_count = 0
        if len(query) == 0:
            self.DataQuerySystemMessage('Error! No Wip data found')
            return
        self.DataQueryWipListInput.setRowCount(0)
        rows = len(query)
        cols = len(query.columns)
        self.DataQueryWipListInput.setRowCount(rows)
        self.DataQueryWipListInput.setColumnCount(cols)
        self.DataQueryWipListInput.setHorizontalHeaderLabels(query.columns.tolist())
        self.DataQueryWipListInput.horizontalHeader().setDefaultAlignment(Qt.AlignHCenter)      
        for i in range(rows):
            for j in range(cols):
                item = QTableWidgetItem(str(query.iloc[i,j]))
                self.DataQueryWipListInput.setItem(i, j, item)
        self.DataQueryWipListInput.resizeColumnsToContents()
        self.DataQueryWipListInput.resizeRowsToContents()
        
        self.DataQueryWipListOutput.setColumnCount(cols)
        self.DataQueryWipListOutput.setHorizontalHeaderLabels(query.columns.tolist())
            
    def DataQueryForward(self):
        # Function to include selected Lot/Bin/Inline/Wip to Selected List in Data Query Selection Menu UI 
        # Indicated as right arrow
        if self.DataQuerySelectionInputListTab.currentIndex() == 0 and self.DataQuerySelectionOutputListTab.currentIndex() == 0:
            item = self.DataQueryLotListInput.selectedIndexes()
            item_list = [item[i:i+self.DataQueryLotListInput.columnCount()] for i in range(0, len(item), self.DataQueryLotListInput.columnCount())]
            for rows in range(len(item_list)):
                self.DataQueryLotListOutput.insertRow(self.DataQueryLotListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.DataQueryLotListOutputTrack):
                        if self.DataQueryLotListOutputTrack != 0 and cols == 0 :
                            if item_list[rows][cols].data(Qt.DisplayRole) == self.DataQueryLotListOutput.item(n,cols).data(Qt.DisplayRole):
                                self.DataQuerySystemMessage("Error! Attempting to add lot already exist in selection list")
                                self.DataQueryLotListOutput.removeRow(self.DataQueryLotListOutputTrack) 
                                return
                    self.DataQueryLotListOutput.setItem(self.DataQueryLotListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.DataQueryLotListOutputTrack += 1   
            self.DataQueryLotListOutput.resizeRowsToContents()
            self.DataQueryLotListOutput.resizeColumnsToContents()
            self.DataQueryCompileLotList()   
            
            if self.DataQueryETSelection.isChecked():
                self.DataQuery_QueryET()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 1 and self.DataQuerySelectionOutputListTab.currentIndex() == 1:
            item = self.DataQueryBinListInput.selectedIndexes()
            item_list = [item[i:i+self.DataQueryBinListInput.columnCount()] for i in range(0, len(item), self.DataQueryBinListInput.columnCount())]
            for rows in range(len(item_list)):
                self.DataQueryBinListOutput.insertRow(self.DataQueryBinListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.DataQueryBinListOutputTrack):
                        if self.DataQueryBinListOutputTrack != 0 and item_list[rows][0].data(Qt.DisplayRole) == self.DataQueryBinListOutput.item(n,0).data(Qt.DisplayRole) \
                            and item_list[rows][2].data(Qt.DisplayRole) == self.DataQueryBinListOutput.item(n,2).data(Qt.DisplayRole):
                                self.DataQuerySystemMessage("Error! Attempting to add bin already exist in selection list")
                                self.DataQueryBinListOutput.removeRow(self.DataQueryBinListOutputTrack)
                                return
                    self.DataQueryBinListOutput.setItem(self.DataQueryBinListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.DataQueryBinListOutputTrack += 1    
            self.DataQueryBinListOutput.resizeRowsToContents()
            self.DataQueryBinListOutput.resizeColumnsToContents()
            self.DataQueryCompileBinList()     
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 2 and self.DataQuerySelectionOutputListTab.currentIndex() == 2:
            item = self.DataQueryETListInput.selectedIndexes()
            item_list = [item[i:i+self.DataQueryETListInput.columnCount()] for i in range(0, len(item), self.DataQueryETListInput.columnCount())]        
            for rows in range(len(item_list)):
                self.DataQueryETListOutput.insertRow(self.DataQueryETListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.DataQueryETListOutputTrack):
                        if self.DataQueryETListOutputTrack != 0 and cols == 0 :
                            if item_list[rows][cols].data(Qt.DisplayRole) == self.DataQueryETListOutput.item(n,cols).data(Qt.DisplayRole):
                                self.DataQuerySystemMessage("Error! Attempting to add ET parameter already exist in selection list")
                                self.DataQueryETListOutput.removeRow(self.DataQueryETListOutputTrack)
                                return
                    self.DataQueryETListOutput.setItem(self.DataQueryETListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.DataQueryETListOutputTrack += 1   
            self.DataQueryETListOutput.resizeRowsToContents()
            self.DataQueryETListOutput.resizeColumnsToContents()
            self.DataQueryCompileETList()    
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 3 and self.DataQuerySelectionOutputListTab.currentIndex() == 3:
            item = self.DataQueryInlineListInput.selectedIndexes()
            item_list = [item[i:i+self.DataQueryInlineListInput.columnCount()] for i in range(0, len(item), self.DataQueryInlineListInput.columnCount())]
            for rows in range(len(item_list)):
                self.DataQueryInlineListOutput.insertRow(self.DataQueryInlineListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.DataQueryInlineListOutputTrack):
                        if self.DataQueryInlineListOutputTrack != 0 and item_list[rows][0].data(Qt.DisplayRole) == self.DataQueryInlineListOutput.item(n,0).data(Qt.DisplayRole) \
                            and item_list[rows][1].data(Qt.DisplayRole) == self.DataQueryInlineListOutput.item(n,1).data(Qt.DisplayRole) \
                            and item_list[rows][2].data(Qt.DisplayRole) == self.DataQueryInlineListOutput.item(n,2).data(Qt.DisplayRole) \
                            and item_list[rows][3].data(Qt.DisplayRole) == self.DataQueryInlineListOutput.item(n,3).data(Qt.DisplayRole):
                                self.DataQuerySystemMessage("Error! Attempting to add inline parameter already exist in selection list")
                                self.DataQueryInlineListOutput.removeRow(self.DataQueryInlineListOutputTrack)
                                return
                    self.DataQueryInlineListOutput.setItem(self.DataQueryInlineListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.DataQueryInlineListOutputTrack += 1   
            self.DataQueryInlineListOutput.resizeRowsToContents()
            self.DataQueryInlineListOutput.resizeColumnsToContents()
            self.DataQueryCompileInlineList()  
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 4 and self.DataQuerySelectionOutputListTab.currentIndex() == 4:
            item = self.DataQueryWipListInput.selectedIndexes()
            item_list = [item[i:i+self.DataQueryWipListInput.columnCount()] for i in range(0, len(item), self.DataQueryWipListInput.columnCount())]
            for rows in range(len(item_list)):
                self.DataQueryWipListOutput.insertRow(self.DataQueryWipListOutputTrack)
                for cols in range(len(item_list[0])):
                    for n in range(self.DataQueryWipListOutputTrack):
                        if self.DataQueryWipListOutputTrack != 0 and item_list[rows][0].data(Qt.DisplayRole) == self.DataQueryWipListOutput.item(n,0).data(Qt.DisplayRole) \
                            and item_list[rows][1].data(Qt.DisplayRole) == self.DataQueryWipListOutput.item(n,1).data(Qt.DisplayRole):
                                self.DataQuerySystemMessage("Error! Attempting to add inline parameter already exist in selection list")
                                self.DataQueryWipListOutput.removeRow(self.DataQueryWipListOutputTrack)
                                return
                    self.DataQueryWipListOutput.setItem(self.DataQueryWipListOutputTrack, cols, QTableWidgetItem(item_list[rows][cols].data(Qt.DisplayRole)))
                self.DataQueryWipListOutputTrack += 1   
            self.DataQueryWipListOutput.resizeRowsToContents()
            self.DataQueryWipListOutput.resizeColumnsToContents()
            self.DataQueryCompileWipList()  
            
        else:
            self.DataQuerySystemMessage("Error! Displaying Available List does not match Selected list")
        
    def DataQueryBackward(self):
        # Function to exclude selected Lot/Bin/Inline/Wip from Selected List in Data Query Selection Menu UI 
        # Indicated as left arrow
        if self.DataQuerySelectionInputListTab.currentIndex() == 0  and self.DataQuerySelectionOutputListTab.currentIndex() == 0 and self.DataQueryLotListOutputTrack !=0:
            item = self.DataQueryLotListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.DataQueryLotListOutput.removeRow(row_remove)
                self.DataQueryLotListOutputTrack -= 1
            
            self.DataQueryLotListOutput.resizeRowsToContents()
            self.DataQueryCompileLotList()
            
            if self.DataQueryETSelection.isChecked():
                self.DataQuery_QueryET()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 1 and self.DataQuerySelectionOutputListTab.currentIndex() == 1 and self.DataQueryBinListOutputTrack !=0:
            item = self.DataQueryBinListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.DataQueryBinListOutput.removeRow(row_remove)
                self.DataQueryBinListOutputTrack -= 1
           
            self.DataQueryBinListOutput.resizeRowsToContents()
            self.DataQueryCompileBinList()
                   
        elif self.DataQuerySelectionInputListTab.currentIndex() == 2 and self.DataQuerySelectionOutputListTab.currentIndex() == 2 and self.DataQueryETListOutputTrack !=0:
            item = self.DataQueryETListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.DataQueryETListOutput.removeRow(row_remove)
                self.DataQueryETListOutputTrack -= 1
           
            self.DataQueryETListOutput.resizeRowsToContents()
            self.DataQueryCompileETList()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 3 and self.DataQuerySelectionOutputListTab.currentIndex() == 3 and self.DataQueryInlineListOutputTrack !=0:
            item = self.DataQueryInlineListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.DataQueryInlineListOutput.removeRow(row_remove)
                self.DataQueryInlineListOutputTrack -= 1
           
            self.DataQueryInlineListOutput.resizeRowsToContents()
            self.DataQueryCompileInlineList()  
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 4 and self.DataQuerySelectionOutputListTab.currentIndex() == 4 and self.DataQueryWipListOutputTrack !=0:
            item = self.DataQueryWipListOutput.selectionModel().selectedRows() 
            sorted_item = sorted(item)
            row_remove = sorted_item[0].row()
            for n in sorted(item):
                self.DataQueryWipListOutput.removeRow(row_remove)
                self.DataQueryWipListOutputTrack -= 1
           
            self.DataQueryWipListOutput.resizeRowsToContents()
            self.DataQueryCompileWipList()  
            
        else:
            self.DataQuerySystemMessage("Error! Displaying Available List does not match Selected list")
        
    def DataQueryFullForward(self):
        # Function to include all of Lot/Bin/Inline/Wip to Selected List in Data Query Selection Menu UI 
        # Indicated as double right arrow
        if self.DataQuerySelectionInputListTab.currentIndex() == 0 and self.DataQuerySelectionOutputListTab.currentIndex() == 0:
            output_rows = self.DataQueryLotListOutput.rowCount()
            rows = self.DataQueryLotListInput.rowCount()
            cols = self.DataQueryLotListInput.columnCount()
            self.DataQueryLotListOutput.setRowCount(self.DataQueryLotListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.DataQueryLotListInput.item(i,j))
                    self.DataQueryLotListOutput.setItem(output_rows + i,j,item)
            
            self.DataQueryLotListOutputTrack = rows + output_rows
            self.DataQueryLotListOutput.resizeRowsToContents()
            self.DataQueryCompileLotList() 
            
            if self.DataQueryETSelection.isChecked():
                self.DataQuery_QueryET()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 1 and self.DataQuerySelectionOutputListTab.currentIndex() == 1:
            output_rows = self.DataQueryBinListOutput.rowCount()
            rows = self.DataQueryBinListInput.rowCount()
            cols = self.DataQueryBinListInput.columnCount()
            self.DataQueryBinListOutput.setRowCount(self.DataQueryBinListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.DataQueryBinListInput.item(i,j))
                    self.DataQueryBinListOutput.setItem(output_rows + i,j,item)
                    
            self.DataQueryBinListOutputTrack = rows + output_rows     
            self.DataQueryBinListOutput.resizeRowsToContents()
            self.DataQueryCompileBinList()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 2 and self.DataQuerySelectionOutputListTab.currentIndex() == 2:
            output_rows = self.DataQueryETListOutput.rowCount()
            rows = self.DataQueryETListInput.rowCount()
            cols = self.DataQueryETListInput.columnCount()
            self.DataQueryETListOutput.setRowCount(self.DataQueryETListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.DataQueryETListInput.item(i,j))
                    self.DataQueryETListOutput.setItem(output_rows + i,j,item)
                    
            self.DataQueryETListOutputTrack = rows + output_rows       
            self.DataQueryETListOutput.resizeRowsToContents()
            self.DataQueryCompileETList()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 3 and self.DataQuerySelectionOutputListTab.currentIndex() == 3:
            output_rows = self.DataQueryInlineListOutput.rowCount()
            rows = self.DataQueryInlineListInput.rowCount()
            cols = self.DataQueryInlineListInput.columnCount()
            self.DataQueryInlineListOutput.setRowCount(self.DataQueryInlineListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.DataQueryInlineListInput.item(i,j))
                    self.DataQueryInlineListOutput.setItem(output_rows + i,j,item)
                    
            self.DataQueryInlineListOutputTrack = rows + output_rows       
            self.DataQueryInlineListOutput.resizeRowsToContents()
            self.DataQueryCompileInlineList()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 4 and self.DataQuerySelectionOutputListTab.currentIndex() == 4:
            output_rows = self.DataQueryWipListOutput.rowCount()
            rows = self.DataQueryWipListInput.rowCount()
            cols = self.DataQueryWipListInput.columnCount()
            self.DataQueryWipListOutput.setRowCount(self.DataQueryWipListOutput.rowCount() + rows)
            for i in range(rows):
                for j in range(cols):
                    item = QTableWidgetItem(self.DataQueryWipListInput.item(i,j))
                    self.DataQueryWipListOutput.setItem(output_rows + i,j,item)
                    
            self.DataQueryWipListOutputTrack = rows + output_rows       
            self.DataQueryWipListOutput.resizeRowsToContents()
            self.DataQueryCompileWipList()
            
        else:
            self.DataQuerySystemMessage("Error! Displaying Available List does not match Selected list")
        
    def DataQueryFullBackward(self):
        # Function to exclude all of Lot/Bin/Inline/Wip from Selected List in Data Query Selection Menu UI 
        # Indicated as double left arrow
        if self.DataQuerySelectionInputListTab.currentIndex() == 0 and self.DataQuerySelectionOutputListTab.currentIndex() == 0:
            self.DataQueryLotListOutput.setRowCount(0)
            self.DataQueryLotListOutputTrack = 0
            self.DataQueryCompileLotList()
            if self.DataQueryETSelection.isChecked():
                self.DataQuery_QueryET()

        elif self.DataQuerySelectionInputListTab.currentIndex() == 1 and self.DataQuerySelectionOutputListTab.currentIndex() == 1:
            self.DataQueryBinListOutput.setRowCount(0)
            self.DataQueryBinListOutputTrack = 0
            self.DataQueryCompileBinList()

        elif self.DataQuerySelectionInputListTab.currentIndex() == 2 and self.DataQuerySelectionOutputListTab.currentIndex() == 2:
            self.DataQueryETListOutput.setRowCount(0)
            self.DataQueryETListOutputTrack = 0
            self.DataQueryCompileETList()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 3 and self.DataQuerySelectionOutputListTab.currentIndex() == 3:
            self.DataQueryInlineListOutput.setRowCount(0)
            self.DataQueryInlineListOutputTrack = 0
            self.DataQueryCompileInlineList()
            
        elif self.DataQuerySelectionInputListTab.currentIndex() == 4 and self.DataQuerySelectionOutputListTab.currentIndex() == 4:
            self.DataQueryWipListOutput.setRowCount(0)
            self.DataQueryWipListOutputTrack = 0
            self.DataQueryCompileWipList()
            
        else:
            self.DataQuerySystemMessage("Error! Displaying Available List does not match Selected list")
            
    def DataQueryCompileLotList(self):
        # Function to consolidate selected Lot list to be used for Data Query
        # Called upon making any changes on the Lot Selected list
        Lot_List = []
        self.DataQueryLot_List_count = 0
        for n in range(self.DataQueryLotListOutputTrack):
            item = self.DataQueryLotListOutput.item(n,0).data(Qt.DisplayRole)
            Lot_List.append(item)
            self.DataQueryLot_List_count += 1
        if len(Lot_List) == 1:
            self.Lot_List = Lot_List[0]
        else:
            self.Lot_List = tuple(Lot_List)   

    def DataQueryCompileBinList(self):
        # Function to consolidate selected Bin list to be used for Data Query
        # Called upon making any changes on the Bin Selected list
        Bin_List = []
        self.DataQueryBin_List_count = 0
        for n in range(self.DataQueryBinListOutputTrack):
            item = self.DataQueryBinListOutput.item(n,0).data(Qt.DisplayRole)
            Bin_List.append(item)
            self.DataQueryBin_List_count += 1
        if len(Bin_List) == 1:
            self.Bin_List = Bin_List[0]
        else:    
            self.Bin_List = tuple(Bin_List)

    def DataQueryCompileETList(self):
        # Function to consolidate selected ET list to be used for Data Query
        # Called upon making any changes on the ET Selected list
        ET_List = []
        self.DataQueryET_List_count = 0
        for n in range(self.DataQueryETListOutputTrack):
            item = self.DataQueryETListOutput.item(n,0).data(Qt.DisplayRole)
            ET_List.append(item)
            self.DataQueryET_List_count += 1
        if len(ET_List) == 1:
            self.ET_List = ET_List[0]
        else:
            self.ET_List = tuple(ET_List)
        
    def DataQueryCompileInlineList(self):
        # Function to consolidate selected Inline list to be used for Data Query
        # Called upon making any changes on the Inline Selected list
        self.Inline_collated = []
        Inline_Route_List = []
        Step_List = []
        Inline_Param_List = []
        for n in range(self.DataQueryInlineListOutputTrack):
            Route = self.DataQueryInlineListOutput.item(n,0).data(Qt.DisplayRole)
            Step = self.DataQueryInlineListOutput.item(n,1).data(Qt.DisplayRole)
            Param = self.DataQueryInlineListOutput.item(n,3).data(Qt.DisplayRole)
            Inline_Route_List.append(Route)
            Step_List.append(Step)
            Inline_Param_List.append(Param)
        self.Inline_collated.append([Step_List, Inline_Route_List, Inline_Param_List])
        
    def DataQueryCompileWipList(self):
        # Function to consolidate selected Wip list to be used for Data Query
        # Called upon making any changes on the Wip Selected list
        self.Wip_collated = []
        Step_List = []
        Step_Name_List = []
        for n in range(self.DataQueryWipListOutputTrack):
            Step = self.DataQueryWipListOutput.item(n,0).data(Qt.DisplayRole)
            Name = self.DataQueryWipListOutput.item(n,1).data(Qt.DisplayRole)
            Step_List.append(Step)
            Step_Name_List.append(Name)
        self.Wip_collated.append([Step_List, Step_Name_List])
    
    def DataQueryShowET(self):
        # Function to show ET Stat + filter selection in Data Query Selection Menu UI
        # Called upon checking onto the 'ET' Checkbox under the 'Data Type' section
        if self.DataQueryETSelection.isChecked():
            self.DataQueryETStatSelection.show()
            self.DataQueryETFilterSelection.show()
            self.DataQueryETFilterSelected.show()
            self.DataQueryET_container_frame.show()
            if self.DataQueryTestTypeBox.currentText() == '':
                self.DataQuerySystemMessage("Error! Test Type field is empty")
            else:
                self.DataQuery_QueryET()
            
        else:    
            self.DataQueryETStatSelection.hide()
            self.DataQueryETFilterSelection.hide()
            self.DataQueryETFilterSelected.hide()
            self.DataQueryET_container_frame.hide()
    
    def DataQueryShowBin(self):
        # Function to show Bin Stat + filter selection in Data Query Selection Menu UI
        # Called upon checking onto the 'Bin' Checkbox under the 'Data Type' section
        if self.DataQueryBinSelection.isChecked():
            self.DataQueryBinStatSelection.show()
            self.DataQueryBinFilterSelection.show()
            self.DataQueryBinFilterSelected.show()
            self.DataQueryBin_container_frame.show()
            if self.DataQueryTestTypeBox.currentText() == '':
                self.DataQuerySystemMessage("Error! Test Type field is empty")
            else:
                self.DataQuery_QueryBin()
        else:    
            self.DataQueryBinStatSelection.hide()
            self.DataQueryBinFilterSelection.hide()
            self.DataQueryBinFilterSelected.hide()
            self.DataQueryBin_container_frame.hide()
            
    def DataQueryShowInline(self):
        # Function to show Inline Stat + filter selection in Data Query Selection Menu UI
        # Called upon checking onto the 'Inline' Checkbox under the 'Data Type' section
        if self.DataQueryInlineSelection.isChecked():
            self.DataQueryInlineStatSelection.show()
            self.DataQueryInline_container_frame.show()
            self.DataQueryInlineFilterSelection.show()
            self.DataQueryInlineFilterDescriptionSelection.show()
            self.DataQueryInlineFilterSelected.show()
            
        else:
            self.DataQueryInlineStatSelection.hide()
            self.DataQueryInline_container_frame.hide()
            self.DataQueryInlineFilterSelection.hide()
            self.DataQueryInlineFilterDescriptionSelection.hide()
            self.DataQueryInlineFilterSelected.hide()
            self.Inline_Route_List = []
            self.Inline_Param_List = []
            self.Inline_collated = []
            
    def DataQueryShowWip(self):
        # Function to show Wip filter selection in Data Query Selection Menu UI
        # Called upon checking onto the 'Wip' Checkbox under the 'Data Type' section
        if self.DataQueryWipSelection.isChecked():
            self.DataQueryWip_container_frame.show()
            self.DataQueryWipFilterSelection.show()
            self.DataQueryWipFilterDescriptionSelection.show()
            self.DataQueryWipFilterSelected.show()
            
        else:
            self.DataQueryWip_container_frame.hide()
            self.DataQueryWipFilterSelection.hide()
            self.DataQueryWipFilterDescriptionSelection.hide()
            self.DataQueryWipFilterSelected.hide()
            self.Wip_Route_List = []
            self.Wip_Param_List = []
            self.Wip_collated = []
        
    def DataQuerySwitchSelectionList(self, index):
        # Function to switch between Lot/Bin/Inline/Wip Available & Selection List, ensure that they are synchronized
        self.DataQuerySelectionInputListTab.setCurrentIndex(index)
        self.DataQuerySelectionOutputListTab.setCurrentIndex(index)
        
    def RunQueryScript(self):
        # Execute Data Query upon clicking the 'Query' Button
        if self.DataQueryLotListOutput.rowCount() == 0:
            self.DataQuerySystemMessage("Error! Lot selection not specified")
            return
        
        if self.DataQueryBinSelection.isChecked():
            if self.DataQueryBinListOutput.rowCount() == 0:
                self.DataQuerySystemMessage("Error! Bin selection not specified")
                return
        else:
            self.Bin_List = 'None'
            
        if self.DataQueryETSelection.isChecked():
            if self.DataQueryETListOutput.rowCount() == 0:
                self.DataQuerySystemMessage("Error! ET selection not specified")
                return
        else:
            self.ET_List = 'None'
            
        if self.DataQueryInlineSelection.isChecked():
            if self.DataQueryInlineListOutput.rowCount() == 0:
                self.DataQuerySystemMessage("Error! Inline selection not specified")
                return
        else:
            self.Inline_collated = 'None'
            
        if self.DataQueryWipSelection.isChecked():
            if self.DataQueryWipListOutput.rowCount() == 0:
                self.DataQuerySystemMessage("Error! Wip selection not specified")
                return
        else:
            self.Wip_collated = 'None'
            
        self.folderpath = QFileDialog.getExistingDirectory(self, 'Select directory to save data')
         
        with QueryThread('Query',[self.DataQueryNode_Box.currentText(),self.DataQueryProcess_tech_category_Box.currentText(),self.DataQueryProduct_No.currentText(),self.DataQueryTestTypeBox.currentText(),self.DataQueryETStatSelection.currentText(),self.Lot_List,self.Bin_List,self.DataQueryBinStatSelection.currentText(), self.ET_List,self.Inline_collated, self.DataQueryInlineStatSelection.currentText(), self.Wip_collated],'None') as self.QueryScriptThread:
            self.QueryScriptThread.start()
            self.QueryScriptThread.system_log.connect(self.DataQuerySystemMessage)
            self.QueryScriptThread.query_result.connect(self.DataQueryExportData)
            
    def DataQuerySearchUp(self):
        # Function to Query available Lot List based on user defined Time Frame selection in Data Query Selection Menu UI
        # Called upon clicking onto the 'Search' Button under 'Data Range' Selection
        self.DataQueryLotListOutputTrack = 0
        self.DataQueryBinListOutputTrack = 0
        self.DataQueryEarlier_Date2_text = self.DataQueryEarlier_Date.text()
        self.DataQueryLater_Date2_text = self.DataQueryLater_Date.text()
        
        with QueryThread('Time',[self.DataQueryTestTypeBox.currentText(),self.DataQueryProduct_No.currentText()],[self.DataQueryEarlier_Date2_text,self.DataQueryLater_Date2_text]) as self.DataQuery_QueryThread:
            self.DataQuery_QueryThread.start()
            self.DataQuery_QueryThread.system_log.connect(self.DataQuerySystemMessage)
            self.DataQuery_QueryThread.query_result.connect(self.DataQueryUpdateLot)
            
    def DataQuerySystemMessage(self, log):
        # Function to update the status of the application as shown in the 'System log:' Dialog Box in Data Query Selection Menu UI
        self.DataQuerySystemLog_Dialog.append(f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: {log}")        
        
class MplCanvasPlot(FigureCanvas):
    def __init__(self, width, height, dpi):
        fig = Figure(figsize=(width, height), dpi=dpi)
        sns.set(style = "darkgrid")
        super(MplCanvasPlot, self).__init__(fig)
        self.axes = fig.add_subplot(111)
    
class QueryThread(QThread):
    # Class for MultiThreading purposes which allow other computation to run alongside main code (UI) to prevent UI from freezing as far as possible
    # Threading specifcally for Query related tasks such as updating of information on UI from MySQL + Execute Data Query
    # How to perform QThread?
        # 1) Name the Thread in main UI 
            # self.QueryScriptThread = QueryThread(Input)
        # 2) Run the Thread
            # self.QueryScriptThread.start()
        # 3) Retrieve information (Signals & Slots)
            # -> In QThread class (Signals):
                # Name the Signals: self.query = pyqtSignal(object type e.g str, int, float, object)
                # Emit Signals (Data/Information): self.query.emit(Data)
            # -> In main body UI class (Slots)
                # Capture the Signals: self.QueryScriptThread.query.connect(self.function())
        #  4) Perform Task based on data/information recieved
            # function(self, Data):
                # GetData = Data
    system_log = pyqtSignal(str)
    progress = pyqtSignal(str)
    query_result = pyqtSignal(object)
    query_result2 = pyqtSignal(object)
    def __init__(self, query_cat, query_input1, query_input2, parent = None):
        super(QueryThread, self).__init__(parent)
        self.query_cat = query_cat
        self.query_input1 = query_input1
        self.query_input2 = query_input2
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.quit()
        
    def run(self):
        with MySQL_Query(self.query_input1, self.query_input2 , self.system_log, self.progress) as mysqlquery:
            if self.query_cat == 'None' and self.query_input1 == 'None':
                query_tech = mysqlquery.query_tech()
                self.query_result.emit(query_tech)
                
            elif self.query_cat == 'Process_Tech_Category':
                query_process_tech_category = mysqlquery.query_process_tech_category()
                self.query_result.emit(query_process_tech_category)
                
            elif self.query_cat == 'Product':
                query_product = mysqlquery.query_product()
                self.query_result.emit(query_product)
                
            elif self.query_cat == 'Test':
                query_test = mysqlquery.query_test()
                self.query_result.emit(query_test)
                
            elif self.query_cat == 'Lot':
                query_lot = mysqlquery.query_lot()
                self.query_result.emit(query_lot)

            elif self.query_cat == 'Bin':
                query_bin = mysqlquery.query_bin()
                self.query_result.emit(query_bin)
                
            elif self.query_cat == 'Lot Bin':
                query_lot = mysqlquery.query_lot()
                self.query_result.emit(query_lot)
                query_bin = mysqlquery.query_bin()
                self.query_result2.emit(query_bin)
                
            elif self.query_cat == 'Filter Bin':
                query_filterbin = mysqlquery.query_filterbin()
                self.query_result.emit(query_filterbin)
                
            elif self.query_cat == 'Time':
                query_lot_time = mysqlquery.query_lot_time()
                self.query_result.emit(query_lot_time)
            
            elif self.query_cat == 'Step':
                query_step = mysqlquery.query_step()
                self.query_result.emit(query_step)
                
            elif self.query_cat == 'Filter Inline Step':
                query_step = mysqlquery.query_filterinlinestep()
                self.query_result.emit(query_step)
                
            elif self.query_cat == 'Filter Inline Step Name':
                query_step_name = mysqlquery.query_filterinlinestepname()
                self.query_result.emit(query_step_name)
                
            elif self.query_cat == 'Filter Wip Step':
                query_step = mysqlquery.query_filterwipstep()
                self.query_result.emit(query_step)
                
            elif self.query_cat == 'Filter Wip Step Name':
                query_step_name = mysqlquery.query_filterwipstepname()
                self.query_result.emit(query_step_name)
                                       
            elif self.query_cat == 'Tool':
                query_tool = mysqlquery.query_tool()
                self.query_result.emit(query_tool)
                
            elif self.query_cat == 'ET':
                query_et = mysqlquery.query_ET()
                self.query_result2.emit(query_et)
                
            elif self.query_cat == 'Filter ET':    
                query_et = mysqlquery.query_filterET()
                self.query_result2.emit(query_et)
                
            elif self.query_cat == 'Query':
                query_data = mysqlquery.query_data()
                self.query_result.emit(query_data)
                
            else:
                self.system_log.emit("Error! Unable to retrieve data from MySQL Database")

class Thread(QThread):
    # MultiThreading class for PCA
    # Contain various function: 
        # 1) Querying ET data for analysis
        # 2) Data Preparation (Uncompressing ET data into dataframe/array)
        # 2) Data Cleaning (Removing Outliers)
        # 3) PCA
        # 4) Emit data for data visualisation and Exporting (Excel)
    progress = pyqtSignal(float)
    system_log = pyqtSignal(str)
    data = pyqtSignal(object)
    eigenpairs = pyqtSignal(object)
    principal_components = pyqtSignal(object)
    PC_barplot = pyqtSignal(object)
    screeplot = pyqtSignal(object)
    biplot = pyqtSignal(object)
    completed = pyqtSignal(str)
    def __init__(self, Input, parent = None):
        super(Thread, self).__init__(parent)
        self.oldstdout = sys.stdout
        self.oldstderr = sys.stderr
        self.DataSource = Input[0]
        self.Product_No = Input[1]
        self.Lot_No = Input[2]
        self.Test_Type = Input[3]
        self.Bin = Input[4]
        self.Class = Input[5]
        self.Class_Type = Input[6]
        self.StepTool = Input[7]
           
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.quit()
        
    def run(self):
        start = time.time() 
        if self.DataSource[0] == 'Import':
            merged_data = []
            merged_data_key = []
            Scribe = []
            ImportedSheets = self.DataSource[1][0]
            ImportedSheets_name = self.DataSource[1][1]
            ImportData_headers_selected = self.DataSource[1][2]
            
            self.system_log.emit('Organizing data..')
            for i in range(len(ImportedSheets_name)):
                if len(ImportData_headers_selected[ImportedSheets_name[i]]) >= 1:
                    Data = ImportedSheets[ImportedSheets_name[i]][ImportData_headers_selected[ImportedSheets_name[i]]]
                    ImportData_Header_list = []
                    ImportData_Header_Change_list = []
                    ImportData_Merge_key = []
                     
                    for j in range(len(ImportData_headers_selected[ImportedSheets_name[i]])):
                        if 'LOT' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])
                            ImportData_Header_Change_list.append('Lot_No')   
                            ImportData_Merge_key.append('Lot_No')
                            
                        elif 'Lot' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])
                            ImportData_Header_Change_list.append('Lot_No')   
                            ImportData_Merge_key.append('Lot_No')
                            
                        else:
                            pass
                            
                        if 'Wafer_Alias' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                            ImportData_Header_Change_list.append('Wafer_Alias')
                            ImportData_Merge_key.append('Wafer_Alias')
                            
                        elif 'WAFER' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            if 'LOT' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                                for n in range(len(Data)):
                                    if len(Data['WAFER'][n]) == 1:
                                        Data['Wafer_Alias'] = Data['LOT'][n].split('.')[0] + '.' + '0' + Data['WAFER'][n]
                                    else:
                                        Data['Wafer_Alias'] = Data['LOT'][n].split('.')[0] + Data['WAFER'][n]
                                        
                                    ImportData_Merge_key.append.append('Wafer_Alias')
                            elif 'Lot_No' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                                for n in range(len(Data)):
                                    if len(Data['WAFER'][n]) == 1:
                                        Data['Wafer_Alias'] = Data['Lot_No'][n].split('.')[0] + '.' + '0' + Data['WAFER'][n]
                                    else:
                                        Data['Wafer_Alias'] = Data['Lot_No'][n].split('.')[0] + Data['WAFER'][n]
                                   
                                    ImportData_Merge_key.append.append('Wafer_Alias')
                            else:
                                pass
                            
                            Data.drop(['WAFER'], axis=1, inplace = True)
                        
                        else:
                            pass
                        
                        if 'SCRIBE' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                            ImportData_Header_Change_list.append('Scribe')
                            ImportData_Merge_key.append('Scribe')
                            
                            for n in range(len(Data[ImportData_headers_selected[ImportedSheets_name[i]][j]].unique())):
                                Scribe.append(Data[ImportData_headers_selected[ImportedSheets_name[i]][j]].unique()[n])
                                
                                self.progress.emit(np.round((n/len(Data[ImportData_headers_selected[ImportedSheets_name[i]][j]].unique()))*100, decimals = 2))
                        
                        elif 'SUBSTRATE' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                            ImportData_Header_Change_list.append('Scribe')
                            ImportData_Merge_key.append('Scribe')
                            
                            for n in range(len(Data[ImportData_headers_selected[ImportedSheets_name[i]][j]].unique())):
                                Scribe.append(Data[ImportData_headers_selected[ImportedSheets_name[i]][j]].unique()[n])
                                
                                self.progress.emit(np.round((n/len(Data[ImportData_headers_selected[ImportedSheets_name[i]][j]].unique()))*100, decimals = 2))
                        else:
                            self.system_log.emit("Error! Please select Data Column consist of Wafer Scribe")
                            return
                            
                        if 'PRODUCT' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                           ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                           ImportData_Header_Change_list.append('Product')
                           ImportData_Merge_key.append('Product')
                           
                        if 'TIME' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                           ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                           ImportData_Header_Change_list.append('Measure')
                           
                        if 'Yield' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                            ImportData_Header_Change_list.append('Yield')
                            
                            if self.Class_Type == 'Yield':
                                wafer_classification = []
                                for n in range(len(Data)):
                                    if Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] < self.Class[0]:
                                        wafer_classification.append('Bad')
                                    elif Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] >= self.Class[0] and Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] <= self.Class[1]:
                                        wafer_classification.append('Mild')
                                    else:
                                        wafer_classification.append('Good')
                                        
                                Data['Classification'] = wafer_classification
                                        
                        elif 'YIELD' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                            ImportData_Header_Change_list.append('Yield')
                            
                            if self.Class_Type == 'Yield':
                                wafer_classification = []
                                for n in range(len(Data)):
                                    if Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] < self.Class[0]:
                                        wafer_classification.append('Bad')
                                    elif Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] >= self.Class[0] and Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] <= self.Class[1]:
                                        wafer_classification.append('Mild')
                                    else:
                                        wafer_classification.append('Good')
                                        
                                Data['Classification'] = wafer_classification
                                
                        elif 'sort' in ImportData_headers_selected[ImportedSheets_name[i]][j]:
                            ImportData_Header_list.append(ImportData_headers_selected[ImportedSheets_name[i]][j])    
                            ImportData_Header_Change_list.append('Yield')
                            
                            if self.Class_Type == 'Yield':
                                wafer_classification = []
                                for n in range(len(Data)):
                                    if Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] < self.Class[0]:
                                        wafer_classification.append('Bad')
                                    elif Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] >= self.Class[0] and Data[ImportData_headers_selected[ImportedSheets_name[i]][j]][n] <= self.Class[1]:
                                        wafer_classification.append('Mild')
                                    else:
                                        wafer_classification.append('Good')
                                        
                                Data['Classification'] = wafer_classification
                        
                        else:
                            self.system_log.emit("Error! Please select Data Column consist of Wafer Yield")
                            return

                    Data.rename(columns={ImportData_Header_list[n]:ImportData_Header_Change_list[n] for n in range(len(ImportData_Header_list))}, inplace=True)
                    merged_data_key.append(ImportData_Merge_key)
                    merged_data.append(Data) 
            
            self.progress.emit(0)
            with MySQL_Query(tuple(Scribe), 'None', self.system_log, self.progress) as mysqlquery:
            
                df_et_wmap_raw = mysqlquery.df_et_wmap_raw_import() #dataframe 
                
                if len(df_et_wmap_raw[0].index) == 0 or len(df_et_wmap_raw[1]) == 0:
                    self.system_log.emit("Error! No data found in MySQL database")
                    return             
        
            prepared_data = self.data_preparation(df_et_wmap_raw)
            df_combined_data = prepared_data
            info_header = []
            data_header = []
            
            for n in range(len(merged_data)):
                df_combined_data = pd.merge(df_combined_data, merged_data[n], how = 'left', on = merged_data_key[n])     

            if self.Class_Type == 'Yield':
                df_combined_data[['Classification','Yield']].replace('', np.nan, inplace=True)
                df_combined_data.dropna(subset=['Classification','Yield'], inplace=True)
                df_combined_data.reset_index(drop = True, inplace = True)
                
            else:
                self.system_log.emit("Error! Please ensure that the 'Yield' CheckBox is Checked under Classification")
                return
                
            df_combined_data.dropna(axis=1, how='all', inplace=True)
            
            for n in range(len(df_combined_data.columns)):
                if type(df_combined_data.iloc[0,n]) == str:
                    info_header.append(df_combined_data.columns[n])
                else:
                    data_header.append(df_combined_data.columns[n])
             
            info_header.append('Measure')
            info_header.append('Yield')
            data_header.remove('Measure')
            data_header.remove('Yield')
            
            cleaned_data = self.data_cleaning([df_combined_data, data_header])
            df_combined_data = cleaned_data[1][info_header + data_header]
            PCA = self.PCA([cleaned_data[0], cleaned_data[1], info_header, data_header])  
 
        elif self.DataSource[0] == 'MySQL':
            with MySQL_Query([self.Product_No,
                            self.Lot_No, 
                            self.Test_Type, self.Bin, self.Class], 'None', self.system_log, self.progress) as mysqlquery:
                
                df_et_wmap_raw = mysqlquery.df_et_wmap_raw() #dataframe 
               
                if len(df_et_wmap_raw[0].index) == 0 or len(df_et_wmap_raw[1]) == 0:
                    self.system_log.emit("Error! No data found in MySQL database")
                    return
                
                if self.Class_Type == 'Bin':
                    df_wafer_class = mysqlquery.wafer_bin()
                if self.Class_Type == 'Yield':
                    df_wafer_class = mysqlquery.wafer_yield()
            
                for count in (df_wafer_class['Classification'].value_counts()).tolist():
                    if count < 2:
                        self.system_log.emit('Error! The classfication criteria defined could not differentiate the selected data')
                        return
            
            
            prepared_data = self.data_preparation(df_et_wmap_raw)
            info_header = ['Product','Lot_No','Wafer_Alias','Scribe']
            data_header = prepared_data.columns[4:].tolist()
            et_cleaned_data = self.data_cleaning([prepared_data, data_header])  
            df_combined_data = pd.merge(df_wafer_class, et_cleaned_data[1] , how='left', on = info_header)
            PCA = self.PCA([et_cleaned_data[0], et_cleaned_data[1], info_header, data_header])
          
        else:
            self.system_log.emit('Error! Unable to locate Data Source')
            return
        
        self.system_log.emit('Performing Data Visualization..')
        
        if len(PCA[1].columns) >= 10:
            NumberOfPC = 11
        else:
            NumberOfPC = len(PCA[1].columns)
    
        if self.DataSource[0] == 'Import':
            df_biplot = PCA[2][info_header]
            for l in range(1,NumberOfPC):
                projected_data = list(PCA[2][f"PC{l}"])
                scale_projected_data = 1.0/(max(projected_data) - min(projected_data))
                scaled_projected_data = list(i*scale_projected_data for i in projected_data)
                df_biplot[f"PC{l}"] = scaled_projected_data
                   
        elif self.DataSource[0] == 'MySQL':
            df_biplot = PCA[2][info_header] 
            for l in range(1,NumberOfPC):
                projected_data = list(PCA[2][f"PC{l}"])
                scale_projected_data = 1.0/(max(projected_data) - min(projected_data))
                scaled_projected_data = list(i*scale_projected_data for i in projected_data)
                df_biplot[f"PC{l}"] = scaled_projected_data 
            
            if type(self.StepTool) == str:       
                df_biplot = pd.merge(df_wafer_class, df_biplot, how = "left", on = info_header)
                df_biplot.drop_duplicates(subset=['Wafer_Alias','Scribe'], inplace = True)
                df_biplot.reset_index(drop = True, inplace = True)
                df_biplot[['Classification']].replace('', np.nan, inplace=True)
                df_biplot.dropna(subset=['Classification'], inplace=True)
                df_biplot.reset_index(drop = True, inplace = True)
                
            else:
                self.StepTool['Tool & Chamber'] = self.StepTool['Tool'] + " " + self.StepTool['Chamber']
                self.StepTool.drop(['Tool','Chamber'], axis=1, inplace = True)
                df_biplot = pd.merge(self.StepTool, pd.merge(df_wafer_class, df_biplot, how = "left", on = info_header), how = "left", on = 'Scribe')
                df_biplot.drop_duplicates(subset=['Wafer_Alias','Scribe'], inplace = True)
                df_biplot.reset_index(drop = True, inplace = True)
                df_biplot[['Classification','Tool & Chamber']].replace('', np.nan, inplace=True)
                df_biplot.dropna(subset=['Classification','Tool & Chamber'], inplace=True)
                df_biplot.reset_index(drop = True, inplace = True)            
        
        self.data.emit([df_combined_data, df_biplot])
        self.principal_components.emit(PCA[1].round(3))
        self.PC_barplot.emit(PCA[1])
        self.screeplot.emit(PCA[0])
        self.biplot.emit([PCA[3], df_biplot])
        end = time.time()
        elapsed = (end - start)/60
        self.system_log.emit('Analysis Completed!')
        self.system_log.emit(f"Elapsed time: {math.floor(float(format(elapsed)))} min {np.round(((float(format(elapsed)) - math.floor(float(format(elapsed))))*60),decimals = 0)} sec")
        
    def data_preparation(self, df_et_wmap_raw):
        df_data = df_et_wmap_raw[0]
        param_name = df_et_wmap_raw[1]
        
        wmap_raw_value_index = df_data.columns.get_loc('wmap')
        stored_wmap_raw_data = []
        stored_data_section = []
        expanded_data_column = []
        expanded_data_wafer = []  
        
        "Finalized data set"
        self.system_log.emit("Preparing data...")

        # Unpack ET site level data from wmap
        for row in range(len(df_data)):
            wmap_raw_value_str = df_data.loc[row][wmap_raw_value_index]
            wmap_raw_value = wmap_raw_value_str.replace(';', '')
            wmap_raw_value = wmap_raw_value.split(',')
            wmap_raw_value = list(filter(None, wmap_raw_value))
            wmap_raw_value_filtered = []
            
            for element in wmap_raw_value:
                element1 = element.split(':')
                element1[0] = int(element1[0])
                element1[1] = float(element1[1])
                element1 = tuple(element1)
                wmap_raw_value_filtered.append(element1)
                
                progression = np.round((row/len(df_data))*100, decimals = 2)
                self.progress.emit(progression)
                
            stored_wmap_raw_data.append(tuple(wmap_raw_value_filtered))
            stored_data_section.append(len(wmap_raw_value_filtered))
            
        # Consolidate measurements for each parameter as a column to perform data cleaning by IQR
        # Afterwhich the wafer averaged ET values are calculated
        self.progress.emit(0)  
        self.system_log.emit("Removing outliers...")
        
        count = 0
        for n in range(len(param_name)):  
            expanded_data_column = []
            expanded_data_wafer = []  
            expanded_data_ET = []
            for i in range(param_name[n][1]):
                for j in range(len(stored_wmap_raw_data[count])):
                    expanded_data_column.append(stored_wmap_raw_data[count][j][1])
                    expanded_data_wafer.append(df_data['Scribe'][count])
                    expanded_data_ET.append(df_data['Param_Name'][count])
                count += 1
            
            data = pd.DataFrame(expanded_data_column, columns = ['Data'])
            
            Q1 = data.quantile(q=.25)
            Q3 = data.quantile(q=.75)
            IQR = data.apply(stats.iqr)            
            
            data.insert(0, 'Param_Name', expanded_data_ET)
            data.insert(0, 'Scribe', expanded_data_wafer)
            
            data_clean = data[~((data[[data.columns[2]]] < (Q1-1.5*IQR)) | (data[[data.columns[2]]] > (Q3+1.5*IQR))).any(axis=1)]
            data_clean = data_clean.groupby(['Scribe','Param_Name'], sort = False).mean().reset_index()
            
            # Adding processed/cleaned ET parameters column by column for every iteration
            if n == 0:
                df_prepared_wmap_raw_data = data_clean
            else:
                df_prepared_wmap_raw_data = pd.concat([df_prepared_wmap_raw_data, data_clean]).reset_index(drop = True)
        
                progression = np.round((n/len(param_name))*100, decimals = 2)
                self.progress.emit(progression)
                    
        self.progress.emit(0) 
        df_prepared_wmap_raw_data = df_prepared_wmap_raw_data.pivot(index = 'Scribe', columns='Param_Name', values='Data').reset_index()
                  
        # Adding other information queried from SQL to the process/cleaned ET parameters
        # Compute missing data by replacing empty cell with the column average (Imputation) if no. of missing data from the column is < 20%
           
        #self.df_prepared_wmap_raw_data = self.df_prepared_wmap_raw_data.dropna(thresh=len(self.df_prepared_wmap_raw_data) - 0.2*len(self.df_prepared_wmap_raw_data), axis=1)
        df_full_et_data = pd.merge(df_data[['Product','Lot_No','Wafer_Alias','Scribe']], df_prepared_wmap_raw_data, how='left', on='Scribe')
        df_full_et_data.drop_duplicates(subset=('Scribe'), inplace = True)
        df_full_et_data.reset_index(drop = True, inplace = True)
        return df_full_et_data
    
    # Perform Standardization
    # Computation of eigenpairs and sorted in descending order
    
    def data_cleaning(self, data):
        df_full_data = data[0]
        variables = data[1]
        df_cleaned_data = df_full_data.fillna(df_full_data[variables].median())
        cleaned_data = df_cleaned_data[variables].to_numpy()    
        return cleaned_data, df_cleaned_data
    
    def PCA(self, cleaned_data):
        cleaned_data_array = cleaned_data[0]
        df_cleaned_data = cleaned_data[1]
        info_header = cleaned_data[2] 
        variables = cleaned_data[3]

        data_std = StandardScaler().fit_transform(cleaned_data_array)
        pca = PCA(n_components=10)
        pca.fit(data_std)
        principal_components = pca.components_.T
        loadings = pca.components_.T * np.sqrt(pca.explained_variance_)
        projected_data = pca.fit_transform(data_std)
        variance_explained_ratio = pca.explained_variance_ratio_
        PC_explained_variance = list(np.round(variance_explained_ratio[i]*100, decimals = 2) for i in range(len(variance_explained_ratio)))
        PC_explained_variance_cumulative = list(sum(PC_explained_variance[0:i+1]) for i in range(len(PC_explained_variance)))
        PC_index = []
        
        for i in range(len(PC_explained_variance)):
            PC_index.append(f"PC{+i+1}")
        df_PC_explained_variance = pd.DataFrame(list(zip(PC_explained_variance, PC_explained_variance_cumulative)), columns = ['Variance Explained (%)', 'Cumulative (%)'], index = PC_index)
        df_principal_components = pd.DataFrame(principal_components, columns = PC_index, index = variables)
        df_loadings = pd.DataFrame(loadings, columns = PC_index, index = variables)
        df_projected_data = pd.DataFrame(projected_data, columns = PC_index)
        df_full_projected_data = pd.concat([df_cleaned_data[info_header],df_projected_data], axis = 1)
        return df_PC_explained_variance, df_principal_components, df_full_projected_data, df_loadings
    
class Main(QMainWindow, Ui):
    # Main Class for initializing the application
    # Consist of function to switch between Main Menu, PCA & Data Query Window
    def __init__(self):     
        super(Main, self).__init__()
        self.oldstdout = sys.stdout
        self.oldstderr = sys.stderr
        self.setupUi(self)
        
        self.menuButton1.clicked.connect(self.menuWindow)
        self.DataQuerymenuButton.clicked.connect(self.menuWindow)
        self.PCAButton.clicked.connect(self.AnalysisWindow)
        self.DataQueryButton.clicked.connect(self.DataQueryWindow)
            
    def menuWindow(self):
        self.frames.setCurrentIndex(0)

    def AnalysisWindow(self):
        self.frames.setCurrentIndex(1)
        
    def DataQueryWindow(self):
        self.frames.setCurrentIndex(2)
        
    def closeEvent(self, event):
        sys.stdout = self.oldstdout
        sys.stderr = self.oldstderr    
        shutil.rmtree(self.outputfolder)
        event.accept()
        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    M = Main()
    sys.exit(app.exec())
