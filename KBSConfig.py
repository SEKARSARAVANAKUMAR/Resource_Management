"""
File Name           : KBSConfig.py

Description         : This file contains all configured parameters detail

Author              : Zan Team
Date Created        : 19-Jan-2021
Date Last modified  : 

Copyright (C) 2021 ZAN COMPUTE - All Rights Reserved

"""

###############################################################################
#
#           LIST of Dependent Packages
#
###############################################################################
'''
Install the following  libraries
1. sudo pip3 install flask
2. sudo pip3 install in_place
3. sudo pip3 install pandas
4.sudo pip3 install scipy
5.sudo pip3 install scikit-learn
'''

from configparser import ConfigParser

KBS_CONFIG_FILE_NAME='KBSconfigData.cfg'
# create configuration parser object
config = ConfigParser()

# read the config properties file
config.read(KBS_CONFIG_FILE_NAME)


###############################################################################
#
#   Below are the parameters that are configured through the configuration app
#
###############################################################################
REPORT_FOLDER_NAME = 'Report'
REPORT_FILE_NAME = 'KBS_Features_For_Model.csv'
REPORT_STAT_MONTH_FILE_NAME = 'KBS_state_month.csv'
REPORT_FILE_NAME_NEWPAGE ='KBS_NEW_PAGE_NEW_FORMAT.csv'

#REPORT_FOLDER_NAME = config.get('{}'.format('KBS'), 'REPORT_FOLDER_NAME')

#REPORT_FILE_NAME = config.get('{}'.format('KBS'), 'REPORT_FILE_NAME')


###############################################################################
#
#   Below are the parameters that are used in the GUI to represent the fileds
#
###############################################################################

GUI_STATE ='state'
GUI_CITY  = 'city'
GUI_POST_CODE = 'postcode'
GUI_SHIFT_TIME = 'shift'
GUI_EMP_TYPE   = 'emp_type'
GUI_CUSTOMET_ID = 'customerid'

###############################################################################
#
#   Flask Server Details
#
###############################################################################


HOST_NAME='172.31.18.23'
#HOST_NAME='127.0.0.1'
PORT=5000
