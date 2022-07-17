"""
File  Name          : KBSLogger.py

Description         : To store the out and error logs

Description         : 

Author              : Zan Team
Date Created        : 19-Jan-2021
Date Last modified  : 

Copyright (C) 2021 ZAN COMPUTE - All Rights Reserved

"""

import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
#RotatingFileHandler,
import os
from datetime import datetime

LOG_FOLDER_NAME = 'Log'
LOG_FILE_NAME   = 'KBS'

#logger = None

###############################################################################
#
#                   handleLoggerInitialization    
#
###############################################################################

#def handleLoggerInitialization():
    # creation of log file path idf it is not existing
if not os.path.exists(LOG_FOLDER_NAME):
    os.makedirs(LOG_FOLDER_NAME)
# Loging file handler
time_val = datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")
LOG_FILE_NAME_NEW = "{}_{}.log".format(LOG_FILE_NAME,time_val)
LOG_FILE_PATH = '{}''{}''{}'.format(LOG_FOLDER_NAME,os.sep,LOG_FILE_NAME_NEW)
#handler = TimedRotatingFileHandler(LOG_FILE_PATH,when='S',interval=5,backupCount=5)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s,%(levelname)s,%(filename)s,%(funcName)s,%(lineno)d,%(message)s')

handler = TimedRotatingFileHandler(LOG_FILE_PATH,when='MIDNIGHT',backupCount=30)

handler.setFormatter(formatter)
logger.addHandler(handler)

