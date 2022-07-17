"""
File Name           : KBSMessageHandling.py

Description         : 

Author              : Zan Team
Date Created        : 19-Jan-2021
Date Last modified  : 

Copyright (C) 2021 ZAN COMPUTE - All Rights Reserved

"""

from KBSConfig import *
import os
import pandas as pd
import json
from datetime import datetime,date, timedelta
from KBSModel import KBSModel 
import sys,traceback
import calendar
from KBSLogger import logger
import logging

logging.basicConfig(level=logging.INFO)


REPORT_STATE = 'state'
REPORT_POSTAL_CODE = 'post_code'
REPORT_CITY = 'city'
REPORT_CATEGORY = 'category'
REPORT_EMPLOYEE_TYPE = 'emp_type'
REPORT_SHIFT = 'Shift_Type'
REPORT_DAY_OF_WEEK = 'Day_of_week'

REPORT_MONTH= 'Month'
REPORT_PUBLIC_HOLIDAYS = 'Public_Holidays'
REPORT_DATE_LOCAL = 'datetime_local'

STATE_CITY = 'state_city'
REPORT_DATE = 'Date'
REPORT_DATE_LIST = 'dates'
REPORT_DATE_PAYLOAD = 'date'
REPORT_POSTCODES = 'postcodes'
REPORT_CITIES = 'cities'
REPORT_STATES = 'states'
REPORT_EMP_VENDOR = 'Vendor'
REPORT_EMP_KBS    = 'KBS'
REPORT_MONTH      = 'Month'
REPORT_MONTH_INIT_MESSAGE = 'month'
REPORT_MONTH_NUMBER = 'month_number'

REPORT_SHIFT_1 = 'shift_1'
REPORT_SHIFT_2 = 'shift_2'
REPORT_SHIFT_3 = 'shift_3'

REPORT_NOSHOW_PROBABILITY = 'noshow_probability'
REPORT_COMP_NOSHOW = 'Complete_NS_Probability'

weekDays = ["Sunday","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]



class MessageHandling(object):
    '''
    
    
    '''
    
    def __init__(self):
        
        self.reportFilePath = None
        self.reportFilePathNewPage = None
        self.stateMonthFilePath = None
        self.monthDateDetails = {}

        self.categoryRecordsList = []
        self.initialize()
        
        self.kbsmodelobj = KBSModel()
       
        
        
    def getPostCodeWiseDetails(self,citydf):
        try:
            postcodegrouped = citydf.groupby(REPORT_POSTAL_CODE)
            
            
            postcodelist =[]
            for name, group in postcodegrouped :
                
                zipcodeDict = {}
                zipcodeDict[REPORT_POSTAL_CODE] = name
                #zipcodeDict[REPORT_CUSTOMERS] = group[REPORT_CUSTOMER_ID].unique().tolist() #self.getPostcodeWiseDetails(group)
                postcodelist.append(zipcodeDict)
            return postcodelist
            
        except Exception as ex:
            logger.error("Exception occurred in getCityWiseDetails Method : {}".format(str(ex)))    
            
            

    def getCityWiseDetails(self,statedf):
        try:
            
            
            citygrouped = statedf.groupby(REPORT_CITY)

            citylist = []

            for name, group in citygrouped :

                postCodeDict = {}
                postcodelist = self.getPostCodeWiseDetails(group)
                postCodeDict[REPORT_CITY]  = name
                postCodeDict[REPORT_POSTCODES] = postcodelist
                citylist.append(postCodeDict)

            return citylist
        except Exception as ex:
            logger.error("Exception occurred in getStateWiseDetails Method : {}".format(str(ex)))       
    def initialize(self):
        
        
        try:
            
            self.monthDateDetails['January']   = [1,31]
            self.monthDateDetails['February']  = [2,28]
            self.monthDateDetails['March']     = [3,31]
            self.monthDateDetails['April']     = [4,30]
            self.monthDateDetails['May']       = [5,31]
            self.monthDateDetails['June']      = [6,30]
            self.monthDateDetails['July']      = [7,31]
            self.monthDateDetails['August']    = [8,31]
            self.monthDateDetails['September'] = [9,30]
            self.monthDateDetails['October']   = [10,31]
            self.monthDateDetails['November']  = [11,30]
            self.monthDateDetails['December']  = [12,31]
            
            
            
            
            cwd = os.getcwd()            
            self.reportFilePath = '{}''{}''{}'.format(REPORT_FOLDER_NAME,os.sep,REPORT_FILE_NAME)
            self.reportFilePathNewPage = '{}''{}''{}'.format(REPORT_FOLDER_NAME, os.sep, REPORT_FILE_NAME_NEWPAGE)
            self.stateMonthFilePath = '{}''{}''{}'.format(REPORT_FOLDER_NAME,os.sep,REPORT_STAT_MONTH_FILE_NAME)
           
            report_df = pd.read_csv(self.reportFilePath)
            statemonth_df = pd.read_csv(self.stateMonthFilePath)
            
            #satemonthgrouped = statemonth_df.groupby(REPORT_STATE)
            
            categoryGrouped = report_df.groupby(REPORT_CATEGORY)
            categoryList =[]
            for category, groupdf in categoryGrouped:
                logging.info("category:{}".format(category))
                categoryDict={}
                categoryDict[REPORT_CATEGORY] = category
                sateGrouped = groupdf.groupby(REPORT_STATE)
                statelist =[]
                for name, stateDf in sateGrouped:
                    logging.info("state:{}".format(name))
                    stateDict = {}
                    monthdf = statemonth_df[ (statemonth_df[REPORT_CATEGORY] == category)]
                    monthdf = monthdf[ (monthdf[REPORT_STATE] == name)]
                    monthdf.sort_values(REPORT_MONTH_NUMBER, axis = 0, ascending = True, inplace = True) 
                    #print(monthdf['month'].tolist())
                    citylist = self.getCityWiseDetails(stateDf)
                    stateDict[REPORT_STATE] = name
                    stateDict[REPORT_MONTH_INIT_MESSAGE] = monthdf[REPORT_MONTH_INIT_MESSAGE].tolist()
                    stateDict[REPORT_CITIES] = citylist
                    statelist.append(stateDict)
                categoryDict[REPORT_STATES]=statelist
                categoryList.append(categoryDict)
            self.categoryRecordsList=categoryList
            '''
            with open('data.txt', 'w') as outfile:
                json.dump(self.stateRecordsList, outfile)
            '''
            del report_df
        except Exception as ex:
            logging.info("Exception occurred in Initialize Method : {}".format(str(ex)))
            
  
            
    def dashboardInitializeRequest(self):
        
        jsonpayload = None
        try:
            
            jsonpayload = json.dumps(self.categoryRecordsList)
            logger.info("Initial Message handled")
        except Exception as ex:
            
            logger.error("Exception occurred : {}".format(str(ex)))
            
        finally:
            return jsonpayload
            
            
    def handleCustomerSpecificNoShowRequest(self,arglist) :
        '''
        arglist = [state,category,month,emptype,shift]
        '''
         
        jsonpayload = None
        try:
             
            kbsdata_df = pd.read_csv(self.reportFilePath)
            #print(arglist) 
            #print(kbsdata_df.head())
            
            data_df = kbsdata_df[ kbsdata_df[REPORT_CATEGORY] == arglist[1] ]
            if data_df.empty:
                print("1.Invalid records")
                return
            #[state,category,fromdate,enddate,emptype,shift]
            if arglist[3] is not None :
                data_df = data_df[ (data_df[REPORT_EMPLOYEE_TYPE] == arglist[4] )]
                if data_df.empty:
                    print("2.Invalid records")
                    return
            if arglist[4] is not None :
                data_df = data_df[ (data_df[REPORT_SHIFT] == arglist[5] )]
                if data_df.empty:
                    print("3.Invalid records")
                    return
            state = arglist[0]
            category = arglist[1]
            #print(data_df)
            
            startdate = datetime.strptime(arglist[2], '%Y-%m-%d')
            enddate = datetime.strptime(arglist[3], '%Y-%m-%d')

            numrecords = data_df.shape[0]     # Number of rows 
            
            
            if arglist[2] == arglist[3] :                
                datelist = [arglist[2]]*numrecords
                data_df[REPORT_DATE_LOCAL] = datelist
                state = data_df.iloc[0][REPORT_STATE]

            else:
                # Generate date lsit from start date and end date
                datelist = []
                delta = enddate - startdate       # as timedelta
                state = data_df.iloc[0][REPORT_STATE]
                for indx in range(delta.days + 1):
                    datelist.append(startdate + timedelta(days=indx))
                    #print(datelist)
                final_df = None
                for day in datelist:    
                    daylist1 = [day]*numrecords
                    df = pd.DataFrame(daylist1,columns=[REPORT_DATE_LOCAL])
                    tmp_df = pd.concat([data_df,df],axis=1)
                    if final_df is None:
                        final_df = tmp_df
                    else:
                        final_df = pd.concat([final_df,tmp_df],ignore_index=True)            
            
                data_df = final_df

            #print(data_df)
            rspmsg = self.kbsmodelobj.model_handling(data_df,state,category)
            #jsonpayload = rspmsg.to_json()
            rspPayload = None
            if rspmsg is None:
                jsonpayload = "No Records for the selected Customer :{}".format(arglist[1])
            else:
                rspPayload = self.getResponsePayload(rspmsg)
                #print("Response Message  :  {}".format(rspPayload))
                jsonpayload = json.dumps(rspPayload)

        except Exception as ex:
            
            logger.error("Exception occurred in handleCustomerSpecificNoShowRequest method: {}".format(str(ex)))
            if hasattr(ex, 'message'):
                print("Exception rasied %s", ex.message)
                error_msg = {'error': ex.message}
            else:
                print("Exception rasied %s", type(ex).__name__)
                error_msg = {'error': type(ex).__name__}
            
            exc_type, exc_obj, exc_tb = sys.exc_info()
    
            #logging.exception("something went wrong with exception")
    
    
            print("something went wrong with exception on line %s", exc_tb.tb_lineno)
    
            traceback.print_exc(file=sys.stdout)
            print("Failed to handle handleCustomerSpecificNoShowRequest method ::: {}".format( str(ex)))
            
            
        finally:
            return jsonpayload  


    def handleStateCitySpecificNoShowRequest(self,arglist) :
        '''
        arglist = [state,city,post_code,category,month,emptype,shift]
        '''
         
        jsonpayload = "No valid records"
        try:
             
            kbsdata_df = pd.read_csv(self.reportFilePath,index_col=False)
            
            data_df = kbsdata_df[( kbsdata_df[REPORT_STATE] == arglist[0]) & \
                                 (kbsdata_df[REPORT_CITY] == arglist[1]) & \
                                 ( kbsdata_df[REPORT_POSTAL_CODE] == arglist[2]) ]
            #[state,cust_id,fromdate,enddate,emptype,shift]
            if data_df.empty:
                print("1.Invalid records")
                return jsonpayload
            '''    
            #print(data_df)
            if arglist[2] is not None :
                data_df = data_df[ (data_df[REPORT_POSTAL_CODE] == arglist[2] )]
                if data_df.empty:
                    print("2.Invalid records")
                    return
            '''
            #print(data_df)
            if arglist[3] is not None :
                data_df = data_df[ (data_df[REPORT_CATEGORY] == arglist[3] )]
                if data_df.empty:
                    print("3.Invalid records")
                    return jsonpayload
                
            if arglist[5] is not None :
                data_df = data_df[ (data_df[REPORT_EMPLOYEE_TYPE] == arglist[5] )]
                if data_df.empty:
                    print("4.Invalid records")
                    return jsonpayload
                
            if arglist[6] is not None :
                data_df = data_df[ (data_df[REPORT_SHIFT] == arglist[6] )]
                if data_df.empty:
                    print("5.Invalid records")
                    return jsonpayload
            
            state = arglist[0]
            category = arglist[3]

            monthnumber = self.monthDateDetails[arglist[4]][0]
            #numberofdays = self.monthDateDetails[arglist[4]][1]
            
            year = datetime.today().year
            num_days = calendar.monthrange(year, monthnumber)

            
            #first_day = datetime.today().replace(day=1)
            
            first_day = date(year, monthnumber, 1)
            last_day = date(year, monthnumber, num_days[1])

            #startdate = datetime.strptime(first_day, '%Y-%m-%d')
            #enddate = datetime.strptime(last_day, '%Y-%m-%d')
            
            startdate = first_day
            enddate = last_day
         
            
            #startdate = datetime.strptime(arglist[4], '%d-%m-%Y')
            #enddate = datetime.strptime(arglist[5], '%d-%m-%Y')

            numrecords = data_df.shape[0]     # Number of rows 
            data_df.reset_index(inplace = True) 

            '''
            if arglist[4] == arglist[5] :                
                datelist = [arglist[4]]*numrecords
                data_df[REPORT_DATE_LOCAL] = datelist
                #state = data_df.iloc[0][REPORT_STATE]

            else:
            '''
            # Generate date lsit from start date and end date
            datelist = []
            delta = enddate - startdate       # as timedelta
            #state = data_df.iloc[0][REPORT_STATE]
            for indx in range(delta.days + 1):
                datelist.append(startdate + timedelta(days=indx))    

            final_df = None
            for day in datelist:    
                daylist1 = [day]*numrecords
                df = pd.DataFrame(daylist1,columns=[REPORT_DATE_LOCAL])
                tmp_df = pd.concat([data_df,df],axis=1)
                if final_df is None:
                    final_df = tmp_df
                else:
                    final_df = pd.concat([final_df,tmp_df],ignore_index=True)            
        
            data_df = final_df

            
            vendor_df,kbsemp_df,holidaydata_df,output_df = self.kbsmodelobj.model_handling(data_df,state,category,1)
            
            rspPayload = {}
            if vendor_df is None and kbsemp_df is None and holidaydata_df is None:
                jsonpayload = "No valid records"
            else: 
                flag = 1
                
                if not vendor_df.empty:  # is not None:
                    #vendor_df = vendor_df.drop(['level_0','index','Public_Holidays','Day_of_Week','Month','CID','Zip_Code','datetime_local'], axis = 1) 
                    #vendor_df = vendor_df.drop(['level_0','index','Public_Holidays','CID','Zip_Code','datetime_local'], axis = 1) 
                    rspPayload = self.getResponsePayload(rspPayload,vendor_df, flag,1)
   
                    flag = 0


                if not kbsemp_df.empty : # is not None:
                    #kbsemp_df = kbsemp_df.drop(['level_0','index','Public_Holidays','CID','Zip_Code','datetime_local'], axis = 1) 
                    rspPayload = self.getResponsePayload(rspPayload,kbsemp_df, flag,2)
                    flag = 0
  
                if not holidaydata_df.empty:  # is not None:
                    #holidaydata_df = holidaydata_df.drop(['level_0','index','Public_Holidays','CID','Zip_Code','datetime_local'], axis = 1) 
                    rspPayload = self.getResponsePayload(rspPayload,holidaydata_df, flag,3)
                    flag = 0


                #rspPayload = self.getResponsePayload(vendor_df,kbsemp_df,holidaydata_df)
                #print("Response Message  :  {}".format(rspPayload))
            if len(rspPayload) != 0:
                jsonpayload = json.dumps(rspPayload)
            else:
                jsonpayload = "No valid records"
                #jsonpayload = json.dumps(rspPayload)

        except Exception as ex:
            jsonpayload = "Internal Error"
            logger.error("Exception occurred in handleStateCitySpecificNoShowRequest method: {}".format(str(ex)))
            if hasattr(ex, 'message'):
                print("Exception rasied %s", ex.message)
                error_msg = {'error': ex.message}
            else:
                print("Exception rasied %s", type(ex).__name__)
                error_msg = {'error': type(ex).__name__}
            
            exc_type, exc_obj, exc_tb = sys.exc_info()

            print("something went wrong with exception on line %s", exc_tb.tb_lineno)
    
            traceback.print_exc(file=sys.stdout)
            print("Failed to handle handleStateCitySpecificNoShowRequest method ::: {}".format( str(ex)))
            
            
        finally:
            return jsonpayload

    def handleStateCategorySpecificNoShowRequest(self, arglist):
        '''
        arglist = [state,category]
        '''

        jsonpayload = "No valid records"
        try:

            kbsdata_df = pd.read_csv(self.reportFilePathNewPage, index_col=False)

            data_df = kbsdata_df[(kbsdata_df[REPORT_STATE] == arglist[0]) & \
                                 (kbsdata_df[REPORT_CATEGORY] == arglist[1]) ]
            # [state,cust_id,fromdate,enddate,emptype,shift]
            if data_df.empty:
                print("1.Invalid records")
                return jsonpayload

            state = arglist[0]
            category = arglist[1]

            vendor_df, kbsemp_df, holidaydata_df, output_df = self.kbsmodelobj.model_handling(data_df, state, category,2)

            rspPayload = {}
            if output_df is None :
                jsonpayload = "No valid records"
            else:
                if not output_df.empty:  # is not None:
                    rspPayload = self.getResponsePayloadSateCategory(rspPayload, output_df)

            if len(rspPayload) != 0:
                jsonpayload = json.dumps(rspPayload)
            else:
                jsonpayload = "No valid records"

        except Exception as ex:
            jsonpayload = "Internal Error"
            logger.error("Exception occurred in handleStateCitySpecificNoShowRequest method: {}".format(str(ex)))
            if hasattr(ex, 'message'):
                print("Exception rasied %s", ex.message)
                error_msg = {'error': ex.message}
            else:
                print("Exception rasied %s", type(ex).__name__)
                error_msg = {'error': type(ex).__name__}

            exc_type, exc_obj, exc_tb = sys.exc_info()

            print("something went wrong with exception on line %s", exc_tb.tb_lineno)

            traceback.print_exc(file=sys.stdout)
            print("Failed to handle handleStateCitySpecificNoShowRequest method ::: {}".format(str(ex)))


        finally:
            return jsonpayload


    def getNoShowProbabilityDetails(self,datadf,version):
        try:
            #logger.info(datadf)
            shiftdetails = []
            shiftgrouped = datadf.groupby("Shift_Type")
            if version == 1 :   # Old GUI
                for name, shiftgroup in shiftgrouped:
                    shiftDict = {}
                    shiftDict["shift"] = name
                    shiftDict["noshowprobability"] = round((float(shiftgroup[REPORT_COMP_NOSHOW].tolist()[0]) * 100),2)

                    shiftdetails.append(shiftDict)
            else:
                for name, shiftgroup in shiftgrouped:
                    shiftDict = {}
                    shiftDict["shift"] = name
                    shiftDict["noshowprobability"] = round((float(shiftgroup[REPORT_COMP_NOSHOW].tolist()[0]) * 100), 2)
                    shiftDict['vendor_ident'] = int(shiftgroup['vendor_ident'].tolist()[0])
                    shiftDict['Complete_NS_Count'] = int(shiftgroup['Complete_NS_Count'].tolist()[0])
                    shiftDict['Show_count'] = int(shiftgroup['Show_count'].tolist()[0])
                    shiftdetails.append(shiftDict)
                
            return shiftdetails
            
            
        except Exception as ex:            
            logger.error("Exception occurred in getNoShowProbabilityDetails method: {}".format(str(ex)))
        
            
            
    def getNoshowReportDetails(self,data_df) :
        
        try:
            noshowdatalist = []
            
            noshowgrouped = data_df.groupby("Day_Of_Week")
            for day in weekDays:
                for name, daygroup in noshowgrouped:
                    if(day == name):
                        noshowDict= {}
                        noshowDict["day"] = name
                        noshowDict["shiftdata"] = self.getNoShowProbabilityDetails(daygroup,1)
                        noshowdatalist.append(noshowDict)
            return noshowdatalist

        
        except Exception as ex:
            logger.error("Exception occurred in getNoshowReportDetails method: {}".format(str(ex)))

    def getPostCodeResponseDetails(self, outputdf,ouyput_type) :
        
        try:
            #customergrouped = outputdf.groupby([REPORT_CUSTOMER_ID,'Month','Day_of_Week','Shift_Type'])
            
            
            postCodeList = []
            if ouyput_type == 3 : # Holiday specific no show probal
                postCodeGrouped = outputdf.groupby([REPORT_POSTAL_CODE,"Date","emp_type"])

                for name, postCodeGroup in postCodeGrouped :

                    postCodeDict = {}
                    postCodeDict[REPORT_POSTAL_CODE] = str(name[0])
                    postCodeDict["date"] = name[1]
                    postCodeDict[REPORT_EMPLOYEE_TYPE] = name[2]
                    postCodeDict["noshowdata"] = self.getNoshowReportDetails(postCodeGroup)
                    postCodeList.append(postCodeDict)
                
            else:
                postCodeGrouped = outputdf.groupby(REPORT_POSTAL_CODE)
                
                for name, postCodeGroup in postCodeGrouped :
                    postCodeDict = {}
                    postCodeDict[REPORT_POSTAL_CODE] = str(name)
    
                    postCodeDict["noshowdata"] = self.getNoshowReportDetails(postCodeGroup)

                    postCodeList.append(postCodeDict)

            return  postCodeList

        except Exception as ex:
            
            logger.error("Exception occurred in getCustomerResponseDetails method: {}".format(str(ex)))

    def getResponsePayload(self,rspDict,output_df, flag,ouyput_type) :
        
        try:

            if flag:
                
                state = output_df[REPORT_STATE].unique().tolist()    
                city = output_df[REPORT_CITY].unique().tolist()    
                postcode = output_df[REPORT_POSTAL_CODE].unique().tolist()   
                month = output_df[REPORT_MONTH].unique().tolist() 
                
                rspDict[REPORT_STATE] = state[0]
                rspDict[REPORT_CITY] = city[0]
                rspDict[REPORT_POSTAL_CODE] = postcode[0]
                rspDict[REPORT_MONTH] = month[0]
            
            
            type = "holidaysdata"
            if ouyput_type == 1:
                type = "kbsvendors"
            
            elif ouyput_type == 2:
                type = "kbsemp"
                
            payload = self.getPostCodeResponseDetails(output_df,ouyput_type)
            rspDict[type] = payload


            return rspDict
        except Exception as ex:
            
            logger.error("Exception occurred in getResponsePayload method: {}".format(str(ex)))

    def getResponsePayloadSateCategory(self, rspDict, output_df):

        try:

            state = output_df[REPORT_STATE].unique().tolist()
            category = output_df[REPORT_CATEGORY].unique().tolist()

            rspDict[REPORT_STATE] = state[0]
            rspDict[REPORT_CATEGORY] = category[0]
            payload = self.getCityResponseDetails(output_df)
            rspDict["citydata"] = payload

            return rspDict
        except Exception as ex:
            logger.error("Exception occurred in getResponsePayloadSateCategory method: {}".format(str(ex)))

    def getCityResponseDetails(self,output_df):
        try:
            cityList = []
            cityGrouped = output_df.groupby(REPORT_CITY)
            for name, cityGroup in cityGrouped:
                cityDict = {}
                cityDict[REPORT_CITY] = str(name)
                cityDict["postaldata"] = self.getNoshowPostCodeReportDetails(cityGroup)

                cityList.append(cityDict)
            return cityList

        except Exception as ex:
            logger.error("Exception occurred in getCityResponseDetails method: {}".format(str(ex)))


    def getNoshowPostCodeReportDetails(self, citygroup_df):

        try:
            pcList = []
            postcodeGrouped = citygroup_df.groupby(REPORT_POSTAL_CODE)
            for name, pcGroup in postcodeGrouped:
                pcDict = {}
                pcDict[REPORT_POSTAL_CODE] = str(name)
                pcDict["emptypedata"] = self.getNoshowEmpTypeReportDetails(pcGroup)

                pcList.append(pcDict)
            return pcList

        except Exception as ex:
            logger.error("Exception occurred in getNoshowPostCodeReportDetails method: {}".format(str(ex)))

    def getNoshowEmpTypeReportDetails(self, pcgroup_df):

        try:
            empList = []
            empGrouped = pcgroup_df.groupby(REPORT_EMPLOYEE_TYPE)
            for name, empGroup in empGrouped:
                empDict = {}
                empDict[REPORT_EMPLOYEE_TYPE] = str(name)
                empDict["seasondata"] = self.getNoshowSeasonReportDetails(empGroup)

                empList.append(empDict)
            return empList
        except Exception as ex:
            logger.error("Exception occurred in getNoshowEmpTypeReportDetails method: {}".format(str(ex)))

    def getNoshowSeasonReportDetails(self, empgroup_df):

        try:
            seasonList = []
            seasonGrouped = empgroup_df.groupby('Season_Type')
            for name, seasonGroup in seasonGrouped:
                seasonDict = {}
                seasonDict['Season_Type'] = str(name)
                seasonDict["shiftdata"] = self.getNoShowProbabilityDetails(seasonGroup,2)

                seasonList.append(seasonDict)
            return seasonList

        except Exception as ex:
            logger.error("Exception occurred in getNoshowSeasonReportDetails method: {}".format(str(ex)))



# Testing Purpose
           
if __name__=='__main__':
    
    msgObj = MessageHandling()

    #fromdate  = '2021-2-18'
    #enddate   = '2021-2-21'
    month     = 'July'
    state     = 'CA'
    city      = 'WENATCHEE'    #enatchee' #'BAKERSFIELD'
    post_code = '30324'   #'93308'
    cust_id   = None   #int('490789')
    emptype   = None  #'KBS_Vendor'
    shift     = None  #'First_Shift'
    category = 'Bank'
    '''
    arglist = ['1',cust_id,fromdate,enddate,emptype,shift]
    msgObj.handleCustomerSpecificNoShowRequest(arglist)
    WA, Wenatchee, ZC:30324, July
    '''
    # arglist = [state,city,post_code,cust_id,month,emptype,shift]
    # msgObj.handleStateCitySpecificNoShowRequest(arglist)
    arglist = [state,category]
    msgObj.handleStateCategorySpecificNoShowRequest(arglist)
