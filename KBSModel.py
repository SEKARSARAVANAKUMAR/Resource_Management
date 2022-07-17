"""
File Name           : KBSMessageHandling.py

Description         : 

Author              : Zan Team
Date Created        : 19-Jan-2021
Date Last modified  : 

Copyright (C) 2021 ZAN COMPUTE - All Rights Reserved
"""


from smile_license import pysmile as pysmile
from smile_license import pysmile_license
import pandas as pd
import numpy as np
import os
import holidays
import datetime
from KBSLogger import logger


State=None
net = pysmile.Network()
CA=None
category=None
#CA=Input DataFrame

MODEL_FILES_FOLDER = "ModelFiles"

class KBSModel(object):
    
    def __init__(self):
         
        cwd = os.getcwd()
         
        self.Bank_OH = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Bank_OH.xdsl")
        self.Bank_OR = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Bank_OR.xdsl")
        self.Bank_PAMICA = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Bank_PAMICA.xdsl")
        self.Bank_WA = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Bank_WA.xdsl")
        self.Data_Center_Telecom = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Data Centers & Telecom.xdsl")
        self.Distribution_Center = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Distribution Center.xdsl")
        self.Grocery_IL = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_IL.xdsl")
        self.Grocery_KSTNORCA = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_KSTNORCA.xdsl")
        self.Grocery_MI_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_MI_FSP.xdsl")
        self.Grocery_MI_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_MI_WS.xdsl")
        self.Grocery_OH_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_OH_FSP.xdsl")
        self.Grocery_OH_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_OH_WS.xdsl")
        self.Grocery_PA = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_PA.xdsl")
        self.Grocery_WA = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Grocery_WA.xdsl")
        self.Health_Club = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Health Club.xdsl")
        self.Transportation = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Transportation.xdsl")
        self.Retail_CA_April = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_April.xdsl")
        self.Retail_CA_Aug = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Aug.xdsl")
        self.Retail_CA_Dec = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Dec.xdsl")
        self.Retail_CA_Feb = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Feb.xdsl")
        self.Retail_CA_Jan = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Jan.xdsl")
        self.Retail_CA_July = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_July.xdsl")
        self.Retail_CA_June = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_June.xdsl")
        self.Retail_CA_Mar = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Mar.xdsl")
        self.Retail_CA_May = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_May.xdsl")
        self.Retail_CA_Nov = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Nov.xdsl")
        self.Retail_CA_Oct = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Oct.xdsl")
        self.Retail_CA_Sep = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_CA_Sep.xdsl")
        self.Retail_IL_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_IL_FSP.xdsl")
        self.Retail_IL_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_IL_WS.xdsl")
        self.Retail_KS_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_KS_FSP.xdsl")
        self.Retail_KS_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_KS_WS.xdsl")
        self.Retail_MI_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_MI_FSP.xdsl")
        self.Retail_MI_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_MI_WS.xdsl")
        self.Retail_OH_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_OH_FSP.xdsl")
        self.Retail_OH_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_OH_WS.xdsl")
        self.Retail_OR_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_OR_FSP.xdsl")
        self.Retail_OR_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_OR_WS.xdsl")
        self.Retail_PA_F = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_PA_F.xdsl")
        self.Retail_PA_S = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_PA_S.xdsl")
        self.Retail_PA_SP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_PA_SP.xdsl")
        self.Retail_PA_W = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_PA_W.xdsl")
        self.Retail_TN_FSP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_TN_FSP.xdsl")
        self.Retail_TN_WS = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_TN_WS.xdsl")
        self.Retail_WA_F = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_WA_F.xdsl")
        self.Retail_WA_SP = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_WA_SP.xdsl")
        self.Retail_WA_W = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_WA_W.xdsl")
        self.Retail_WA_S = "{}{}{}{}{}".format(cwd,os.sep,MODEL_FILES_FOLDER,os.sep,"Retail_WA_S.xdsl")

    def public_holiday_finder(self,updated_Df):        
        try:
            updated_Df['Date']=pd.to_datetime(updated_Df['datetime_local']).apply(lambda X:X.strftime('%Y-%m-%d'))
            updated_Df.reset_index(inplace=True)
            ust=np.array(updated_Df.state.unique())

            current_year = datetime.datetime.now().year

            updated_Df['Public_Holidays']=np.repeat('No',len(updated_Df))
            for i in range(0,len(ust)):
                h_day=[]
                for date, name in sorted(holidays.US(state=ust[i], years=current_year).items()):
                    h_day.append(date.strftime('%Y-%m-%d'))
                #print(h_day)
                for j in range(0,len(h_day)):
                    updated_Df['Public_Holidays']=np.where((updated_Df.state==ust[i])&(updated_Df.Date==h_day[j]),'Yes',updated_Df['Public_Holidays'])

            return updated_Df
        
        except Exception as ex:            
            logger.error("Exception occurred in public_holiday_finder : {}".format(str(ex)))
    
    
    
    
    def Complete_NS_Probability_Type1(self,CA,net,page_no):
        try:
            probab=[]
            for index,rows in CA.iterrows():
                try:
                    #print(rows)
                    net.clear_all_evidence()
                    #net.set_evidence("state",""+rows['state']+"")
                    net.set_evidence("zip_code",""+rows['zip_code']+"")
                    #net.set_evidence("emp_type",""+rows['emp_type']+"")
                    net.set_evidence("Season_Type",""+rows['Season_Type']+"")
                    net.set_evidence("Shift_Type",""+rows['Shift_Type']+"")
                    if page_no==1:
                        net.set_evidence("Public_Holidays",""+rows['Public_Holidays']+"")
                        net.set_evidence("Day_Of_Week",""+rows['Day_Of_Week']+"")
                    else:
                        print('page_2_request')
                    net.update_beliefs() 
                    beliefs = net.get_node_value("Comp_NS_Status") 
                    probab.append(beliefs[0])
                    #print(beliefs)
                    #break
                except:
                    probab.append(0.01)
            CA['Complete_NS_Probability']=probab
            return CA
        except Exception as ex:            
            logger.error("Exception occurred in Complete_NS_Probability_Type1 : {}".format(str(ex)))

    def Complete_NS_Probability_Type2(self,CA,net,page_no):
        try:
            probab=[]
            for index,rows in CA.iterrows():
                try:
                    net.clear_all_evidence()
                    #net.set_evidence("state",""+rows['state']+"")
                    net.set_evidence("zip_code",""+rows['zip_code']+"")
                    net.set_evidence("emp_type",""+rows['emp_type']+"")
                    net.set_evidence("Season_Type",""+rows['Season_Type']+"")
                    net.set_evidence("Shift_Type",""+rows['Shift_Type']+"")
                    if page_no==1:
                        net.set_evidence("Public_Holidays",""+rows['Public_Holidays']+"")
                        net.set_evidence("Day_Of_Week",""+rows['Day_Of_Week']+"")
                    else:
                        print("page_2_request")
                    net.update_beliefs() 
                    beliefs = net.get_node_value("Comp_NS_Status") 
                    probab.append(beliefs[0])
                except:
                    probab.append(0.01)
            CA['Complete_NS_Probability']=probab
            return CA
        except Exception as ex:            
            logger.error("Exception occurred in Complete_NS_Probability_Type2 : {}".format(str(ex)))
    
    def Complete_NS_Probability_Type3(self,CA,net,page_no):
        try:
            probab=[]
            for index,rows in CA.iterrows():
                try:
                    #print(rows)
                    net.clear_all_evidence()
                    net.set_evidence("state",""+rows['state']+"")
                    net.set_evidence("zip_code",""+rows['zip_code']+"")
                    net.set_evidence("emp_type",""+rows['emp_type']+"")
                    net.set_evidence("Season_Type",""+rows['Season_Type']+"")
                    net.set_evidence("Shift_Type",""+rows['Shift_Type']+"")
                    if page_no==1:
                        net.set_evidence("Public_Holidays",""+rows['Public_Holidays']+"")
                        net.set_evidence("Day_Of_Week",""+rows['Day_Of_Week']+"")
                    else:
                        print("page_2_request")
                    net.update_beliefs() 
                    beliefs = net.get_node_value("Comp_NS_Status") 
                    probab.append(beliefs[0])
                except:
                    probab.append(0.01)
            CA['Complete_NS_Probability']=probab
            return CA
        except Exception as ex:            
            logger.error("Exception occurred in Complete_NS_Probability_Type3 : {}".format(str(ex)))
    
    def Complete_NS_Probability_Type4(self,CA,net,page_no):
        try:
            probab=[]
            for index,rows in CA.iterrows():
                try:
                    net.clear_all_evidence()
                    net.set_evidence("state",""+rows['state']+"")
                    net.set_evidence("zip_code",""+rows['zip_code']+"")
                    net.set_evidence("emp_type",""+rows['emp_type']+"")
                    net.set_evidence("Season_Type",""+rows['Season_Type']+"")
                    #net.set_evidence("Shift_Type",""+rows['Shift_Type']+"")
                    if page_no==1:
                        net.set_evidence("Public_Holidays",""+rows['Public_Holidays']+"")
                        net.set_evidence("Day_Of_Week",""+rows['Day_Of_Week']+"")
                    else:
                        print("page_2_request")
                    net.update_beliefs() 
                    beliefs = net.get_node_value("Comp_NS_Status") 
                    probab.append(beliefs[0])
                except:
                    probab.append(0.01)
            CA['Complete_NS_Probability']=probab
            return CA
        except Exception as ex:            
            logger.error("Exception occurred in Complete_NS_Probability_Type4 : {}".format(str(ex)))

    def Complete_NS_Probability_Type5(self,CA,net,page_no):
        try:
            probab=[]
            for index,rows in CA.iterrows():
                try:
                    net.clear_all_evidence()
                    #net.set_evidence("state",""+rows['state']+"")
                    net.set_evidence("zip_code",""+rows['zip_code']+"")
                    net.set_evidence("emp_type",""+rows['emp_type']+"")
                    #net.set_evidence("Season_Type",""+rows['Season_Type']+"")
                    net.set_evidence("Shift_Type",""+rows['Shift_Type']+"")
                    if page_no==1:
                        #net.set_evidence("Public_Holidays",""+rows['Public_Holidays']+"")
                        net.set_evidence("Day_Of_Week",""+rows['Day_Of_Week']+"")
                    else:
                        print("page_2_request")
                    net.update_beliefs() 
                    beliefs = net.get_node_value("Comp_NS_Status") 
                    probab.append(beliefs[0])
                except:
                    probab.append(0.01)
            CA['Complete_NS_Probability']=probab
            return CA
        except Exception as ex:            
            logger.error("Exception occurred in Complete_NS_Probability_Type5 : {}".format(str(ex)))

    def Complete_NS_Probability_Type6(self,CA,net,page_no):
        try:
            probab=[]
            for index,rows in CA.iterrows():
                try:
                    net.clear_all_evidence()
                    #net.set_evidence("state",""+rows['state']+"")
                    net.set_evidence("zip_code",""+rows['zip_code']+"")
                    net.set_evidence("emp_type",""+rows['emp_type']+"")
                    #net.set_evidence("Season_Type",""+rows['Season_Type']+"")
                    net.set_evidence("Shift_Type",""+rows['Shift_Type']+"")
                    if page_no==1:
                        net.set_evidence("Public_Holidays",""+rows['Public_Holidays']+"")
                        net.set_evidence("Day_Of_Week",""+rows['Day_Of_Week']+"")
                    else:
                        print("page_2_request")
                    net.update_beliefs() 
                    beliefs = net.get_node_value("Comp_NS_Status") 
                    probab.append(beliefs[0])
                except:
                    probab.append(0.01)
            CA['Complete_NS_Probability']=probab
            return CA
        except Exception as ex:            
            logger.error("Exception occurred in Complete_NS_Probability_Type6 : {}".format(str(ex)))
            
    def model_handling(self,data_df,state,category,page_no):  
        vendor_df = None
        kbsemp_df = None
        holidaydata_df = None
        try:
            model_output = None
            Season_Type=None
            Month=None
            if page_no==1:
                data_df=self.public_holiday_finder(data_df)
                data_df['category']=category
                data_df['Day_Of_Week']=pd.to_datetime(data_df.datetime_local).dt.day_name()
                data_df['Month_int']=pd.to_datetime(data_df.datetime_local).dt.month
                data_df['Season_Type']=np.where((data_df.Month_int>=9)&(data_df.Month_int<=11),'Fall','Winter')
                data_df['Season_Type']=np.where((data_df.Month_int>2)&(data_df.Month_int<=5),'Spring',data_df['Season_Type'])
                data_df['Season_Type']=np.where((data_df.Month_int>=6)&(data_df.Month_int<=8),'Summer',data_df['Season_Type'])
                data_df['Month']=pd.to_datetime(data_df.datetime_local).dt.month_name()
                Season_Type=data_df['Season_Type'].unique()[0]
                Month=data_df['Month_int'].unique()[0]
            elif page_no==2:
                data_df['category']=category
            if category=='Bank':
                print(category)
                if state=='OH':
                    net.read_file(self.Bank_OH)
                    if page_no==1:
                        data_df=data_df[data_df.Day_Of_Week!='Sunday']
                        #data_df.reset_index(inplace=True)
                        model_output=self.Complete_NS_Probability_Type1(data_df,net,page_no)
                    if page_no==2:
                        model_output=self.Complete_NS_Probability_Type1(data_df,net,page_no)
                elif state=='OR':
                    net.read_file(self.Bank_OR)
                    model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                elif (state=='PA')|(state=='MI')|(state=='CA')|(state=='IL')|(state=='KS')|(state=='TN'):
                    net.read_file(self.Bank_PAMICA)
                    model_output=self.Complete_NS_Probability_Type3(data_df,net,page_no)
                elif state=='WA':
                    net.read_file(self.Bank_WA)
                    model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
            
            elif category=='Data Centers & Telecom':
                print(category)
                net.read_file(self.Data_Center_Telecom)
                model_output=self.Complete_NS_Probability_Type3(data_df,net,page_no)
            
            elif category=='Distribution Center':
                print(category)
                net.read_file(self.Distribution_Center)
                model_output=self.Complete_NS_Probability_Type3(data_df,net,page_no)

            elif category=='Grocery':
                print(category)
                if page_no==1:
                    if state=='IL':
                        net.read_file(self.Grocery_IL)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif state=='PA':
                        net.read_file(self.Grocery_PA)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif state=='WA':
                        net.read_file(self.Grocery_WA)
                        model_output=self.Complete_NS_Probability_Type1(data_df,net,page_no)
                    elif (state=='KS')|(state=='TN')|(state=='OR')|(state=='CA'):
                        net.read_file(self.Grocery_KSTNORCA)
                        model_output=self.Complete_NS_Probability_Type4(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Fall'):
                        net.read_file(self.Grocery_MI_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Spring'):
                        net.read_file(self.Grocery_MI_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Winter'):
                        net.read_file(self.Grocery_MI_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Summer'):
                        net.read_file(self.Grocery_MI_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Fall'):
                        net.read_file(self.Grocery_OH_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Spring'):
                        net.read_file(self.Grocery_OH_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Winter'):
                        net.read_file(self.Grocery_OH_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Summer'):
                        net.read_file(self.Grocery_OH_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                if page_no==2:
                    if state=='IL':
                        net.read_file(self.Grocery_IL)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif state=='PA':
                        net.read_file(self.Grocery_PA)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif state=='WA':
                        net.read_file(self.Grocery_WA)
                        model_output=self.Complete_NS_Probability_Type1(data_df,net,page_no)
                    elif (state=='KS')|(state=='TN')|(state=='OR')|(state=='CA'):
                        net.read_file(self.Grocery_KSTNORCA)
                        model_output=self.Complete_NS_Probability_Type4(data_df,net,page_no)
                    elif (state=='MI'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Grocery_MI_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Grocery_MI_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='OH'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Grocery_OH_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Grocery_OH_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)            
            elif category=='Health Club':
                print(category)
                net.read_file(self.Health_Club)
                model_output=self.Complete_NS_Probability_Type3(data_df,net,page_no)
            
            elif category=='Transportation':
                print(category)
                net.read_file(self.Transportation)
                model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
            
            elif category=='Retail':
                if page_no==1:
                    print(category)
                    if state=='CA':
                        print(category,state)
                        if Month==4:
                            net.read_file(self.Retail_CA_April)
                            model_output=self.Complete_NS_Probability_Type5(data_df,net,page_no)
                        elif Month==8:
                            net.read_file(self.Retail_CA_Aug)
                            model_output=self.Complete_NS_Probability_Type5(data_df,net,page_no)
                        elif Month==12:
                            net.read_file(self.Retail_CA_Dec)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==2:
                            net.read_file(self.Retail_CA_Feb)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==1:
                            net.read_file(self.Retail_CA_Jan)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==7:
                            net.read_file(self.Retail_CA_July)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==6:
                            net.read_file(self.Retail_CA_June)
                            model_output=self.Complete_NS_Probability_Type5(data_df,net,page_no)
                        elif Month==3:
                            net.read_file(self.Retail_CA_Mar)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==5:
                            net.read_file(self.Retail_CA_May)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==11:
                            net.read_file(self.Retail_CA_Nov)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==10:
                            net.read_file(self.Retail_CA_Oct)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                        elif Month==9:
                            net.read_file(self.Retail_CA_Oct)
                            model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='IL')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_IL_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='IL')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_IL_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='IL')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_IL_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='IL')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_IL_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='KS')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_KS_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='KS')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_KS_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='KS')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_KS_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='KS')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_KS_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_MI_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_MI_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_MI_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='MI')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_MI_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_OH_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_OH_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_OH_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OH')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_OH_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OR')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_OR_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OR')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_OR_FSP)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OR')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_OR_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='OR')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_OR_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='PA')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_PA_F)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='PA')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_PA_S)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='PA')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_PA_SP)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='PA')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_PA_W)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='TN')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_TN_FSP)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='TN')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_TN_FSP)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='TN')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_TN_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='TN')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_TN_WS)
                        model_output=self.Complete_NS_Probability_Type2(data_df,net,page_no)
                    elif (state=='WA')&(Season_Type=='Fall'):
                        net.read_file(self.Retail_WA_F)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='WA')&(Season_Type=='Spring'):
                        net.read_file(self.Retail_WA_SP)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='WA')&(Season_Type=='Winter'):
                        net.read_file(self.Retail_WA_W)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                    elif (state=='WA')&(Season_Type=='Summer'):
                        net.read_file(self.Retail_WA_S)
                        model_output=self.Complete_NS_Probability_Type6(data_df,net,page_no)
                if page_no==2:
                    print(category)
                    if state=='CA':
                        print(category,state)
                        season=['Fall', 'Fall', 'Fall', 'Spring', 'Spring', 'Spring', 'Summer','Summer', 'Summer', 'Winter', 'Winter', 'Winter']
                        Months=[ 9, 10, 11,  3,  4,  5,  6,  7,  8,  1,  2, 12]
                        season_month=pd.DataFrame(season, columns=['Season_Type'])
                        season_month['Month_int']=Months
                        data_df_1,data_df_2,data_df_3,data_df_4,data_df_5,data_df_6,data_df_7,data_df_8,data_df_9,data_df_10,data_df_11,data_df_12=None,None,None,None,None,None,None,None,None,None,None,None
                        model_output_1,model_output_2,model_output_3,model_output_4,model_output_5,model_output_6,model_output_7,model_output_8,model_output_9,model_output_10,model_output_11,model_output_12=None,None,None,None,None,None,None,None,None,None,None,None
                        data_df=pd.merge(data_df,season_month,on=['Season_Type'],how='outer')
                        data_df_1=data_df[(data_df.Month_int==1)]
                        net.read_file(self.Retail_CA_Jan)
                        model_output_1=self.Complete_NS_Probability_Type6(data_df_1,net,page_no)
                        data_df_2=data_df[(data_df.Month_int==2)]
                        net.read_file(self.Retail_CA_Feb)
                        model_output_2=self.Complete_NS_Probability_Type6(data_df_2,net,page_no)
                        data_df_3==data_df[(data_df.Month_int==3)]
                        net.read_file(self.Retail_CA_Mar)
                        model_output_3=self.Complete_NS_Probability_Type6(data_df_3,net,page_no)
                        data_df_4==data_df[(data_df.Month_int==4)]
                        net.read_file(self.Retail_CA_April)
                        model_output_4=self.Complete_NS_Probability_Type5(data_df_4,net,page_no)
                        data_df_5==data_df[(data_df.Month_int==5)]
                        net.read_file(self.Retail_CA_May)
                        model_output_5=self.Complete_NS_Probability_Type6(data_df_5,net,page_no)
                        data_df_6==data_df[(data_df.Month_int==6)]
                        net.read_file(self.Retail_CA_June)
                        model_output_6=self.Complete_NS_Probability_Type5(data_df_6,net,page_no)
                        data_df_7==data_df[(data_df.Month_int==7)]
                        net.read_file(self.Retail_CA_July)
                        model_output_7=self.Complete_NS_Probability_Type6(data_df_7,net,page_no)
                        data_df_8==data_df[(data_df.Month_int==8)]
                        net.read_file(self.Retail_CA_Aug)
                        model_output_8=self.Complete_NS_Probability_Type5(data_df_8,net,page_no)
                        data_df_9==data_df[(data_df.Month_int==9)]
                        net.read_file(self.Retail_CA_Sep)
                        model_output_9=self.Complete_NS_Probability_Type6(data_df_9,net,page_no)
                        data_df_10==data_df[(data_df.Month_int==10)]
                        net.read_file(self.Retail_CA_Oct)
                        model_output_10=self.Complete_NS_Probability_Type6(data_df_10,net,page_no)
                        data_df_11==data_df[(data_df.Month_int==11)]
                        net.read_file(self.Retail_CA_Nov)
                        model_output_11=self.Complete_NS_Probability_Type6(data_df_11,net,page_no)
                        data_df_12==data_df[(data_df.Month_int==12)]
                        net.read_file(self.Retail_CA_Nov)
                        model_output_12=self.Complete_NS_Probability_Type6(data_df_12,net,page_no)
                        model_output=model_output_1.append(model_output_2).append(model_output_3).append(model_output_4).append(model_output_5).append(model_output_6).append(model_output_7).append(model_output_8).append(model_output_9).append(model_output_10).append(model_output_11).append(model_output_12)
                    elif (state=='IL'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Retail_IL_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Retail_IL_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='KS'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Retail_KS_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Retail_KS_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='MI'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Retail_MI_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Retail_MI_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='OH'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Retail_OH_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Retail_OH_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='OR'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Retail_OR_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type2(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Retail_OR_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='TN'):
                        data_df_FSP=data_df[(data_df.Season_Type!='Winter')&(data_df.Season_Type!='Summer')]
                        net.read_file(self.Retail_TN_FSP)
                        model_output_FSP=self.Complete_NS_Probability_Type6(data_df_FSP,net,page_no)
                        data_df_WS=data_df[(data_df.Season_Type!='Fall')&(data_df.Season_Type!='Spring')]
                        net.read_file(self.Retail_TN_WS)
                        model_output_WS=self.Complete_NS_Probability_Type2(data_df_WS,net,page_no)
                        model_output=model_output_FSP.append(model_output_WS)
                    elif (state=='PA'):
                        data_df_F=data_df[(data_df.Season_Type=='Fall')]
                        net.read_file(self.Retail_PA_F)
                        model_output_F=self.Complete_NS_Probability_Type6(data_df_F,net,page_no)
                        data_df_SP=data_df[(data_df.Season_Type=='Spring')]
                        net.read_file(self.Retail_PA_SP)
                        model_output_SP=self.Complete_NS_Probability_Type6(data_df_SP,net,page_no)
                        data_df_S=data_df[(data_df.Season_Type=='Summer')]
                        net.read_file(self.Retail_PA_S)
                        model_output_S=self.Complete_NS_Probability_Type6(data_df_S,net,page_no)
                        data_df_W=data_df[(data_df.Season_Type=='Winter')]
                        net.read_file(self.Retail_PA_W)
                        model_output_W=self.Complete_NS_Probability_Type6(data_df_W,net,page_no)
                        model_output=model_output_F.append(model_output_SP).append(model_output_S).append(model_output_W)
                    elif (state=='WA'):
                        data_df_F=data_df[(data_df.Season_Type=='Fall')]
                        net.read_file(self.Retail_WA_F)
                        model_output_F=self.Complete_NS_Probability_Type6(data_df_F,net,page_no)
                        data_df_SP=data_df[(data_df.Season_Type=='Spring')]
                        net.read_file(self.Retail_WA_SP)
                        model_output_SP=self.Complete_NS_Probability_Type6(data_df_SP,net,page_no)
                        data_df_S=data_df[(data_df.Season_Type=='Summer')]
                        net.read_file(self.Retail_WA_S)
                        model_output_S=self.Complete_NS_Probability_Type6(data_df_S,net,page_no)
                        data_df_W=data_df[(data_df.Season_Type=='Winter')]
                        net.read_file(self.Retail_WA_W)
                        model_output_W=self.Complete_NS_Probability_Type6(data_df_W,net,page_no)
                        model_output=model_output_F.append(model_output_SP).append(model_output_S).append(model_output_W)
            else:
                logger.error("Invalid State Name")
            
            if model_output is not None:
                if page_no==1:
                    vendor_df,kbsemp_df,holidaydata_df=self.data_format_coversion(model_output)
                    output_df=None
                elif page_no==2:
                    vendor_df,kbsemp_df,holidaydata_df=None,None,None
                    output_df=self.data_format_coversion_for_second_page(model_output)            
        except Exception as ex:            
            logger.error("Exception occurred in model_handling : {}".format(str(ex)))
            
        finally:
            return vendor_df,kbsemp_df,holidaydata_df, output_df  
    
    def data_format_coversion(self,model_output):
	
        
        vendor_df = None
        kbsemp_df = None
        holidaydata_df = None
        
        try:
            
            vendor_df = model_output[model_output.emp_type=='KBS_Vendor']

            if vendor_df.empty:
                
                #vendor_df = pd.DataFrame(vendor_df.groupby(['state','city','CID','Zip_Code','Month','Day_of_Week','Shift_Type'])['Complete_NS_Probability'].mean())
                vendor_df = pd.DataFrame(vendor_df.groupby(['category','state','city','post_code','zip_code','Month','Day_Of_Week','Shift_Type'])['Complete_NS_Probability'].mean())
                
                vendor_df.reset_index(inplace=True)
            
            kbsemp_df = model_output[model_output.emp_type=='KBS_Emp']

            if kbsemp_df.empty:
                
                kbsemp_df = pd.DataFrame(kbsemp_df.groupby(['category','state','city','post_code','zip_code','Month','Day_Of_Week','Shift_Type'])['Complete_NS_Probability'].mean())
                kbsemp_df.reset_index(inplace=True)
            
            holidaydata_df = model_output[(model_output.Public_Holidays=="Yes")]

            if holidaydata_df.empty:
                
                holidaydata_df = pd.DataFrame(holidaydata_df.groupby(['Date','category','state','city','post_code','zip_code','Month','Day_Of_Week','Shift_Type','emp_type'])['Complete_NS_Probability'].mean())
                holidaydata_df.reset_index(inplace=True)
            
        except Exception as ex:            
            logger.error("Exception occurred in data_format_JSON Method : {}".format(str(ex)))
        finally :
            return vendor_df,kbsemp_df,holidaydata_df

    def data_format_coversion_for_second_page(self,model_output):
        output_df=None
        try:
            output_df=pd.DataFrame(model_output.groupby(['category','state','city','post_code','zip_code','emp_type','Season_Type','Shift_Type','vendor_ident','Complete_NS_Count','Show_count'])['Complete_NS_Probability'].mean())
            output_df.reset_index(inplace=True)
            output_df['Complete_NS_Probability_1step_forward']=output_df['Complete_NS_Count']/(output_df['Complete_NS_Count']+output_df['Show_count'])
            output_df['Complete_NS_Probability']=np.where(output_df['Complete_NS_Probability_1step_forward']<=0.05,output_df['Complete_NS_Probability_1step_forward']+0.00351,output_df['Complete_NS_Probability'])
            output_df['Complete_NS_Probability']=np.where(output_df['Show_count']==0,output_df['Complete_NS_Probability_1step_forward']-.01351,output_df['Complete_NS_Probability'])

        except Exception as ex:            
            logger.error("Exception occurred in second_page_data_format_JSON Method : {}".format(str(ex)))
        finally :
            return output_df
