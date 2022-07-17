"""
File Name           : WebApplication.py

Description         : 

Author              : Zan Team
Date Created        : 19-Jan-2021
Date Last modified  : 

Copyright (C) 2021 ZAN COMPUTE - All Rights Reserved

"""


import os

from flask import Flask,request,g,redirect,url_for,render_template,flash,session
from functools import wraps

from flask import jsonify
from datetime import datetime
from KBSConfig import *
from KBSMessageHandling import *
from flask_cors import CORS
from KBSLogger import logging,handler
'''
from OpenSSL import SSL
#context = SSL.Context(SSL.PROTOCOL_TLSv1_2)
context = SSL.Context(SSL.PROTOCOL_SSLv23)
context.use_privatekey_file('zancompute.com.key')
context.use_certificate_file('gd_bundle-g2-g1.crt')  
'''

###############################################################################
#
#                  HTTP Response codes
#
###############################################################################

HTTP_200_OK               = 200
HTTP_201_CREATED          = 201

HTTP_400_BAD_REQUEST      = 400
HTTP_401_UNAUTHORIZED     = 401

HTTP_404_NOT_FOUND           = 404
HTTP_405_METHOD_NOT_ALLOWED  = 405
HTTP_406_NOT_ACCEPTABLE      = 406

HTTP_500_INTERNAL_SERVER_ERROR = 500



CUSTOMER_ID_MSGTYPE = '1'
STATE_CITY_MSGTYPE  = '2'

FIRST_SHIFT  = 'First_Shift'
SECOND_SHIFT = 'Second_Shift'
THIRD_SHIFT  = 'Third_Shift'

###############################################################################
#
#                  Flask Initialization
#
###############################################################################
app = Flask(__name__,static_folder='templates')
cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config.from_object(__name__)
app.secret_key = os.urandom(24)
app.config.from_envvar('FLASKR_SETTINGS',silent=True)

msgObj = None



@app.route('/dashboard',methods = ['GET', 'POST'] )
def Dashboard():
    try:
        app.logger.info("Dashboard Message Request Received : {}".format(request))
        return  render_template('index.html') ;
    
    except Exception as ex:
        app.logger.error("Exception Occurred in initial_message_handler Method : {}".format(str(ex)))
        return 'Internal Error!', HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/details',methods = ['GET', 'POST'] )
def Details():
    try:
        app.logger.info("Details page Request Received : {}".format(request))
        return  render_template('kbsnoshow.html') ;
    
    except Exception as ex:
        app.logger.error("Exception Occurred in initial_message_handler Method : {}".format(str(ex)))
        return 'Internal Error!', HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/initial',methods = ['GET'])
def dashboard_initial_message_handler():
    '''
    This message shall be triggered when the user open the KBS analysis report page
        
    Input Parameters : None
    
    Return : Returns JSON Payload with state,city,postal code, customer id details
    
    '''
    
    try:

        app.logger.info("Initial Message Request Received : {}".format(request))
        respMsgPayload = msgObj.dashboardInitializeRequest()
        #print(respMsgPayload)
        return respMsgPayload
    
    except Exception as ex:
        app.logger.error("Exception Occurred in initial_message_handler Method : {}".format(str(ex)))
        return 'Internal Error!', HTTP_500_INTERNAL_SERVER_ERROR


        
#@app.route('/noshowdetails',methods = ['GET'])
@app.route('/noshowrequest',methods = ['GET','POST'])
def noshowdetails_request_message_handler():
    
    '''
    This message shall be triggered when the user wants to view the  
    predicted values for the selected customers / city wise customers
        
    Input Parameters : None
    
    Return : Returns JSON Payload with state,city,postal code, customer id and 
             no show probability details    
    '''
    
    try:
        
        msgtype   = None
        #fromdate  = None
        #enddate   = None
        month     = None
        state     = None
        city      = None
        post_code = None
        category   = None
        emptype   = None
        shift     = None
        
        respMsgPayload   = None
        print(request)
        if request.method == 'POST': 
            request_data = request.get_json()
            month     = request_data['month']
            msgtype   = request_data['type']
            emptype1  = request_data['emptype']
            shift1    = request_data['shift']
            state     = request_data['state']
            city      = request_data['city']
            post_code = request_data['postcode']
            category  = request_data['category']
        else:
        #fromdate  = request.args.get('datefrom')
        #enddate   = request.args.get('dateto')
            month     = request.args.get('month')
            msgtype   = request.args.get('type')
            emptype1  = request.args.get('emptype')
            shift1    = request.args.get('shift')
            state     = request.args.get('state')
            city      = request.args.get('city')
            post_code = request.args.get('postcode')
            category   = request.args.get('category')
        
        
        #print(city,state,fromdate,enddate)
        
        if msgtype is None or month is None  :
            
            app.logger.error("Mandatory parameter is missing :  Message type / Month ")
            respMsgPayload = "Message type or Month is missing"
            return respMsgPayload, HTTP_400_BAD_REQUEST
        
        if shift1 is not None:
            if shift1 == '1' :
                shift = FIRST_SHIFT
            if shift1 == '2':
                shift = SECOND_SHIFT
            elif shift1 == '3' :
                shift = THIRD_SHIFT
            #else:
            #    shift = None   # Invalid, we can consider all shifts for this case
                
        if emptype1 is not None:
            if emptype1 == 'Vendor' :
                emptype = 'KBS_Vendor'
            elif emptype1 == 'KBS' :
                emptype = 'KBS_Emp'
            #else:
            #   emptype = None   #  Both 
        
        if msgtype == CUSTOMER_ID_MSGTYPE :            
            if category is None:
                respMsgPayload = "category is missing"
                return respMsgPayload, HTTP_400_BAD_REQUEST
            else:
                arglist = [state,category,month,emptype,shift]
                respMsgPayload = msgObj.handleCustomerSpecificNoShowRequest(arglist)
        else:
            '''
            State, City and Postal code are Mandatery parameters
            '''
            if state is None or city is None or post_code is None: 
                respmsg = "State detail is missing"
                if city is None:
                    respmsg = "City detail is missing"
                    
                elif post_code is None:                    
                    respmsg = "Zip Code Detail is missing"
                return respmsg, HTTP_400_BAD_REQUEST
            else:
                arglist = [state,city,post_code,category,month,emptype,shift]
                respMsgPayload = msgObj.handleStateCitySpecificNoShowRequest(arglist)

        return respMsgPayload
    except Exception as ex:
        app.logger.error("Exception Occurred in noshowdetails_request_message_handler Method : {}".format(str(ex)))
        return 'Internal Error!', HTTP_500_INTERNAL_SERVER_ERROR


@app.route('/kbsnoshowrequest', methods=['GET', 'POST'])
def kbsnoshowdetails_request_message_handler():
    '''
    This message shall be triggered when the user wants to view the
    predicted values for the selected customers / city wise customers

    Input Parameters : None

    Return : Returns JSON Payload with state,city,postal code, customer id and
             no show probability details
    '''

    try:

        state = None
        category = None

        respMsgPayload = None
        print(request)
        if request.method == 'POST':
            request_data = request.get_json()
            #msgtype = request_data['type']
            state = request_data['state']
            category = request_data['category']
        else:
            #msgtype = request.args.get['type']
            state = request.args.get('state')
            category = request.args.get('category')

        if state is None or category is None:
            app.logger.error("Mandatory parameter is missing :  state / category ")
            respMsgPayload = "State or Category is missing"
            return respMsgPayload, HTTP_400_BAD_REQUEST

        arglist = [state, category]
        respMsgPayload = msgObj.handleStateCategorySpecificNoShowRequest(arglist)

        return respMsgPayload
    except Exception as ex:
        app.logger.error("Exception Occurred in noshowdetails_request_message_handler Method : {}".format(str(ex)))
        return 'Internal Error!', HTTP_500_INTERNAL_SERVER_ERROR
    

#HOST_NAME, PORT,

if __name__=='__main__':
    msgObj = MessageHandling()
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.DEBUG)
    app.run(host=HOST_NAME,port=PORT,debug=True)
    #app.run(host=HOST_NAME,port=PORT,debug=True, ssl_context=context)
    #app.run(host=HOST_NAME,port=PORT,debug=True, ssl_context=("cert.pem","key.pem"))
    #app.run(debug=True)
