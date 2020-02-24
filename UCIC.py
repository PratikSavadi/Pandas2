import pandas as pd
import numpy as np
import datetime as dt
import zipfile
import re
import glob
import os
import shutil

class GenerateUCICReport():
    def __init__(self):
        pass

    def getUCICReport(self, query):
	
        date = query['date']
        newPath = '/usr/share/nginx/smartrecon/mft/'

        if not os.path.exists(newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date']):
            print "========================="
            print newPath
        if not os.path.exists(newPath + query['fname'].split('_')[0] + '_' + 'REPORT'):
            os.mkdir(newPath + query['fname'].split('_')[0] + '_' + 'REPORT')
        if not os.path.exists(newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date']):
            os.mkdir(newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date'])

        if os.path.exists(newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date']):
            shutil.rmtree(newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date'])
            os.mkdir(newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date'])

        shutil.copy(query['path'] + query['fname'],
                    newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date'])
        readPath = newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + query['date']
        path = readPath + '/' + query['fname']
        dest = readPath

        with zipfile.ZipFile(path, 'r') as zf:
            listOfFileNames = zf.namelist()
            for fileName in listOfFileNames:
                if fileName.endswith('.csv'):
                    zf.extract(fileName, dest)
                # if fileName.endswith('.txt'):
                #     zf.extract(fileName, dest)

        if os.path.exists(dest):
            files = os.listdir(dest)
	    print files
	
	dfcbs = pd.DataFrame()
	dfwiz =pd.DataFrame()
	dfT24 =pd.DataFrame()
	dfsvs=pd.DataFrame()	
        for i in files:
            if i.startswith('CBS'):
		
                dfcbs = pd.read_csv(dest + '/' + i, sep='|')
		dfcbs.columns = dfcbs.columns.str.replace(' ', '')
            elif i.startswith('WIZARD'):
		
                dfwiz = pd.read_csv(dest + '/' + i, sep='|')
		dfwiz.columns = dfwiz.columns.str.replace(' ', '')

            elif i.startswith('T24'):
                dfT24 = pd.read_csv(dest + '/' + i, sep='|')
		dfT24.columns = dfT24.columns.str.replace(' ', '')
            elif i.startswith('SVS'):
                dfsvs = pd.read_csv(dest + '/' + i, sep='|')
		dfsvs.columns = dfsvs.columns.str.replace(' ', '')
        if not os.path.exists(dest + '/' + 'OUTPUT'):
            os.mkdir(dest + '/' + 'OUTPUT')
        destpath = dest + '/' + 'OUTPUT/'
        rmpath = destpath + '*'
        rmfiles = glob.glob(rmpath)
        for i in rmfiles:
            os.remove(i)

        dfchk = pd.DataFrame()
        dffndobmob = pd.DataFrame()
        dffndob = pd.DataFrame()
        dfcomp = pd.DataFrame()

        dfcomp1 = pd.DataFrame()
        dfsummarycount = pd.DataFrame(columns=['Names', 'Count'])
	dfsummarycount['Names'] = ['UCIC Provided by Wizard', 'UCIC Provided by CBS', 'UCIC Provided by SVS',
                           'UCIC Provided by T24',
                           'UCIC present in wizard but not in CBS', 'UCIC present in wizard but not in SVS',
                           'UCIC present in wizard but not in T24',
                           'UCIC present in CBS but not in Wizard', 'UCIC present in CBS but not in SVS',
                           'UCIC present in CBS but not in T24']

        if len(dfwiz) > 0 and len(dfcbs) > 0:
	   
            wiz = len(dfwiz['UCIC'])
	    
            cbs = len(dfcbs['UCIC'])
            dfwiz['SOURCE'] = 'WIZARD'
            dfcbs['SOURCE'] = 'CBS'
            df = dfwiz.merge(dfcbs, on='UCIC', how='outer', indicator=True)

            onlywiznotcbs = len(df[df['_merge'] == 'left_only'])
            onlycbsnotwiz = len(df[df['_merge'] == 'right_only'])

            df.loc[df['_merge'] == 'both', 'Remarks_Wizard'] = 'Present in wizard'
            df.loc[df['_merge'] == 'left_only', 'Remarks_Wizard'] = 'Present in wizard'
            df.loc[df['_merge'] == 'right_only', 'Remarks_Wizard'] = 'Not Present in wizard'

            dfleft = df[df['_merge'] == 'left_only']
            dfleft = dfleft[['UCIC', 'SOURCE_x', 'Remarks_Wizard']]
            dfleft.rename(columns={'SOURCE_x': 'SOURCE'}, inplace=True)
            dfright = df[df['_merge'] == 'right_only']
            dfright = dfright[['UCIC', 'SOURCE_y', 'Remarks_Wizard']]
            dfright.rename(columns={'SOURCE_y': 'SOURCE'}, inplace=True)
            dfboth = df[df['_merge'] == 'both']
            dfboth = dfboth[['UCIC', 'SOURCE_x', 'SOURCE_y', 'Remarks_Wizard']]
            dfboth.rename(columns={'SOURCE_y': 'SOURCE'}, inplace=True)
            df = pd.concat([dfleft, dfright, dfboth])
            df = df[['UCIC', 'SOURCE', 'Remarks_Wizard']]
            dfchk = dfchk.append(df)

            df1 = dfwiz.merge(dfcbs, left_on=['FirstName', 'MobileNo', 'DOB'],
                              right_on=['FirstName', 'MobileNo', 'DOB'], how='outer', indicator=True)
            df1left = df1[df1['_merge'] == 'left_only']
            df1left = df1left[['UCIC_x', 'FirstName', 'DOB', 'MobileNo', 'SOURCE_x']]
            df1left.rename(columns={'UCIC_x': 'UCIC', 'SOURCE_x': 'SOURCE'}, inplace=True)
            df1right = df1[df1['_merge'] == 'right_only']
            df1right = df1right[['UCIC_y', 'FirstName', 'DOB', 'MobileNo', 'SOURCE_y']]
            df1right.rename(columns={'UCIC_y': 'UCIC', 'SOURCE_y': 'SOURCE'}, inplace=True)
            df1 = pd.concat([df1left, df1right])
            dffndobmob = dffndobmob.append(df1)

            df2 = dfwiz.merge(dfcbs, left_on=['FirstName', 'DOB'], right_on=['FirstName', 'DOB'],
                              how='outer', indicator=True)
            df2left = df2[df2['_merge'] == 'left_only']
            df2left = df2left[['UCIC_x', 'FirstName', 'DOB', 'SOURCE_x']]
            df2left.rename(columns={'UCIC_x': 'UCIC', 'SOURCE_x': 'SOURCE'}, inplace=True)
            df2right = df2[df2['_merge'] == 'right_only']
            df2right = df2right[['UCIC_y', 'FirstName', 'DOB', 'SOURCE_y']]
            df2right.rename(columns={'UCIC_y': 'UCIC', 'SOURCE_y': 'SOURCE'}, inplace=True)
            df2 = pd.concat([df2left, df2right])
            dffndob = dffndob.append(df2)

            # dftest = dfwiz.merge(dfcbs, on='UCIC', how='inner', indicator=True)
            # dftest['PAN_x'] = dftest['PAN_x'].astype(str)
            # dftest['PAN_y'] = dftest['PAN_y'].astype(str)
            # dftest['VoterID_x'] = dftest['VoterID_x'].astype(str)
            # dftest['VoterID_y'] = dftest['VoterID_y'].astype(str)
            # dftest['Passport_x'] = dftest['Passport_x'].astype(str)
            # dftest['Passport_y'] = dftest['Passport_y'].astype(str)
            # dftest['CKYCNumber_x'] = dftest['CKYCNumber_x'].astype(str)
            # dftest['CKYCNumber_y'] = dftest['CKYCNumber_y'].astype(str)
            # dftest['DrivingLicense_x'] = dftest['DrivingLicense_x'].astype(str)
            # dftest['DrivingLicense_y'] = dftest['DrivingLicense_y'].astype(str)
            # dftest['CIN(CorporateIdentityNumber)_x'] = dftest['CIN(CorporateIdentityNumber)_x'].astype(str)
            # dftest['CIN(CorporateIdentityNumber)_y'] = dftest['CIN(CorporateIdentityNumber)_y'].astype(str)
            # dftest['Remarks with Wizard as base'] = ''
            #
            # for i in range(len(dftest)):
            #     b = i
            #     a = dftest.iloc[i]
            #
            #     if a['CustomerID_x'] != a['CustomerID_y']:
            #         dftest['Remarks with Wizard as base'][b] = 'CustomerID'
            #     if a['FirstName_x'] != a['FirstName_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'FirstName'
            #     if a['LastName_x'] != a['LastName_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'LastName'
            #     if a['MobileNo_x'] != a['MobileNo_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'MobileNo'
            #     if a['DOB_x'] != a['DOB_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'DOB'
            #     a['CustomerType_x'] = a['CustomerType_x'].upper()
            #     a['CustomerType_y'] = a['CustomerType_y'].upper()
            #     if a['CustomerType_x'] != a['CustomerType_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'CustomerType'
            #     if a['AadhaarNo_x'] != a['AadhaarNo_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'AadhaarNo'
            #     if a['AadhaarNo_x'] != a['AadhaarNo_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'AadhaarNo'
            #
            #     if a['PAN_x'] != a['PAN_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'PAN'
            #     if a['VoterID_x'] != a['VoterID_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'VoterID'
            #     if a['DrivingLicense_x'] != a['DrivingLicense_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'DrivingLicense'
            #     if a['Passport_x'] != a['Passport_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'Passport'
            #     if a['CKYCNumber_x'] != a['CKYCNumber_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'CKYCNumber'
            #     if a['CIN(CorporateIdentityNumber)_x'] != a['CIN(CorporateIdentityNumber)_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'CIN'
            #     if a['GST_x'] != a['GST_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'GST'
            #     if a['Address_1_x'] != a['Address_1_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'Address_1'
            #     if a['Address_2_x'] != a['Address_2_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'Address_2'
            #     if a['Address_3_x'] != a['Address_3_y']:
            #         dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
            #                                                        b] + ',' + 'Address_2'
            # dfcomp = dfcomp.append(dftest)
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC Provided by Wizard', 'Count'] = wiz
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC Provided by CBS', 'Count'] = cbs
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC present in wizard but not in CBS', 'Count'] = onlywiznotcbs
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC present in CBS but not in Wizard', 'Count'] = onlycbsnotwiz

        if len(dfwiz) > 0 and len(dfT24) > 0:
            t24 = len(dfT24['UCIC'])

            dfT24['SOURCE'] = 'T24'
            df = dfwiz.merge(dfT24, on='UCIC', how='outer', indicator=True)

            df.loc[df['_merge'] == 'both', 'Remarks_Wizard'] = 'Present in wizard'
            df.loc[df['_merge'] == 'left_only', 'Remarks_Wizard'] = 'Present in wizard'
            df.loc[df['_merge'] == 'right_only', 'Remarks_Wizard'] = 'Not Present in wizard'
            onlywiznott24 = len(df[df['_merge'] == 'left_only'])
            dfleft = df[df['_merge'] == 'left_only']
            dfleft = dfleft[['UCIC', 'SOURCE_x', 'Remarks_Wizard']]
            dfleft.rename(columns={'SOURCE_x': 'SOURCE'}, inplace=True)
            dfright = df[df['_merge'] == 'right_only']
            dfright = dfright[['UCIC', 'SOURCE_y', 'Remarks_Wizard']]
            dfright.rename(columns={'SOURCE_y': 'SOURCE'}, inplace=True)
            dfboth = df[df['_merge'] == 'both']
            dfboth = dfboth[['UCIC', 'SOURCE_x', 'SOURCE_y', 'Remarks_Wizard']]
            dfboth.rename(columns={'SOURCE_y': 'SOURCE'}, inplace=True)
            df = pd.concat([dfleft, dfright, dfboth])
            df = df[['UCIC', 'SOURCE', 'Remarks_Wizard']]
            dfchk = dfchk.append(df)

            df1 = dfwiz.merge(dfT24, left_on=['FirstName', 'MobileNo', 'DOB'],
                              right_on=['FirstName', 'MobileNo', 'DOB'],
                              how='outer', indicator=True)
            df1left = df1[df1['_merge'] == 'left_only']
            df1left = df1left[['UCIC_x', 'FirstName', 'DOB', 'MobileNo', 'SOURCE_x']]
            df1left.rename(columns={'UCIC_x': 'UCIC', 'SOURCE_x': 'SOURCE'}, inplace=True)
            df1right = df1[df1['_merge'] == 'right_only']
            df1right = df1right[['UCIC_y', 'FirstName', 'DOB', 'MobileNo', 'SOURCE_y']]
            df1right.rename(columns={'UCIC_y': 'UCIC', 'SOURCE_y': 'SOURCE'}, inplace=True)
            df1 = pd.concat([df1left, df1right])
            dffndobmob = dffndobmob.append(df1)

            df2 = dfwiz.merge(dfT24, left_on=['FirstName', 'DOB'], right_on=['FirstName', 'DOB'],
                              how='outer', indicator=True)
            df2left = df2[df2['_merge'] == 'left_only']
            df2left = df2left[['UCIC_x', 'FirstName', 'DOB', 'SOURCE_x']]
            df2left.rename(columns={'UCIC_x': 'UCIC', 'SOURCE_x': 'SOURCE'}, inplace=True)
            df2right = df2[df2['_merge'] == 'right_only']
            df2right = df2right[['UCIC_y', 'FirstName', 'DOB', 'SOURCE_y']]
            df2right.rename(columns={'UCIC_y': 'UCIC', 'SOURCE_y': 'SOURCE'}, inplace=True)
            df2 = pd.concat([df2left, df2right])
            dffndob = dffndob.append(df2)

            df = dfcbs.merge(dfT24, on='UCIC', how='outer', indicator=True)
            onlycbsnott24 = len(df[df['_merge'] == 'left_only'])


        #     dftest = dfwiz.merge(dfT24, on='UCIC', how='inner', indicator=True)
        #     dftest['Remarks with Wizard as base'] = ''
	    # dftest['PAN_x'] = dftest['PAN_x'].astype(str)
	    # dftest['PAN_y'] = dftest['PAN_y'].astype(str)
	    # dftest['VoterID_x'] = dftest['VoterID_x'].astype(str)
	    # dftest['VoterID_y'] = dftest['VoterID_y'].astype(str)
	    # dftest['Passport_x'] = dftest['Passport_x'].astype(str)
	    # dftest['Passport_y'] = dftest['Passport_y'].astype(str)
	    # dftest['CKYCNumber_x'] = dftest['CKYCNumber_x'].astype(str)
	    # dftest['CKYCNumber_y'] = dftest['CKYCNumber_y'].astype(str)
	    # dftest['DrivingLicense_x'] = dftest['DrivingLicense_x'].astype(str)
	    # dftest['DrivingLicense_y'] = dftest['DrivingLicense_y'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_x'] = dftest['CIN(CorporateIdentityNumber)_x'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_y'] = dftest['CIN(CorporateIdentityNumber)_y'].astype(str)
        #
        #     for i in range(len(dftest)):
        #         b = i
        #         a = dftest.iloc[i]
        #
        #         if a['CustomerID_x'] != a['CustomerID_y']:
        #             dftest['Remarks with Wizard as base'][b] = 'CustomerID'
        #         if a['FirstName_x'] != a['FirstName_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'FirstName'
        #         if a['LastName_x'] != a['LastName_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'LastName'
        #         if a['MobileNo_x'] != a['MobileNo_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'MobileNo'
        #         if a['DOB_x'] != a['DOB_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'DOB'
        #         a['CustomerType_x'] = a['CustomerType_x'].upper()
        #         a['CustomerType_y'] = a['CustomerType_y'].upper()
        #         if a['CustomerType_x'] != a['CustomerType_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'CustomerType'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #
        #         if a['PAN_x'] != a['PAN_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'PAN'
        #         if a['VoterID_x'] != a['VoterID_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'VoterID'
        #         if a['DrivingLicense_x'] != a['DrivingLicense_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'DrivingLicense'
        #         if a['Passport_x'] != a['Passport_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Passport'
        #         if a['CKYCNumber_x'] != a['CKYCNumber_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'CKYCNumber'
        #         if a['CIN(Corporate Identity Number)_x'] != a['CIN(Corporate Identity Number)_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'CIN'
        #         if a['GST_x'] != a['GST_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'GST'
        #         if a['Address_1_x'] != a['Address_1_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Address_1'
        #         if a['Address_2_x'] != a['Address_2_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Address_2'
        #         if a['Address_3_x'] != a['Address_3_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Address_2'
        #     dfcomp = dfcomp.append(dftest)

        #     dftest = dfcbs.merge(dfT24, on='UCIC', how='outer', indicator=True)
        #     dftest['Remarks with CBS as base'] = ''
	    # dftest['PAN_x'] = dftest['PAN_x'].astype(str)
	    # dftest['PAN_y'] = dftest['PAN_y'].astype(str)
	    # dftest['VoterID_x'] = dftest['VoterID_x'].astype(str)
	    # dftest['VoterID_y'] = dftest['VoterID_y'].astype(str)
	    # dftest['Passport_x'] = dftest['Passport_x'].astype(str)
	    # dftest['Passport_y'] = dftest['Passport_y'].astype(str)
	    # dftest['CKYCNumber_x'] = dftest['CKYCNumber_x'].astype(str)
	    # dftest['CKYCNumber_y'] = dftest['CKYCNumber_y'].astype(str)
	    # dftest['DrivingLicense_x'] = dftest['DrivingLicense_x'].astype(str)
	    # dftest['DrivingLicense_y'] = dftest['DrivingLicense_y'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_x'] = dftest['CIN(CorporateIdentityNumber)_x'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_y'] = dftest['CIN(CorporateIdentityNumber)_y'].astype(str)
        #
        #     for i in range(len(dftest)):
        #         b = i
        #         a = dftest.iloc[i]
        #
        #         if a['CustomerID_x'] != a['CustomerID_y']:
        #             dftest['Remarks with CBS as base'][b] = 'CustomerID'
        #         if a['FirstName_x'] != a['FirstName_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'FirstName'
        #         if a['LastName_x'] != a['LastName_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'LastName'
        #         if a['MobileNo_x'] != a['MobileNo_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'MobileNo'
        #         if a['DOB_x'] != a['DOB_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'DOB'
        #         a['CustomerType_x'] = a['CustomerType_x'].upper()
        #         a['CustomerType_y'] = a['CustomerType_y'].upper()
        #         if a['CustomerType_x'] != a['CustomerType_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'CustomerType'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #
        #         if a['PAN_x'] != a['PAN_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'PAN'
        #         if a['VoterID_x'] != a['VoterID_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'VoterID'
        #         if a['DrivingLicense_x'] != a['DrivingLicense_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'DrivingLicense'
        #         if a['Passport_x'] != a['Passport_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Passport'
        #         if a['CKYCNumber_x'] != a['CKYCNumber_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'CKYCNumber'
        #         if a['CIN(Corporate Identity Number)_x'] != a['CIN(Corporate Identity Number)_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'CIN'
        #         if a['GST_x'] != a['GST_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'GST'
        #         if a['Address_1_x'] != a['Address_1_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Address_1'
        #         if a['Address_2_x'] != a['Address_2_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Address_2'
        #         if a['Address_3_x'] != a['Address_3_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Address_2'
        #     dfcomp1 = dfcomp1.append(dftest)
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC Provided by T24', 'Count'] = t24
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC present in wizard but not in T24', 'Count'] = onlywiznott24
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC present in CBS but not in T24', 'Count'] = onlycbsnott24

        if len(dfwiz) > 0 and len(dfsvs) > 0:
            svs = len(dfsvs['UCIC'])

            dfsvs['SOURCE'] = 'SVS'
            df = dfwiz.merge(dfsvs, on='UCIC', how='outer', indicator=True)

            df.loc[df['_merge'] == 'both', 'Remarks_Wizard'] = 'Present in wizard'
            df.loc[df['_merge'] == 'left_only', 'Remarks_Wizard'] = 'Present in wizard'
            df.loc[df['_merge'] == 'right_only', 'Remarks_Wizard'] = 'Not Present in wizard'
            onlywiznottsvs = len(df[df['_merge'] == 'left_only'])
            dfleft = df[df['_merge'] == 'left_only']
            dfleft = dfleft[['UCIC', 'SOURCE_x', 'Remarks_Wizard']]
            dfleft.rename(columns={'SOURCE_x': 'SOURCE'}, inplace=True)
            dfright = df[df['_merge'] == 'right_only']
            dfright = dfright[['UCIC', 'SOURCE_y', 'Remarks_Wizard']]
            dfright.rename(columns={'SOURCE_y': 'SOURCE'}, inplace=True)
            dfboth = df[df['_merge'] == 'both']
            dfboth = dfboth[['UCIC', 'SOURCE_x', 'SOURCE_y', 'Remarks_Wizard']]
            dfboth.rename(columns={'SOURCE_y': 'SOURCE'}, inplace=True)
            df = pd.concat([dfleft, dfright, dfboth])
            df = df[['UCIC', 'SOURCE', 'Remarks_Wizard']]
            dfchk = dfchk.append(df)

            df1 = dfwiz.merge(dfsvs, left_on=['FirstName', 'MobileNo', 'DOB'],
                              right_on=['FirstName', 'MobileNo', 'DOB'],
                              how='outer', indicator=True)
            df1left = df1[df1['_merge'] == 'left_only']
            df1left = df1left[['UCIC_x', 'FirstName', 'DOB', 'MobileNo', 'SOURCE_x']]
            df1left.rename(columns={'UCIC_x': 'UCIC', 'SOURCE_x': 'SOURCE'}, inplace=True)
            df1right = df1[df1['_merge'] == 'right_only']
            df1right = df1right[['UCIC_y', 'FirstName', 'DOB', 'MobileNo', 'SOURCE_y']]
            df1right.rename(columns={'UCIC_y': 'UCIC', 'SOURCE_y': 'SOURCE'}, inplace=True)
            df1 = pd.concat([df1left, df1right])
            dffndobmob = dffndobmob.append(df1)

            df2 = dfwiz.merge(dfsvs, left_on=['FirstName', 'DOB'], right_on=['FirstName', 'DOB'],
                              how='outer', indicator=True)
            df2left = df2[df2['_merge'] == 'left_only']
            df2left = df2left[['UCIC_x', 'FirstName', 'DOB', 'SOURCE_x']]
            df2left.rename(columns={'UCIC_x': 'UCIC', 'SOURCE_x': 'SOURCE'}, inplace=True)
            df2right = df2[df2['_merge'] == 'right_only']
            df2right = df2right[['UCIC_y', 'FirstName', 'DOB', 'SOURCE_y']]
            df2right.rename(columns={'UCIC_y': 'UCIC', 'SOURCE_y': 'SOURCE'}, inplace=True)
            df2 = pd.concat([df2left, df2right])
            dffndob = dffndob.append(df2)

            df = dfcbs.merge(dfsvs, on='UCIC', how='outer', indicator=True)
            onlycbsnotsvs = len(df[df['_merge'] == 'left_only'])


        #     dftest = dfwiz.merge(dfsvs, on='UCIC', how='inner', indicator=True)
        #     dftest['Remarks with Wizard as base'] = ''
	    # dftest['PAN_x'] = dftest['PAN_x'].astype(str)
	    # dftest['PAN_y'] = dftest['PAN_y'].astype(str)
	    # dftest['VoterID_x'] = dftest['VoterID_x'].astype(str)
	    # dftest['VoterID_y'] = dftest['VoterID_y'].astype(str)
	    # dftest['Passport_x'] = dftest['Passport_x'].astype(str)
	    # dftest['Passport_y'] = dftest['Passport_y'].astype(str)
	    # dftest['CKYCNumber_x'] = dftest['CKYCNumber_x'].astype(str)
	    # dftest['CKYCNumber_y'] = dftest['CKYCNumber_y'].astype(str)
	    # dftest['DrivingLicense_x'] = dftest['DrivingLicense_x'].astype(str)
	    # dftest['DrivingLicense_y'] = dftest['DrivingLicense_y'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_x'] = dftest['CIN(CorporateIdentityNumber)_x'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_y'] = dftest['CIN(CorporateIdentityNumber)_y'].astype(str)
        #
        #     for i in range(len(dftest)):
        #         b = i
        #         a = dftest.iloc[i]
        #
        #         if a['CustomerID_x'] != a['CustomerID_y']:
        #             dftest['Remarks with Wizard as base'][b] = 'CustomerID'
        #         if a['FirstName_x'] != a['FirstName_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'FirstName'
        #         if a['LastName_x'] != a['LastName_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'LastName'
        #         if a['MobileNo_x'] != a['MobileNo_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'MobileNo'
        #         if a['DOB_x'] != a['DOB_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'DOB'
        #         a['CustomerType_x'] = a['CustomerType_x'].upper()
        #         a['CustomerType_y'] = a['CustomerType_y'].upper()
        #         if a['CustomerType_x'] != a['CustomerType_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'CustomerType'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #
        #         if a['PAN_x'] != a['PAN_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'PAN'
        #         if a['VoterID_x'] != a['VoterID_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'VoterID'
        #         if a['DrivingLicense_x'] != a['DrivingLicense_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'DrivingLicense'
        #         if a['Passport_x'] != a['Passport_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Passport'
        #         if a['CKYCNumber_x'] != a['CKYCNumber_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'CKYCNumber'
        #         if a['CIN(Corporate Identity Number)_x'] != a['CIN(Corporate Identity Number)_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'CIN'
        #         if a['GST_x'] != a['GST_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][b] + ',' + 'GST'
        #         if a['Address_1_x'] != a['Address_1_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Address_1'
        #         if a['Address_2_x'] != a['Address_2_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Address_2'
        #         if a['Address_3_x'] != a['Address_3_y']:
        #             dftest['Remarks with Wizard as base'][b] = dftest['Remarks with Wizard as base'][
        #                                                            b] + ',' + 'Address_2'
        #     dfcomp = dfcomp.append(dftest)

        #     dftest = dfcbs.merge(dfsvs, on='UCIC', how='outer', indicator=True)
        #     dftest['Remarks with CBS as base'] = ''
	    # dftest['PAN_x'] = dftest['PAN_x'].astype(str)
	    # dftest['PAN_y'] = dftest['PAN_y'].astype(str)
	    # dftest['VoterID_x'] = dftest['VoterID_x'].astype(str)
	    # dftest['VoterID_y'] = dftest['VoterID_y'].astype(str)
	    # dftest['Passport_x'] = dftest['Passport_x'].astype(str)
	    # dftest['Passport_y'] = dftest['Passport_y'].astype(str)
	    # dftest['CKYCNumber_x'] = dftest['CKYCNumber_x'].astype(str)
	    # dftest['CKYCNumber_y'] = dftest['CKYCNumber_y'].astype(str)
	    # dftest['DrivingLicense_x'] = dftest['DrivingLicense_x'].astype(str)
	    # dftest['DrivingLicense_y'] = dftest['DrivingLicense_y'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_x'] = dftest['CIN(CorporateIdentityNumber)_x'].astype(str)
	    # dftest['CIN(CorporateIdentityNumber)_y'] = dftest['CIN(CorporateIdentityNumber)_y'].astype(str)
        #
        #     for i in range(len(dftest)):
        #         b = i
        #         a = dftest.iloc[i]
        #
        #         if a['CustomerID_x'] != a['CustomerID_y']:
        #             dftest['Remarks with CBS as base'][b] = 'CustomerID'
        #         if a['FirstName_x'] != a['FirstName_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'FirstName'
        #         if a['LastName_x'] != a['LastName_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'LastName'
        #         if a['MobileNo_x'] != a['MobileNo_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'MobileNo'
        #         if a['DOB_x'] != a['DOB_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'DOB'
        #         a['CustomerType_x'] = a['CustomerType_x'].upper()
        #         a['CustomerType_y'] = a['CustomerType_y'].upper()
        #         if a['CustomerType_x'] != a['CustomerType_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'CustomerType'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #         if a['AadhaarNo_x'] != a['AadhaarNo_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'AadhaarNo'
        #
        #         if a['PAN_x'] != a['PAN_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'PAN'
        #         if a['VoterID_x'] != a['VoterID_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'VoterID'
        #         if a['DrivingLicense_x'] != a['DrivingLicense_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'DrivingLicense'
        #         if a['Passport_x'] != a['Passport_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Passport'
        #         if a['CKYCNumber_x'] != a['CKYCNumber_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'CKYCNumber'
        #         if a['CIN(Corporate Identity Number)_x'] != a['CIN(Corporate Identity Number)_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'CIN'
        #         if a['GST_x'] != a['GST_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][b] + ',' + 'GST'
        #         if a['Address_1_x'] != a['Address_1_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Address_1'
        #         if a['Address_2_x'] != a['Address_2_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Address_2'
        #         if a['Address_3_x'] != a['Address_3_y']:
        #             dftest['Remarks with CBS as base'][b] = dftest['Remarks with CBS as base'][
        #                                                            b] + ',' + 'Address_2'
        #     dfcomp1 = dfcomp1.append(dftest)
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC Provided by SVS', 'Count'] = svs
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC present in wizard but not in SVS', 'Count'] = onlywiznottsvs
	    dfsummarycount.loc[dfsummarycount['Names'] == 'UCIC present in CBS but not in SVS', 'Count'] = onlycbsnotsvs

     
        if len(dfchk):
            dfchk.to_csv(destpath + 'UCIC_CHK_vs_WIZ' + '.csv', index=False)

        if len(dffndobmob):
            dffndobmob.to_csv(destpath + 'Match FN_DOB_MOB' + '.csv', index=False)

        if len(dffndob):
            dffndob.to_csv(destpath + 'Match FN_DOB' + '.csv', index=False)

        if len(dfcomp):
            dfcomp.to_csv(destpath + 'SOURCE_COMPARE' + '.csv', index=False)

        if len(dfcomp1):
            dfcomp1.to_csv(destpath + 'SOURCE_COMPARE1' + '.csv', index=False)

        if len(dfsummarycount):
            dfsummarycount.to_csv(destpath + 'Summary_Rpt' + '.csv', index=False)

        newPath = newPath + query['fname'].split('_')[0] + '_' + 'REPORT' + '/' + date + '/' + 'OUTPUT/'

        if os.path.exists(newPath):

            shutil.make_archive('UCIC', 'zip', newPath)

            if os.path.exists('/usr/share/nginx/www/ngerecon/ui/files/Outputs/UCIC.zip'):
                os.remove('/usr/share/nginx/www/ngerecon/ui/files/Outputs/UCIC.zip')
            shutil.move('UCIC.zip', '/usr/share/nginx/www/ngerecon/ui/files/Outputs/')
            fpath = '/files/Outputs/' + 'UCIC.zip'
            print fpath
            return True, fpath
        else:
            return False, 'No FIle Found'

