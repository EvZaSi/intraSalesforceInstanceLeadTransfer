import json
from simple_salesforce import Salesforce
import os
import boto3
import datetime


def lambda_handler(event, context):

    #gathers JSON file from S3 that was posted from Chrome River SFDC via the transfer_leads_trigger lambda function
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    s3=boto3.resource('s3')
    obj = s3.Object(event['Records'][0]['s3']['bucket']['name'],event['Records'][0]['s3']['object']['key'])
    body = obj.get()['Body'].read()
    input_body = json.loads(body)
    idList = input_body.get('Idlist')
    #gathers leads via SOQL through simple Salesforce library
    lead_list = _get_lead_list(idList)
    #standardizes picklist field values and creates value for Chrome River Transfer Notes field
    standardized_list = add_notes_and_standardize(lead_list)
    #sends to Certify SFDC instance
    result_dict = send_to_certify(standardized_list)
    print(result_dict)
    
    #posts notification to Slack upon failure to insert to Certify SFDC
    if(result_dict[0].get('success') == False):
        message = f"LEAD TRANSFER TO CERTIFY FAILURE \n"
        message += f"failed lead insert for the following IDs: \n"
        for num in range(len(lead_list)):
            idval = lead_list[num].get("Id")
            message += idval
            if num != (len(idList) - 1):
                message += f", "
            else:
                message += "\n"
        message += f"Returned error log from Salesforce: \n"
        message += result_dict[0].get('errors')[0].get('message')
        _publish_alert(message)
    else:
        #deletes JSON file within S3        
        s3 = boto3.client('s3')
        s3.delete_object(Bucket=bucket,Key=key)
    return {
        'statusCode': 200,
        'body': json.dumps('Transfer complete')
    }
    
def _get_lead_list(idList):
    query_string = "SELECT ID,FirstName,LastName,Company,Phone,MobilePhone,Email,Fax,LinkedIn_Profile__c,Title,Status,Street,State,City,PostalCode,Country,NumberOfEmployees,Industry,LeadSource,Website,Recent_Conversion__c,Recent_Conversion_Date__c,(SELECT Subject,Type FROM Tasks WHERE Type = 'Form Submission'),(SELECT Campaign_Name__c,Status FROM CampaignMembers) FROM Lead WHERE "
    id_query_string = ""
    for num in range(len(idList)):
        id_query_string += "(ID = '" + idList[num] + "')"
        if num != (len(idList) - 1):
            id_query_string += " OR "
    query_string += id_query_string
    sf = Salesforce(username=os.environ['cr_sf_username'], password=os.environ['cr_sf_password'], security_token=os.environ['cr_sf_token'],domain=os.environ['cr_sf_host'])
    sf_data = sf.query_all(query_string)
    return sf_data['records']

    
def create_new_dict(lead_dict):
    new_dict = {}
    new_dict['FirstName'] = lead_dict.get('FirstName')
    new_dict['LastName'] = lead_dict.get('LastName')
    new_dict['Company'] = lead_dict.get('Company')
    new_dict['Title'] = lead_dict.get('Title')
    new_dict['Phone'] = lead_dict.get('Phone')
    new_dict['Email'] = lead_dict.get('Email')
    new_dict['Fax'] = lead_dict.get('Fax')
    new_dict['Linkedin_Profile__c'] = lead_dict.get('LinkedIn_Profile__c')
    new_dict['Street'] = lead_dict.get('Street')
    new_dict['State'] = lead_dict.get('State')
    new_dict['City'] = lead_dict.get('City')
    new_dict['PostalCode'] = lead_dict.get('PostalCode')
    new_dict['Country'] = lead_dict.get('Country')
    new_dict['Website'] = lead_dict.get('Website')
    new_dict['NumberOfEmployees'] = lead_dict.get('NumberOfEmployees')
    new_dict['Industry'] = lead_dict.get('Industry')
    new_dict['LeadSource'] = lead_dict.get('LeadSource')
    new_dict['Chrome_River_Transfer_Notes__c'] = lead_dict.get('Chrome_River_Transfer_Notes__c')
    new_dict['Employee_Range__c'] = lead_dict.get('Employee_Range__c')
    new_dict['Chrome_River_MQL__c'] = lead_dict.get('Chrome_River_MQL__c')
    print(new_dict)
    return new_dict

def send_to_certify(lead_list):
    sf = Salesforce(username=os.environ['cert_sf_username'], password=os.environ['cert_sf_password'], security_token=os.environ['cert_sf_token'],domain=os.environ['cert_sf_host'])
    return sf.bulk.Lead.insert(lead_list,batch_size=200)
    
def add_notes_and_standardize(lead_list):
    new_dict_array = []
    for lead in lead_list:
        lead.__setitem__('Chrome_River_Transfer_Notes__c', generate_cr_notes_field(lead))
        lead.__setitem__('Employee_Range__c',standardize_employee_range(lead))
        lead.__setitem__('Chrome_River_MQL__c',mql_verify(lead))
        lead['LeadSource'] = 'Chrome River Transfer'
        lead['Industry'] = standardize_industry(lead)
        if(lead.get('Country') != None):
            lead['Country'] = standardize_country(lead)
        if(lead.get('State') != None):
            lead['State'] = standardize_state(lead)
        new_dict_array.append(create_new_dict(lead))
    return new_dict_array
    
def mql_verify(lead_dict):
    mql_status = False
    if (lead_dict.get('Recent_Conversion__c') != None):
        mql_status = True
    print(mql_status)
    return mql_status
    

def generate_cr_notes_field(lead_dict):
    note_text = ''
    if(lead_dict.get('LeadSource') != None):
        note_text += 'LeadSource: ' + lead_dict.get('LeadSource') + '   | '
    if(lead_dict.get('Recent_Conversion__c') != None):
        note_text += 'Recent Conversion: ' + lead_dict.get('Recent_Conversion__c') + '  |'
    if(lead_dict.get('Tasks') != None):
        note_text += generate_task_summary(lead_dict.get('Tasks').get('records'))
    if(lead_dict.get('CampaignMembers') != None):
        note_text += generate_campaign_summary(lead_dict.get('CampaignMembers').get('records'))
        
    return note_text

def generate_task_summary(task_list):
    task_text = 'Tasks: '
    for task in task_list:
        task_text += ' ( ' + task.get('Subject') + '  )   '
    task_text += '| '
    return task_text
    
def generate_campaign_summary(campaign_mem_list):
    campaign_mem_text = 'Campaigns: '
    for campaign in campaign_mem_list:
        campaign_mem_text += ' (  ' + campaign.get('Campaign_Name__c') + '  Status: ' + campaign.get('Status') + '  )  '
    campaign_mem_text += '| '
    return campaign_mem_text
    
    
    
def standardize_employee_range(lead_dict):
    e_count = lead_dict.get('NumberOfEmployees')
    e_range = ''
    if(e_count < 26):
        e_range = '1-25'
    elif(e_count > 26):
        e_range = '26-200'
    return e_range

def standardize_industry(lead_dict):
    cr_industry = lead_dict.get('Industry')
    cert_industry = lead_dict.get('Industry')
    if(cr_industry == 'Accounting'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Advertising'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Apparel'):
        cert_industry = 'Manufacturing'
    elif(cr_industry == 'Architecture'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Banking'):
        cert_industry = 'Finance'
    elif(cr_industry == 'Biotechnology'):
        cert_industry = 'Healthcare'
    elif(cr_industry == 'Chemicals'):
        cert_industry = 'Manufacturing'
    elif(cr_industry == 'Communications'):
        cert_industry = 'Telecommunications'
    elif(cr_industry == 'Consulting'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Electronics'):
        cert_industry = 'Manufacturing'
    elif(cr_industry == 'Energy'):
        cert_industry = 'Energy, Utilities & Waste Treatment'
    elif(cr_industry == 'Engineering'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Entertainment'):
        cert_industry = 'Consumer Services'
    elif(cr_industry == 'Environmental'):
        cert_industry = 'Energy, Utilities & Waste Treatment'
    elif(cr_industry == 'Food & Beverage'):
        cert_industry = 'Consumer Services'
    elif(cr_industry == 'Machinery'):
        cert_industry = 'Industrial'
    elif(cr_industry == 'Media'):
        cert_industry = 'Media & Internet'
    elif(cr_industry == 'Not For Profit'):
        cert_industry = 'Organizations'
    elif(cr_industry == 'Other'):
        cert_industry = 'Industrial'
    elif(cr_industry == 'Professional Service'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Public Relations'):
        cert_industry = 'Business Services'
    elif(cr_industry == 'Recreation'):
        cert_industry = 'Consumer Services'
    elif(cr_industry == 'Shipping'):
        cert_industry = 'Transportation'
    elif(cr_industry == 'Sports'):
        cert_industry = 'Media & Internet'
    elif(cr_industry == 'Technology'):
        cert_industry = 'Software'
    elif(cr_industry == 'Telecom'):
        cert_industry = 'Telecommunications'
    elif(cr_industry == 'Travel'):
        cert_industry = 'Consumer Services'
    elif(cr_industry == 'Utilities'):
        cert_industry = 'Energy, Utilities & Waste Treatment'
    else:
        cert_industry = 'Industrial'
        
    return cert_industry
        
    
def standardize_country(lead_dict):
    cr_country = lead_dict.get('Country')
    cert_country = lead_dict.get('Country')
    if(cr_country == 'Bolivia'):
        cert_country = 'Bolivia, Plurinational State of'
    elif(cr_country == 'Iran'):
        cert_country = 'Iran, Islamic Republic of'
    elif(cr_country == 'North Korea'):
        cert_country = 'Korea, Democratic People\'s Republic of'
    elif(cr_country == 'South Korea'):
        cert_country = 'Korea, Republic of'
    elif(cr_country == 'Laos'):
        cert_country = 'Lao People\'s Democratic Republic'
    elif(cr_country == 'Moldova'):
        cert_country = 'Moldova, Republic of'
    elif(cr_country == 'Marshall Islands'):
        cert_country = 'Saint Martin (French part)'
    elif(cr_country == 'Macedonia'):
        cert_country = 'Greece'
    elif(cr_country == 'Russia'):
        cert_country = 'Russian Federation'
    elif(cr_country == 'Saint Helena'):
        cert_country = 'Saint Helena, Ascension and Tristan da Cunha'
    elif(cr_country == 'Tanzania'):
        cert_country = 'Tanzania, United Republic of'
    elif(cr_country == 'Vatican City State'):
        cert_country = 'Holy See (Vatican City State)'
    elif(cr_country == 'Venezuela'):
        cert_country = 'Venezuela, Bolivarian Republic of'
    elif(cr_country == 'Viet nam'):
        cert_country = 'Vietnam'
    
    return cert_country

def standardize_state(lead_dict):
    cert_country = lead_dict.get('Country')
    cr_state = lead_dict.get('State')
    cert_state = lead_dict.get('State')
    if(cert_country == 'Australia'):
        if(cr_state == 'Brisbane'):
            cert_state = 'Queensland'
    if(cert_country == 'China'):
        if(cr_state == 'Chinese Taipei'):
            cert_state = 'Taiwan'
    if(cert_country == 'United Kingdom'):
        cert_state = None
    return cert_state

def _publish_alert(alert_message):
    data = {'message':alert_message}
    json_data = json.dumps(data)
    sns = boto3.client('sns')
    sns.publish(
    TopicArn='arn:aws:sns:us-east-1:374175877904:hamster_alerts',    
    Message=str(json_data))
