import boto3
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime, timedelta, date
import logging
logging.basicConfig(filename='report.log', filemode= 'w', format= '%(asctime)s %(message)s')
logger = logging.getLogger()
today_date = date.today()
prev_date = today_date - timedelta(days=40)

dynamodb = boto3.resource('dynamodb')

table= dynamodb.Table('sharekh-test')
table2= dynamodb.Table('image-reuse-reporting-dev')

index = "Index-updatedOnDateHasClash-fileHash"

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def files_seen():
    output= []
    for p in daterange(prev_date, today_date):
      p = p.strftime("%Y%m%d")
      response = table.query(
        IndexName = index,
        KeyConditionExpression = Key('updatedOnDateHasClash').eq(p + ":TRUE")
      )
      data = response['Items']
      while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
      if(len(data)!=0):
        output.append(data)
    return output

def claims_with_clash():
    output= []
    myset = set()
    for p in daterange(prev_date, today_date):
      p = p.strftime("%Y%m%d")
      response = table.query(
        IndexName = index,
        KeyConditionExpression = Key('updatedOnDateHasClash').eq(p + ":TRUE")
      )
      data = response['Items']
      while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
      if(len(data)!=0):
        output.append(data)
    
    for entry_for_a_date in output:
      for item in entry_for_a_date:
        for details in item['requests']:
          claim_no= details['sourceElementId']
          myset.add(claim_no)
    unique_output = []
    for val in myset:
      unique_output.append(val)

    return unique_output

def total_claims():
    output= []
    myset = set()
    for p in daterange(prev_date, today_date):
      p = p.strftime("%Y%m%d")
      response = table.query(
        IndexName = index,
        KeyConditionExpression = Key('updatedOnDateHasClash').eq(p + ":FALSE")
      )
      data = response['Items']
      while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
      if(len(data)!=0):
        output.append(data)
    
    for entry_for_a_date in output:
      for item in entry_for_a_date:
        for details in item['requests']:
          claim_no= details['sourceElementId']
          myset.add(claim_no)
    unique_output = []
    for val in myset:
      unique_output.append(val)
    
    unique_output = unique_output + claims_with_clash()

    return unique_output

def clashes_detected():
    output= []
    p = prev_date.strftime("%Y%m%d")
    response = table2.query(
      KeyConditionExpression = Key('lob_subLob').eq('motor:unknown') & Key('date').gt(p),
    )
    data = response['Items']
    print(data)

    while 'LastEvaluatedKey' in response:
      response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'])
      data.extend(response['Items'])

    if(len(data)!=0):
      output.append(data)
    clashes =0
    for item in output:
      for i in item:
        clashes = clashes + i['total_clashes']
    return clashes

if __name__ == '__main__':
    # res = files_seen()
    # for item in res:
    #   print(item, '\n')
    # print('Count res', len(res))
    # print('FILES SEEN : ',res)
    #claims_with_clashes = claims_with_clash()
    #total_clashes = clashes_detected()
    # print('CLAIMS DETECTED : ',res['Count'])
    all_claims = total_claims()
    number_of_all_claims = len(all_claims)
    print(all_claims)
