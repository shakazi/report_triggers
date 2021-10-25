import boto3
import botocore
from datetime import datetime, timezone
import os
import fitz

_bucket='image-reuse-test'
s3 = boto3.client("s3")
today = datetime.now(timezone.utc)
today = today.strftime('%y-%m-%d')
print(today)

def download_from_s3(bucket, key, filename):
    s3_resource = boto3.resource('s3')
    arr_key = key.split('/')
    filename = str(arr_key[-1])
    try:
        s3_resource.Bucket(bucket).download_file(key, '/tmp/'+ filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    kwargs = {'Bucket': bucket}
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            ans = []
            key = obj['Key']
            date = obj['LastModified']
            date = str(date.strftime('%y-%m-%d'))
            ans = [key, date]
            if key.startswith(prefix) and key.endswith(suffix): #and date == today
                yield ans
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def upload_pdf(pdf_address):
    name = pdf_address.split('/')[-1]
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(pdf_address, _bucket, 'dump/'+name)
    print('pdf_uploaded ::', pdf_address)

def generate_pdf(imglist,claim_no):
    doc = fitz.open()  # PDF with the pictures
    imgdir = ""  # where the pics are
    imgcount = len(imglist)  # pic count

    for i, f in enumerate(imglist):
        img = fitz.open(os.path.join(imgdir, f))  # open pic as document
        rect = img[0].rect  # pic dimension
        pdfbytes = img.convert_to_pdf()  # make a PDF stream
        img.close()  # no longer needed
        imgPDF = fitz.open("pdf", pdfbytes)  # open stream as PDF
        page = doc.new_page(width = rect.width,  # new page with ...
                        height = rect.height)  # pic dimension
        page.show_pdf_page(rect, imgPDF, 0)  # image fills the page
    pdf_address = '/tmp/'+str(claim_no)+'combined.pdf'
    doc.save(pdf_address)
    upload_pdf(pdf_address)

def main(event,context):
    all_keys= list(get_matching_s3_keys(_bucket, suffix=('.jpg','.JPG','.png','.PNG','.jpeg','.JPEG')))
    dict = {}
    for key in all_keys:
        arr = key[0].split("/")
        claim_no = arr[1]
        dict[claim_no]=[]
    for key in all_keys:
        arr = key[0].split("/")
        claim_no = arr[1]
        dict[claim_no].append(key[0])
    for item in dict:
        imagelist = []
        for address in dict[item]:
            print(address)
            file = address.split("/")[-1]
            download_from_s3(_bucket, address, file)
            imagelist.append('/tmp/'+file)
            generate_pdf(imagelist,item)
    
    return {
    "statusCode": 200,
    "body": "Success"
    }

if __name__=="__main__":
    main(0,0)