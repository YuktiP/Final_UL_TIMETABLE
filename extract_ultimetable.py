from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import time
from io import StringIO
from google.cloud import storage

url = 'https://www.timetable.ul.ie/UA/CourseTimetable.aspx'
courseDropdown = 'ctl00$HeaderContent$CourseDropdown'
yearDropdown = 'ctl00$HeaderContent$CourseYearDropdown'
timetable_df = {}
project_name = '20097786-etl-spark-timetable'
bucket_name = '20097786-ultimetable'

def createRow(rowname, data,course,year):
    timetablerow = data.split("|")
    dict = {"course_code":course.split('-')[0],"course":course.split('-')[1],"year":year,"day": rowname}
    for y in timetablerow:
        if re.match("^.*:.* - .*:[0-9]+$", y):
            dict.update({"time":y})
        elif((re.match("^.*-.*-$",y) or  re.match("^.*-.*$",y)) and ("LEC" in y or "TUT" in y or "LAB" in y)):
            arr = y.split("-")
            if len(arr) < 2:
                dict.update({"module":arr[0].strip(),"type":'',"location":''})
            elif len(arr) < 3:
                dict.update({"module":arr[0].strip(),"type":arr[1].strip(),"location":''})
            else:
                dict.update({"module":arr[0].strip(),"type":arr[1].strip(),"location":arr[2].strip()})
        elif("Wks" in y):
            dict.update({"weeks":y.replace("Wks:","")})
        elif("Online" in y or "Yr" in y or "Postgrad" in y):
            dict.update({"course_for":y})
        else:
            dict.update({"professor":y})
    return dict

def extractData(html,course,year):
    #Use pandas read_html() function to quickly parsing HTML tables in pages
    parsed_rows = []
    htmltable = pd.read_html(html)
    htmltable_df = htmltable[0]
    htmltable_df = htmltable_df.T
    htmltable_df = htmltable_df.dropna(axis=1, how='all')
    htmltable_df = htmltable_df.fillna(0)
    htmltable_df = htmltable_df.loc[:, (htmltable_df != 0).any(axis=0)]
   
    columns = list(htmltable_df)
    for index, row in htmltable_df.iterrows():
        for i in columns:
            if row[i] == 0:
                continue
            cellcontent = row[i].split("| |") 
            for cell in cellcontent:
                #timetable_df.update(createRow(index,cell,course,year))
                parsed_rows.append(createRow(index,cell,course,year))
    return parsed_rows

def createRequest(valuetype,value,soup):
    dropdown = ""
    dropdownvalue = ""
    data = {
                '__EVENTARGUMENT': soup.find('input', {'name': '__EVENTARGUMENT'}).get('value', ''),
                '__VIEWSTATE': soup.find('input', {'name': '__VIEWSTATE'}).get('value', ''),
                '__VIEWSTATEGENERATOR': soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value', ''),
                '__EVENTVALIDATION': soup.find('input', {'name': '__EVENTVALIDATION'}).get('value', ''),
            }
    dict_request_headers = {}
    if valuetype ==  "course":
        dict_request_headers = { courseDropdown : value, '__EVENTTARGET': courseDropdown}
    if valuetype ==  "year":
        dict_request_headers = { yearDropdown : value, '__EVENTTARGET': yearDropdown }
    if valuetype == "none":
        return data
    
    data.update(dict_request_headers)
    return data

def parseHtml():
    with requests.Session() as session:
        session.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'}
        
        # Get Request to set Cookies 
        response = session.get(url)
        soup = BeautifulSoup(response.content,features="lxml")
        #Need 3 requests to (1)set state (2)Set Course (3) Set Year
        postdata = createRequest('none',0,soup)
        response = session.post(url, data=postdata)
        soup = BeautifulSoup(response.content,features="lxml")
        df = pd.DataFrame(columns=['course_code','course','year','day','time','module','type','location','professor','weeks','course_for'])
    
        courseDropdown = soup.find(id ='HeaderContent_CourseDropdown')   
        
        for courseOption in courseDropdown.find_all('option'):
            course = courseOption.get('value','')
            if course  != '-1':
                postdata = createRequest('course',course,soup)
                response = session.post(url, data=postdata)
                soup = BeautifulSoup(response.content,features="lxml")
                yearDropdown = soup.find(id ='HeaderContent_CourseYearDropdown')
                for yearOption in yearDropdown.find_all('option'):
                    year = yearOption.get('value','')
                    if year  != '-1':
                        postdata = createRequest('year',year,soup)  
                        response = session.post(url, data=postdata)
                        soup = BeautifulSoup(response.content,features="lxml")
                        html = re.sub('<br\s*/>','|', str(soup))
                        df= df.append(extractData(html,course,year),ignore_index=True)
                        
        return df

def uploadToGCloudBucket(filename):
    gcs = storage.Client(project_name)
    f = StringIO()
    timetable_df.to_csv(f)
    f.seek(0)
    gcs.get_bucket(bucket_name).blob(filename).upload_from_file(f, content_type='text/csv')
    print("Extracted 'csv' uploaded to Google Cloud bucket...")

if __name__ == "__main__":
	# execute main function
	timetable_df = {}
	print("Start Extraction......")
	timetable_df = parseHtml()
	print("Time table data extracted successfully")
	ts = time.time() 
	filename = str(ts) + '_ultimetable.csv'
	timetable_df.to_csv(filename, index=False)
	print("Extracted 'csv' file saved...")
	uploadToGCloudBucket(filename)
