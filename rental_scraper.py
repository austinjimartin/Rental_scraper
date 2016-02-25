#!/usr/bin/env python
# coding: utf-8
import sys,os
#sys.path.insert(0, "/usr/local/lib/python2.7/dist-packages")
import urllib2,urllib,re
import pymysql
from datetime import datetime

####################################################################
#rotating proxies
MAX_WAIT = 60*1 # default - 1 min
last_timestamp = datetime.now()
PROXY_TIME_OUT=50


proxies = []
def readProxies():
    global proxies
    global MAX_WAIT
    proxies = []
    ins = open( "proxies.list", "r" )
    for line in ins:
        p = re.compile(r'MAX_LIMIT_SECONDS=(?P<param>\d+)')
        m = p.search( line.rstrip() )
        if m is not None and m.group('param') is not None:
            MAX_WAIT = int(m.group('param'))
        else:
            proxies.append( line.rstrip() )

#readProxies()
#print "==================================================================================="
#print "Rotate proxies in every per " + str(MAX_WAIT) + " Seconds"
#print "proxies list:"
#print proxies
####################################################################
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='rentscan')
cities_lst = []
searchkey_lst = []

#cur = conn.cursor()
#cur.execute("SELECT * FROM rentals;")
#r = cur.fetchall()
#print r
#for r in cur.fetchall():
#    conn.user in r
#cur.close()

#db.commit()
# user functions #########################################################################

def get_cities_lst_fromDB():
    rtn=0
    global cities_lst

    cur = conn.cursor()
    sql = "SELECT city,state_short FROM cities Order By rank;"
    cur.execute(sql)
    for r in cur.fetchall():
        cities_lst.append(r[0] + ", " + r[1])
    cur.close()

    return rtn

def get_searchkey_lst_fromDB():
    rtn=0
    global searchkey_lst

    cur = conn.cursor()
    sql = "SELECT search_keywords FROM searchkws Order By id;"
    cur.execute(sql)
    for r in cur.fetchall():
        searchkey_lst.append(r[0])
    cur.close()

    return rtn

def parse_oneEntry(entry_str,search_keyword):
    rtn=0

    name=""
    address=""
    phone=""
    city=""
    state=""
    zip=""
    website=""
    stars=""
    lat=""
    lng=""
    street_no=""
    street=""
    url=""
    #can be skipped
    reviews=""
    reg_state=""
    reg_city=""
    reg_street=""
    status=""
    reg_status=""

    #get name
    p = re.compile(r",name:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        name = m.group('param')
        #print "name: " + name

    #get address
    p = re.compile(r"addressLines:\['(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        address = m.group('param')
        #print "address: " + address

    #get phone
    p = re.compile(r"phones:\[{number:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        phone = m.group('param')
        #print "phone: " + phone

    #get city
    p = re.compile(r"sxct:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        city = m.group('param')
        #print "city: " + city
    #addressLines:['715 Boylston Street #3b','Boston, MA 02116']
    if city == "":
        p = re.compile(r"addressLines:\['(?P<param1>.*?)','(?P<param2>.*?),")
        m = p.search( entry_str )
        if m is not None and m.group('param2') is not None:
            city = m.group('param2')
            #print "city: " + city


    #get state
    p = re.compile(r"sxpr:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        state = m.group('param')
        #print "state: " + state

    #get zip
    p = re.compile(r"sxpo:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        zip = m.group('param')
        #print "zip: " + zip

    #get website
    p = re.compile(r"actual_url:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        website = m.group('param')
        #print "website: " + website

    #get stars
    #stars:5,
    p = re.compile(r"stars:(?P<param>[+-]?((\d+(\.\d*)?)|\.\d+))")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        stars = m.group('param')
        #print "stars: " + stars

    #get lat
    p = re.compile(r"lat:(?P<param>[+-]?((\d+(\.\d*)?)|\.\d+))")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        lat = m.group('param')
        #print "lat: " + lat

    #get lng
    p = re.compile(r"lng:(?P<param>[+-]?((\d+(\.\d*)?)|\.\d+))")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        lng = m.group('param')
        #print "lng: " + lng

    #get street_no
    p = re.compile(r"sxsn:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        street_no = m.group('param')
        #print "street_no: " + street_no

    #get street
    p = re.compile(r"sxst:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        street = m.group('param')
        #print "street: " + street

    #get url
    #http://maps.google.com/maps/place?cid=10780510743725632587
    p = re.compile(r"cid:'(?P<param>.*?)'")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        url = "http://maps.google.com/maps/place?cid="+m.group('param')
        #print "url: " + url

    #get reviews
    #reviews:17,
    p = re.compile(r"reviews:(?P<param>[+-]?((\d+(\.\d*)?)|\.\d+))")
    m = p.search( entry_str )
    if m is not None and m.group('param') is not None:
        reviews = m.group('param')
        #print "reviews: " + reviews

    cur = conn.cursor()
    sql = "SELECT * FROM rentals WHERE phone=%s and name=%s and website=%s and url=%s;"
    cur.execute(sql, (phone,name,website,url))
    exist_flag = False
    for r in cur.fetchall():
        exist_flag = True
    cur.close()
    if exist_flag == True:
        return rtn

    if name=="" and address=="" and phone=="" and city=="" and state=="":
        return rtn

    reg_state=state
    reg_city=city
    reg_street=street
    reg_status=status

    name=name.decode("string_escape")
    address=address.decode("string_escape")
    phone=phone.decode("string_escape")
    city=city.decode("string_escape")
    state=state.decode("string_escape")
    zip=zip.decode("string_escape")
    website=website.decode("string_escape")
    stars=stars.decode("string_escape")
    lat=lat.decode("string_escape")
    lng=lng.decode("string_escape")
    street_no=street_no.decode("string_escape")
    street=street.decode("string_escape")
    url=url.decode("string_escape")
    reviews=reviews.decode("string_escape")
    reg_state=reg_state.decode("string_escape")
    reg_city=reg_city.decode("string_escape")
    reg_street=reg_street.decode("string_escape")
    status=status.decode("string_escape")
    reg_status=reg_status.decode("string_escape")

    cur = conn.cursor()
    sql = """INSERT INTO `rentscan`.`rentals` (`search_keyword`, `name`, `address`, `phone`, `city`, `state`, `zip`, `website`, `stars`, `reviews`, `lat`, `lng`, `street_no`, `street`, """ \
          """`url`, `reg_state`, `reg_city`, `reg_street`, `status`, `reg_status`, `ts`) """ \
          """VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());"""
    cur.execute(sql, (search_keyword, name, address, phone, city, state, zip, website, stars, reviews, lat, lng, street_no, street,
                      url, reg_state, reg_city, reg_street, status, reg_status))
    #print "name: " + name
    #print "stars: " + stars
    #print "reviews: " + reviews

#sql = "INSERT INTO people (name, email, age) VALUES (%s, %s, %s)"
    #cur.execute( sql, ( "Jay", "jay@example.com", '77') )
    cur.close()


    return rtn

def scrape_onepage(page_url,search_keyword):
# scrape one page
    rtn=0
    global PROXY_TIME_OUT

    data=urllib2.urlopen(page_url,timeout=PROXY_TIME_OUT)
    print page_url
    page_html = data.read()

    #regex = [r'{id:\'A\'.*},{id:\'B\'',
#             r'{id:\'B\'.*},{id:\'C\'',
#             r'{id:\'C\'.*},{id:\'D\'',
#             r'{id:\'D\'.*},{id:\'E\'',
#             r'{id:\'E\'.*},{id:\'F\'',
#             r'{id:\'F\'.*},{id:\'G\'',
#             r'{id:\'G\'.*},{id:\'H\'',
#             r'{id:\'H\'.*},{id:\'I\'',
#             r'{id:\'I\'.*},{id:\'J\'',
#             r'{id:\'J\'.*},{id:\'\'']
    #regex = r'{id:\''+'A'+'\'.*},{id:\'''B\''
    x="ABCDEFGHIJ"
    for i in range(len(x)):
        #print x[i]

        regex = ""
        if i == len(x)-1:
            regex = "{id:'"+x[i]+"'.*elms:"
        else:
            regex = "{id:'"+x[i]+"'.*},{id:'"+x[i+1]+"'"

        matches = re.findall(regex,page_html)
        #print regex
        match_flag = False
        for match in matches:
            #print match
            parse_oneEntry(match,search_keyword)
            match_flag = True
        #if count of search results is smaller than 10
        if match_flag == False:
            regex = "{id:'"+x[i]+"'.*elms:"
            matches = re.findall(regex,page_html)
            #print regex

            for match in matches:
                #print match
                parse_oneEntry(match,search_keyword)
    conn.commit()
    return rtn

#from here, main process
old_city_no=0
old_searchkey_no=0
old_start_no=0
isfilevalid=False
statusfilename="rental_status"
try:
    ins = open( statusfilename, "r" )
    for line in ins:
        p = re.compile(r'city=(?P<param>\d+),searchkey=(?P<param2>\d+),start_no=(?P<param3>\d+)')
        m = p.search( line.rstrip() )
        if m is not None and m.group('param') is not None:
            old_city_no = int(m.group('param'))
            isfilevalid = True
            print "Last position - you scraped by this position."
            print "before city_no: "+str(old_city_no)
        if m is not None and m.group('param2') is not None:
            old_searchkey_no = int(m.group('param2'))
            isfilevalid = True
            print "before searchkey_no: "+str(old_searchkey_no)
        if m is not None and m.group('param3') is not None:
            old_start_no = int(m.group('param3'))
            isfilevalid = True
            print "before start_no: "+str(old_start_no)
except:
    pass

if isfilevalid == False:
    #already done or start from begin
    print "==============================================================================="
    print "<"+statusfilename+"> file stands for saving scraping staus. Don't touch this file!"
    print "Saving status into this file, scraper continues from the last position."
    print "In the case when scraper is terminated unsuccessfully, scraper continues from the position in the next time."
    var = raw_input("Are you going to start scraping from begin?(yes/no):")
    if var.lower() != "yes":
        sys.exit()
print "==================================================================================="
print "scraping running..."

get_cities_lst_fromDB()
#print cities_lst

get_searchkey_lst_fromDB()

#print searchkey_lst
city_no=0
for city in cities_lst:
    if city_no>=old_city_no:
        print str(city_no)+" - City: "+city
        searchkey_no=0
        for searchkey in searchkey_lst:
            if city_no==old_city_no and searchkey_no<old_searchkey_no:
                pass
            else:
                print str(searchkey_no)+" - Search Key: "+searchkey
                for idx in range(15):
                    start_no=idx*10
                    if city_no==old_city_no and searchkey_no==old_searchkey_no and start_no<old_start_no:
                        pass
                    else:
                        #write status in file
                        fo = open(statusfilename, "wb")
                        #city=(?P<param>\d+),searchkey=(?P<param2>\d+),start_no=(?P<param3>\d+)
                        fo.write( "city="+str(city_no)+",searchkey="+str(searchkey_no)+",start_no="+str(start_no));
                        fo.close()
                        #make url
                        searchstr=searchkey+" near "+city
                        query_args = { 'q':searchstr, 'start':str(start_no) }
                        encoded_args = urllib.urlencode(query_args)
                        page_url="https://maps.google.com/maps?"+encoded_args

                        #print "Page NO: "+str(idx)
                        scrape_onepage(page_url,searchstr)

            searchkey_no+=1
    city_no+=1
#page_url="https://maps.google.com/maps?q=party+rentals+near+boston+ma&hl=en&ll=42.319209,-71.104717&spn=0.119946,0.264187&sll=42.34294,-71.088924&sspn=0.119901,0.264187&vpsrc=0&hq=party+rentals&hnear=Boston,+Suffolk,+Massachusetts&t=m&z=13"


#entry_str = "{id:'A',cid:'14228390577189220164',latlng:{lat:42.349458,lng:-71.080197},image:'https://maps.gstatic.com/intl/en_ALL/mapfiles/markers2/markerA.png',sprite:{width:20,height:34,top:0,image:'https://maps.gstatic.com/mapfiles/markers2/red_markers_A_J2.png'},icon_id:'A',drg:true,laddr:'Rentals Unlimited, 715 Boylston Street #3b, Boston, MA 02116',geocode:'CWrj8Jx5vaC2FZIzhgId-2bD-ylzo16FDnrjiTFE7-A-2GV1xQ',sxti:'Rentals Unlimited',sxst:'Boylston Street',sxsn:'715',sxct:'Boston',sxpr:'MA',sxpo:'02116',sxcn:'US',sxph:'+16175170480',name:'Rentals Unlimited',infoWindow:{title:'\x3cb\x3eRentals\x3c/b\x3e Unlimited',addressLines:['715 Boylston Street #3b','Boston, MA 02116'],phones:[{number:'(617) 517-0480'}],hp:{url:'/local_url?dq\x3dparty+rentals+near+boston+ma\x26q\x3dhttp://www.rentals-unlimited.net/\x26oi\x3dmiw\x26sa\x3dX\x26ct\x3dmiw_link\x26cd\x3d1\x26cad\x3dhomepage,cid:14228390577189220164\x26ei\x3d3cWHUKuxI7GziQeBsYHwBw\x26s\x3dANYYN7k1JcGV7eKrlTzJ-9c3UUPT1hQEew',domain:'rentals-unlimited.net',actual_url:'http://www.rentals-unlimited.net/'},basics:'\x3cdiv transclude\x3d\x22iw\x22\x3e\x3c/div\x3e',moreInfo:'more info',place_url:'https://maps.google.com/local_url?dq\x3dparty+rentals+near+boston+ma\x26q\x3dhttps://plus.google.com/113135578475901198730/about%3Fhl%3Den\x26s\x3dANYYN7mZr7zFzT8y1NVWnu56I1i0IZdwYQ',zrvOk:true,loginUrl:'https://www.google.com/accounts/ServiceLogin?service\x3dlocal\x26hl\x3den\x26nui\x3d1\x26continue\x3dhttps://maps.google.com/maps/place%3Fcid%3D14228390577189220164%26q%3Dparty%2Brentals%2Bnear%2Bboston%2Bma%26hl%3Den%26t%3Dm%26cd%3D1%26cad%3Dsrc:ppwrev%26ei%3D3cWHUKuxI7GziQeBsYHwBw%26action%3Dopenratings',photoUrl:'https://lh3.googleusercontent.com/-Ou2QHd518hs/T8JJPOZE2WI/AAAAAAA-IBo/IeHq-S3ce2k/s90/Rentals%2BUnlimited',photoType:1,cat2:[{name:'Rental Agencies',lang:'en'}],lbcurl:'http://www.google.com/local/add/choice?hl\x3den\x26gl\x3dUS\x26latlng\x3d14228390577189220164\x26q\x3dparty+rentals\x26near\x3dBoston,+Suffolk,+Massachusetts',lbcclaimed:true,link_jsaction:''},ss:{edit:true,detailseditable:true,deleted:false,rapenabled:true,mmenabled:true},b_s:2,viewcode_data:[{preferred_panoid:'y4inff74GUf-bdlglk5Ewg',viewcode_lat_e7:423495591,viewcode_lon_e7:3584167629,yaw:5.8094606399536133,pitch:-0.91553682088851929,unsnapped_lat_e7:423495591,unsnapped_lng_e7:3584167629,source:274,debug_key:'B:14228390577189220164',pano_lat_e7:423493984,pano_lng_e7:3584167462},{preferred_panoid:'wAo-Ua-DMfptEdFz7qX8bQ',viewcode_lat_e7:423495301,viewcode_lon_e7:3584167602,yaw:2.6102972030639648,pitch:-1.0501999855041504,unsnapped_lat_e7:423495301,unsnapped_lng_e7:3584167602,source:274,debug_key:'B:14228390577189220164',pano_lat_e7:423493739,pano_lng_e7:3584167557},{preferred_panoid:'fDLAS0AiLRp2uqfjcSqAEg',viewcode_lat_e7:423496253,viewcode_lon_e7:3584166992,yaw:358.79000854492188,pitch:-3.25,unsnapped_lat_e7:423496253,unsnapped_lng_e7:3584166992,source:273,debug_key:'B:14228390577189220164',pano_lat_e7:423494604,pano_lng_e7:3584167093}],hover_snippet:'Absolutely the BEST!!!',hover_snippet_attr:'cityvoter.com',elms:[4,1,6,1,10,2,12,1,9,1,5,2,11],known_for_term:['larry green','tosca drive','wedding services','special event']},{id:'B'"
#entry_str = "{id:'A',cid:'14228390577189220164',latlng:{lat:42.349458,lng:-71.080197},image:'https://maps.gstatic.com/intl/en_ALL/mapfiles/markers2/markerA.png',sprite:{width:20,height:34,top:0,image:'https://maps.gstatic.com/mapfiles/markers2/red_markers_A_J2.png'},icon_id:'A',drg:true,laddr:'Rentals Unlimited, 715 Boylston Street #3b, Boston, MA 02116',geocode:'CWrj8Jx5vaC2FZIzhgId-2bD-ylzo16FDnrjiTFE7-A-2GV1xQ',sxti:'Rentals Unlimited',sxst:'Boylston Street',sxsn:'715',sxct:'Boston',sxpr:'MA',sxpo:'02116',sxcn:'US',sxph:'+16175170480',name:'Rentals Unlimited',infoWindow:{title:'\x3cb\x3eRentals\x3c/b\x3e Unlimited',addressLines:['715 Boylston Street #3b','Boston, MA 02116'],phones:[{number:'(617) 517-0480'}],hp:{url:'/local_url?dq\x3dparty+rentals+near+boston+ma\x26q\x3dhttp://www.rentals-unlimited.net/\x26oi\x3dmiw\x26sa\x3dX\x26ct\x3dmiw_link\x26cd\x3d1\x26cad\x3dhomepage,cid:14228390577189220164\x26ei\x3d3cWHUKuxI7GziQeBsYHwBw\x26s\x3dANYYN7k1JcGV7eKrlTzJ-9c3UUPT1hQEew',domain:'rentals-unlimited.net',actual_url:'http://www.rentals-unlimited.net/'},basics:'\x3cdiv transclude\x3d\x22iw\x22\x3e\x3c/div\x3e',moreInfo:'more info',place_url:'https://maps.google.com/local_url?dq\x3dparty+rentals+near+boston+ma\x26q\x3dhttps://plus.google.com/113135578475901198730/about%3Fhl%3Den\x26s\x3dANYYN7mZr7zFzT8y1NVWnu56I1i0IZdwYQ',zrvOk:true,loginUrl:'https://www.google.com/accounts/ServiceLogin?service\x3dlocal\x26hl\x3den\x26nui\x3d1\x26continue\x3dhttps://maps.google.com/maps/place%3Fcid%3D14228390577189220164%26q%3Dparty%2Brentals%2Bnear%2Bboston%2Bma%26hl%3Den%26t%3Dm%26cd%3D1%26cad%3Dsrc:ppwrev%26ei%3D3cWHUKuxI7GziQeBsYHwBw%26action%3Dopenratings',photoUrl:'https://lh3.googleusercontent.com/-Ou2QHd518hs/T8JJPOZE2WI/AAAAAAA-IBo/IeHq-S3ce2k/s90/Rentals%2BUnlimited',photoType:1,cat2:[{name:'Rental Agencies',lang:'en'}],lbcurl:'http://www.google.com/local/add/choice?hl\x3den\x26gl\x3dUS\x26latlng\x3d14228390577189220164\x26q\x3dparty+rentals\x26near\x3dBoston,+Suffolk,+Massachusetts',lbcclaimed:true,link_jsaction:''},ss:{edit:true,detailseditable:true,deleted:false,rapenabled:true,mmenabled:true},b_s:2,viewcode_data:[{preferred_panoid:'y4inff74GUf-bdlglk5Ewg',viewcode_lat_e7:423495591,viewcode_lon_e7:3584167629,yaw:5.8094606399536133,pitch:-0.91553682088851929,unsnapped_lat_e7:423495591,unsnapped_lng_e7:3584167629,source:274,debug_key:'B:14228390577189220164',pano_lat_e7:423493984,pano_lng_e7:3584167462},{preferred_panoid:'wAo-Ua-DMfptEdFz7qX8bQ',viewcode_lat_e7:423495301,viewcode_lon_e7:3584167602,yaw:2.6102972030639648,pitch:-1.0501999855041504,unsnapped_lat_e7:423495301,unsnapped_lng_e7:3584167602,source:274,debug_key:'B:14228390577189220164',pano_lat_e7:423493739,pano_lng_e7:3584167557},{preferred_panoid:'fDLAS0AiLRp2uqfjcSqAEg',viewcode_lat_e7:423496253,viewcode_lon_e7:3584166992,yaw:358.79000854492188,pitch:-3.25,unsnapped_lat_e7:423496253,unsnapped_lng_e7:3584166992,source:273,debug_key:'B:14228390577189220164',pano_lat_e7:423494604,pano_lng_e7:3584167093}],hover_snippet:'Absolutely the BEST!!!',hover_snippet_attr:'cityvoter.com',elms:[4,1,6,1,10,2,12,1,9,1,5,2,11],known_for_term:['larry green','tosca drive','wedding services','special event']}"

conn.close()
#done successfully!
print "==================================================================================="
print "Congratulations! Scraping successfully finished!"
print "==================================================================================="
os.remove(statusfilename)
