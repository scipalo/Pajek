from selenium import webdriver
from bs4 import BeautifulSoup as Soup
import psycopg2
import mimetypes

import uuid
import hashlib
import traceback

import urllib3
import requests
import time

global conn
salt = uuid.uuid4().hex

#------------------
# SQL CONNECTION
#------------------

try:
    conn = psycopg2.connect("dbname='fridb' user='spela' password='scipajme.'")
    print("connected")
    cur = conn.cursor()

except:
    print("Err: unable to cennect")


#------------------
# CHROME DRIVERS
#------------------

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox') # required when running as root user. otherwise you would get no sandbox errors.
driver = webdriver.Chrome()

#----------------------
# ROBOTS LINK EXTRACTOR
#-----------------------

def get_sitemaps_url(url):
    r = requests.get(url)
    soup = Soup(r.text)
    locs = [url.find('loc').text for url in soup.findAll('url')]
    return locs

def get_robots_links(url, robots):
    allowed = set()
    links = set()
    lines = robots.splitlines()
    true = False
    sitemap = ''

    for line in lines:
        line = line.split(">")[-1]
        if line[0:4] == 'User':
            if line[12] == '*':
                true = True
            else:
                true = False
        if line[0:8] == 'Disallow' and true:
            new = url + line[9:]
            new = new.replace(" ", "")
            links.add(new)
        if line[0:5] == 'Allow' and true:
            new = url + line[6:]
            new = new.replace(" ", "")
            allowed.add(new)
        if line[0:7] == 'Sitemap':
            sitemap_url = line[9:]
            #sitemap = get_sitemaps_url(sitemap_url)

    return links, allowed, sitemap

#---------------
# SQL
#---------------

def insert_page(site_id, code_type, url, level, html, hash):
    try:
        cur.execute("""INSERT into crawldb.page(site_id, page_type_code, url, html_content, bfslevel, html_hash) VALUES (%s, %s, %s, %s, %s, %s)""",
                   (site_id, code_type, url, html, level, hash))
        conn.commit()
        return 1

    except Exception:
        #traceback.print_exc()
        conn.commit()
        #print("Page already exisits")
        return 0

def add_binary_page(site_id, link, level, type):
    # BINARY

    try:
        insert_page(site_id, 'BINARY', link, level + 1, "", "")

        # add to page data
        # get page id

        cur.execute("""SELECT id FROM crawldb.page WHERE url = %s AND page_type_code = 'BINARY'""", [link])
        page_id = cur.fetchone()
        cur.execute("""INSERT into crawldb.page_data(page_id, data_type_code, data) VALUES (%s, %s, %s)""", (page_id, type, ""))

        conn.commit()
    except Exception:
        #traceback.print_exc()
        conn.commit()
        #print("Binary add error")

#---------------
# WEBSITE
#---------------

#   id
#   domain
#   robots
#   sitemap

def site_fun(site_id, domain, http_prep):

    salt = uuid.uuid4().hex
    site_domain = domain
    site_url = http_prep + site_domain + "/"
    robots_content, sitemap_content = "", ""

    # ADD SITE TO TABLE SITE and TABLE PAGE - FRONTIER

    try:
        cur.execute("""INSERT into crawldb.site(id, "domain") VALUES (%s, %s)""", (site_id, site_domain))
        conn.commit()

    except:
        conn.commit()
        print("SITE already exists")

    html = driver.page_source
    hash = hashlib.sha256(salt.encode() + html.encode()).hexdigest()
    html = ""
    insert_page(site_id, 'FRONTIER' , site_url, 0, html, hash)

    # CHECK ROBOT.TXT

    site_robot_url = site_url + "robots.txt"
    driver.get(site_robot_url)
    robots_content = driver.page_source
    robots_links, allow, s_map = get_robots_links(site_url[:-1], robots_content)
    robots_content = hashlib.sha256(salt.encode() + robots_content.encode()).hexdigest()

    if len(robots_links) == 0:
        robots_content = ""

    robots_links.add(site_robot_url)
    for link in robots_links:
        insert_page(site_id, 'ROBOT', link, 0, "", "")

    for link in allow:
        html = driver.page_source
        hash = hashlib.sha256(salt.encode() + html.encode()).hexdigest()
        html = ""
        insert_page(site_id, 'FRONTIER', link, 0, html, hash)

    # SITE MAP

    for link in s_map:
        html = driver.page_source
        hash = hashlib.sha256(salt.encode() + html.encode()).hexdigest()
        html = ""
        insert_page (site_id, 'FRONTIER', link, 0, html, hash)

    cur.execute(
        "UPDATE crawldb.site SET robots_content = %s, sitemap_content = %s WHERE id = %s",
        [robots_content[0:50], sitemap_content[0:50], site_id])
    conn.commit()



#---------------
# PAGE
#---------------

def page_funtcion(page_url, site_id, level, domain_table):

    driver.get(page_url)
    acc_time = time.strftime('%Y-%m-%d %H:%M:%S')

    http = urllib3.PoolManager()
    http_status = http.request('GET', page_url).status
    #http_status = 0

    cur.execute(
        "UPDATE crawldb.page SET page_type_code = 'HTML', http_status_code = %s, accessed_time = %s WHERE url = %s",
        [http_status, acc_time, page_url])
    conn.commit()

    #
    # IMAGES
    #

    #print("Images ...")
    images = driver.find_elements_by_tag_name('img')

    for image_link in images:

        if image_link == None: continue

        image = image_link.get_attribute('src')
        #print(image)

        try: domain = image.split("//")[1].split("/")[0]
        except: continue

        if (not (domain in domain_table)): continue
        if(len(image)>300): continue

        acc_time = time.strftime('%Y-%m-%d %H:%M:%S')
        name = image.split("//")[1].split("/")[-1]

        if insert_page(site_id, 'BINARY', image, level+1, "", ""):
            cur.execute("""SELECT id FROM crawldb.page WHERE url = %s AND page_type_code = 'BINARY'""", [image])
            page_id = cur.fetchone()
            cur.execute("""INSERT into crawldb.image(page_id, filename, content_type, data, accessed_time)
                        VALUES (%s, %s, %s, %s, %s)""", (page_id, name, 'IMAGE', '\\001', acc_time))

    conn.commit()

    #
    # LINKS
    #
    print("Links ...")

    links = [ a.get_attribute('href') for a in driver.find_elements_by_xpath('.//a')]
    #for link in links:
    #    print(link)

    for link in links:

        if link == None: continue
        #print('link: '+link)

        # ALLOWED DOMAIN

        try: domain = link.split("//")[1].split("/")[0]
        except: continue

        if (not (domain in domain_table)): continue
        if(len(link)>300): continue

        # SEARCH FOR DUPLICATES:

        cur.execute("""SELECT id FROM crawldb.page WHERE url = %s""", [link])
        list_id = cur.fetchone()
        if list_id is not None: continue

        # SAME CONTENT

        driver.get(link)
        html = driver.page_source
        hash = hashlib.sha256(salt.encode() + html.encode()).hexdigest()
        html= ""

        cur.execute( """SELECT id FROM crawldb.page WHERE html_hash = %s AND NOT page_type_code = 'DUPLICATE'""", [hash])
        org_id = cur.fetchone()

        if org_id is not None:

            # '''Add to duplicates'''
            insert_page(site_id,'DUPLICATE', link, level + 1, "", "")
            cur.execute("""SELECT id FROM crawldb.page WHERE url = %s AND page_type_code = 'DUPLICATE' """, [link])
            dup_id = cur.fetchone()
            cur.execute("""INSERT into crawldb.link(from_page, to_page) VALUES (%s, %s)""", (org_id, dup_id[0]))
            conn.commit()

        # BINARY/HTML

        mimeType, _ = mimetypes.guess_type(link)
        if mimeType == None:
            insert_page(site_id, 'FRONTIER', link, level + 1, html, hash)
            continue

        mime_arr = mimeType.split('/')
        if 'html' in mime_arr:
            insert_page(site_id, 'FRONTIER', link, level + 1, html, hash)
        elif 'pdf' in mime_arr:
            add_binary_page(site_id, link, level, 'PDF')  # pdf
        elif 'powerpoint' in mime_arr:
            add_binary_page(site_id, link, level, 'PPT') # ppt
        elif 'presentation' in mime_arr:
            add_binary_page(site_id, link, level, 'PPTX') # pptx
        elif 'msword' in mime_arr:
            add_binary_page(site_id, link, level, 'DOC') # doc
        elif 'document' in mime_arr:
            add_binary_page(site_id, link, level, 'DOCX') # docx

def main_funtcion():

    level = 2
    counter = 0
    frontier_is_not_empty = True

    domain_table = ["evem.gov.si", "e-uprava.gov.si", "podatki.gov.si", "www.e-prostor.gov.si" ,
                    "www.mizs.gov.si", "www.mddsz.gov.si", "www.mf.gov.si", "www.mgrt.gov.si"]
    http_table = ["http://", "https://", "https://", "http://",
                  "http://", "http://", "http://", "http://"]

    #for id, url in enumerate(domain_table):
    #   site_fun(id, url, http_table[id])

    # exit()
    # LOOP

    while frontier_is_not_empty:

        counter = counter + 1
        print('\nRendering page id: ' + str(counter) + ' ...')

        # FIND PAGE WITH CURRENT LEVEL

        try:
            cur.execute("""SELECT url, site_id FROM crawldb.page
                        WHERE bfslevel = %s AND page_type_code = 'FRONTIER' LIMIT 1""", [level])
            page_data = cur.fetchone()
            page_funtcion(page_data[0], page_data[1], level, domain_table)

        except Exception:
            traceback.print_exc()

            print('\nLevel up: ')
            # RISE LEVEL AND FIND PAGE

            try:
                level = level + 1
                #print(level)
                cur.execute("""SELECT url, site_id FROM crawldb.page
                            WHERE bfslevel = %s AND page_type_code = 'FRONTIER' LIMIT 1""", [level])

                page_data = cur.fetchone()
                page_funtcion(page_data[0], page_data[1], level, domain_table)

                #print('Finished page - rise level')

            except:

                print("Err: frontier is empty")
                frontier_is_not_empty = False

if __name__ == '__main__': main_funtcion()
driver.close()