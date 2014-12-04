#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'akshat'
'''

input - Wikipedia dump file with pages an meta data
output - store
            1. text
            2. outbound links
            3. inbound links
            4. all link words and sense

aim : 1. create a list of all available topics on wikipedia
      2. create a graph of all connected topics on wikipedia.

'''

from xml.dom import minidom
from lxml import etree
import re
import os
import time
import bz2
from os import listdir
from os.path import isfile, join
from HTMLParser import HTMLParser
import MySQLdb
from memory_profiler import profile


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)


def getChildrenByAttribute(item, attribute):
    for child in item.childNodes:
        if child.localName == attribute:
            yield child


def CleanWikipediaText(main_content):
    references = re.findall("((<ref)(.*?)(/[ref]*>))", main_content, re.MULTILINE | re.VERBOSE)
    for reference in references:
        main_content = main_content.replace(reference[0], '')

    quotes = re.findall("((\{\{)([^\{\}]*)(\}\}))", main_content)
    for quote in quotes:
        main_content = main_content.replace(quote[0], '')

    main_content = ' '.join(main_content.split())
    return main_content


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def insert_mysql(database, database_cursor, sql, c=0):

    try:
        database_cursor.execute(sql)
        if c == 1:
            database.commit()
        return cursor.lastrowid

    except Exception, e:
        print str(e)
        return 'none'
        database.rollback()


def gen_out_links(outlinks, text):
    graph_elem = re.findall("((\[\[)([^\[\]]*)(\]\]))", text)
    for item in graph_elem:
        links = item[2].split('|')
        link_size = len(links)
        if link_size == 2:          # redirect link found , TODO: remove links with : in surface form
            if ':' not in links[0]:
                out_link = links[0].strip()
                out_link_text = links[1].strip()
                outlinks[out_link_text] = out_link
        elif link_size == 1:        # direct link found
            out_link = links[0].strip()
            out_link_text = links[0].strip()
            outlinks[out_link_text] = out_link

    return outlinks


def gen_cat_links(catlinks, reference_text):
    cat_elem = re.findall("((\[\[)([^\[\]]*)(\]\]))", reference_text)
    for item in cat_elem:
        links = item[2].split(':')
        if links[0] == 'Category' and not links[1].strip().endswith('|'):
            cat_link = links[1].strip()
            catlinks.append(cat_link)
    return catlinks


def gen_node_data(item):
    nodedata = {"title": "", "uniqueid": "", "redirect": "", "text" :""}
    for elem in item:
        if(elem.tag == 'title'):
            nodedata['title'] = elem.text.encode('utf-8', 'ignore')
        elif(elem.tag == 'ns'):
            nodedata['ns'] = elem.text
        elif(elem.tag == 'id'):
            nodedata['id'] = elem.text
        elif(elem.tag == 'redirect'):
            nodedata['redirect'] = elem.get('title').encode('utf-8', 'ignore')

    nodedata['uniqueid'] = (nodedata['ns'] + '_' + nodedata['id'])
    page_text = item.find(".//revision").find(".//text")
    try:
        nodedata['text'] = page_text.text.encode('ascii','ignore')
    except:
        pass
    return nodedata


def split_article_text(text):
    text = text.replace('\n', ' ')
    text = text.replace(' References ', 'References')
    text_ar = text.split('=References=')
    return text_ar



readpath = '/media/akshat/OS/akshat_work/wiki_part/'                     # read path of the xml file to read the wikipedia metadata
onlyfiles = [f for f in listdir(readpath) if isfile(join(readpath,f)) ]
partwritepath = readpath + 'partsdone/'


if not os.path.exists(partwritepath):
    os.mkdir(partwritepath)

db = MySQLdb.connect("localhost", "root", "bubbledb", "bubble" )
cursor = db.cursor()


@profile
def my_func():

    for f in onlyfiles:
        textfilename = str(f).split('.')[0] + '.txt'
        print textfilename
        donefiles = [df for df in listdir(partwritepath) if isfile(join(partwritepath, df))]

        if textfilename not in donefiles:
            f_to_write = open(partwritepath + textfilename, 'w')
            print '\n new file', f
            dump_file = readpath + f
            bz_file = bz2.BZ2File(dump_file)
            pages_xml = etree.parse(bz_file) # XML parsing of the file
            itemlist = pages_xml.findall(".//page")
            start_time = time.time()  #start measuring run time

            for item in itemlist:     # reading all page tags in the xml (all wikipedia pages)'

                nodedata = gen_node_data(item)
                titletype = nodedata['title'].split(':')

                if len(titletype) == 1:

                    try:
                        print nodedata['title']
                        text = nodedata['text']
                        text_ar = split_article_text(text)
                        text = text_ar[0]
                        text = CleanWikipediaText(text)
                        outlinks = {}
                        catlinks = []
                        cat_insert_string = ''
                    except:
                        continue
                        pass

                    try:
                        outlinks = gen_out_links(outlinks, text)
                    except Exception,e:
                        print str(e)


                    try:
                        reference_text = CleanWikipediaText(text_ar[1])
                        catlinks = gen_cat_links(catlinks, reference_text)
                    except:
                        pass


                    w_id = MySQLdb.escape_string(nodedata['uniqueid'])
                    title = MySQLdb.escape_string(nodedata['title'])
                    sql = "INSERT INTO page(w_id, title) VALUES ('" + w_id + "', '" + title + "')"
                    insertedpage = insert_mysql(db, cursor, sql)

                    if nodedata['redirect'] != '':
                        if insertedpage != 'none':
                            article_redirect_link = MySQLdb.escape_string(nodedata['redirect'])
                            sql = "INSERT INTO redirect(topic_id, redirect_link) VALUES ('" + str(insertedpage) + "', '" + article_redirect_link + "')"
                            inserted_redirect = insert_mysql(db,cursor,sql,0)

                    else:
                        if insertedpage != 'none':
                            if len(catlinks) > 0:
                                for article_category in catlinks:
                                    article_category = article_category.decode('utf-8').encode('ascii', 'ignore')
                                    cat_insert_string += "('"+ str(insertedpage) + "','" + MySQLdb.escape_string(article_category)+"'),"

                                cat_insert_string = cat_insert_string.strip(',')
                                sql = "INSERT INTO category(topic_id, category_link) VALUES " + cat_insert_string
                                inserted_category = insert_mysql(db,cursor,sql,0)
                                #print 'inserted redirect ', inserted_category

                            if (len(catlinks) == 0) or (inserted_category != 'none'):
                                link_insert_string =''
                                for key,value in outlinks.iteritems():
                                    surface_form = key.decode('utf-8').encode('ascii','ignore')
                                    link_page = value.decode('utf-8').encode('ascii','ignore')
                                    link_insert_string += "('"+ str(insertedpage) + "','" + MySQLdb.escape_string(surface_form)+"','" + MySQLdb.escape_string(link_page)+"'),"
                                link_insert_string = link_insert_string.strip(',')
                                sql = "INSERT INTO link(topic_id,surface, link) VALUES " + link_insert_string
                                inserted_links = insert_mysql(db,cursor,sql,1)


                elif titletype[0] == 'Category':

                    try:
                        print nodedata['title']
                        text = nodedata['text']
                        text = text.replace('\n', ' ')
                        text = CleanWikipediaText(text)
                        catlinks = []
                        cat_insert_string = ''

                    except:
                        print 'exception in part 1'
                        continue
                        pass

                    try:
                        catlinks = gen_cat_links(catlinks,text)
                        continue
                    except:
                        pass


                    w_id = MySQLdb.escape_string(nodedata['uniqueid'])
                    title = MySQLdb.escape_string(nodedata['title'])
                    sql = "INSERT INTO page(w_id, title) VALUES ('" + w_id + "', '" + title + "')"
                    print sql
                    insertedpage = insert_mysql(db, cursor, sql)

                    if insertedpage != 'none':
                        if len(catlinks) > 0:
                            for article_category in catlinks:
                                article_category = article_category.decode('utf-8').encode('ascii','ignore')
                                cat_insert_string += "('"+ str(insertedpage) + "','" + MySQLdb.escape_string(article_category)+"'),"

                            cat_insert_string = cat_insert_string.strip(',')
                            sql = "INSERT INTO category(topic_id, category_link) VALUES " + cat_insert_string
                            inserted_category = insert_mysql(db,cursor,sql,1)
                else:
                    continue


            f_to_write.write('1')
            f_to_write.close()

        else:
            print 'file exists'



if __name__ == '__main__':
    my_func()