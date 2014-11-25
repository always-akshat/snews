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
import re
from py2neo import neo4j, node, rel
import time


'''
graph_nodes = graph_db.create(node('topic', name='sex'),
                node('method', name='book'))
print graph_nodes

relation = graph_db.create(rel(graph_nodes[0], 'belongs', graph_nodes[1]))
print relation
exit(),
'''

def getChildrenByAttribute(item, attribute):
    for child in item.childNodes:
        if child.localName == attribute:
            yield child

def addnodetograph(key,value,label):
    prev_nodes = graph_db.find(label,key,value)
    already_present = 0 if sum(1 for x in prev_nodes) == 0 else 1

    if already_present == 0 :
        added_node, = graph_db.create(node(key = value))
        added_node.add_labels(label)
        topicindex.add_if_none(key, value, added_node)

def CleanWikipediaText(main_content):
    references = re.findall("((<ref)(.*?)(/[ref]*>))", main_content, re.MULTILINE | re.VERBOSE)
    for reference in references:
        main_content = main_content.replace(reference[0], '')

    quotes = re.findall("((\{\{)([^\{\}]*)(\}\}))", main_content)
    for quote in quotes:
        main_content = main_content.replace(quote[0], '')

    main_content = ' '.join(main_content.split())
    return main_content



readpath = '/home/akshat/data/'
dump_file = readpath + 'wiki-1.xml-p000000010p000010000' # 'wiki-1.xml-p000000010p000010000'
xml_file = open(dump_file, 'r')
xml_string = xml_file.read()

xmldoc = minidom.parse(dump_file)
itemlist = xmldoc.getElementsByTagName('page')
#print len(itemlist)





graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
graph_db.clear()
start_time = time.time()

print time.time() - start_time

topicindex = graph_db.get_or_create_index(neo4j.Node, 'topic')
surfaceindex = graph_db.get_or_create_index(neo4j.Node, 'surface')
pageindex = graph_db.get_or_create_index(neo4j.Node, 'page')
categoryindex = graph_db.get_or_create_index(neo4j.Node, 'category')



for item in itemlist:

    nodedata ={}
    nodedata['title'] = item.getElementsByTagName('title')[0].firstChild.nodeValue.encode('utf-8', 'ignore')
    nodedata['uniqueid'] = (item.getElementsByTagName('ns')[0].firstChild.nodeValue + '_' + item.getElementsByTagName('id')[0].firstChild.nodeValue).encode('utf-8','ignore')
    try:
        nodedata['redirect'] = item.getElementsByTagName('redirect')[0].attributes['title'].value.encode('ascii','ignore')
    except:
        nodedata['redirect'] = ''
    lookuptime = time.time()
    prev_nodes = list(graph_db.find('topic', 'title', nodedata['title']))
    #print prev_nodes


    already_present = len(prev_nodes)

    if already_present == 0:
        #print 'node is not present'
        article_node, = graph_db.create(node(title = nodedata['title']))
        article_node.set_labels("topic")
        topicindex.add_if_none(article_node['title'], article_node['title'], article_node)
    else:
        #print 'node was already present'
        article_node = prev_nodes[0]


    if nodedata['redirect'] == '':
        article_node.add_labels('page')
        pageindex.add_if_none(article_node['title'], article_node['title'], article_node)
    else:
        article_node.add_labels('surface')
        surfaceindex.add_if_none(article_node['title'], article_node['title'], article_node)
        prev_redirect_node = list(graph_db.find('topic', 'title', nodedata['redirect']))
        redirect_node_already_present = len(prev_redirect_node)

        if len(prev_redirect_node) ==0:
            redirect_node, = graph_db.create(node(title = nodedata['redirect']))
            redirect_node.set_labels("topic")
            topicindex.add_if_none(nodedata['redirect'], nodedata['redirect'], redirect_node)
            graph_db.create(rel(article_node, "redirect", redirect_node, {"cost": 0}))
            article_node = redirect_node
        else:
            article_node = prev_redirect_node[0]
    '''
    print nodedata
    print article_node.get_properties()
    print article_node.get_labels()
    print 'topicindex', topicindex.get('title', article_node['title'])
    print 'surfaceindex', surfaceindex.get('title', article_node['title'])
    print 'pageindex', pageindex.get('title', article_node['title'])
    print 'categoryindex', categoryindex.get('title', article_node['title'])
    '''

    workingnode = article_node
    print 'working node', workingnode


    text = item.getElementsByTagName('revision')[0].getElementsByTagName('text')[0].firstChild.nodeValue
    text = text.replace('\n', ' ')
    text = text.replace(' References ', 'References')
    text = text.split('=References=')
    text = text[0]
    reference_text = text[1]

    text = CleanWikipediaText(text)

    try:
        graph_elem = re.findall("((\[\[)([^\[\]]*)(\]\]))", text)
        outlinks = []
        print graph_elem
        for item in graph_elem:
            links = item[2].split('|')
            link_size = len(links)

            if link_size == 2:          # redirect link found , TODO: remove links with : in surface form
                if ':' not in links[0]:
                    mainpage = links[0]
                    redirectpage = links[1]
                    print 'this -----> ' + redirectpage + '-------> ' + mainpage
                    prev_mainpage_node = list(graph_db.find('topic', 'title', mainpage))
                    prev_mainpage_node_exists = len(prev_mainpage_node)
                    if prev_mainpage_node_exists == 0:
                        print 'main doesnt exists'
                        mainpage_node, = graph_db.create(node(title = mainpage))
                        mainpage_node.add_labels("topic", "page")
                        #topicindex.add_if_none('title', mainpage, mainpage_node)
                        #pageindex.add_if_none('title', mainpage, mainpage_node)
                    else:
                        print 'main exists'
                        mainpage_node = prev_mainpage_node[0]


                    prev_redirectpage_node = list(graph_db.find('topic', 'title', redirectpage))
                    prev_redirectpage_node_exists = len(prev_redirectpage_node)

                    if prev_redirectpage_node_exists == 0:
                        print 'redirect doesnt exists'
                        redirectpage_node, = graph_db.create(node(title = redirectpage))
                        redirectpage_node.add_labels("topic", "surface")
                        #topicindex.add_if_none('title', redirectpage, redirectpage_node)
                        #pageindex.add_if_none('title', redirectpage, redirectpage_node)
                    else:
                        print 'redirect exists'
                        redirectpage_node = prev_redirectpage_node[0]

                    graph_db.create(rel(workingnode, "links", redirectpage_node, {"cost": 1}))
                    graph_db.create(rel(redirectpage_node, "redirect", mainpage_node, {"cost": 0}))

            elif link_size == 1:        # direct link found
                mainpage = links[0]
                print 'this -----> ' +  mainpage
                prev_mainpage_node = list(graph_db.find('topic', 'title', mainpage))
                prev_mainpage_node_exists = len(prev_mainpage_node)
                if prev_mainpage_node_exists == 0:
                    print 'doesnt exists'
                    mainpage_node, = graph_db.create(node(title = mainpage))
                    mainpage_node.add_labels("topic", "page")
                    #topicindex.add_if_none('title', mainpage, mainpage_node)
                    #pageindex.add_if_none('title', mainpage, mainpage_node)
                else:
                    print 'exists'
                    mainpage_node = prev_mainpage_node[0]

                graph_db.create(rel(workingnode, "links", mainpage_node, {"cost": 1}))

    except:
        pass
#    exit()
    print '\n'
    print time.time() - start_time





