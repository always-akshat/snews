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

from lxml import etree
from xml.dom import minidom
from io import StringIO, BytesIO
import re
import redis
import string
import unicodedata

def getChildrenByAttribute(node, attribute):
    for child in node.childNodes:
        if child.localName== attribute:
            yield child





redis = redis.StrictRedis(host='localhost', port=6379, db=0)


readpath = '/home/akshat/data/'
dump_file = readpath + 'wiki.xml'
xml_file = open(dump_file, 'r')
xml_string = xml_file.read()


xmldoc = minidom.parse('/home/akshat/data/wiki.xml')
itemlist = xmldoc.getElementsByTagName('page')
print len(itemlist)


for node in itemlist:
    nodedata ={}
    nodedata['title'] = str(node.getElementsByTagName('title')[0].firstChild.nodeValue.encode('ascii', 'replace')).lower()
    nodedata['uniqueid'] = str(node.getElementsByTagName('ns')[0].firstChild.nodeValue) + '_' + str(node.getElementsByTagName('id')[0].firstChild.nodeValue)
    try:
        nodedata['redirect'] = str(node.getElementsByTagName('redirect')[0].attributes['title'].value.encode('ascii', 'replace')).lower()
    except:
        nodedata['redirect'] = ''
    text = node.getElementsByTagName('revision')[0].getElementsByTagName('text')[0].firstChild.nodeValue
    text = text.lower().replace("\"",'')
    #nodedata['text'] = str(text.encode('ascii', 'replace')).lower()
    #print text

    references = re.findall("(<ref)(.*?)(</ref>)", text)
    print references
    for reference in references:
        ref = reference[0] + reference[1] + reference[2]
        #print ref
        text = text.replace(ref, '')

    print text
    '''
    references = re.findall("((\{\{)([^\{\}]*)(\}\}))", text)
    for reference in references:
        ref = reference[0] + reference[1] + reference[2]
        print ref
        text = text.replace(ref, '')
    '''

    continue
    try:
        y = re.findall("((\[\[)([^\[\]]*)(\]\]))", nodedata['text'])
        outlinks = []
        for item in y:
            links = item[2].split('|')
            for link in links:
                link = str(link).lower()
                redis.sadd('terms', link)
            outlinks.append(str(item[2]).lower())
        nodedata['outlinks'] = outlinks
    except:
        pass
    #print nodedata
    print '\n'



