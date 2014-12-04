__author__ = 'akshat'


from lxml import etree
import bz2
from memory_profiler import profile


@profile
def my_func():
    dump_file = '/media/akshat/OS/akshat_work/wiki_part/wiki_part-303.xml.bz2'
    bz_file = bz2.BZ2File(dump_file)
    pages_xml = etree.parse(bz_file)



    reviews = pages_xml.findall(".//page")
    print len(reviews)
    for wiki_article in reviews:
        nodedata = {"title": "", "uniqueid": "", "redirect": ""}
        for elem in wiki_article:
            if(elem.tag == 'title'):
                nodedata['title'] = elem.text.encode('utf-8','ignore')
            elif(elem.tag == 'ns'):
                nodedata['ns'] = elem.text
            elif(elem.tag == 'id'):
                nodedata['id'] = elem.text
            elif(elem.tag == 'redirect'):
                nodedata['redirect'] = elem.get('title').encode('utf-8','ignore')
        nodedata['uniqueid'] = (nodedata['ns'] + '_' + nodedata['id']).encode('utf-8','ignore')

        page_text= wiki_article.find(".//revision").find(".//text")
        print page_text.text

        print nodedata
        exit()


if __name__ == '__main__':
    my_func()