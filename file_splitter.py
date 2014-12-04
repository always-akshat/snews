import os
import bz2

def split_xml(filename):
    ''' The function gets the filename of wiktionary.xml.bz2 file as  input and creates
    smallers chunks of it in a the diretory chunks
    '''
    # Check and create chunk diretory
    if not os.path.exists("/media/akshat/OS/akshat_work/wiki_part"):
        os.mkdir("/media/akshat/OS/akshat_work/wiki_part")
    # Counters
    pagecount = 0
    filecount = 1
    #open chunkfile in write mode
    chunkname = lambda filecount: os.path.join("/media/akshat/OS/akshat_work/wiki_part", "wiki_part-"+str(filecount)+".xml.bz2")
    chunkfile = bz2.BZ2File(chunkname(filecount), 'w')
    # Read line by line
    bzfile = bz2.BZ2File(filename)
    start =0
    for line in bzfile:
        if start == 1 and not '</mediawiki>' in line:
            chunkfile.write(line)
            # the </page> determines new wiki page
            if '</page>' in line:
                pagecount += 1
            if pagecount > 10000:
                #print chunkname() # For Debugging
                chunkfile.write('</text>')
                chunkfile.close()
                pagecount = 0 # RESET pagecount
                filecount += 1 # increment filename
                chunkfile = bz2.BZ2File(chunkname(filecount), 'w')
                chunkfile.write('<text>')
        else:
            if '</siteinfo>' in line:
                start =1
                chunkfile.write('<text>')
    chunkfile.write('</text>')
    try:
        chunkfile.close()
    except:
        print 'Files already close'

if __name__ == '__main__':
    # When the script is self run
    split_xml('/media/akshat/OS/akshat_work/enwiki-20141106-pages-articles.xml.bz2')