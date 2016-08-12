# -*- coding: utf-8 -*-  
import urllib2
import solr
import hashlib
import time
import os
###############
conn = solr.SolrConnection('http://hadoop.co.spb.ru:8983/solr/keywords_only_shard1_replica1')
#base = "English44Keywords.txt"  
start_time = time.time()
path = '/z/backup_JBOD/pasthukov/keywords_only_to_solr'
ids = 0
count = 0
###############

##generates uid from keyword string##
def gen_ID (st):   
    uid = int(hashlib.md5(st).hexdigest(), 16)
    #uid = str(int(hashlib.md5(st).hexdigest(), 16))[:16] ### 16 char ID
    return uid

def mainWork(keyw_file):
    global ids
    global count
    with open(keyw_file) as f:
        dlist=[]
        for line in f:
            if not line.strip():                       ## skip blank lines
                continue                     
            line = line.rstrip('\r\n')            ## delete Windows end-of-line delimiter
            doc = {}
            #line = line.split()
            ids +=1
            count +=1
            keyw=line
            #wal1=line[1]
            #wal2=line[2]
            doc = dict(id=ids, keyword=keyw, uid=gen_ID(keyw))
            dlist.append(doc)
            if count ==100000:
                conn.add_many(dlist)
                conn.commit()
                count = 0
                dlist=[] # dropping list
                print "Current ID added to Solr: " + str(ids)
                print "--- %s seconds ---" % round(time.time() - start_time)
    ###remaining add&commit(less then 100 000)
    conn.add_many(dlist)
    conn.commit()
    print "Base " + str(keyw_file) + "uploaded to Solr successfully!" 
    print "Last ID for base was - " + str(ids)

def baseLoop ():
    os.chdir(path)
    for filename in os.listdir(os.getcwd()):
        mainWork(filename)
    
baseLoop()
print "Done!"
print "Elapsed time: " + "--- %s seconds ---" % round(time.time() - start_time)
