#!/usr/bin/python

#port forward redis somewhere
#ssh -R localhost:6380:localhost:6379 guyromm

import time,subprocess,sys,redis,commands,re,urllib,json,htmlentitydefs

pattern = re.compile("&(\w+?);")

def html_entity_decode_char(m, defs=htmlentitydefs.entitydefs):
    try:
        return defs[m.group(1)]
    except KeyError:
        return m.group(0)

def html_entity_decode(string):
    return pattern.sub(html_entity_decode_char, string)


class AppURLopener(urllib.FancyURLopener):
    version = "Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)"

urllib._urlopener = AppURLopener()


rd = redis.Redis('localhost')
fp = open('fb_pages.txt','r')

    
def fillq():
    ex=0 ; added=0
    print 'got %s in toscrape queue'%rd.scard('toscrape')
    while True:
        ln = fp.readline()
        #print ln
        if not ln:
            log.info('done file, breaking')
            break
        g = rd.get(ln)
        if g:
            print 'ex+ %s'%ln
            ex+=1
        else:
            #print 'have no key %s'%ln
            cnt = rd.scard('toscrape')
            if cnt<2000:
                print 'k+ %s'%ln
                rd.sadd('toscrape',ln.strip('\r\n\t '))
                added+=1
            else:
                break
                print 'k='
    print '%s keys exist, %s added, toscrape is %s long'%(ex,added,rd.scard('toscrape'))

#span class=\"subtitle fsm fcg\">Interest\u003c\/span>
catrg = re.compile('span class=(.{1,4})"subtitle fsm fcg(.{1,4})">(.*)\/span');
#span class=\"uiNumberGiant fsxxl fwb\">3\u003c\/span>
likesrg = re.compile('span class=(.{1,4})"(placePageStatsNumber|uiNumberGiant fsxxl fwb)(.{1,4})">(.{1,10})\/span');
#dt>About:\u003c\/dt>\u003cdd>Teknologia, bideo jokoak, informatika, internte..... hau dena eta askoz gehiago &#64; Bilduan. larunbatero 09:30etan Euskadi Irratian, eta Noiznahi interneten.\u003c\/dd
descrg = re.compile('dt>About:(.{0,22})>(.*)/dd');
lk2 = re.compile('([0-9\,]+) (People Like This|Person Likes This)');


def scrapeone(fn=None):
    scr=0
    if fn:
        cont = open(fn,'r').read()
        url = fn
        nm = fn
        ts = url+';;;;'+nm
    else:
        ts = rd.spop('toscrape').strip('\n\r\t ')
        if not ts:
            print 'nothing to scrape'
            return None
        nm,uri = ts.split(';;;;')
        url = 'http://www.facebook.com/'+uri
        d = urllib.urlopen(url)
        cont = d.read()
    print 'processing %s long content from %s'%(len(cont),url)
    if not len(cont):
        print 'empty content'
        pagetp='empty'
        tp=None
        descr=None
        likes=None
    elif 'It\'s free, and always will be.' in cont:
        pagetp='homepage-redir'
        tp=None
        descr=None
        likes=None
    else:
        try:
            tpe = catrg.search(cont)
            if tpe:
                pagetp = 'reg'
                tp = tpe.group(3)[0:-7]
                descr = None
                likes = likesrg.search(cont).group(4)[0:-7].replace(',','')
            else:
                tp =None
                pagetp = 'group'
                descres = descrg.search(cont)
                if descres: descr = html_entity_decode(descres.group(2)[0:-7])
                else: descr='NODESCR'
                likes = lk2.search(cont).group(1).replace(',','')

        except:
            fpt = open('lastfailed.txt','w') ; fpt.write(cont) ; fpt.close()
            print 'just written last failure in lastfailed.txt'
            raise
    op = {'pagetp':pagetp,'tp':tp,'descr':descr,'likes':likes,'url':url,'nm':html_entity_decode(nm)};
    print op
    rd.set(ts,json.dumps(op))
    return True
if len(sys.argv)>1:
    if sys.argv[1]=='fillq':
        fillq()
    elif sys.argv[1]=='testscrape':
        st,op = commands.getstatusoutput('ls *.html')
        for fn in op.split('\n'):
            scrapeone(fn)
    elif sys.argv[1]=='scrapeone':
        while scrapeone():
            pass
        print 'done'
    elif sys.argv[1]=='scrape':
        procs={}
        cmds = ['./fbscrape.py','scrapeone']
        while True:
            for i in range(1,20):
                if i not in procs:
                    print 'kicking off %s'%cmds
                    procs[i]=subprocess.Popen(cmds)
                else:
                    pollres = procs[i].poll()
                    print '%s poll = %s'%(i,pollres)
                    if pollres!=None:
                        procs[i]=subprocess.Popen(cmds)
            time.sleep(5)
