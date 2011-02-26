#!/usr/bin/python

#port forward redis somewhere
#ssh -R localhost:6380:localhost:6380 rhost

import time,subprocess,sys,redis,commands,re,urllib,json,htmlentitydefs,codecs

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

if len(sys.argv)>2: prt = int(sys.argv[2])
else: prt= 6379
hst = '127.0.0.1'
print 'connecting to redis at %s:%s'%(hst,prt)
rd = redis.Redis(host=hst,port=prt)

artifact = """\u00b7"""
lre = re.compile('^(\d+)$')
unire = re.compile(re.escape('\\u00')+'(.{2})')
#print unichr(int(unire.search(src).group(1),16))
def fillq(output=False,inspect=False):
    fp = codecs.open('fb_pages.txt','r','utf-8')
    ex=0 ; added=0 ; deled=0 ; cnt=0
    #print 'got %s in toscrape queue'%rd.scard('toscrape')
    while True:
        ln = fp.readline()
        cnt+=1
        #print ln
        if not ln:
            log.info('done file, breaking')
            break
        ln = ln.strip('\r\n\t ')
        g = rd.get(ln)
        if g:
            if output or inspect:
                dt = json.loads(g)
                if dt['likes']:
                    likes = unicode(dt['likes'])
                    if inspect:
                        if ',' in likes or '.' in likes: likes=likes.replace(',','').replace('.','')
                        if not lre.search(likes):
                            print('BAD LIKES in %s'%dt)
                            rd.delete(ln)
                            deled+=1
                else: likes='UNAVAIL'
                if dt['tp']:
                    tp = dt['tp'].replace(artifact,'')
                    fi = unire.finditer(tp)
                    fre=False
                    for it in fi:
                        och = '\\u00'+it.group(1)
                        nch = unichr(int(it.group(1),16))
                        tp = tp.replace(och,nch)
                        #print u'%s => %s'%(och,nch)
                        fre=True
                    tp = tp.replace('\\/','/')
                    #if fre: raise Exception(u'new:%s'%tp)
                    if inspect:
                        if unire.search(tp): raise Exception(u'still have shit in %s'%tp)
                        if '\\/' in tp: raise Exception(u'still have crap in %s'%tp)
                    #print tp

                else: tp = 'UNAVAIL'

                if output: print unicode(ln)+u';;;;'+likes+u';;;;'+tp+';;;;'+unicode(dt) # %s'%ln
                if inspect and cnt % 100 ==0: print '%s cnt'%cnt
                    
            ex+=1
        elif not inspect and not output:
            #print 'have no key %s'%ln
            cnt = rd.scard('toscrape')
            if cnt<2000:
                #print 'k+ %s'%ln
                rd.sadd('toscrape',ln.strip('\r\n\t '))
                added+=1
            else:
                break
                print 'k='
        else:
            print 'no filling queue with %s'%ln
            #break
    print '%s keys exist, %s added, toscrape is %s long, %s deled'%(ex,added,rd.scard('toscrape'),deled)

#span class=\"subtitle fsm fcg\">Interest\u003c\/span>
catrg = re.compile('span class=(.{1,4})"subtitle fsm fcg(.{1,4})">(.*)\/span');
#span class=\"uiNumberGiant fsxxl fwb\">3\u003c\/span>
likesrg = re.compile('span class=(.{1,4})"(placePageStatsNumber|uiNumberGiant fsxxl fwb)(.{1,4})">(.{1,25})\/span');
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
        ts = rd.spop('toscrape')
        if not ts:
            print 'out of scraping queue'
            time.sleep(30)
            return None
        ts = ts.strip('\n\r\t ')
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
                likesre = likesrg.search(cont)
                likes = likesre.group(4)[0:-7].replace(',','')
            else:
                tp =None
                pagetp = 'group'
                descres = descrg.search(cont)
                if descres: descr = html_entity_decode(descres.group(2)[0:-7])
                else: descr='NODESCR'
                lk2re = lk2.search(cont)
                if lk2re:
                    likes = lk2re.group(1).replace(',','')
                else:
                    likes = None

        except Exception,e:
            fpt = open('lastfailed-%s.html'%(unicode(e)),'w') ; fpt.write(cont) ; fpt.close()
            print 'just written last failure in lastfailed.txt'
            raise
    op = {'pagetp':pagetp,'tp':tp,'descr':descr,'likes':likes,'url':url,'nm':html_entity_decode(nm)};
    print '%s=%s'%(ts,op)
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
    elif sys.argv[1]=='output':
        fillq(output=True)
    elif sys.argv[1]=='inspect':
        fillq(inspect=True)
    elif sys.argv[1]=='scrape':
        procs={}
        cmds = ['./fbscrape.py','scrapeone']
        fillqargs = ['./fbscrape.py','fillq']
        procs['fillq'] = {'args':fillqargs,'proc':subprocess.Popen(fillqargs)}
        for i in range(1,20):
            print 'kicking off %s'%cmds
            procs[i]={'args':cmds,'proc':subprocess.Popen(cmds)}
        while True:
            for i in procs:
                pollres = procs[i]['proc'].poll()
                print '%s poll = %s'%(i,pollres)
                if type(pollres)==int:
                    procs[i]['proc']=subprocess.Popen(procs[i]['args'])
            time.sleep(5)
