#!/usr/bin/env python

#port forward redis somewhere
#ssh -R localhost:6380:localhost:6380 rhost

import os,datetime,time,subprocess,sys,redis,commands,re,urllib,json,htmlentitydefs,codecs

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
sourcefn = 'fb_pages.txt'
firstnline=None
def fillq(output=False,inspect=False,dump=False,fr=None,to=None):
    if dump:
        dfn = 'dump_%s-%s.txt'%(fr,to)
        dfp = None
        print 'dumping to %s'%dfn
    global firstnline
    fp = codecs.open(sourcefn,'r','utf-8')
    ex=0 ; added=0 ; deled=0 ; counter=0 ; dumped=0 ; skipped=0
    ints = rd.scard('toscrape')
    print 'got %s in toscrape queue (output=%s,inspect=%s)'%(ints,output,inspect)
    while True:
        ln = fp.readline()
        counter+=1
        if firstnline and counter<firstnline:
            if counter % 100000 ==0: print 'cntskip (%s<%s)'%(counter,firstnline)
            continue
        if (fr and counter<fr):
            if counter % 100000 ==0 : print 'cntskip2 %s (%s-%s)'%(counter,fr,to)
            continue
        if  to and counter>=to:
            print 'cntskip3 %s (%s-%s)'%(counter,fr,to)
            break
        #print ln
        if not ln:
            print ('done file, breaking')
            break
        ln = ln.strip('\r\n\t ')
        g = rd.get(ln)
        if g:
            if output or inspect or dump:
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
                if inspect and counter % 100 ==0: print '%s cnt'%counter

                if dump and counter>=fr and counter<to:
                    dstr = ln+';;;;'+json.dumps(dt)+'\n'
                    if not dfp:
                        print 'opening dfp for writing %s'%dfn
                        dfp = codecs.open(dfn,'w','utf-8')
                    dfp.write(dstr)
                    dumped+=1
                    rd.delete(ln)
                    if dumped % 1000==0: print 'dumped %s'%dumped
                elif dump and counter>=to:
                    print 'done dumping'
                    break
            ex+=1
            if ex % 10000 ==0: print '%s ex (cnt=%s)'%(ex,counter)
        elif not inspect and not output and not dump:
            firstnline = counter
            print 'have no key %s (added=%s;ints=%s)'%(ln,added,ints)
            if (added+ints)<10000:
                print 'k+ %s'%ln
                ra = rd.sadd('toscrape',ln.strip('\r\n\t '))
                if ra:
                    added+=1
                    if added % 100 ==0: print 'added %s'%added
            else:
                break
                print 'k='
        else:
            #print 'no filling queue with %s'%ln
            skipped+=1
            if skipped % 100000==0: print 'skip %s'%skipped
            #break
    print '%s keys exist, %s added, toscrape is %s long, %s deled, %s dumped, skipped=%s'%(ex,added,rd.scard('toscrape'),deled,dumped,skipped)
    if dump and dfp:
        print 'closing dump file'
        dfp.close()
    
#span class=\"subtitle fsm fcg\">Interest\u003c\/span>
catrg = re.compile('span class=(.{1,4})"subtitle fsm fcg(.{1,4})">(.*)\/span');
#span class=\"uiNumberGiant fsxxl fwb\">3\u003c\/span>
likesrg = re.compile('span class=(.{1,4})"(placePageStatsNumber|uiNumberGiant fsxxl fwb)(.{1,4})">(.{1,25})\/span');
#dt>About:\u003c\/dt>\u003cdd>Teknologia, bideo jokoak, informatika, internte..... hau dena eta askoz gehiago &#64; Bilduan. larunbatero 09:30etan Euskadi Irratian, eta Noiznahi interneten.\u003c\/dd
descrg = re.compile('dt>About:(.{0,22})>(.*)/dd');
lk2 = re.compile('([0-9\,]+) (People Like This|Person Likes This)');

def restore(fr,to):
    fn = 'dump_%s-%s.txt'%(fr,to)
    fp = codecs.open(fn,'r','utf-8')
    rest=0
    while True:
        ln = fp.readline()
        if not ln:
            print 'eof'
            break
        ln = ln.strip('\n\t ')
        nm,url,js = ln.split(';;;;')
        tk = nm+';;;;'+url
        g = rd.get(tk)
        #if g and json.loads(js)!=json.loads(g): raise Exception('data differs for %s. abort\n%s\n%s'%(tk,js,g))
        rd.set(tk,js)
        rest+=1
        if rest % 1000  ==0: print 'rest %s'%rest
    print 'done %s'%rest
    fp.close()
    
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
                if not likesre:
                    likes=None
                else:
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
dumpre = re.compile('^(dump|restore|dumpall|scrape|scrapeone|fillqloop|fillq)\[(\d+)\:(\d+)(,(\d+)|)\]$')
if len(sys.argv)>1:
    dumpres = dumpre.search(sys.argv[1])
    if dumpres:
        if dumpres.group(5):
            incr = int(dumpres.group(5))
        else:
            incr=100000

    if sys.argv[1]=='fillq'  or (dumpres and dumpres.group(1)=='fillq'):
        if dumpres:
            fr=int(dumpres.group(2))
            to=int(dumpres.group(3))
        else:
            fr=None
            to=None

        fillq(fr=fr,to=to)
    elif sys.argv[1]=='status':
        print datetime.datetime.now()
        print '%s keys'%rd.info()['db0']['keys']
        print 'got %s in queue'%(rd.scard('toscrape'))
    elif sys.argv[1]=='fillqloop' or (dumpres and dumpres.group(1)=='fillqloop'):
        if dumpres:
            fr=int(dumpres.group(2))
            to=int(dumpres.group(3))
        else:
            fr=None
            to=None
                   
        while True:
            fillq(fr=fr,to=to)
            print 'fillq run done, sleeping'
            time.sleep(60)
    elif sys.argv[1]=='testscrape':
        st,op = commands.getstatusoutput('ls *.html')
        for fn in op.split('\n'):
            scrapeone(fn)
    elif sys.argv[1]=='scrapeone' or (dumpres and dumpres.group(1)=='scrapeone'):
        while scrapeone():
            pass
        print 'done'
    elif sys.argv[1]=='output':
        fillq(output=True)
    elif sys.argv[1]=='inspect':
        fillq(inspect=True)
    elif dumpres and dumpres.group(1)=='dump':
        fillq(dump=True,fr=int(dumpres.group(2)),to=int(dumpres.group(3)))
    elif dumpres and dumpres.group(1)=='dumpall':
        cur = int(dumpres.group(2))
        while cur<int(dumpres.group(3)):
            print 'dumping %s - %s'%(cur,cur+incr)
            fillq(dump=True,fr=cur,to=cur+incr)
            cur+=incr
    elif dumpres and dumpres.group(1)=='restore':
        restore(fr=int(dumpres.group(2)),to=int(dumpres.group(3)))
        
    elif sys.argv[1]=='scrape' or (dumpres and dumpres.group(1)=='scrape'):
        procs={}
        if dumpres:
            fr= int(dumpres.group(2))
            to = int(dumpres.group(3))
            cmds = ['./fbscrape.py','scrapeone[%s:%s]'%(fr,to)]
            fillqargs = ['./fbscrape.py','fillqloop[%s:%s]'%(fr,to)]
        else:
            fr = None
            to = None
            cmds = ['./fbscrape.py','scrapeone']
            fillqargs = ['./fbscrape.py','fillqloop']
        if os.path.exists(sourcefn): procs['fillq'] = {'args':fillqargs,'proc':subprocess.Popen(fillqargs)}
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
