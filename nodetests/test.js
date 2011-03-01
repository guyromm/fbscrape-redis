var request = require('request'),
_ = require('underscore'),
jsdom = require('jsdom'),
sys = require('sys'),
fs = require('fs')
redis = require('redis');

rd = redis.createClient();

var fname = 'fb_pages_sample.txt';
var st = fs.statSync(fname);
var fd = fs.openSync(fname,'r');
var cnt=0;
var buf='';

if (process.argv[2]=='delq') 
{
    var dcnt=0;
    rd.del('toscrape',function() { process.exit(); });
    console.log('sent del cmnds');
}
else if (process.argv[2]=='statq')
{
    m = rd.multi();
    m.scard('toscrape');
    m.exec(function(err,replies) {
	console.log(replies);
	process.exit();
    });
}
else if (process.argv[2]=='noop')
{
    console.log('just done');
    process.exit();
}
else if (process.argv[2]=='fillq')
{
    fillq();
}
else if (process.argv[2]=='scrape')
{
    checkscrape();

}   
function fillq() {
    fs.open(fname,'r',0666,function(err,fd) {
	doread();
    });
    process.on('exit',function() {
	console.log('finally got row - ',cnt);
    });

}
function checkscrape() {
    console.log('checkscrape()');
    rd.scard('toscrape',function(err,repl) {
	console.log(repl,'TOSCRAPE');
	if (repl>=30) var mx = 30;
	else var mx = repl;

	if (repl>0)
	{
	    for (var i=0; i<mx; i ++)
	    {
		doscrape();
	    }

	    setTimeout(checkscrape,50000);
	}
	else
	{
	    process.exit();
	    /*console.log('filling queue');
	    fillq();*/
	}
    });
}
//span class=\"subtitle fsm fcg\">Interest\u003c\/span>
var catrg = new RegExp('span class=(.{1,4})"subtitle fsm fcg(.{1,4})">(.*)\/span');
//span class=\"uiNumberGiant fsxxl fwb\">3\u003c\/span>
var likesrg = new RegExp('span class=(.{1,4})"(placePageStatsNumber|uiNumberGiant fsxxl fwb)(.{1,4})">(.*)\/span');
//dt>About:\u003c\/dt>\u003cdd>Teknologia, bideo jokoak, informatika, internte..... hau dena eta askoz gehiago &#64; Bilduan. larunbatero 09:30etan Euskadi Irratian, eta Noiznahi interneten.\u003c\/dd
var descrg = new RegExp('dt>About:(.{0,22})>(.*)/dd');

var lk2 = new RegExp('([0-9\,]+) (People Like This|Person Likes This)');

function doscrape() { 

    rd.spop('toscrape',function(err,reply) {
	//console.log(err,reply);
	if (err) throw "toscrape spop returned "+err;
	if (!reply) 
	{
	    console.log('empty spop!');
	    return;
	}
	var spl = reply.split(';;;;');
	var nm = spl[0] ; var url = 'http://www.facebook.com/'+spl[1];
	console.log('collecting',url);
	var ua ='Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)';
	var reqopts = {uri:url,headers:{'User-Agent':ua}};
	reqopts['maxSockets']=50;
	request(reqopts, _.bind(function (error, response, body) {
	    if (!error && response.statusCode == 200) {
		console.log(this.url,'got 200 and a response of ',body.length);
		var tpe = catrg.exec(body);
		//console.log(tpe);
		if (tpe)
		{
		    var pagetp = 'reg';
		    var tp = tpe[3].slice(0,-7);
		    console.log('type',tp);
		    var descr=null;
		    var likes = likesrg.exec(body)[4].slice(0,-7);
		    console.log('likes',likes);
		}
		else
		{
		    var pagetp = 'group';
		    var descres = descrg.exec(body);
		    if (descres)
			var descr = descres[2].slice(0,-7);
		    else
			var descr='NODESCR';
		    var tp=null;
		    console.log('descr',descr);
		    var likes = parseInt(lk2.exec(body)[1].replace(',',''));
		    console.log('likes',likes);
		}
		var op = {pagetp:pagetp,tp:tp,descr:descr,likes:likes,url:this.url,nm:this.nm};
		console.log(op);
		rd.set(reply,JSON.stringify(op),function(err,reply) {
		    if (reply!='OK') throw "rd.set went bad";
		});
	    }
	},{url:url,nm:nm}));	
    });
}
function doread() {
    fs.read(fd,1000,null,'utf8',function(err,str,count) {
	//fs.close(fd);
	if (!str.length)
	{
	    console.log('looks like eof');
	    process.exit();
	    return;
	}
	var lines = str.split('\n');
	lines[0]=buf+lines[0];
	//console.log('lines[0]=',lines[0]);
	for (var i =0;i<lines.length-1;i++)
	{
	    var ln = lines[i];
	    //console.log('got line',i,ln);

	    rd.get(ln,_.bind(function(err,reply) {
		if (!reply) 
		{
		    //console.log('could not find',this.ln);
		    rd.scard('toscrape',_.bind(function(err,reply) {
			//console.log('so i got scard with ln in mind',this.ln);
			if (parseInt(reply)<1000 || reply==null)
			{

			    rd.sadd('toscrape',this.ln,_.bind(function(err,reply) {
				//console.log('multi.exec()',err,reply);
				if (reply!=1) throw "looks like i failed to add"+this.ln+":"+reply;
			    },_.clone({ln:this.ln})));
			}
			else
			{
			    console.log('queuelim',err,reply,this.ln);
			    if (process.argv[2]=='scrape')
			    {
				checkscrape();
			    }
			    else
				process.exit();
			}
		    },_.clone({ln:this.ln})));
		}
		else
		{
		    console.log('+key',this.ln);
		}
	    },_.clone({ln:ln})))
	}
	buf=lines[lines.length-1];
	//console.log('buf=',buf);
	//console.log('got ',lines.length,'lines');
	//sys.puts(str);
	cnt++;
	doread();
    });
}



