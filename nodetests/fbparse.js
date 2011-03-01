var request = require('request'),
_ = require('underscore'),
jsdom = require('jsdom'),
sys = require('sys'),
fs = require('fs')
redis = require('redis');

var cont = fs.readFileSync(process.argv[2]);
//\u003cspan class=\"subtitle fsm fcg\">Interest\u003c\/span>
var rg = new RegExp('span class=(.{1,4})"subtitle fsm fcg(.{1,4})">(.*)\/span');
var lk2 = new RegExp('([0-9\,]+) People Like This');

var tpe = rg.exec(cont);
if (tpe)
{
    var tp = tpe[3].slice(0,-7);
    console.log('type',tp);

    //span class=\"uiNumberGiant fsxxl fwb\">3\u003c\/span>
    var rg = new RegExp('span class=(.{1,4})"(placePageStatsNumber|uiNumberGiant fsxxl fwb)(.{1,4})">(.*)\/span');
    var rge = rg.exec(cont);
    //console.log(rge);
    var likes = rge[4].slice(0,-7);
    console.log('likes',likes);

}
else
{
    //dt>About:\u003c\/dt>\u003cdd>Teknologia, bideo jokoak, informatika, internte..... hau dena eta askoz gehiago &#64; Bilduan. larunbatero 09:30etan Euskadi Irratian, eta Noiznahi interneten.\u003c\/dd
    var ab = new RegExp('dt>About:(.{0,22})>(.*)/dd');
    var descr = ab.exec(cont)[2].slice(0,-7);
    console.log('descr',descr);

    var likes = parseInt(lk2.exec(cont)[1].replace(',',''));
    console.log('likes',likes);
}

//console.log(cont.length);
//var window = jsdom.jsdom(cont).createWindow();
/*jsdom.env(cont.toString(), ['/home/milez/Desktop/fbscrape/jquery.js'], function (errors,window) {
    var $ = window.$;
    //console.log(errors);
    console.log(window.document.getElementsByClassName('subtitle')._element.span);
});*/
    //var tags = window.document.getElementsByClassName('subtitle');
//console.log('tags',tags);