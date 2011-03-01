var httpAgent = require('http-agent'),
jsdom = require('jsdom'),
sys = require('sys');

var agent = httpAgent.create('www.google.com', ['finance', 'news', 'images']);

agent.addListener('next', function (err, agent) {
    console.log('about to create body'); return;
    var window = jsdom.jsdom(agent.body).createWindow();
    
    jsdom.jQueryify(window, 'path/to/jquery.js', function (window, jquery) {
    // jQuery is now loaded on the jsdom window created from 'agent.body'
	jquery('.someClass').each(function () { /* Your Custom Logic */ });

	agent.next();
    });
});

agent.addListener('stop', function (agent) {
    sys.puts('the agent has stopped');
});

agent.start();
