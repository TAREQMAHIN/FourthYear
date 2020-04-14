/*
this entire code works for a single olap session which is determined by the traffic of olap queries
As and when the olap_queue becomes empty , the child process dies and a new olap session is created.
*/

var olap_queue = require('secondary-client').olap_queue;
var ioClient = require('secondary-client').ioClient;

//this receives the current data from parent to work on
process.on('message', (msg) => {
	//copy this object and use it for this query
 console.log('current data snapshot received', msg);
});

//this processes the current query in a sequential manner
while(olap_queue.length!=0){

	//this must be synchronous
	//service the current query i.e call eval function

	ioClient.emit('processed', query, res);
	olap_queue.splice(0,1);
}