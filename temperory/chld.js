const cluster = require('cluster')
var query_queue = new Array();
var pro = require('process');

function process(q){

	if(q==0){
		if(cluster.isMaster){

		 console.log("oltp\n");
		}
	}
	else{
		 if(cluster.isMaster){
		 	cluster.fork();
		 }
		 else
		 {
		 	setTimeout(function(){console.log("olap\n");
		 pro.kill(pro.pid, 'SIGINT');},1000);
		 	
		 }
	}
}


function process_query(q) {
	query_queue.push(q);
    if (query_queue.length > 0) {
        var q = query_queue[0];
        query_queue.splice(0, 1);
        process(q);
    }
}
 
process_query(0);
process_query(0);
process_query(0);
process_query(1);
process_query(1);
process_query(1);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);
process_query(0);

