/**
 * Primary Client
 * 
 * Sends queries to server and displays the received result
 */

// connect to server on given port
const
    io = require("socket.io-client"),
    ioClient = io.connect("http://localhost:8003");

/**
 * function to send a query
 *  
 * @param query_msg : query to be processed at server
 * 
 */ 
function query(query_msg) {
  ioClient.emit("query",query_msg);
}

/**
 * Receiver to listen for result for sent query
 *  
 * @param query : query sent to server
 * @param result : result of query
 * 
 */ 
ioClient.on('result', (query, result) => {
  console.log('result of query \" '+query+' \" is : '+result);
});

// dummy implementation to send query to server at interval
var count = 1;
setInterval(function() {
    query("query "+count);
    count = count + 1;
  },1000
);