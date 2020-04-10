/**
 * Secondary Client
 * 
 * Receives queries from server and sends result after processing it
 */


// connect to server on given port
const
    io = require("socket.io-client"),
    ioClient = io.connect("http://localhost:8003");

// default connection is as primary client
// change to secondary client
// target the receiver with pre-defined password
ioClient.emit("changeClientType","12345678");

// TODO : priority_queue with priority considering Data Hazrads
var query_queue = new Array();

// function to get current date and time
function getDateTime() {
    var date = new Date().toJSON().slice(0,10);
    var time = new Date().toJSON().slice(11,19)
    return date+' '+time;
}
// Dummy name of client to distinguish at receiving client
var name = "secondary-client "+(Math.floor(Math.random()*100)).toString()
/**
 * 
 * @param query : query to be processed
 * 
 * TODO: Actual result after processing query
 * @returns : Dummy string showing processing time and name of processing client
 */
function process(query) {
    return query+' is processed at '+getDateTime()+' by '+name;
}

// function which processes query from query_queue and removes it from query_queue and sends the result
function process_query() {
    if(query_queue.length > 0) {
        var q = query_queue[0];
        console.log(q);
        query_queue.splice(0,1);
        var res = process(q);
        ioClient.emit('processed', q, res);
    }
}

/**
 * Receiver to listen for incoming query
 * 
 * @param query: query recived from server
 * 
 * Places the query on query_queue and calls process_query() to process the queue
 * NOTE : requires some better mechanism to handle asynochronity
 */
ioClient.on("process", (query) => {
    query_queue.push(query);
    process_query();
});