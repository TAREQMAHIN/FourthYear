const
    io = require("socket.io-client"),
    ioClient = io.connect("http://localhost:8003");

ioClient.emit("changeClientType","12345678");

// TODO : priority_queue with priority considering Data Hazrads
var query_queue = new Array();

function process_query() {
    if(query_queue.length > 0) {
        var q = query_queue[0];
        console.log(q);
        query_queue.splice(0,1);
        var res = 'done '+q;
        ioClient.emit('processed', q, res);
    }
}

ioClient.on("process", (query) => {
    query_queue.push(query);
    process_query();
});