const
    io = require("socket.io-client"),
    ioClient = io.connect("http://localhost:8003");

function query(query_msg) {
  ioClient.emit("query",query_msg);
}

ioClient.on('result', (query, result) => {
  console.log('result of query \" '+query+' \" is : '+result);
});

var count = 1;
setInterval(function() {
    query("query "+count);
    count = count + 1;
  },1000
);