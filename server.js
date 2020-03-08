
var ConsistentHashing = require('consistent-hashing');
var clientAvailable = new ConsistentHashing(['node1']);
clientList = new Map()
clientAvailable.removeNode('node1');

// function to start server on given port
function startServer(port) {
    const io = require("socket.io"),
    server = io.listen(port);

    /**
     * Events listener for server
     */

    // event fired every time a new client connects:
    server.on("connection", (socket) => {
        // console.log('New connection from ' + socket.request.connection.remoteAddress);
        clientAvailable.addNode(socket);
        clientList.set(socket.id, socket);
        console.log('client size: ', clientList.size)
        console.info(`Client connected [id=${socket.id}]`);
    
        // dummy event to receive message from client
        // socket.on("msg", (msg) => console.info(
        //     'message from '+ socket.id+" : "+msg
        // ));

        // when socket disconnects, remove it from the list:
        socket.on("disconnect", () => {
            clientAvailable.removeNode(socket);
            clientList.delete(socket.id);
            console.log('client size: ', clientList.size)
            console.info(`Client gone [id=${socket.id}]`);
        });
    });
}

startServer(8003);

setInterval(function() {
    if(clientList.size > 0) {
        var query = 'SELECT * from ABCD '+Math.random();
        var node = clientAvailable.getNode(query)
        node.emit('data',query);
    }
    },1000
);
