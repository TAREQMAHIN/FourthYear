
// to create unique random string
var crypto = require('crypto');


/***
 * Secondary Clients 
 * 
 * They store database.
 * Server requests them to perform operations on database.
 * Their structure is like a circular doubly linked list.
 *  
 * TableName is hashed and stored on appropriate node 
 * i.e. node-x stores data having hashes [node-x, node-y)
 * 
 * There is only one instance of a node in UniversalHashing 
 * to deal with left - right node chaining
 */ 

var ConsistentHashing = require('consistent-hashing');

var sClientAvailable = new ConsistentHashing(['node1']);
sClientList = new Map()
sClientAvailable.removeNode('node1');

/***
 * Primary Clients
 * 
 * They provide CRUD queries for database.
 * 
 */
var pClientList = new Map()


/***
 * Request
 * 
 * Stores information of (query, pclient, sclient)
 * On receiving a query from pclient, store it in request
 * along-with info of sclient.
 * 
 * When sClients returns a result, send it to pClient
 */
var queryPClient = new Map();
var querySClient = new Map();


var queueAddition = new Map();


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
        pClientList.set(socket.id, socket);
        console.info(`Client connected [id=${socket.id}]`);
    
        // primary client can opt to work as secondary client
        socket.on("changeClientType", (key) => {
            if(key == "12345678" && pClientList.has(socket.id)) {
                pClientList.delete(socket.id);
                sClientList.set(socket.id, socket);
                sClientAvailable.addNode(socket);

                socket.emit('clientTypeChange','success: '+socket.id);
            }
            else
                socket.emit('clientTypeChange','failed');

            console.log('No of Users : '+pClientList.size);
            console.log('No of Nodes : '+sClientList.size);
        });

        socket.on("initiateDataTransfer", ()=>
        {
            console.log("initiateDataTransfer");

            var left = sClientAvailable.getLeftNode(socket);

            if(left.id != socket.id)
            {
                socket.emit('updateNeighbour', JSON.stringify({socketId: left.id, direction: 'left', cause: 'addition', new: 'true'}));
                left.emit('updateNeighbour', JSON.stringify({socketId: socket.id, direction: 'right', cause: 'addition'}));
            }

            var right = sClientAvailable.getRightNode(socket);

            if(right.id != socket.id)
            {
                socket.emit('updateNeighbour', JSON.stringify({socketId: right.id, direction: 'right', cause: 'addition', new: 'true'}));
                right.emit('updateNeighbour', JSON.stringify({socketId: socket.id, direction: 'left', cause: 'addition'}));
            }
        })

        socket.on("itemsList", (p)=>
        {

            console.log("itemsList");

            var params = JSON.parse(p);

            console.log(params);

            var tablesToMove = [];

            params.tableNames.forEach((tableName)=>{
                if(sClientAvailable.getNode(tableName)==params.dest)
                    itemsToMove.push(tableName);
            })

            socket.emit('filteredItemsList', JSON.stringify({dest: params.dest, tableNames: tablesToMove}));
        })

        socket.on('filteredItems', (p)=>
        {
            var params = JSON.parse(p);

            console.log(`filteredItems: source: ${socket.id}, dest: ${params.dest}`);

            var destSocket = sClientList.get(params.dest);

            destSocket.emit('takeYourItems', JSON.stringify({
                source: socket.id,
                tableInfo: params.tableInfo,
                tableData: params.tableData
            }))
        })

        socket.on('passMyItems', (p)=>
        {
            var params = JSON.parse(p);

            console.log(`passMyItems: source: ${socket.id}, dest: ${params.dest}`);

            var destSocket = sClientList.get(params.dest);

            destSocket.emit('addMyItems', JSON.stringify({
                source: socket.id,
                tableInfo: params.tableInfo,
                tableData: params.tableData
            }))
        })

        /***
         * receive a query from primary client
         * 
         * send it to associated secondary client for processing
         * return the received result to primary client
         */ 
        socket.on("query", (query) => {
            console.log('Incoming query : '+ query.hash);
            console.log('Received from user : '+socket.request.connection.remoteAddress);

            let secondaryClient = sClientAvailable.getNode(query.table_name);
            console.log('Forwarded to node : '+secondaryClient.request.connection.remoteAddress);
            console.log('\n');
            // left and right nodes of main node.
            // var leftSecondaryCLient = ConsistentHashing.getLeftNode(secondaryClient);
            // var rightSecondaryCLient = ConsistentHashing.getRightNode(secondaryClient);
            queryPClient.set(query.hash,socket);
            querySClient.set(query.hash,socket);
            secondaryClient.emit('process',query,1);
        });

        /***
         * receive result of query from sClient
         * 
         * forward the result to concerned pClient
         * remove data from remaining query container
         * i.e. queryPClient and querySClient
         */
        socket.on("processed", (query, result) => {
            var sendToClient = queryPClient.get(query.hash);
            sendToClient.emit("result",query,result);
            queryPClient.delete(sendToClient);
            querySClient.delete(socket);
        });

        // when socket disconnects, remove it from the list:
        socket.on("disconnect", () => {
            if(pClientList.get(socket.id)) {
                pClientList.delete(socket.id);
            }
            else if(sClientList.get(socket.id)) {

                var left = sClientAvailable.getLeftNode(socket);
                var right = sClientAvailable.getRightNode(socket);

                left.emit('updateNeighbour', JSON.stringify({socketId: right.id, direction: 'right', cause: 'removal'}));
                right.emit('updateNeighbour', JSON.stringify({socketId: left.id, direction: 'left', cause: 'removal'}));

                sClientList.delete(socket.id);
                sClientAvailable.removeNode(socket);
            }
            console.info(`Client gone [id=${socket.id}]`);
        });
    });
}









// start the server at specified port
startServer(8003);
console.log("server started at http://127.0.0.1:8003");

// Dummy function to send query to clients
// Not required now since primary client sends query is implemented
// setInterval(function() {
//     if(sClientList.size > 0) {
//         var query = 'SELECT * from ABCD '+Math.random();
//         var node = sClientAvailable.getNode(query)
//         node.emit('data',query);
//     }
//     sClientList.forEach(element => {
//         console.log("Node ID: "+element.id);
//         console.log("Left Node: "+sClientAvailable.getLeftNode(element).id);
//         console.log("Right Node: "+sClientAvailable.getRightNode(element).id);
//         console.log("----------------------------------------------------------");
//     });
//     },5000
// );
