
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
var sClientList = new Map()
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

                socket.emit('clientTypeChange','success');
            }
            else {
                socket.emit('clientTypeChange','failed');
            }
            console.log('No of Users : '+pClientList.size);
            console.log('No of Nodes : '+sClientList.size);
        });

        /***
         * receive a query from primary client
         * 
         * send it to associated secondary client for processing
         * return the received result to primary client
         */ 
        socket.on("query", (query) => {
            // console.log('Incoming query : '+ query.hash);
            // console.log('Received from user : '+socket.request.connection.remoteAddress);

            let secondaryClient = sClientAvailable.getNode(query.table_name);
            queryPClient.set(query.hash,socket);
            querySClient.set(query.hash,socket);
            secondaryClient.emit('process',query,1);
            if(query.operation != 'R') {
                // left and right nodes of main node.
                let leftSecondaryCLient = sClientAvailable.getLeftNode(secondaryClient);
                let rightSecondaryCLient = sClientAvailable.getRightNode(secondaryClient);
                leftSecondaryCLient.emit('process',query,2);
                rightSecondaryCLient.emit('process',query,0);
            }
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
                let nodes = sClientAvailable.getNodes();
                for (let index = 0; index < nodes.length; index++) {
                    const element = nodes[index];
                    element.emit("stats");
                }
            }
            else if(sClientList.get(socket.id)) {
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
