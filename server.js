
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
// consistent hash storage of node_id
var sClientAvailable = new ConsistentHashing(['node1']);
// mapping from node_id to socket
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
    const io = require("socket.io")(),
    server = io.listen(port);

    // middleware to configure connection as user or node
    io.use((socket, next) => {
        let token = socket.handshake.query.token;
        if(token == "connect_as_node") {
            sClientList.set(socket.id, socket);
            let id = socket.handshake.query.id;
            console.log(id);
        }
        else {
            pClientList.set(socket.id, socket);
        }
        return next();
    });

    /**
     * Events listener for server
     */

    // event fired every time a new client connects:
    server.on("connection", (socket) => {
        // console.log('New connection from ' + socket.request.connection.remoteAddress);
        console.info(`Client connected [id=${socket.id}]`);
    
        if(sClientList.get(socket.id)) {
            sClientAvailable.addNode(socket);
            socket.emit('clientTypeChange','success');

            let left = sClientAvailable.getLeftNode(socket);
            let right = sClientAvailable.getRightNode(socket);
            if(left != socket)
            left.emit('updateNeighbour', JSON.stringify({socketId: right.id, direction: 'right', cause: 'addition'}));
            if(right != socket && right != left)
            right.emit('updateNeighbour', JSON.stringify({socketId: left.id, direction: 'left', cause: 'addition'}));

        }
        else {
            socket.emit('clientTypeChange','failed');
        }
        console.log('No of Users : '+pClientList.size);
        console.log('No of Nodes : '+sClientList.size);

        socket.on("initiateDataTransfer", ()=>
        {
            console.log("initiateDataTransfer");

            let left = sClientAvailable.getLeftNode(socket);

            if(left.id != socket.id)
            {
                socket.emit('updateNeighbour', JSON.stringify({socketId: left.id, direction: 'left', cause: 'addition', new: 'true'}));
                left.emit('updateNeighbour', JSON.stringify({socketId: socket.id, direction: 'right', cause: 'addition'}));
            }

            let right = sClientAvailable.getRightNode(socket);

            if(right.id != socket.id)
            {
                socket.emit('updateNeighbour', JSON.stringify({socketId: right.id, direction: 'right', cause: 'addition', new: 'true'}));
                right.emit('updateNeighbour', JSON.stringify({socketId: socket.id, direction: 'left', cause: 'addition'}));
            }
        })

        socket.on("itemsList", (p)=>
        {

            console.log("itemsList");

            let params = JSON.parse(p);

            console.log(params);

            let tablesToMove = [];

            params.tableNames.forEach((tableName)=>{
                if(sClientAvailable.getNode(tableName)==params.dest)
                    itemsToMove.push(tableName);
            })

            socket.emit('filteredItemsList', JSON.stringify({dest: params.dest, tableNames: tablesToMove}));
        })

        socket.on('filteredItems', (p)=>
        {
            let params = JSON.parse(p);

            console.log(`filteredItems: source: ${socket.id}, dest: ${params.dest}`);

            let destSocket = sClientList.get(params.dest);

            destSocket.emit('takeYourItems', JSON.stringify({
                source: socket.id,
                tableInfo: params.tableInfo,
                tableData: params.tableData
            }))
        })

        socket.on('passMyItems', (p)=>
        {
            let params = JSON.parse(p);

            console.log(`passMyItems: source: ${socket.id}, dest: ${params.dest}`);

            let destSocket = sClientList.get(params.dest);

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
            let sendToClient = queryPClient.get(query.hash);
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

                if(sClientList.size > 1) {
                    let left = sClientAvailable.getLeftNode(socket);
                    let right = sClientAvailable.getRightNode(socket);

                    left.emit('updateNeighbour', JSON.stringify({socketId: right.id, direction: 'right', cause: 'removal'}));
                    if(left != right) right.emit('updateNeighbour', JSON.stringify({socketId: left.id, direction: 'left', cause: 'removal'}));
                }

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
