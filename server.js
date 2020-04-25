
// to create unique random string
var crypto = require('crypto');

// server.io module
const io = require("socket.io")();


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

// collection of all nodes arranged in ring by consistent hashing (stores {node_id,key} internally)
var sClientAvailable = new ConsistentHashing(['node1']);
sClientAvailable.removeNode('node1');

// mapping from node_id to socket
var sClientList = new Map()



/***
 * Primary Clients
 * 
 * They provide CRUD queries for database.
 * 
 */
// stores the mapping from socket.id to socket of user
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
    const server = io.listen(port);

    // middleware to configure connection as user or node
    io.use((socket, next) => {
        let password = socket.handshake.headers['password'];
        if (password == "pass12345678") {
            sClientList.set(socket.handshake.headers['clientid'], socket);
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
        // clientid is a valid key in consistent hash ring for a node, and undefined for user
        let clientid = socket.handshake.headers['clientid'];

        // console.log('New connection from ' + socket.request.connection.remoteAddress);
        console.info(`Client connected [id=${socket.id}]`);
        console.log('No of Users : ' + pClientList.size);
        console.log('No of Nodes : ' + sClientList.size);

        // if new connection is done by a node, distribute the data to keep 3-node structure consistent
        if (sClientList.get(clientid)) {
            // add the new to consistent hash ring
            sClientAvailable.addNode(clientid);
            // temporary function to make node shows their storage and processing details when connection is established
            socket.emit('clientTypeChange', 'success');

            // left : clientid of left node according to consistent hash ring
            // right : clientid of right node according to consistent hash ring
            let left = sClientList.get(sClientAvailable.getLeftNode(clientid)).handshake.headers['clientid'];
            let right = sClientList.get(sClientAvailable.getRightNode(clientid)).handshake.headers['clientid'];

            if (left != clientid) {
                sClientList.get(left).emit('updateNeighbour', JSON.stringify({
                    socketId: right,
                    direction: 'right', cause: 'addition'
                }));
            }

            if (right != clientid && right != left) {
                sClientList.get(right).emit('updateNeighbour', JSON.stringify({
                    socketId: left,
                    direction: 'left', cause: 'addition'
                }));
            }

        }


        socket.on("initiateDataTransfer", () => {
            console.log("initiateDataTransfer");

            let left = sClientList.get(sClientAvailable.getLeftNode(clientid)).handshake.headers['clientid'];
            let right = sClientList.get(sClientAvailable.getRightNode(clientid)).handshake.headers['clientid'];

            if (left != clientid) {
                socket.emit('updateNeighbour', JSON.stringify({ socketId: left, direction: 'left', cause: 'addition', new: 'true' }));
                sClientList.get(left).emit('updateNeighbour', JSON.stringify({ socketId: clientid, direction: 'right', cause: 'addition' }));
            }

            if (right != clientid) {
                socket.emit('updateNeighbour', JSON.stringify({ socketId: right, direction: 'right', cause: 'addition', new: 'true' }));
                sClientList.get(right).emit('updateNeighbour', JSON.stringify({ socketId: clientid, direction: 'left', cause: 'addition' }));
            }
        })

        socket.on("itemsList", (p) => {
            console.log("itemsList");
            let params = JSON.parse(p);
            console.log(params);

            let tablesToMove = [];

            params.tableNames.forEach((tableName) => {
                if (sClientAvailable.getNode(tableName) == params.dest)
                    itemsToMove.push(tableName);
            })

            socket.emit('filteredItemsList', JSON.stringify({ dest: params.dest, tableNames: tablesToMove }));
        })

        socket.on('filteredItems', (p) => {
            let params = JSON.parse(p);
            console.log(`filteredItems: source: ${clientid}, dest: ${params.dest}`);
            let destSocket = sClientList.get(params.dest);

            destSocket.emit('takeYourItems', JSON.stringify({
                source: clientid,
                tableInfo: params.tableInfo,
                tableData: params.tableData
            }))
        })

        socket.on('passMyItems', (p) => {
            let params = JSON.parse(p);
            console.log(`passMyItems: source: ${clientid}, dest: ${params.dest}`);
            let destSocket = sClientList.get(params.dest);

            destSocket.emit('addMyItems', JSON.stringify({
                source: clientid,
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

            let secondaryClientId = sClientAvailable.getNode(query.table_name);
            let secondaryClient = sClientList.get(secondaryClientId);

            // store the mapping of query and user and associated node
            queryPClient.set(query.hash, socket.id);
            querySClient.set(query.hash, secondaryClientId);
            secondaryClient.emit('process', query, 1);

            // process OLAP queries on neighbouring nodes, to keep them consistent with target node
            // reduces the chance of loss of data till failure of at max two consecutive node
            // NOTE : These queries can be queued and executed later to balance work-load
            if (query.operation != 'R') {
                // left and right nodes of main node.
                let leftSecondaryCLient = sClientList.get(sClientAvailable.getLeftNode(secondaryClientId));
                let rightSecondaryCLient = sClientList.get(sClientAvailable.getRightNode(secondaryClientId));
                leftSecondaryCLient.emit('process', query, 2);
                rightSecondaryCLient.emit('process', query, 0);
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
            let userid = queryPClient.get(query.hash);
            if(userid !== undefined) {
                pClientList.get(userid).emit("result", query, result);
                queryPClient.delete(query.hash);
                querySClient.delete(query.hash);
            }
        });

        // when socket disconnects, remove it from the list:
        socket.on("disconnect", () => {
            if (pClientList.get(socket.id)) {
                pClientList.delete(socket.id);
            }
            else if (sClientList.get(clientid)) {
                if (sClientList.size > 1) {
                    let left = sClientAvailable.getLeftNode(clientid);
                    let right = sClientAvailable.getRightNode(clientid);

                    sClientList.get(left).emit('updateNeighbour', JSON.stringify({ socketId: right, direction: 'right', cause: 'removal' }));
                    if (left != right) sClientList.get(right).emit('updateNeighbour', JSON.stringify({ socketId: left, direction: 'left', cause: 'removal' }));
                }

                sClientList.delete(clientid);
                sClientAvailable.removeNode(clientid);
            }
            console.info(`Client gone [id=${socket.id}]`);
        });
    });
}

// start the server at specified port
startServer(8003);
console.log("server started at http://127.0.0.1:8003");
