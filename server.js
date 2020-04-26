
// to create unique random string
const crypto = require('crypto');

// server.io module
const io = require("socket.io")();


const QueryResponse = require('./query-response.js');

var tables = new Map();

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
var sClientAvailable = new ConsistentHashing();

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
 * Stores information of (query_hash, user, initial[as per consistent has], last[last attempted node], query)
 * On receiving a query from pclient, store it in request
 * along-with info of sclient.
 * 
 * When sClients returns a result, send it to pClient
 */
var pendingQuery = new Map();



// function to start server on given port
function startServer(port) {
    const server = io.listen(port);

    // middleware to configure connection as user or node
    io.use((socket, next) => {
        let password = socket.handshake.headers['password'];
        if (password == "pass12345678") {
            sClientList.set(socket.handshake.headers['clientid'], socket);
            // add the new to consistent hash ring
            sClientAvailable.addNode(socket.handshake.headers['clientid']);
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

        socket.emit('getTablesList');

        socket.on('tablesList', (tableInfo) => {
            let tablesList = JSON.parse(tableInfo);
            for (const key in tablesList) {
                tables.set(tablesList[key].name,1);
            }
        });

        // console.log('New connection from ' + socket.request.connection.remoteAddress);
        console.log(`Client connected [id=${socket.id}]`);
        console.log('No of Users : ' + pClientList.size);
        console.log('No of Nodes : ' + sClientList.size);

        // if new connection is done by a node, distribute the data to keep 3-node structure consistent
        if (sClientList.get(clientid)) {
            // temporary function to make node shows their storage and processing details when connection is established
            socket.emit('clientTypeChange', 'success');

            console.log("Synchronization of data(add) started at: " + Date.now());
            // left : clientid of left node according to consistent hash ring
            // right : clientid of right node according to consistent hash ring
            let left = sClientList.get(sClientAvailable.getLeftNode(clientid)).handshake.headers['clientid'];
            let right = sClientList.get(sClientAvailable.getRightNode(clientid)).handshake.headers['clientid'];

            sClientList.get(left).emit('updateNeighbour', JSON.stringify({
                socketId: right,
                direction: 'right', cause: 'addition'
            }));

            sClientList.get(right).emit('updateNeighbour', JSON.stringify({
                socketId: left,
                direction: 'left', cause: 'addition'
            }));
            console.log("Synchronization of data(add) finished at: " + Date.now());

        }


        socket.on("initiateDataTransfer", () => {
            // console.log("initiateDataTransfer");

            let left = sClientList.get(sClientAvailable.getLeftNode(clientid)).handshake.headers['clientid'];
            let right = sClientList.get(sClientAvailable.getRightNode(clientid)).handshake.headers['clientid'];

            socket.emit('updateNeighbour', JSON.stringify({ socketId: left, direction: 'left', cause: 'addition', new: 'true' }));
            sClientList.get(left).emit('updateNeighbour', JSON.stringify({ socketId: clientid, direction: 'right', cause: 'addition' }));
            

            socket.emit('updateNeighbour', JSON.stringify({ socketId: right, direction: 'right', cause: 'addition', new: 'true' }));
            sClientList.get(right).emit('updateNeighbour', JSON.stringify({ socketId: clientid, direction: 'left', cause: 'addition' }));
            
        })

        socket.on("itemsList", (p) => {
            // console.log("itemsList");
            let params = JSON.parse(p);
            // console.log(params);

            let tablesToMove = [];

            params.tableNames.forEach((tableName) => {
                if (sClientAvailable.getNode(tableName) == params.dest)
                    itemsToMove.push(tableName);
            })

            socket.emit('filteredItemsList', JSON.stringify({ dest: params.dest, tableNames: tablesToMove }));
        })

        socket.on('filteredItems', (p) => {
            let params = JSON.parse(p);
            // console.log(`filteredItems: source: ${clientid}, dest: ${params.dest}`);
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
            console.log(tables);
            // console.log('Incoming query : '+ query.hash);
            // console.log('Received from user : '+socket.request.connection.remoteAddress);

            // add table to bloom filter and continue to process
            if(query.operation == 'C') {
                tables.set(query.table_name, 1);
            }
            else {
                // if table doesn't exist, send response to client without processing
                if(tables.get(query.table_name) === undefined) {
                    let response = new QueryResponse(query.hash, 200, "Table doesn't exist", null, null);
                    socket.emit('result', response);
                    return;
                }
            }

            let secondaryClient = sClientAvailable.getNode(query.table_name);

            // store the mapping of query and user and associated node
            let temp_query = new Map();
            temp_query.set('user', socket.id);
            temp_query.set('initial', secondaryClient);
            temp_query.set('last', secondaryClient);
            temp_query.set('query', query);

            pendingQuery.set(query.hash, temp_query);
            sClientList.get(secondaryClient).emit('process', query, 1);
        });

        /***
         * receive result of query from sClient
         * 
         * forward the result to concerned pClient
         * remove data from remaining query container
         */
        socket.on("processed", (result) => {
            // console.log("result");
            let response = JSON.parse(result);

            let queryid = pendingQuery.get(response.query_hash);
            if (queryid !== undefined) {
                // failed or table not found
                if (response.status == 400) {
                    // currently only looking at fail status of node storing data (and not neighbours)
                    if(response.block == 1) {
                        let next_probe_node = sClientAvailable.getLeftNode(queryid.get('last'));
                        // cycle completed. No need to probe anymore
                        if (next_probe_node == queryid.get('initial')) {
                            pClientList.get(queryid.get('user')).emit("result", result);
                            pendingQuery.delete(response.query_hash);
                        }

                        // update last attempted node and probe
                        pendingQuery.get(response.query_hash).set('last',next_probe_node);
                        sClientList.get(next_probe_node).emit('process', queryid.get('query'), 1);
                        
                    }
                }
                
                // target node found
                else {
                    // update left and right node for OLTP
                    if (response.block == 1 && queryid.get('query').operation != 'R') {
                        pClientList.get(queryid.get('user')).emit("result", result);
                        pendingQuery.delete(response.query_hash);

                        let tquery = queryid.get('query');
                        let left_node = sClientAvailable.getLeftNode(clientid);
                        sClientList.get(left_node).emit('process', tquery, 2);

                        let right_node = sClientAvailable.getRightNode(clientid);
                        sClientList.get(right_node).emit('process', tquery, 0);                    
                    }
                }
            }
        });

        // when socket disconnects, remove it from the list:
        socket.on("disconnect", () => {
            if (pClientList.get(socket.id)) {
                pClientList.delete(socket.id);
            }
            else if (sClientList.get(clientid)) {
                console.log("Synchronization of data(remove) started at: " + Date.now());

                let left = sClientAvailable.getLeftNode(clientid);
                let right = sClientAvailable.getRightNode(clientid);

                sClientList.get(left).emit('updateNeighbour', JSON.stringify({ socketId: right, direction: 'right', cause: 'removal' }));
                sClientList.get(right).emit('updateNeighbour', JSON.stringify({ socketId: left, direction: 'left', cause: 'removal' }));

                console.log("Synchronization of data(remove) finished at: " + Date.now());

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
