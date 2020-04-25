/**
 * Secondary Client
 * 
 * Receives queries from server and sends result after processing it
 */




// to store table information
const table = require('./table.js');

const sys_process = require('process');

const time = require('./time.js');

const node_id = 'node_'+sys_process.pid;
console.log("Node Id : "+node_id);

// connect to server on given port
const
    io = require("socket.io-client"),
    // var URL = encodeURIComponent()
    ioClient = io.connect("http://localhost:8003",{
        transportOptions: {
            polling: {
                extraHeaders: {
                    'clientid' : node_id,
                    'password' : "pass12345678",
                }
            }
        }
    });

const { fork } = require('child_process');


// default connection is as primary client
// change to secondary client
// target the receiver with pre-defined password
// ioClient.emit("changeClientType", "12345678");

// Log to show client type change
ioClient.on('clientTypeChange', (msg) => {
    print_stat();
    // console.log('Client Type Change to Secondary-Client: ' + msg);
});

// TODO : priority_queue with priority considering Data Hazrads
var query_queue = new Array();
var olap_queue = new Array();
var dict = new Array();


// information of tables on itself and associated left and right node
var table_record = new Array(3);
table_record[0]  = new Map(); // left node
table_record[2]  = new Map(); // right node
table_record[1] = new Map(); // self

// data storage of tables
var table_data = new Array(3);
table_data[0] = new Map(); // left node
table_data[2] = new Map(); // right node
table_data[1] = new Map(); // self


var leftId, rightId, leftNeighbour, rightNeighbour;


var query_stat = new Array(6);
var min_time = new Array(6);
var max_time = new Array(6);
var time_spent = new Array(6);

for(let i=0; i<6; i++) {
    query_stat[i] = 0;
    time_spent[i] = 0;
}
//create 
function createOperation(query,node) {
    let st = Date.now();
    if(min_time[0] == null) min_time[0] = time.timestamp();
    max_time[0] = time.timestamp();
    query_stat[0] += 1;
    let res = null;
    if (table_record[node].has(query.table_name)) {
        res = "A table with same name already exists";
    }
    else {
        table_record[node][query.table_name] = new table.Table(query.table_name, query.property, query.primary_key);
        table_data[node][query.table_name] = new Map();
        // console.log('All tables');
        // console.log(table_record[node]);
        res = "Table Created Successfully";
    }
    time_spent[0] += Date.now()-st;
    return res;
}

//search
function searchOperation(query, node) {
    let st = Date.now();
    if(min_time[1] == null) min_time[1] = time.timestamp();
    max_time[1] = time.timestamp();
    query_stat[1] += 1;
    let res;
    // console.log('All table data on associated node');
    // console.log(table_data[node]);
    // console.log('\n');
    if (table_record[node][query.table_name] === undefined) {
        res = "Queried table does not exist";
    }
    else {
        let match = new Array();
        // query parameters contain primary_key
        pk = table_record[node][query.table_name].primary_key;
        if (query.property[pk] === undefined) {
            for (const pkval in table_record[node][query.table_name]) {
                matches = true;
                for (const property in query.property) {
                    if (table_record[node][query.table_name][pkval][property] != query.property[key]) {
                        matches = false;
                        break;
                    }
                }
                if (matches) {
                    match.push(table_record[node][query.table_name][pkval]);
                }
            }
        }
        else if(table_data[node][query.table_name][query.property[pk]] !== undefined){
            let temp = table_data[node][query.table_name][query.property[pk]];
            matches = true;
            for (const key in query.property) {
                if (temp[key] != query.property[key]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                match.push(temp);
            }
        }
        if (match.length > 0)
            res = "Matching results are \n" + match.toString();
        else res = "No matching record found";
    }
    
    time_spent[1] += Date.now()-st;
    return res;
}

//update
function updateOperation(query, node) {
    let st = Date.now();
    if(min_time[2] == null) min_time[2] = time.timestamp();
    max_time[2] = time.timestamp();
    query_stat[2] += 1;
    let res;
    if (table_record[node][query.table_name] !== undefined) {
        // Insert Query
        if (query.property == null) {
            pk_val = query.new_property[table_record[node][query.table_name].primary_key];
            if (table_data[node][query.table_name][pk_val] !== undefined) {
                res = "An entry with same primary key already exists\n";
            }
            else {
                // console.log('Table data before insertion');
                // console.log(table_data[node][query.table_name]);
                // console.log('\n');
                table_data[node][query.table_name][pk_val] = query.new_property;
                // console.log('Table data after insertion');
                // console.log(table_data[node][query.table_name]);
                // console.log('\n');
                res = "Data inserted successfully";
            }
        }
        // Update Query
        else {
            // console.log('Table data before Update');
            // console.log(table_data[node][query.table_name]);
            // console.log('\n');
            // query parameters contain primary_key
            pk = table_record[node][query.table_name].primary_key;
            if (query.property[pk] !== undefined) {
                let temp = table_data[node][query.table_name][query.property[pk]];
                if(temp !== undefined) {
                    matches = true;
                    for (const key in query.property) {
                        if (temp[key] != query.property[key]) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        // changes requested in primary key
                        if (query.new_property[pk] !== undefined) {
                            if (table_data[node][query.table_name][query.new_property[pk]] !== undefined) {
                                res = "An existing entry with same primary key exist";
                            }
                            else {
                                table_data[node][query.table_name][query.new_property[pk]] = table_data[node][query.table_name][query.property[pk]];
                                for (const key in query.new_property) {
                                    table_data[node][query.table_name][query.new_property[pk]][key] = query.new_property[key];
                                }
                                delete table_data[node][query.table_name][query.property[pk]];
                                // console.log('Table data after update');
                                // console.log(table_data[node][query.table_name]);
                                // console.log('\n');
                                res = "1 record updated successfully\n";
                            }
                        }
                        else {
                            for (const key in query.new_property) {
                                table_data[node][query.table_name][query.property[pk]][key] = query.new_property[key];
                            }
                            // console.log('Table data after update');
                            // console.log(table_data[node][query.table_name]);
                            // console.log('\n');
                            res = "1 record updated successfully\n";
                        }
                    }
                    else {
                        res = "No matching record to update";
                    }
                }
                else {
                    res = "No matching record to update";
                }
            }
            else {
                let cnt = 0;
                for (const pkval in table_record[node][query.table_name]) {
                    matches = true;
                    for (const property in query.property) {
                        if (table_record[node][query.table_name][pkval][property] != query.property[key]) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        // changes requested in primary key
                        if (query.new_property[pk] !== undefined) {
                            if (table_data[node][query.table_name][query.new_property[pk]] !== undefined) {
                                res = "An existing entry with same primary key exist";
                            }
                            else {
                                table_data[node][query.table_name][query.new_property[pk]] = table_data[node][query.table_name][query.property[pk]];
                                for (const key in query.new_property) {
                                    table_data[node][query.table_name][query.new_property[pk]][key] = query.new_property[key];
                                }
                                delete table_data[node][query.table_name][query.property[pk]];
                                cnt = cnt + 1;
                            }
                        }
                        else {
                            for (const key in query.new_property) {
                                table_data[node][query.table_name][query.property[pk]][key] = query.new_property[key];
                            }
                            cnt = cnt + 1;
                        }
                        // console.log('Table data after update');
                        // console.log(table_data[node][query.table_name]);
                        // console.log('\n');
                        res = cnt + " record updated successfully\n";
                    }
                    else {
                        res = "No matching record to update";
                    }
                }
            }
        }
    }
    else {
        res = "Queried table does not exist";
    }
    time_spent[2] += Date.now()-st;
    return res;
}

//delete
function deleteOperation(query, node) {
    if(min_time[3] == null) min_time[3] = time.timestamp();
    max_time[3] = time.timestamp();
    query_stat[3] += 1;
    let st = Date.now();
    let res;
    if (table_record[node][query.table_name] !== undefined) {
        if (query.property == null) {
            // console.log('All table before deletion');
            // console.log(table_record[node]);
            // console.log('\n');
            delete table_record[node][query.table_name];
            delete table_data[node][query.table_name];
            // console.log('All table after deletion');
            // console.log(table_record[node]);
            // console.log('\n');
            res = "Table deleted sucessfully";
        }
        else {
            // console.log('Table data before deletion');
            // console.log(table_record[node][query.table_name]);
            // console.log('\n');
            cnt = 0;
            pk = table_record[node][query.table_name].primary_key;
            // query parameters contain primary_key
            if (query.property[pk] !== undefined) {
                let temp = table_data[node][query.table_name][query.property[pk]];
                if(temp !== undefined) {
                    matches = true;
                    for (const key in query.property) {
                        if (temp[key] != query.property[key]) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        delete table_data[node][query.table_name][query.property[pk]];
                        cnt = cnt + 1;
                    }
                }
                else {
                    res = "No matching record to delete";
                }
            }
            else {
                for (const pkval in table_record[node][query.table_name]) {
                    matches = true;
                    for (const property in query.property) {
                        if (table_record[node][query.table_name][pkval][property] != query.property[key]) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        delete table_record[node][query.table_name][pkval];
                        cnt = cnt + 1;
                    }
                }
            }
            // console.log('Table data after deletion');
            // console.log(table_record[node][query.table_name]);
            // console.log('\n');
            res = cnt + " records deleted successfully";
        }
    }
    else {
        res = "No such table exists";
    }
    time_spent[0] += Date.now()-st;
    return res;
}

function print_stat() {
    console.log("***************** Current state of Node *****************");

    console.log("No of Create Queries : "+query_stat[0]);
    console.log("No of Read Queries : "+query_stat[1]);
    console.log("No of Update Queries : "+query_stat[2]);
    console.log("No of Delete Queries : "+query_stat[3]);

    
    console.log("First Create Query at: "+min_time[0]);
    console.log("Last Create Query at: "+max_time[0]);
    console.log("First Read Query at: "+min_time[1]);
    console.log("Last Read Query at: "+max_time[1]);
    console.log("First Update Query at: "+min_time[2]);
    console.log("Last Update Query at: "+max_time[2]);
    console.log("First Delete Query at: "+min_time[3]);
    console.log("Last Delete Query at: "+max_time[3]);

    console.log("NOTE: Only Individual query taking more than 1ms is recorded");
    console.log("Time taken in overall create query : "+time_spent[0]);
    console.log("Time taken in overall read query : "+time_spent[1]);
    console.log("Time taken in overall update query : "+time_spent[2]);
    console.log("Time taken in overall delete query : "+time_spent[3]);

    console.log("Size occupied by data of Left Node : "+JSON.stringify(table_data[0]).length);
    console.log("Size occupied by data of Self Node : "+JSON.stringify(table_data[1]).length);
    console.log("Size occupied by data of Right Node : "+JSON.stringify(table_data[2]).length);
    
    console.log("#########################################################");
    console.log("");
}

// identfies the type of query and sends it to proper function and return the result of operation
function query_processor(query, node) {
    let res;
    switch (query.operation) {
        case 'C':
            res = createOperation(query, node);
            return res;
        case 'R':
            res = searchOperation(query, node);
            return res;
        case 'U':
            res = updateOperation(query, node);
            return res;
        case 'D':
            res = deleteOperation(query, node);
            return res;
        default:
            return "Invalid Query";
    }
}

/**
 * Events to update neighbouring nodes on addition or removal
 */

ioClient.on('updateNeighbour', (p)=>
{
    console.log('updateNeighbour');

    var params = JSON.parse(p);

    if(params.direction == 'left')
    {
        if(!leftId || leftId != params.socketId)
        {
            leftId = params.socketId;
            console.log("leftId: "+leftId);

            if(params.cause == 'addition')
            {
                if(!params.new)
                {
                    ioClient.emit('passMyItems', JSON.stringify({
                        dest: leftId,
                        tableInfo: table_record[1],
                        tableData: table_data[1]
                    }))    
                }
            }
            else if(params.cause == 'removal')
            {
                ioClient.emit('passMyItems', JSON.stringify({
                    dest: leftId,
                    tableInfo: table_record[1],
                    tableData: table_data[1]
                }))
            }
        }
    }
    else if(params.direction == 'right')
    {
        if(!rightId || rightId != params.socketId)
        {
            rightId = params.socketId;
            console.log("rightId: "+rightId);

            if(params.cause == 'addition')
            {
                if(!params.new)
                {
                    ioClient.emit('itemsList', JSON.stringify({dest: rightId, tableNames: [...table_record[1].keys()]}));

                    ioClient.emit('passMyItems', JSON.stringify({
                        dest: rightId,
                        tableInfo: table_record[1],
                        tableData: table_data[1]
                    })) 
                }    
            }
            else if(params.cause == 'removal')
            {
                ioClient.emit('passMyItems', JSON.stringify({
                    dest: rightId,
                    tableInfo: table_record[1],
                    tableData: table_data[1]
                }))

                ioClient.emit('passMyItems', JSON.stringify({
                    dest: leftId,
                    tableInfo: table_record[2],
                    tableData: table_data[2]
                }))

                //merge right with own
                table_record[2].forEach((value, key)=>
                {
                    table_record[1].set(key, value);
                    table_data[1].set(key, table_data[2].get(key));

                    table_record[2].delete(key);
                    table_data[2].delete(key);
                })
            }
        }
    } 
})

ioClient.on('filteredItemsList', (p)=>
{
    console.log('filteredItemsList');

    var params = JSON.parse(p);

    var toSendTableInfo = new Map();
    var toSendTableData = new Map();

    params.tableNames.forEach((tableName)=>
    {
        toSendTableInfo.set(tableName, table_record[1].get(tableName));
        toSendTableData.set(tableName, table_data[1].get(tableName));

        table_record[2].set(tableName, table_record[1].get(tableName));
        table_data[2].set(tableName, table_data[1].get(tableName));

        table_record[1].delete(tableName);
        table_data[1].delete(tableName);
    })

    ioClient.emit("filteredItems", JSON.stringify({
        dest: params.dest,
        tableInfo: toSendTableInfo,
        tableData: toSendTableData
    }));
})

ioClient.on('takeYourItems', (p)=>
{
    console.log('takeYourItems');

    var params = JSON.parse(p);

    Object.keys(params.tableInfo).forEach((key)=>{
        table_record[1].set(key, params.tableInfo[key]);
        table_data[1].set(key, params.tableData[key]);
    })
})

ioClient.on('addMyItems', (p)=>
{
    console.log('addMyItems');

    let params = JSON.parse(p);

    let index;

    if(params.source == leftId)
        index = 0;
    else if(params.source == rightId)
        index = 2;

    Object.keys(params.tableInfo).forEach((key)=>{
        table_record[index].set(key, params.tableInfo[key]);
        table_data[index].set(key, params.tableData[key]);
    })
})

/**
 * 
 * @param query : query to be processed
 * 
 * TODO: Actual result after processing query
 * @returns : Dummy string showing processing time and name of processing client
 */

function process(query, node) {
    let res = query_processor(query, node);
    // console.log(query);
    // console.log(res);
    ioClient.emit('processed', query, res);
    return;
    if (query.operation != 'R') {
        //process oltp query as it is and record answer in res
        ioClient.emit('processed', query, res);
    }
    else {
        if (olap_queue.length > 0) {
            olap_queue.push(query);
        }
        else {
            const forked = fork('node_base.js');
            forked.send({ dict: dict });

        }
    }
}

// function which processes query from query_queue and removes it from query_queue and sends the result
function process_query(query, node) {
    if (query_queue.length > 0) {
        var q = query_queue[0];
        // console.log('processing query ' + q.hash);
        query_queue.splice(0, 1);
        process(q);
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
ioClient.on("process", (query, node) => {
    // console.log('received query: ' + query.hash);
    process(query, node);
});

ioClient.on("stats",() => {
    print_stat();
});

module.exports = {
    olap_queue: olap_queue,
    ioClient: ioClient

};