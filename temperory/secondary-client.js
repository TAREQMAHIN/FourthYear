/**
 * Secondary Client
 * 
 * Receives queries from server and sends result after processing it
 */



//for child process
const cluster = require('cluster');
var pro =require("process");


// to store table information
const table = require('./table.js');




// connect to server on given port
const
    io = require("socket.io-client"),
    ioClient = io.connect("http://localhost:8003");



// default connection is as primary client
// change to secondary client
// target the receiver with pre-defined password
ioClient.emit("changeClientType", "12345678");

// Log to show client type change
ioClient.on('clientTypeChange', (msg) => {
    console.log('Client Type Change to Secondary-Client: ' + msg);
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


//datastructure for storage


// function to get current date and time
function getDateTime() {
    var date = new Date().toJSON().slice(0,10);
    var time = new Date().toJSON().slice(11,19)
    return date+' '+time;

// data storage of tables
var table_data = new Array(3);
table_data[0] = new Map(); // left node
table_data[2] = new Map(); // right node
table_data[1] = new Map(); // self

//create 
function createOperation(query,node) {
    if (table_record[node].has(query.table_name)) {
        return "A table with same name already exists";
    }
    else {
        table_record[node][query.table_name] = new table.Table(query.table_name, query.property, query.primary_key);
        table_data[node][query.table_name] = new Map();
        console.log('All tables');
        console.log(table_record[node]);
        return "Table Created Successfully";
    }

}

//search
function searchOperation(query, node) {
    console.log('All table data on associated node');
    console.log(table_data[node]);
    console.log('\n');
    if (table_record[node][query.table_name] === undefined) {
        return "Queried table does not exist";
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
            return "Matching results are \n" + match.toString();
        else return "No matching record found";
    }

}

//update
function updateOperation(query, node) {
    if (table_record[node][query.table_name] !== undefined) {
        // Insert Query
        if (query.property == null) {
            pk_val = query.new_property[table_record[node][query.table_name].primary_key];
            if (table_data[node][query.table_name][pk_val] !== undefined) {
                return "An entry with same primary key already exists\n";
            }
            else {
                console.log('Table data before insertion');
                console.log(table_data[node][query.table_name]);
                console.log('\n');
                table_data[node][query.table_name][pk_val] = query.new_property;
                console.log('Table data after insertion');
                console.log(table_data[node][query.table_name]);
                console.log('\n');
                return "Data inserted successfully";
            }
        }
        // Update Query
        else {
            console.log('Table data before Update');
            console.log(table_data[node][query.table_name]);
            console.log('\n');
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
                                return "An existing entry with same primary key exist";
                            }
                            else {
                                table_data[node][query.table_name][query.new_property[pk]] = table_data[node][query.table_name][query.property[pk]];
                                for (const key in query.new_property) {
                                    table_data[node][query.table_name][query.new_property[pk]][key] = query.new_property[key];
                                }
                                delete table_data[node][query.table_name][query.property[pk]];
                                console.log('Table data after update');
                                console.log(table_data[node][query.table_name]);
                                console.log('\n');
                                return "1 record updated successfully\n";
                            }
                        }
                        else {
                            for (const key in query.new_property) {
                                table_data[node][query.table_name][query.property[pk]][key] = query.new_property[key];
                            }
                            console.log('Table data after update');
                            console.log(table_data[node][query.table_name]);
                            console.log('\n');
                            return "1 record updated successfully\n";
                        }
                    }
                    else {
                        return "No matching record to update";
                    }
                }
                else {
                    return "No matching record to update";
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
                                return "An existing entry with same primary key exist";
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
                        console.log('Table data after update');
                        console.log(table_data[node][query.table_name]);
                        console.log('\n');
                        return cnt + " record updated successfully\n";
                    }
                    else {
                        return "No matching record to update";
                    }
                }
            }
        }
    }
    else {
        return "Queried table does not exist";
    }
}

//delete
function deleteOperation(query, node) {
    if (table_record[node][query.table_name] !== undefined) {
        if (query.property == null) {
            console.log('All table before deletion');
            console.log(table_record[node]);
            console.log('\n');
            delete table_record[node][query.table_name];
            delete table_data[node][query.table_name];
            console.log('All table after deletion');
            console.log(table_record[node]);
            console.log('\n');
            return "Table deleted sucessfully";
        }
        else {
            console.log('Table data before deletion');
            console.log(table_record[node][query.table_name]);
            console.log('\n');
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
                    return "No matching record to delete";
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
            console.log('Table data after deletion');
            console.log(table_record[node][query.table_name]);
            console.log('\n');
            return cnt + " records deleted successfully";
        }
    }
    else {
        return "No such table exists";
    }
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
 * 
 * @param query : query to be processed
 * 
 * TODO: Actual result after processing query
 * @returns : Dummy string showing processing time and name of processing client
 */





function process(query, node) {
   
     if (query.operation != 'R' ) {

        //oltp query

        if(cluster.isMaster){
            //processing oltp query
                let res = query_processor(query, node);
                console.log(query);
                console.log(res);
                ioClient.emit('processed', query, res);
               
        }
    }
    else{
        //process olap query in child process
           
           if(cluster.isMaster){

            //i.e it is a the main process
            //create a new child

             cluster.fork();

           }
           else
           {
            //process the olap query in child process
            let res = query_processor(query, node);
             console.log(query);
             console.log(res);
             ioClient.emit('processed', query, res);
             pro.kill(pro.pid, 'SIGINT');
           }
    }   
}

// function which processes query from query_queue and removes it from query_queue and sends the result
function process_query(query, node) {
    
    if (query_queue.length > 0) {
        var q = query_queue[0];
        console.log('processing query ' + q.hash);
        query_queue.splice(0, 1);
        process(q,node);
    }
}

/*
*
 * Receiver to listen for incoming query
 * 
 * @param query: query recived from server
 * 
 * Places the query on query_queue and calls process_query() to process the queue
 * NOTE : requires some better mechanism to handle asynochronity
 */




ioClient.on("process", (query, node) => {
    console.log('received query: ' + query.hash);
    process(query, node);
});
