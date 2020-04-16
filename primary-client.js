/**
 * Primary Client
 * 
 * Sends queries to server and displays the received result
 */

// connect to server on given port
const
  io = require("socket.io-client"),
  ioClient = io.connect("http://localhost:8003");

// to create unique random string
var crypto = require('crypto');

// Query function
const query = require('./query.js');
const table = require('./table.js');


// stores table data to make queries on them
var table_record = new Array();



/**
 * Function to create random queries
 * 
 * Useful to demonstrate working with various queries at large speed
 * 
 * Uses probability to choose which type of query to generate
 */
function createRandomQuery() {
  let prob = Math.random();
  // create
  if (prob < 0.1 || table_record.length == 0) {
    let tableName = 'table_' + crypto.randomBytes(20).toString('hex');
    let property = new Array();
    // populating property array with some random property set of length l
    l = Math.floor(Math.random() * 1000) % 5 + 1;
    for (let i = 1; i <= l; i++) {
      property.push('prop_' + i);
    }
    let primary_key = property[0];
    table_record.push(new table.Table(tableName, property, primary_key));
    return query.createTableQuery(tableName, property, primary_key);
  }
  // search
  else if (prob < 0.35) {
    let table = table_record[Math.floor(Math.random() * 1000) % table_record.length];
    let table_name = table.name;
    let property = table.property;
    let primary_key = table.primary_key;

    property_dict = new Map();
    property_dict[property[0]] = Math.floor(Math.random() * 1000) % 10;
    return query.searchQuery(table_name, property_dict, primary_key);
  }
  // insert 
  else if (prob < 0.6) {
    let table = table_record[Math.floor(Math.random() * 1000) % table_record.length];
    let table_name = table.name;
    let property = table.property;
    let primary_key = table.primary_key;

    // new value
    new_property_dict = new Map();
    for (let i = 0; i < property.length; i++)
      new_property_dict[property[i]] = Math.floor(Math.random() * 1000) % 10;
    return query.insertQuery(table_name, new_property_dict, primary_key);
  }
  // update 
  else if (prob < 0.8) {
    let table = table_record[Math.floor(Math.random() * 1000) % table_record.length];
    let table_name = table.name;
    let property = table.property;
    let primary_key = table.primary_key;

    // old value
    property_dict = new Map();
    property_dict[property[0]] = Math.floor(Math.random() * 1000) % 100;

    // new value
    new_property_dict = new Map();
    new_property_dict[property[0]] = Math.floor(Math.random() * 1000) % 100 + 100;

    return query.updateQuery(table_name, property_dict, new_property_dict, primary_key);
  }
  // delete an entry
  else if (prob < 0.9) {
    let table = table_record[Math.floor(Math.random() * 1000) % table_record.length];
    let table_name = table.name;
    let property = table.property;
    let primary_key = table.primary_key;
    property_dict = new Map();
    property_dict[property[0]] = Math.floor(Math.random() * 1000) % 200;

    return query.deleteQuery(table_name, property_dict, primary_key);
  }
  // delete 
  else if (prob < 1) {
    let idx = Math.floor(Math.random() * 1000) % table_record.length;
    let table = table_record[idx];
    let name = table.name;
    table_record.splice(idx,1);

    return query.deleteTableQuery(name);
  }
}


/**
 * function to send a query
 *  
 * @param query_packet : query to be processed at server
 * 
 */
function sendQuery(query_packet) {
  ioClient.emit("query", query_packet);
}

/**
 * Receiver to listen for result for sent query
 *  
 * @param query : query sent to server
 * @param result : result of query
 * 
 */
ioClient.on('result', (query, result) => {
  console.log('response received');
  console.log(query);
  console.log(result);
  console.log('\n');
});

// dummy implementation to send query to server at interval
var count = 1;
setInterval(function () {
  let newQuery = createRandomQuery();
  // newQuery.print();
  sendQuery(newQuery);
  count = count + 1;
}, 2000
);