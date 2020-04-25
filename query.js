// to create hash
var crypto = require('crypto');

class Query {
    /**
     * @param {Character} operation : C(create table),R(search,read),U(insert,write),D(delete record/table)
     * @param {String} table_name : Name of table on which operation is to be performed
     * @param {Dictionary} property : {string} key (property), {anything} value(value_of_property) [used at creation]
     * @param {Dictionary} new_property : {string} key (property), {anything} value(value_of_property) [used for updation]
     * @param {String} primary_key : primary_key of given table (must be one of property)
     * 
     */
    constructor(operation, table_name, property, new_property, primary_key) {
        this.operation = operation;
        this.table_name = table_name;
        this.property = property;
        this.new_property = new_property;
        this.primary_key = primary_key;
        this.random_id = Math.floor(Math.random()*1000000000);
        this.hash = this.hash();
    }

    hash() {
        let str = this.operation+' '+this.table_name+' ';
        if(this.property) {
            for (const key in this.property) {
                if (this.property.hasOwnProperty(key)) {
                    str += this.property[key]+' ';
                }
            }
        }

        if(this.new_property) {
            for (const key in this.new_property) {
                if (this.new_property.hasOwnProperty(key)) {
                    str += this.new_property[key]+' ';
                }
            }
        }

        if(this.primary_key) str += this.primary_key+' ';
        str += this.random_id;
        return crypto.createHash('md5').update(str).digest('hex');
    }

    print() {
        console.log('Operation Type : ' + this.operation);
        console.log('TableName : ' + this.table_name);
        if (this.property != null) {
            console.log('Property : ');
            console.log(this.property);
        }
        if (this.new_property != null) {
            console.log('Updated Property : ');
            console.log(this.new_property);
        }
        if (this.primary_key != null)
            console.log('Primary Key : ' + this.primary_key);

        console.log('hash : '+this.hash);
    }
}

/**
 * 
 * @param {String} table_name : Name of Table on which Query is to be performed
 * @param {List} property : List of Strings i.e. property name
 * @param {String} primary_key : One of properties that is to be used as primary key
 * 
 * @return : Object of Query
 */
function createTableQuery(table_name, property, primary_key) {
    // create dummy dict of property to match the constructor of Query
    var dictProperty = new Map();
    property.forEach(element => {
        dictProperty[element] = "";
    });

    return new Query('C', table_name, property, null, primary_key);
}

/**
 * 
 * @param {String} table_name : Name of Table on which Query is to be performed
 * @param {Dictionary} property : Key-Value pairs which the matching object should have. {string}Key-{any}Value.
 * 
 * @return : Object of Query
 */
function searchQuery(table_name, property) {
    return new Query('R', table_name, property, null, null);
}

/**
 * 
 * @param {String} table_name : Name of Table on which Query is to be performed
 * @param {Dictionary} property : Key-Value pairs which the matching object should have. {string}Key-{any}Value.
 * @param {Dictionary} new_property : Key-Value to overwrite existing key-value. {string}Key-{any}Value.
 * 
 * @return : Object of Query
 */
function updateQuery(table_name, property, new_property) {
    return new Query('U', table_name, property, new_property, null);
}

/**
 * 
 * @param {String} table_name : Name of Table on which Query is to be performed
 * @param {Dictionary} property : Key-Value pairs which the matching object should have. {string}Key-{any}Value.
 * @param {Dictionary} new_property : Key-Value to overwrite existing key-value. {string}Key-{any}Value.
 * 
 * @return : Object of Query
 */
function insertQuery(table_name, new_property) {
    return new Query('U', table_name, null, new_property, null);
}

/**
 * 
 * @param {String} table_name : Name of Table on which Query is to be performed
 * @param {Dictionary} property : Key-Value pairs which the matching object should have. {string}Key-{any}Value.
 * 
 * @return : Object of Query
 */
function deleteQuery(table_name, property) {
    return new Query('D', table_name, property, null, null);
}

/**
 * 
 * @param {String} table_name : Name of Table on which Query is to be performed
 * @return : Object of Query
 */
function deleteTableQuery(table_name) {
    return new Query('D', table_name, null, null, null);
}


module.exports = {
    Query: Query,
    createTableQuery: createTableQuery,
    updateQuery: updateQuery,
    searchQuery: searchQuery,
    deleteQuery: deleteQuery,
    insertQuery: insertQuery,
    deleteTableQuery: deleteTableQuery,
};
