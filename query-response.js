
/**
 * @param query_hash : The hash of query, used to identify and associate it at server
 * @param status : status code
 * @param message : message 
 * @param data : data to be sent to user
 * @param block : left-0, self-1, right-2 (integer)
 * 
 * # status_codes
 * 200: Query Executed Successfully
 * 400: Query failed or table not found
 */
class QueryResponse {
    constructor(query_hash, status, message, data, block) {
        this.query_hash = query_hash;
        this.status = status;
        this.message = message;
        this.data = data;
        this.block = block;
    }
}

module.exports = QueryResponse;