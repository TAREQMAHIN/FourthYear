var express = require('express');
var router = express.Router();

var ConsistentHashing = require('consistent-hashing');
var cons = new ConsistentHashing(["node1", "node2", "node3"]);
 
var nodes = {};
var chars = [
  'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
  'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
  'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
];
 
chars.forEach(function(c) {
  var node = cons.getNode(c);
 
  if (nodes[node]) {
    nodes[node].push(c);
  } else {
    nodes[node] = [];
    nodes[node].push(c);
  }
});
 
 /*new node*/
// cons.addNode("node4");
//cons.removeNode("node1");
console.log(nodes.node1);
 

module.exports = router;
