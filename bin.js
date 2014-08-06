#!/usr/local/bin/node

var replicate = require("./main");
var colors = require('colors');
var args = process.argv.slice(0);

// shift off node and script name
args.shift(); args.shift();

if(args.length < 2) throw "syntax: couch-cp http://somecouch/sourcedb http://admin:pass@somecouch/destinationdb"

replicate(args[0], args[1]);

