var request = require('request'),
    events = require('events'),
    util = require('util'),
    follow = require('follow'),
	colors = require('colors'), 
	fs = require('fs'),
	r = request.defaults({json: true});

// requests
/*
function requests() {

    var args = Array.prototype.slice.call(arguments),
        cb = args.pop(),
        results = [],
        errors = [];

    for (var i = 0; i < args.length; i++) {
        (function (i) {
        
            r(args[i], function (e, resp, body) {
                if (e) errors[i] = e
                else if (resp.statusCode !== 200) errors[i] = new Error("status is not 200.")
                results.push([i, body])
                if (results.length === args.length) {
                    var fullresults = [errors.length ? errors : null]
                    results.forEach(function (res) {
                        fullresults[res[0] + 1] = res[1]
                    })
                    cb.apply(this, fullresults)
                }
            })
        })(i)
    }

}
*/

function Replicator(options) {
    for (i in options) this[i] = options[i]
	fs.existsSync(options.dir) || fs.mkdirSync(options.dir);
    //console.log("Replicator".magenta + " launched.");
    // events.EventEmitter.prototype.call(this)
}

util.inherits(Replicator, events.EventEmitter);

//push data
Replicator.prototype.push = function (cb) {
    var options = this
    if (options.from[options.from.length - 1] !== '/') options.from += '/'
    if (options.to[options.to.length - 1] !== '/') options.to += '/'
	options.cb = cb;

    //r
    //requests(options.from, options.to, function (err, fromInfo, toInfo) {
       // if (err) throw err
       // console.log(fromInfo);
       // options.fromInfo = fromInfo
       // options.toInfo = toInfo

    var sourceChangeUrl = options.from + '_changes',
    	cacheFile = options.dir + encodeURIComponent(sourceChangeUrl) + "_" +(new Date()).getDate(),
    	isCached = fs.existsSync(cacheFile);
    		
	isCached?console.log("FILE".grey + " LOAD ".red + cacheFile):console.log("HTTP".grey + " GET  ".red + sourceChangeUrl);

	if (!isCached) {
		r(sourceChangeUrl, function (e, resp, body) {
            if (e) throw e

            console.log("HTTP ".grey + (resp.statusCode).toString().red + "  " + sourceChangeUrl);

            if (resp.statusCode !== 200) {throw new Error("status is not 200.")}
            
            fs.writeFile(cacheFile, JSON.stringify(body) ,function(err){
            	if (err) {
	            	
	            	console.log("FILE ".grey + "SAVE Error".red);
	            	return;
				}
	        	console.log("FILE ".grey + "SAVE ".red + cacheFile)
            });
            
         	options.getMissRevs(body);               
        })
    }else{
        fs.readFile(cacheFile, function(e, data){
	        if (e) throw e
	        options.getMissRevs(JSON.parse(data));
        });   
    }
    //});
}

//get miss Revs
Replicator.prototype.getMissRevs = function (body) {
 	var options = this;
	var byid = {};
	
    //options.since = body.results[body.results.length - 1].seq;
    
    body.results.forEach(function (value) {
    	//replace Object Value to Sring;
        byid[value.id] = value.changes.map(function (r) {
            return r.rev
        })
    });
    
    //Total changes.
    console.log((Object.keys(byid).length).toString().green + " changes was founded. ");
    
    var missRevsUrl =  options.to + "_missing_revs";
	
	console.log("HTTP".grey + " POST ".red + missRevsUrl + " (it will take a long time. please wait...)");
	//console.log(byid);
	
	//post get missing revs
    r.post({
        url: missRevsUrl,
        json: byid
    }, function (e, resp, body) {
        var results = [];
        
        console.log("HTTP ".grey + (resp.statusCode).toString().red + "  " + missRevsUrl);
        
        fs.writeFile(options.dir + "missing_revs", JSON.stringify(body) ,function(err){
        	if (err) {
            	return;
			}
	    });
	    
        options.taskBody = body.missing_revs;
        
        var tasks = Object.keys(options.taskBody);
        var taskNumber = 10; //并行数量
        var chunk = parseInt(tasks.length/taskNumber);
        
        options.taskLength = tasks.length;
        options.count = 0;
        
        if (options.taskLength === 0) {
        	options.cb({});
        	return;
        }
        
        for (var i=0;i<taskNumber;i++){
        	var next = (taskNumber-i==1)?tasks.slice(i*chunk):tasks.slice(i*chunk, ((i+1)*chunk - 1));
        	//console.log(next.length);
        	options.pushTask(next,i);
        }
    })
    //get missing revs end.
}

Replicator.prototype.pushTask = function(tasks,taskid){
	var options = this;
	options.count++;
	
    if (tasks.length == 0){
    	options.cb && options.cb();
    	options.cb == null;
	    return;
    } 
    
	var id = tasks.shift();
	
	if (!id || id.indexOf("_design")>-1) { //ignore blank id, & _design
	console.log("REGISTRY".grey + " PUSH ".red + ("(" + padLeft(this.count," ",this.taskLength.toString().length) + "/" +this.taskLength + ")").yellow + (" ignore blank id || _design.").red);
	
		options.pushTask(tasks,taskid);
		return ;
	}
	
    var rev = options.taskBody[id][0];
    
    options.pushDoc(id, rev, function (obj) {
        if (obj.error) {
        	options.emit('failed', obj)
        } else {
        	options.emit('pushed', obj)  
        }
        options.pushTask(tasks,taskid);
    },taskid);
}

//push Document to Registry.
Replicator.prototype.pushDoc = function (id, rev, cb, taskid) {
    console.log(("TASK_"+taskid).grey + " PUSH ".red + ("(" + padLeft(this.count," ",this.taskLength.toString().length) + "/" +this.taskLength + ")").yellow + " " + padRight(id,"-",30) + " rev ".magenta + rev);
    
    var options = this,
        headers = {
            'accept': "multipart/related,application/json"
		};

    if (!cb) cb = function () {}
/*

    if (options.filter && options.filter(id, rev) === false) return cb({
        id: id,
        rev: rev,
        filter: false
    })
*/

/*     if (!options.mutation) { */
        request
            .get({
                url: options.from + encodeURIComponent(id) + '?attachments=true&revs=true&rev=' + rev,
                headers: headers
            })
            .pipe(request.put(options.to + encodeURIComponent(id) + '?new_edits=false&rev=' + rev, function (e, resp, b) {
                if (e) {
                    cb({
                        error: e,
                        id: id,
                        rev: rev,
                        body: b
                    })
                } else if (resp.statusCode > 199 && resp.statusCode < 300) {
                    cb({
                        id: id,
                        rev: rev,
                        success: true,
                        resp: resp,
                        body: b
                    })
                } else {
                    cb({
                        error: "status code is not 201.",
                        id: id,
                        resp: resp,
                        body: b
                    })
                }

            }))
}


//continuous
Replicator.prototype.continuous = function () {
    var options = this
    options.push(function () {
        follow({
            db: options.from,
            since: options.since
        }, function (e, change) {
            if (e) return
            change.changes.forEach(function (o) {
                options.pushDoc(change.id, o.rev, function (obj) {
                    if (obj.error) options.emit('failed', obj)
                    else options.emit('pushed', obj)
                })
            })
        })
    })
}

function _pad(type,str,pad,length){
	if(str.length >= length) {
		return str.length > length?(str.substring(0,length-3) + "..."):str; 
	}
	else {
		return _pad(type, type == 0?((pad || " ") + str):(str + (pad || " ")) ,pad,length);
	}
}

function padLeft(str,pad,length){ 
	return _pad(0,str,pad,length);
}

function padRight(str,pad,length){ 
	return _pad(1,str,pad,length);
}



function getUserHome() {
  return process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE;
}

function replicate(from, to, cb) {
    console.log("replicate ".magenta + from + " to ".red + to);
    
    if (typeof from === 'object') {
    	var options = from
    }else {
        var options = {
            from: from,
            to: to
        }
    }
    
/*
    follow({
	    db : to
    },function(error,change){
	   if(!error) {
	     console.log("Change " + change.seq + " has " + Object.keys(change.doc).length + " fields");
	   }
    });
*/
    
    options.dir = getUserHome() + "/replicator/";

    var rep = new Replicator(options)
    rep.push(cb)
    return rep
}

module.exports = replicate
replicate.Replicator = Replicator