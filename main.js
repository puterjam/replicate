var request = require('request'),
    events = require('events'),
    util = require('util'),
    colors = require('colors'),
    cluster = require('cluster'),
    numCPUs = require('os').cpus().length,
    fs = require('fs'),
    r = request.defaults({
        json: true
    });

/**
 * [CouchDB Replicator]
 */
function Replicator(options) {
    for (i in options) {
        this[i] = options[i];
    }

    if (this.from[this.from.length - 1] !== '/') {
        this.from += '/'
    };
    if (this.to[this.to.length - 1] !== '/') {
        this.to += '/'
    };

    fs.existsSync(options.dir) || fs.mkdirSync(options.dir);
    //console.log("Replicator".magenta + " launched.");
    // events.EventEmitter.prototype.call(this)
}

util.inherits(Replicator, events.EventEmitter);

//push data
Replicator.prototype.start = function(cb) {
    var options = this

    options.cb = cb;

    //r
    //requests(options.from, options.to, function (err, fromInfo, toInfo) {
    // if (err) throw err
    // console.log(fromInfo);
    // options.fromInfo = fromInfo
    // options.toInfo = toInfo

    var sourceChangeUrl = options.from + '_changes',
        cacheFile = options.dir + encodeURIComponent(sourceChangeUrl) + "_" + (new Date()).getDate(),
        isCached = fs.existsSync(cacheFile);

    isCached ? console.log("FILE".grey + " LOAD ".red + cacheFile) : console.log("HTTP".grey + " GET  ".red + sourceChangeUrl);

    if (!isCached) {
        r(sourceChangeUrl, function(e, resp, body) {
            if (e) throw e

            console.log("HTTP ".grey + (resp.statusCode).toString().red + "  " + sourceChangeUrl);

            if (resp.statusCode !== 200) {
                throw new Error("status is not 200.")
            }

            fs.writeFile(cacheFile, JSON.stringify(body), function(err) {
                if (err) {

                    console.log("FILE ".grey + "SAVE Error".red);
                    return;
                }
                console.log("FILE ".grey + "SAVE ".red + cacheFile)
            });

            options.getMissRevs(body);
        })
    } else {
        fs.readFile(cacheFile, function(e, data) {
            if (e) throw e
            options.getMissRevs(JSON.parse(data));
        });
    }
    //});
}

//get miss Revs
Replicator.prototype.getMissRevs = function(body) {
    var options = this;
    var byid = {};

    //options.since = body.results[body.results.length - 1].seq;

    body.results.forEach(function(value) {
        //replace Object Value to Sring;
        byid[value.id] = value.changes.map(function(r) {
            return r.rev
        })
    });

    //Total changes.
    console.log((Object.keys(byid).length).toString().green + " changes was founded. ");

    var missRevsUrl = options.to + "_missing_revs";

    console.log("HTTP".grey + " POST ".red + missRevsUrl + " (it will take a long time. please wait...)");
    //console.log(byid);

    //post get missing revs
    r.post({
        url: missRevsUrl,
        json: byid
    }, function(e, resp, body) {
        var results = [];

        console.log("HTTP ".grey + (resp.statusCode).toString().red + "  " + missRevsUrl);

        fs.writeFile(options.dir + "missing_revs", JSON.stringify(body), function(err) {
            if (err) {
                return;
            }
        });

        options.taskBody = body.missing_revs;

        var tasks = Object.keys(options.taskBody);
        // var taskNumber = 5; //5; //并行数量
        //  var chunk = parseInt(tasks.length / taskNumber);

        options.taskLength = tasks.length;
        options.count = 0;

        if (options.taskLength === 0) {
            options.cb({});
            return;
        }

        //fork worker;
        if (cluster.isMaster) {
            // Fork workers.
            for (var i = 0; i < numCPUs; i++) {
                cluster.fork();
            }

            Object.keys(cluster.workers).forEach(function(id) {
                cluster.workers[id].on('message', function(msg) {

                    if (msg.cmd == "ready") {
                        if (tasks.length <= 0) {
                            cluster.workers[id].send({
                                cmd: "end"
                            });
                            return;
                        }

                        options.count++;
                        var taskId = tasks.shift();
                        var rev = options.taskBody[taskId][0];

                        console.log(("Worker").grey + " PUSH".red + (padLeft(options.count, " ", options.taskLength.toString().length) + "/" + options.taskLength).yellow + " " + padRight(taskId, "-", 30) + " rev ".magenta + rev);

                        cluster.workers[id].send({
                            cmd: "push",
                            arguments: [
                                taskId,
                                rev,
                                options.count,
                                options.taskLength
                            ]
                        });
                    }
                });
            });
        }

        // for (var i = 0; i < taskNumber; i++) {
        //     var next = (taskNumber - i == 1) ? tasks.slice(i * chunk) : tasks.slice(i * chunk, ((i + 1) * chunk - 1));
        //     //console.log(next.length);
        //     options.pushTask(next, i);
        // }
    })
    //get missing revs end.
}

Replicator.prototype.pushTask = function(id, rev, count, total) {
    var options = this;

    if (!id || id.indexOf("_design") > -1) { //ignore blank id, & _design
        //console.log("REGISTRY".grey + " PUSH ".red + ("(" + padLeft(this.count, " ", this.taskLength.toString().length) + "/" + this.taskLength + ")").yellow + (" ignore blank id || _design.").red);
        //send ready for master
        console.log("[" + id + "]" + "is a design document. Ignored.");

        process.send({
            cmd: "ready"
        });
        return;
    }

    process.nextTick(function() {
        options.pushDoc(id, rev, function(obj) {
            if (obj.error) {
                options.emit('failed', obj)
            } else {
                options.emit('pushed', obj)
            }

            //get next task
            process.send({
                cmd: "ready"
            });
        });
    })
}

//push Document to Registry.
Replicator.prototype.pushDoc = function(id, rev, cb) {
    var options = this,
        headers = {
            'accept': "multipart/related,application/json"
        };

    if (!cb) {
        cb = function() {}
    };

    request
        .get({
            url: options.from + encodeURIComponent(id) + '?attachments=true&revs=true&rev=' + rev,
            headers: headers
        })
        .pipe(request.put(options.to + encodeURIComponent(id) + '?new_edits=false&rev=' + rev, function(e, resp, b) {
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

function _pad(type, str, pad, length) {
    if (str.length >= length) {
        return (str.length > length && type == 1) ? (str.substring(0, length - 3) + "...") : str;
    } else {
        return _pad(type, type == 0 ? ((pad || " ") + str) : (str + (pad || " ")), pad, length);
    }
}

function padLeft(str, pad, length) {
    return _pad(0, str, pad, length);
}

function padRight(str, pad, length) {
    return _pad(1, str, pad, length);
}

function getUserHome() {
    return process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE;
}

//程序主入口
function replicate(from, to, cb) {
    if (typeof from === 'object') {
        var options = from
    } else {
        var options = {
            from: from,
            to: to
        }
    }

    options.dir = getUserHome() + "/replicator/";

    var rep = new Replicator(options);

    //多线程启动
    if (cluster.isMaster) {
        console.log("replicate ".magenta + from + " to ".red + to);

        rep.start(cb);

        cluster.on('exit', function(worker, code, signal) {
            console.log(('worker ' + worker.process.pid + 'complete.').rainbow);
        });

        //console.log('replication complete.'.rainbow);
    } else {
        //接收任务
        process.on('message', function(msg) {
            // console.log(msg);
            if (msg.cmd == "push") {
                rep.pushTask.apply(rep, msg.arguments);
            }

            if (msg.cmd == "end") {
                process.exit();
            }
        });

        //send ready for master
        process.send({
            cmd: "ready"
        });
    }

    return rep
}

module.exports = replicate
replicate.Replicator = Replicator