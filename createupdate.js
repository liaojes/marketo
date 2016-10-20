var request = require('request');
var async = require('async');
var fs = require ('fs');
var Converter = require('csvtojson').Converter;
var _ = require('lodash');
var JSONStream = require('JSONStream');
var es = require('event-stream');

var batch_size = 300;
var total_request_time = 0;

/*var client_id = "c10880ec-0318-48da-b297-903f4aa1adda";
var client_secret = "HBlqqfAqYcyScn6O5H2YE8LsuWMKDh8b";
var munchkin_id = "677-VDU-948";*/

/* UPS Production */
var client_id = "20c3c976-780c-4fee-be2d-14810c11968a";
var client_secret = "uZxLKu5lknsUULqxPUbXU2hLZM86EkPi";
var munchkin_id = "935-KKE-240";

var access_token = false;
var access_token_error_codes = ['600','601', '602', '603'];
var at_least_one_request_sent = false;

var cb = function(err) {
    if (err) {
        console.error( "ERROR in cb()", err);
        process.exit(1);
    }
};

function getToken( callback ) {
    request.get( {
        url: 'https://' + munchkin_id + '.mktorest.com/identity/oauth/token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret
    }, function(er, res, body) {
        console.log("getToken()", er, body );
        var resbody = JSON.parse(body);
        access_token = resbody.access_token;
        if( typeof callback == "function" ) {
            callback();
        }
    }
    );
}


function runRequests( filename, data, counter, cb) {
    //async.eachSeries(data, function(x, cb) {
    // console.log(x);

    //var start = Date.now();
    var end = 0;
    console.time("requestTime");

    // some reason some emails arent email, so gonna clean it
    //_.each( data, function( v, k ) {
    //    data[k].email = data[k].email.split(',')[0];
    //} );

    //console.log(data);
    request.post({
        url: 'https://' + munchkin_id + '.mktorest.com/rest/v1/leads.json?access_token=' + access_token,
        json: { action: 'createOrUpdate', lookupField: 'email', input: data}
    }
    ,
    function(error, response, body) {
        if (error){
            console.log("ERROR in runRequests()", error);
            console.log(error.code);
            return cb(error);
        }
        else {
            if(!body.success ) {
                if( access_token_error_codes.indexOf( body.errors[0].code ) === -1 ) {
                    console.log(body);
                    body.batch = counter;
                    fs.appendFile( filename + '-log-error.txt', JSON.stringify(body) );
                    console.log("Filename: " + filename + " #" + counter + " Failed!");
                    setTimeout(function() {
                        return runRequests( filename, data, counter, cb );
                    }, (Math.floor(Math.random() * 5) + 1) * 1000);
                }
                else {
                    getToken( runRequests( filename, data, counter, cb ) );
                }
            }
            else {
                console.log("requests");
                var inserts = [];
                //console.log(body);
                _.each( body, function( v, k ) {
                    _.each( v, function( z, k2 ) {
                        // console.log(z)
                        if( typeof z == "object" && ( z.code && access_token_error_codes.indexOf( z.code ) !== -1 ) ) {
                            getToken( runRequests( filename, data, counter, cb ) );
                        }
                        else if( typeof z == "object" && !z.id && ( !z.code || ( z.code && access_token_error_codes.indexOf( z.code ) === false ) ) ) {
                            fs.appendFile( filename + '-log-error.txt', JSON.stringify(z) );
                        }
                        if( typeof z == "object" ) {
                            var pushed_obj = { email: data[k2].email, status: z.status };
                            if( z.id ) {
                                pushed_obj.marketo_id = z.id;
                            }
                            if( z.reasons ) {
                                pushed_obj.reasons = z.reasons;
                            }
                            inserts.push( pushed_obj );
                        }

                    } );
                } );
                fs.appendFile( filename + '-log.txt', JSON.stringify(body) + "\n" );
                fs.appendFile( filename + '-opt.json', JSON.stringify(inserts) + "\n" );
                _.each( inserts, function( v, k ) {
                    if(!v.marketo_id) { 
                        v.marketo_id = ''; 
                    } 
                    if(!v.reasons) { 
                        v.reasons = ''; 
                    } 
                    else { 
                        v.reasons = '"' + JSON.stringify(v.reasons) + '"'; 
                    } 

                    fs.appendFile( filename + '-opt.csv', v.email + ',' + v.status + ',' + v.marketo_id + ',' + v.reasons + "\n" );
                });
                //end = Date.now() - start;
                end = console.timeEnd("requestTime");
                console.log("    Filename: " + filename + ", batch #" + counter + ", Request time = " + end + "ms");
                total_request_time += end;
                console.log("        Cumalative Request Time = " + total_request_time + "ms");
                
                cb();
            }
        }
    });
    //url, }, cb);
}
var queue = async.queue(function(task, callback) {
    setTimeout( function() {
        runRequests(task.filename, task.data, task.counter,
                function( err ) {
                    cb( err );
                    callback(task);
                } );
    }, (Math.floor(Math.random() * 5) + 1) * 1000 );
}, 9);

queue.drain = function() {
    console.log("queue empty");
    at_least_one_request_sent = true;
};

wait();

getToken( function() {
    //console.log(access_token);
    //async.eachSeries for sequential
    /* process files all at the same time */
    var args = process.argv;
    args = args.slice(2);

    async.each(args, function(filename, cb) {
        process_file(filename, cb);
    }, cb);
});


function process_file( filename, cb ) {
    var json_leads = [];
    var batch_counter = 0;

    var getStream = function () {
        var stream = fs.createReadStream(filename, {encoding: 'utf8'}),
        parser = JSONStream.parse('*');
        return stream.pipe(parser);
    };

    getStream().pipe( es.through(function (data) {
        json_leads.push(data);
        if (json_leads.length == batch_size) {
            console.log( 'queueing request for ' + filename + ' batch ' + batch_counter + ' containing ' + json_leads.length + ' leads' );
            queue.push({filename: filename, data: json_leads, counter: batch_counter}, function(datasent) {
                console.log( 'finished request for ' + datasent.filename + ' batch ' + datasent.counter + ' containing ' + datasent.data.length + ' leads' );
            });
            batch_counter += 1;
            json_leads = [];
        }
    }, function end() {
        queue.push({filename: filename, data: json_leads, counter: batch_counter}, function(datasent) {
            console.log( 'finished request for ' + datasent.filename + ' batch ' + datasent.counter + ' containing ' + datasent.data.length + ' leads' );
        });
        batch_counter += 1;
        json_leads = [];
        console.log('done with ' + filename);
    }
    ));
}

function wait () {
    if (!at_least_one_request_sent || queue.length() != 0) {
        setTimeout(wait, 3000);
    }
    else {
        console.log(total_request_time);
    }
    console.log( "Remainder in queue: " + queue.length() )
}
