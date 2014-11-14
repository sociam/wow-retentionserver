var pg = require("pg");
var amqp = require("amqplib");
var config = require("./config");

var showErr = function (err) {
    console.error("Error:", err, err.stack);
};

pg.connect(config.pg_connection_string, function (err, client, done) {
    if (err) {
        done();
        console.error("Error connecting to postgresql", err);
        return;
    }

    var insertMsg = function (outName, msg) {
        var msgString = JSON.stringify(msg);
        client.query("INSERT INTO entries (sourcequeue, jsondata, fulltext, created) VALUES ($1, $2, $2::tsvector, CURRENT_TIMESTAMP)", [outName, msg], function (err, result) {
            if (err) {
                showErr(err);
            }
            done();
        });
    };

    var connectQueue = function (queueName, outName) {
        return amqp.connect(config.connection_string).then(function(conn) {
            process.once('SIGINT', function() { conn.close(); }); 
            return conn.createChannel().then(function(ch) {
                var ok = ch.assertExchange(queueName, 'fanout', {durable: false});

                ok = ok.then(function() { return ch.assertQueue('', {exclusive: true}); }); 

                ok = ok.then(function(qok) {
                    return ch.bindQueue(qok.queue, queueName, '').then(function() {
                        return qok.queue;
                    }); 
                }); 

                ok = ok.then(function(queue) {
                    return ch.consume(queue, function (msg) { insertMsg(outName, msg); }, {noAck: true});
                }); 

                return ok; 
            }); 
        }); 
    };

    var connect = connectQueue("wikipedia_hose", "wikipedia_revisions");
    connect = connect.then(function() { return connectQueue("twitter_hose", "tweets"); }, showErr);
    connect = connect.then(function() { return connectQueue("trends_hose", "trends"); }, showErr);
    connect = connect.then(function() { return connectQueue("spinn3r_hose", "spinn3r"); }, showErr);
    connect = connect.then(function() {
        console.log("Ready.");
    }, showErr);
});

