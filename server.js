var request = require('request');
var googleAuth = require('google-auto-auth');
var config = require('./config.js');
var Hapi = require('hapi');
var https = require('https');
var gcloud = require('gcloud');
var async = require('async');

var server = new Hapi.Server();
server.connection({ port: process.env.PORT || config.port });

var agent = new https.Agent({
  maxSockets: 100
});

var message = {foo: "bar"};

server.route({
  path: '/wreck',
  method: 'GET',
  config: {
    auth: false, 
    handler: function(req, reply) {
      console.log('test via wreck');

      var wreck = require('wreck');
      wreck.agents.https.maxSockets = 100;

      // Create a client
      var auth = googleAuth({
        keyFilename: config.gcloudKeyPath,
        scopes: [
            'https://www.googleapis.com/auth/pubsub',
            'https://www.googleapis.com/auth/cloud-platform'
          ]
      });

      // get token
      auth.getToken(function (err, token) {
        if(err) reply(err);
        // console.log(authorizedReqOpts);

        var data = new Buffer(JSON.stringify(message)),
            calls = [],
            count = 0;

        var options = {
          headers: {Authorization: 'Bearer '+token},
          payload: JSON.stringify({messages: [{data: data.toString('base64')}]}),
          timeout: 10000
        };

        // console.log('options', options);

        // prepare calls for parallel
        for(var i = 0; i<100; i++) {
          calls.push(function(callback){

            var httpRequest = wreck.post('https://pubsub.googleapis.com/v1/projects/'+config.gcloudProject+'/topics/'+config.topic+':publish', options, function(error, response, payload){
              if(error) return callback(error);

              count = count+1;

              var body = payload.toString('utf-8'),
                  json = JSON.parse(body);

              if(json.error) return callback(null, json.error.message);

              callback(null, json.messageIds[0]);
            });
          });
        }

        async.parallel(calls,
          function(error, results){
            if(error) return reply(error);
          
            console.log('processed', count);
            // agent.destroy();
            reply(results);
          }
        );
      });
    }
  }
});

server.route({
  path: '/request',
  method: 'GET',
  config: {
    auth: false, 
    handler: function(req, reply) {
      console.log('test via request');

      // Create a client
      var auth = googleAuth({
        keyFilename: config.gcloudKeyPath,
        scopes: [
            'https://www.googleapis.com/auth/pubsub',
            'https://www.googleapis.com/auth/cloud-platform'
          ]
      });

      // get token
      auth.getToken(function (err, token) {
        if(err) reply(err);
        // console.log(authorizedReqOpts);

        var data = new Buffer(JSON.stringify(message)),
            calls = [],
            count = 0;

        var requestOptions = {
          agent: agent,
          method: 'POST',
          url: 'https://pubsub.googleapis.com/v1/projects/'+config.gcloudProject+'/topics/'+config.topic+':publish',
          headers: {Authorization: 'Bearer '+token},
          json: {
            messages: [{data: data.toString('base64')}]
          },
          timeout: 15000
        };

        console.log('options', requestOptions);

        // prepare calls for parallel
        for(var i = 0; i<100; i++) {
          calls.push(function(callback){

            request(requestOptions, function(error, response, body){
              if(error) return callback(error);

              count = count+1;

              return callback(null, body.messageIds[0]);
            });
          });
        }

        async.parallel(calls,
          function(error, results){
            if(error) return reply(error);
          
            console.log('processed', count);
            // agent.destroy();
            reply(results);
          }
        );
      });
    }
  }
});

server.route({
  path: '/gcloud',
  method: 'GET',
  config: {
    auth: false, 
    handler: function(req, reply) {
      console.log('test via gcloud');

      var pubsub = gcloud({
        projectId: config.gcloudProject, 
        keyFilename: config.gcloudKeyPath
      }).pubsub();

      // add agent for maxSockets
      pubsub.interceptors.push({
        request: function(requestOptions) {
          requestOptions.agent = agent;
          console.log('interceptor options', requestOptions);
          return requestOptions;
        }
      });

      var topic = pubsub.topic(config.topic),
          calls = [],
          count = 0;

      // prepare calls for parallel
      for(var i = 0; i<100; i++) {
        calls.push(function(callback){

          topic.publish({data: message}, function (error, messageIds) {
            if(error) return callback(error);
            count = count+1;

            callback(null, messageIds[0]);
          });
        });
      }

      async.parallel(calls,
        function(error, results){
          if(error) return reply(error);

          console.log('processed', count);

          reply(results);
        }
      );
    }
  }
});

server.start(function (err) {
  if(err) throw err;
  console.log('running');
  // server.log('info', 'Server running');
});

