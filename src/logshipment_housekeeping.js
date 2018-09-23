//const router = require('../lib/router')
const { Pool } = require('pg')
var pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
var housekeepingdays=process.env.keepdays;
pool.connect(function(err, client, done) {
    if(err) {
        return console.error('error fetching client from pool', err);
    }
    ///--DELETE from loyaltycore.ivls__transactionlog__c where ivls__status__c=\'Complete\';
      var querystring =  'Select loyaltycore.housekeeping('+housekeepingdays+');';
      console.log(querystring);
      client.query(querystring, function(err, result) {
        done();
       if(err) {
            return console.error('error running query', err);
        }
       //client.release();
    });
  });
