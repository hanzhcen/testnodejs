const router = require('../lib/router')
const pool = router.getPool();
pool.connect(function(err, client, done) {
    if(err) {
        return console.error('error fetching client from pool', err);
    }
    ///--DELETE from loyaltycore.ivls__transactionlog__c where ivls__status__c=\'Complete\';
      var querystring =  'DELETE FROM loyaltycore.ivls__logshipment__c where ivls__status__c=\'Complete\' or _hc_err is not null or _hc_err<>\'\'';
      console.log(querystring);
      client.query(querystring, function(err, result) {
        done();
       if(err) {
            return console.error('error running query', err);
        }
       //client.release();
    });
  });