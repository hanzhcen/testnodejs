
const router = require('../lib/router')
const pool = router.getPool();
var schedule = require('node-schedule');
//var dateFormat = require('dateformat');
schejob1();


function schejob1(){
    schedule.scheduleJob('*/5 * * * * *', function(){
      //var day=dateFormat(new Date(), "yyyy-mm-dd h:MM:ss");
      //console.log(day);
      try{
          syncrecord();
        }
        catch(error){
        console.error(error);
        }
    });
}
function syncrecord(){
    pool.connect(function(err, client, done) {
      console.error('Call syncrecord connect to DB!');
    if(err) {
        console.error('error fetching client from pool', err);
        return;
    }

      var querystring =  'Select loyaltycore.syncshipmentrec();';
      client.query(querystring, function(err, result) {
        console.error('Call Query to Run Function!');
       if(err) {
          console.error('error running query', err);
          return;
        }
        console.log('Successful!');
        console.log(result);
        client.release();
    });
  });
}