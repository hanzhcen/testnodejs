
//const { Client } = require('pg');
//const router = require('../lib/router')
//const pool = router.getPool();
//const pool1 = router.getPool();
const sleep = require('system-sleep');
console.log('Current Connect String is:'+
    process.env.DATABASE_URL );
const { Pool } = require('pg')
var pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
var pool1 = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
var idno='';
(async () => {
//const client = new Client({ user: 'u8u5obf9qe75gq', host: 'ec2-18-233-254-125.compute-1.amazonaws.com', database: 'd3e6v86hr1aenn', password: 'p2257769c0deea148a027e6a6ecd32297d0bb37af183447155d03243e4459ed81', port: 5432,ssl:true });


//await pool.connect();

await pool.connect(function(err, client, done) {
     console.log('DB connect..........');
    if(err) {
        console.error('error fetching client from pool', err);
        return;
    }
  client.query('LISTEN "watchers"');
  client.on('notification', function(data) {
  	var str=data.payload;
  	var str1=str.split(',');
    idno=str1[2];
    console.log('have connect.....');
  	//console.log(str1[0].toLowerCase());
    if (str1[0].toLowerCase()=='ivls__logshipment__c')
    {
      //distributeSFDC();
    }
    else
    {
      sleep(1000);
      newmempoints();
    }
  });
});
})();
/*
function distributeSFDC(){
      console.log(idno);
      pool1.connect(function(err, client, done) {
     //console.error('Call function for Distribute SFDC records to Heroku!',i=i+1);
    if(err) {
        console.error('error fetching client from pool', err);
        return;
    }
      var querystring =  'Select loyaltycore.distributesfdc('+'\''+idno+'\''+');';
      console.log(querystring);
      client.query(querystring, function(err, result) {
        console.error('Call Query to Run Function!');
       if(err) {
          console.error('error running query', err);
          //done();
          //client.release();
          return;
        }
        console.log('Successful!');
        console.log(result);
        //done();
        client.release();
    });
  });
}
*/
function newmempoints(){
    pool.connect(function(err, client, done) {
      console.error('Call function for calc new member points!');
    if(err) {
        console.error('error fetching client from pool', err);
        return;
    }
    /*
      var currdate=new Date();
      var dt=currdate.getFullYear() + '-' + 
    ("0" + (currdate.getMonth() + 1)).slice(-2) + '-' + 
    ("0" + (currdate.getDate() + 1)).slice(-2);
    */
    //console.log(dt);
      var querystring =  'Select loyaltycore.gennewjoinpoints();';
      //console.log('***********************************************');
      console.log(querystring);
      console.log('***********************************************');
      client.query(querystring, function(err, result) {
        //console.error('Call Query to Run Function!');
       if(err) {
          console.error('error running query', err);
          return;
        }
        console.log('Successful!');
        //console.log(result);
        client.release();
    });
  });
}