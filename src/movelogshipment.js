
const { Pool } = require('pg')
var pool1 = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
var pool2 = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
var schedule = require('node-schedule');
//var dateFormat = require('dateformat');
var isRunclearerror=true;
var isRunmovement=true;
schejob2();
schejob3();
function schejob2(){
      //console.log(day);
       // setInterval(function(){ }, 300); 
        moveerror(); 
        }
       
function moveerror(){
  if (isRunclearerror)
  {
    isRunclearerror=false;
    pool1.connect(function(err, client, done) {
      console.error('Call clear error log shipment connect to DB!');
    if(err) {
      isRunclearerror=true;
        console.error('error fetching client from pool', err);
        return;
     }

      var querystring =  'Select loyaltycore.errshipment();';
      client.query(querystring, function(err, result) {
        console.error('Call Query to Run Function!');
        isRunclearerror=true;
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
}

function schejob3(){
      //console.log(day);
       // setInterval(function(){/*call function()*/}, 2000);
            movecompleted(); 
        }
function movecompleted(){
  if (isRunmovement)
  {
     isRunmovement=false;
    pool2.connect(function(err, client, done) {
      console.error('Call move completed log shipment connect to DB!');
    if(err) {
      isRunmovement=true;
        console.error('error fetching client from pool', err);
        return;
      }

      var querystring =  'Select loyaltycore.completedshipment();';
      client.query(querystring, function(err, result) {
        console.error('Call Query to Run Function!');
        isRunmovement=true;
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
}