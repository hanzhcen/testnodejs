
console.log('Current SFDC Connect String is:'+
    process.env.DATABASE_URL );
const { Pool } = require('pg')
var pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
var isDistribute=true;

schejobdistribute();
function schejobdistribute(){
  setInterval(function(){ 
      distributeSFDC(); 
  }, 500);
}
function distributeSFDC(){
      if (isDistribute)
    {
      isDistribute=false;
      pool.connect(function(err, client, done) {
     console.error('Call function for Distribute SFDC records to Heroku!');
    if(err) {
        isDistribute=true;
        console.error('error fetching client from pool', err);
        return;
    } 
        var querystring =  'Select loyaltycore.distributesfdc();';
        console.log(querystring);
        client.query(querystring, function(err, result) {
         isDistribute=true;
        console.error('Call Query to Run Function!');
         if(err) {
            console.error('error running query', err);
            //done();
            //client.release();
            return;
          }
          
          console.log('Successful!');
          //console.log(result);
          //done();
          client.release();
      });
  });
  }
}

