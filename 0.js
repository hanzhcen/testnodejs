const { Pool, Client } = require('pg')


const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: true
});


var querystring =  'SELECT NOW()';
pool.query(querystring, (err, res) => {
  console.log(err, res)
  pool.end()
})  

pool.on('error', function (err, client) {
  console.error('idle client error', err.message, err.stack)
})