"use strict";
//var pg = require('pg')
const { Pool } = require('pg')
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: true,
    idleTimeoutMillis: 5
});
//console.log(process.env.DATABASE_URL);

/*
     var config = {
         user: 'u8u5obf9qe75gq', //env var: PGUSER
         database: 'd3e6v86hr1aenn', //env var: PGDATABASE
         password: 'p2257769c0deea148a027e6a6ecd32297d0bb37af183447155d03243e4459ed81', //env var: PGPASSWORD
         host:  'ec2-18-233-254-125.compute-1.amazonaws.com', // Server hosting the postgres database
         port: 5432, //env var: PGPORT
         max: 10, // max number of clients in the pool
         ssl: true,
         idleTimeoutMillis: 30, // how long a client is allowed to remain idle before being closed
     };


 var pool = new pg.Pool(config);
 */
//process.env.DATABASE_URL
module.exports = {
    query: (text, params, callback) => {
        const start = Date.now()
        return pool.query(text, params, (err, res) => {
            const duration = Date.now() - start
            console.log('executed query', { text, duration, err })
            callback(err, res)
        })
    },

    getRecordType: (callback) => {
       // console.log('have to here!');
        var query = "SELECT name, developername, sobjecttype, sfid " +
                    "FROM loyaltycore.recordtype ";
        return pool.query(query, (err, res) => {
            if (err)
                {console.log(err);}
            callback(err, res)
        });
    }
}


