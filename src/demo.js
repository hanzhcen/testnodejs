var pg = require('pg');
const router = require('../lib/router.js')
const gentx = require('../lib/gentx.js')
const calcpoints = require('../lib/calcpoints.js')
const dashboard = require('../lib/dashboard.js')
const pointreset = require('../lib/pointreset.js')
const expireredemption = require('../lib/expireredemption.js')
const expiremembertier = require('../lib/expiremembertier.js')
const tierevaluate = require('../lib/tierevaluate.js')
const recalemembers = require('../lib/recalemembers.js')
var crypto = require('asymmetric-crypto');
var fs = require('fs');


var environment = "";
var processDate = "";
// environment = 'production';
var processDate = "T+0";
if(environment !== 'production'){
    environment = router.getEnvironment();
}
if(processDate !== 'T+0'){
    processDate = router.getProcessDate();
}


console.log("//////////////////////////////   START   //////////////////////////////") 

const pool = router.getPool();

async function main () {

    // try{
    //     //// Check Authentication ////
    //     var connected = await router.checkAuth ();
    //     if(connected !== true || typeof(connected) == "undefined"){
    //         console.log("Authentication fail!!!");
    //         return;
    //     }
    // }catch(e){
    //     console.log("Authentication fail!!!");
    //     console.log(e);
    //     return;
    // }
    
    // console.log("Authentication Successfull!!!");
    var str = "My's day";
    console.log(str);
    str = str.replace(/'/g, "''");
    console.log(str);
    // var server_subscription = {"data":"qSz4QaDp/c7ZLy0ZK3Ejk+hF32zyfsyQCwobUWYKGLtfgsqyRIxIXgpQY8adqzXe0wsiHTyh/hvuBP3zrH6xs4xIlsf8dQt4/3OdWqIZZqHCmdbkZjFqgki6BRcVjGIP+ZkpMGrtJeQxmfWGMF/gRck=","nonce":"NvPQfrONhYayvrJf+L6+HPOzWhtqGGcG","signature":"BmriDB4KrOvyMgfQnl4M91Y0H6nStv6FBqDW9Et6PY6GoyWRI3P6oT025Wq3lOHwgCvhadWRnRPL0CJL4M1cDA=="};
    // fs.writeFile('./lib/auth.json', JSON.stringify(server_subscription), function(err) {
    //     if(err) {
    //         return console.log(err);
    //     }
    //     console.log("auth.json was updated");

    // }); 

    // var local_json_is_integrity = await router.check_local_json();
    // console.log(local_json_is_integrity) 
    
}


main()


pool.on('error', function (err, client) {
  console.error('idle client error', err.message, err.stack)
})


