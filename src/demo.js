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

    var crypto;
    try {
        crypto = require('crypto');
    } catch (err) {
        console.log('crypto support is disabled!');
    }
    var crypto = require('crypto');
    // var algorithm = "AES-256-CBC";
    // var password = "d6F3Efeq";
    var algorithm = router.getALG();
    var password = router.getCYP();
    console.log(algorithm);
    console.log(password);

    
    
    var hw = encrypt("2018-09-26T03:12:46.391Z", algorithm, password, crypto)
    // outputs hello world
    console.log(hw);
    console.log(decrypt(hw, algorithm, password, crypto));
}


main()


pool.on('error', function (err, client) {
  console.error('idle client error', err.message, err.stack)
})


