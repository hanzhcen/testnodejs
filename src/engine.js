const router = require('../lib/router.js')
const gentx = require('../lib/gentx.js')
const calcpoints = require('../lib/calcpoints.js')
const dashboard = require('../lib/dashboard.js')
const pointreset = require('../lib/pointreset.js')
const expireredemption = require('../lib/expireredemption.js')
const tierevaluate = require('../lib/tierevaluate.js')
const recalemembers = require('../lib/recalemembers.js')


var log_level = "debug";
// var debug_mode = router.getDebugMode();
// if(router.isNotNull(debug_mode)){
//     if(debug_mode == true){
//         log_level = "debug";
//     }
// }

var environment = "";
var processDate = "";
var recordtime = false;


environment = 'production';
// var processDate = "T+0";
recordtime = true;


if(environment !== 'production'){
    environment = router.getEnvironment();
}
if(processDate !== 'T+0'){
    processDate = router.getProcessDate();
}



console.log("//////////////////////////////   START   //////////////////////////////") 
console.log("///////////////////////   Version 1.20180922.1   //////////////////////") 

const pool = router.getPool();

async function main () {
    
    try{
        //// Check Authentication ////
        var connected = await router.checkAuth ();
        if(connected !== true || typeof(connected) == "undefined"){
            console.log("Authentication fail!!!");
            return;
        }
    }catch(e){
        console.log("Authentication fail!!!");
        console.log(e);
        return;
    }
    
    console.log("Authentication Successfull!!!");
   

    try{
        var testName = 'Go-live Test 20180922';
        var starttime = "";
        var middletime = "";
        var endtime = "";
        var finishtime = "";

        if(recordtime){
            querystring = "INSERT INTO loyaltycore.error_log (object_name, createddate, remarks) VALUES ('start time', NOW(), '"+testName+"');";
            var result = (await router.query_one_way(querystring));
        }
       

        try{
            querystring = "SELECT NOW() as now";
            starttime = (await router.query_one_way(querystring)).rows[0].now;
        }catch(e){
            console.log("starttime error");
            console.log(e);
        }


        try{
            await gentx.gentransactions(processDate,environment, log_level);
        }catch(e){
            console.log("gentransactions error");
            console.log(e);

            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'gentransactions error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
           
        }

        if(recordtime){
            querystring = "INSERT INTO loyaltycore.error_log (object_name, createddate, remarks) VALUES ('time finish generate transaction', NOW(), '"+testName+"');";
            var result = (await router.query_one_way(querystring));
        }
       
        try{
            querystring = "SELECT NOW() as now";
            middletime = (await router.query_one_way(querystring)).rows[0].now;
        }catch(e){
            console.log("middletime error");
            console.log(e);
        }

        // try{
        //     await recalemembers.recalemembers (pool, processDate,environment)
        // }catch(e){
        //     console.log("recalemembers error");
        //     console.log(e);
        //     if(log_level == "debug"){
        //         var querystring_error_log = router.get_Querystring_error_log();
        //         var values = ['', '', 'recalemembers error', e.toString()];
        //         var result  = (await router.query_with_parameter(querystring_error_log, values));
        //         console.log(result);
        //     }
           
        // }


        try{
            await calcpoints.calculatepoints(pool, processDate,environment, log_level);
        }catch(e){
            console.log("calculatepoints error");
            console.log(e);
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'calculatepoints error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
            
        }


        if(recordtime){
            querystring = "INSERT INTO loyaltycore.error_log (object_name, createddate, remarks) VALUES ('time finish calculate points', NOW(), '"+testName+"');";
            var result = (await router.query_one_way(querystring));
        }

        

        try{
            querystring = "SELECT NOW() as now";
            endtime = (await router.query_one_way(querystring)).rows[0].now;
           
        }catch(e){
            console.log("endtime error");
            console.log(e);
        }


        try{
            await dashboard.updateDashboard (pool, processDate,environment);
        }catch(e){
            console.log("updateDashboard error");
            console.log(e);

            if(log_level == "debug"){   
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'updateDashboard error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }


        try{
            await pointreset.pointreset (pool, processDate,environment);
        }catch(e){
            console.log("pointreset error");
            console.log(e);

            if(log_level == "debug"){   
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'pointreset error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }


        try{
            await expireredemption.expireredemption (pool, processDate,environment)
        }catch(e){
            console.log("expireredemption error");
            console.log(e);

            if(log_level == "debug"){   
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'expireredemption error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }


        try{
            await tierevaluate.tierevaluate (pool, processDate,environment)
        }catch(e){
            console.log("tierevaluate error");
            console.log(e);

            if(log_level == "debug"){   
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'tierevaluate error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }

        try{
            await recalemembers.recalemembers (pool, processDate,environment)
        }catch(e){
            console.log("recalemembers error");
            console.log(e);

            if(log_level == "debug"){   
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'recalemembers error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }
        
        if(recordtime){
            querystring = "INSERT INTO loyaltycore.error_log (object_name, createddate, remarks) VALUES ('time finish daily jobs', NOW(), '"+testName+"');";
            var result = (await router.query_one_way(querystring));
        }



        try{
            querystring = "SELECT NOW() as now";
            finishtime = (await router.query_one_way(querystring)).rows[0].now;
           
        }catch(e){
            console.log("finishtime error");
            console.log(e);
        }

        
        console.log(testName);
        console.log("starttime");
        console.log(starttime);
        console.log("time finish generate transaction");
        console.log(middletime);
        console.log("time finish calculate points");
        console.log(endtime);
        console.log("time finish daily jobs");
        console.log(finishtime);


    }catch(e){
        console.log("engine.js error");
        console.log(e);
        if(log_level == "debug"){
            var querystring_error_log = router.get_Querystring_error_log();
            var values = ['', '', 'engine.js error', e.toString()];
            var result  = (await router.query_with_parameter(querystring_error_log, values));
            console.log(result);
        }
       
    }
}


main()


pool.on('error', function (err, client) {
  console.error('idle client error', err.message, err.stack)
})


