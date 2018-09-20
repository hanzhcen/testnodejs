const router = require('../lib/router.js')

//define sql elements
const dbname = router.getDBname();
var querystring = '';
var querystring_list = [];

module.exports = {
    pointreset:function(pool, processDate,environment){
        return pointreset (pool, processDate,environment);
    },
    
};


async function pointreset (pool, processDate,environment) {
    try{
        querystring =  'SELECT id, ivls__pointsnet__c FROM loyaltycore.ivls__transaction__c WHERE ivls__pointsearned__c > 0 AND DATE_PART(\'day\', ivls__datepointsexpiry__c - NOW()) < 0';
        // querystring =  "SELECT id, ivls__pointsnet__c FROM loyaltycore.ivls__transaction__c  WHERE id = 2888 " ;
        var transaction_list = (await router.query_one_way(querystring)).rows;

        if(transaction_list.length == 0){
            console.log('No transactions needed to be reset points');
            return;
        }
        
        console.log(transaction_list.length+" transactions's points needed to be reset");

        for (var i=0; i<transaction_list.length;i++){
            console.log("Processing :"+transaction_list[i].id);
            console.log("Lapsed Points :"+transaction_list[i].ivls__pointsnet__c);
            console.log("...");
            querystring =  'UPDATE loyaltycore.ivls__transaction__c SET ivls__pointslapsed__c = ' + transaction_list[i].ivls__pointsnet__c +', ivls__pointsnet__c = 0' +' WHERE id = '+transaction_list[i].id;
            querystring_list.push(querystring);
        }
        
        console.log(querystring_list);
        console.log("querystring_list:"+querystring_list.length);
        if(querystring_list.length>0){
            var totalQueryStr='';
            for (var q=0;q<querystring_list.length;q++){
                if(q == 0){
                    totalQueryStr =  totalQueryStr + querystring_list[q];
                }else{
                    totalQueryStr =  totalQueryStr + ' ; '+ querystring_list[q];
                }
               
            }
            if(environment == 'production'){
                try{
                    var result= await router.query_one_way(totalQueryStr);
                    console.log('Process querystring_list successful !!!');
                    console.log(result);
                }catch (err) {
                    console.log('Process querystring_list fail !!!')
                    console.log(err)
                    return false;
                }
            }
        }
        

        console.log("////////////////////////////// COMPLETED //////////////////////////////") 
    }catch (err) {
        console.log('Overall error ' + err)
    }
}


// main()


// pool.on('error', function (err, client) {
//   console.error('idle client error', err.message, err.stack)
// })
