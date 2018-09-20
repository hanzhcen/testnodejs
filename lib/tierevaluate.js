const router = require('../lib/router.js')

//define sql elements
const dbname = router.getDBname();
var querystring = '';
var querystring_list = [];


module.exports = {
    tierevaluate:function(pool, processDate,environment){
        return tierevaluate (pool, processDate,environment);
    },
    
};

async function tierevaluate (pool, processDate,environment) {
    try{
        ////    get membershiptier list ////
        var membershiptiers_list = await router.getMembershiptier();
        
        //// get expired members    ////
        querystring =  "SELECT * FROM loyaltycore.account WHERE ivls__dateexpired__pc::date < NOW() AND ivls__neverdowngrade__pc = false";
        var member_list = (await router.query_one_way(querystring)).rows;

        if(member_list.length == 0){
            console.log('No member need to be evaluated');
            return;
        }

        for(var i=0; i<member_list.length;i++){
            try{
                var oldMemberObj = member_list[i];
                var memberObj= router.getCurrentMemberInfo(oldMemberObj, membershiptiers_list);
                var CurrentMembershipTier = memberObj.CurrentMembershipTier;

                // console.log('memberObj:');
                // console.log(memberObj);
            
                var pointsFr = memberObj.CurrentMembershipTier.ivls__pointsfrom__c;
                if(router.isNotNull(pointsFr) == false){
                    console.log('ivls__pointsfrom__c is null :'+oldMemberObj.ivls__pgid__c);
                    pointsFr = 0;
                    // continue;
                }
            
                querystring = "SELECT SUM(t.ivls__pointsearned__c) as pointsearned FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c+" AND DATE_PART('day', t.ivls__transactiondate__c - a.ivls__datetier__pc ) >=0"
                console.log('querystring:');
                console.log(querystring);
                var pointsEarnedSinceLastRenewal = (await router.query_one_way(querystring)).rows[0].pointsearned;
                
                if(router.isNotNull(pointsEarnedSinceLastRenewal)== false){
                    pointsEarnedSinceLastRenewal = 0;
                }
                console.log('pointsEarnedSinceLastRenewal:');
                console.log(pointsEarnedSinceLastRenewal);
                
                console.log('pointsFr:'+pointsFr);
        
                tablename = 'account';
                var newMemberObj= false;
                if(pointsEarnedSinceLastRenewal <= pointsFr){ 
                    //90000  < 100000, or. 100000 = 100000
                    if(memberObj.hasPrevMembershipTier){
                        newMemberObj = router.getEvaluateObj(memberObj.PrevMembershipTier);
                    }else{
                        newMemberObj = router.getEvaluateObj(memberObj.CurrentMembershipTier);
                    }
                }else{
                    //120000 > 100000
                    newMemberObj = router.getEvaluateObj(memberObj.CurrentMembershipTier);
                    // var CurrentMembershipTier = memberObj.CurrentMembershipTier;
                    // querystring = "select ivls__transactiondate__c as last_transactiondate from loyaltycore.ivls__transaction__c where ivls__memberpgid__c= "+memberObj.Account.ivls__pgid__c+" and ivls__pointspool__c='Spending' and ivls__status__c='Complete' and ivls__pointsearned__c>0 order by ivls__transactiondate__c desc limit 1;";
                    // var result =(await router.query_one_way(querystring));
                    // var last_transactiondate = result.rows[0].last_transactiondate;
                    // // last_transactiondate = router.getSQLDateTimeFormatWithSeconds(last_transactiondate);
                    // console.log("last_transactiondate0:");
                    // console.log(last_transactiondate);

                 
                    // var year = last_transactiondate.getFullYear();
                    // var month = last_transactiondate.getMonth();
                    // var day = last_transactiondate.getDate();
                    // year = year + Number(CurrentMembershipTier.ivls__mexpirationdate1y__c);
                    // month = month +1;
                    // var datestr = year + '/' + month + '/' + day;


                    // console.log("datestr:");
                    // console.log(datestr);
                    
                    // newMemberObj.ivls__dateexpired__pc =  router.AddSingleQuoteSymbol(datestr);
                    // newMemberObj.ivls__datetier__pc = router.AddSingleQuoteSymbol(last_transactiondate);
                    // newMemberObj.ivls__datetiertime__c = router.AddSingleQuoteSymbol(last_transactiondate);

                }
                if(newMemberObj !== false){
                    console.log('newMemberObj:');
                    console.log(newMemberObj);
                    querystring = router.getSQL_Update(newMemberObj, dbname, 'account', 'ivls__pgid__c', oldMemberObj.ivls__pgid__c);
                    querystring_list.push(querystring);
                
                }
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log('Error Processing :member_list[i].ivls__pgid__c:'+ member_list[i].ivls__pgid__c);
                console.log('Error Message:');
                console.log(e);
                continue;
            }
            
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




