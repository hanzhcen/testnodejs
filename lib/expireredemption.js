const router = require('../lib/router.js')

//define sql elements
const dbname = router.getDBname();
var querystring = '';
var querystring_list = [];

module.exports = {
    expireredemption:function(pool, processDate,environment){
        return expireredemption (pool, processDate,environment);
    },
    
};

async function  expireredemption (pool, processDate,environment) {
    try{
        var recordtype_list = await router.selectALL('recordtype');
        var recordtypeid_open =  router.getRecordTypeId('IVLS__Redemption__c','Open', recordtype_list);
        console.log('recordtypeid_open:'+recordtypeid_open);
        if(router.isNotNull(recordtypeid_open)){
            querystring = "SELECT * FROM loyaltycore.ivls__redemption__c WHERE DATE_PART('day',  NOW() - ivls__dateexpiry__c ) >= 0 AND recordtypeid = '"+recordtypeid_open+"'";
        }else{
            querystring = "SELECT * FROM loyaltycore.ivls__redemption__c WHERE DATE_PART('day',  NOW() - ivls__dateexpiry__c ) >= 0 AND ivls__status__c = 'Open'";
        }
        
        var redemption_list = (await router.query_one_way(querystring)).rows;
        var involvedMemberObj = {};
        if(redemption_list.length == 0){
            console.log('No redemption needed to be reset status');
            return;
        }
      

        console.log(redemption_list.length+" redemptions points needed to be reset");

        for (var i=0; i<redemption_list.length;i++){
            var redemptionObj = redemption_list[i];
           
            var matched_detail_list = [];
            querystring = "SELECT * FROM loyaltycore.ivls__redemptiondetail__c WHERE ivls__redemptionpgid__c = " +redemptionObj.ivls__pgid__c;
            matched_detail_list = (await router.query_one_way(querystring)).rows;
            
            if(matched_detail_list.length == 0 || router.isNotNull(matched_detail_list)== false){
                console.log('No matched redemption detail for :'+ redemptionObj.ivls__pgid__c);
                continue;
            }
            for(var j=0;j<matched_detail_list.length;j++){
                var detailObj = matched_detail_list[j];
                var ivls__pointsredeem__c = detailObj.ivls__pointsredeem__c;
                var matched_transaction_list = [];
                querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE id = " +detailObj.ivls__transactionpgid__c;
                matched_transaction_list = (await router.query_one_way(querystring)).rows;

                if(matched_transaction_list.length == 0 || router.isNotNull(matched_transaction_list)== false){
                    console.log('No matched transaction (points redeeming) for detail:'+ detailObj.id);
                    continue;
                }
            
                for(var k=0;k<matched_transaction_list.length;k++){
                    var ivls__pointsredeeming__c = matched_transaction_list[k].ivls__pointsredeeming__c;
                    var ivls__pointsredeemed__c = matched_transaction_list[k].ivls__pointsredeemed__c;
                    var new_ivls__pointsredeemed__c = Number(ivls__pointsredeemed__c) + Number(ivls__pointsredeem__c);
                    var new_ivls__pointsredeeming__c = Number(ivls__pointsredeeming__c) - Number(ivls__pointsredeem__c);

                    querystring =  'UPDATE loyaltycore.ivls__transaction__c SET ivls__pointsredeemed__c = '+ new_ivls__pointsredeemed__c +', ivls__pointsredeeming__c = '+new_ivls__pointsredeeming__c+'  WHERE id = '+matched_transaction_list[k].id;
                    querystring_list.push(querystring);
                    var tempStr = matched_transaction_list[k].ivls__member__c;
                    involvedMemberObj[tempStr] = tempStr;
                }
            }
            console.log("Processing :"+redemptionObj.ivls__pgid__c);
            console.log("...");
            var recordtypeid_lapsed =  router.getRecordTypeId('IVLS__Redemption__c','Lapsed', recordtype_list);
            console.log('recordtypeid_lapsed:'+recordtypeid_lapsed);
            if(router.isNotNull(recordtypeid_lapsed)){
                querystring =  "UPDATE loyaltycore.ivls__redemption__c SET ivls__status__c = 'Lapsed',  recordtypeid = '"+recordtypeid_lapsed+"' WHERE ivls__pgid__c = "+redemptionObj.ivls__pgid__c;
            }else{
                querystring =  "UPDATE loyaltycore.ivls__redemption__c SET ivls__status__c = 'Lapsed' WHERE ivls__pgid__c = "+redemptionObj.ivls__pgid__c;
            }
           
            querystring_list.push(querystring);

            var involvedMember_list = [];
            for(index in involvedMemberObj){
                involvedMember_list.push(index);
            }
            // for(var s=0;s<involvedMember_list.length;s++){
            //     querystring = "SELECT SUM(t.ivls__pointsearned__c) as pointsearned FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__member__c = a.ivls__pgid__c WHERE t.ivls__member__c = "+involvedMember_list[s]+" AND DATE_PART('day', t.ivls__transactiondate__c - a.ivls__datetier__pc ) >=0"
            //     var pointsEarnedSinceLastRenewal = (await router.query_one_way(querystring)).rows[0].pointsearned;
            // }
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
            if(environment == "production"){
                pool.query(totalQueryStr, (err, res) => {
                    if(err) {
                        console.log('UPDATE ERROR');
                        console.log(err);
                    }else{
                        console.log('UPDATE FINISHED');
                        console.log(res);
                    }
                    
                })
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
