const router = require('../lib/router.js')
const updatemember = require('../lib/updatemember.js')

//define sql elements
const dbname = router.getDBname();
var tablename = '';
var querystring = '';
var querystring_list = [];


var maxTierSFID = '';


//top



module.exports = {
    recalemembers:function(pool, processDate,environment){
        return recalemembers (pool, processDate,environment);
    },
    
};


async function recalemembers (pool, processDate,environment) {
    try{

        var membershiptiers_list = await router.getMembershiptier();



        querystring = "SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = 'MySony'";
        var membership_list =  (await router.query_one_way(querystring)).rows;
        var membershipObj =   membership_list[0];
        var yearstart = membershipObj.ivls__yearstart__c;
        var membershipId = membershipObj.sfid;

        var ActiveMemberDuration1 = membershipObj.ivls__activememberd1__c;//e.g. 3 
        var ActiveMemberDuration2 = membershipObj.ivls__activememberd2__c;//e.g. 6 
        var RFMduration = membershipObj.ivls__rfmduration__c;//e.g. 12
    
        var today = new Date();
        var currentYear = today.getFullYear();
        var cutoffDate = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));

       
        var FYS = '';
        var FYS_lastYear = '';
        if(today < cutoffDate){
            //2018-03-23
            FYS = new Date(currentYear-1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2017-04-01
            FYS_lastYear = new Date(currentYear-2, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2016-04-01
        }else{
            //2018-06-23
            FYS = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2018-04-01
            FYS_lastYear = new Date(currentYear-1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2017-04-01
        }
        //FYS = Fiscal year start

       
      
        // console.log("cutoffDate:"+cutoffDate);
        // console.log("today:"+today);
       
        // console.log("membershipId:"+membershipId);
        // console.log("ActiveMemberDuration1:"+ActiveMemberDuration1);
        // console.log("ActiveMemberDuration2:"+ActiveMemberDuration2);


        ////        If today is the fiscal year start, move YTD numbers to PYE       ////
        var todayIsYearStart = sameDay(cutoffDate, today);
        console.log("todayIsYearStart:"+todayIsYearStart);
        if(todayIsYearStart){
            //reset every member's PYE data
            querystring = 'UPDATE loyaltycore.account SET ivls__spendingpye__pc = ivls__spendingytd__pc, ivls__pointsearnedpye__pc = ivls__pointsearnedytd__pc, ivls__pointsredeemedpye__pc = ivls__pointsredeemedytd__pc, ivls__spendingsincelastrenewal__pc = 0, ivls__spendingytd__pc = 0, ivls__pointsearnedsincelastrenewal__pc = 0, ivls__pointsearnedytd__pc = 0, ivls__pointsredeemedsincelastrenewal__pc = 0, ivls__pointsredeemedytd__pc = 0';
            await router.query_one_way(querystring);
            //reset the whold membership's PYE data
            querystring = 'UPDATE loyaltycore.ivls__membership__c SET ivls__spendingpye__c = ivls__spendingytd__c, ivls__pointsearnedpye__c = ivls__pointsearnedytd__c, ivls__pointsredeemedpye__c = ivls__pointsredeemedytd__c, ivls__noofmemberpye__c = ivls__noofmember__c, ivls__noofvippye__c = ivls__noofvip__c, ivls__spendingytd__c = 0, ivls__pointsearnedytd__c = 0, ivls__pointsredeemedytd__c = 0  WHERE ivls__membershipcode__c = \'MySony\'';
            await router.query_one_way(querystring);
        }
        
        var newMembershipObj ={};
        newMembershipObj.ivls__noofmember__c = 0;
        newMembershipObj.ivls__noofvip__c = 0;
        newMembershipObj.ivls__activemember1__c = 0;//3M
        newMembershipObj.ivls__activemember2__c = 0;//6M

        newMembershipObj.ivls__spendingytd__c = 0;
        newMembershipObj.ivls__pointsearnedytd__c = 0;
        newMembershipObj.ivls__pointsredeemedytd__c = 0;
        newMembershipObj.ivls__accumulatedpoints__c = 0;

        querystring = "SELECT count(ivls__vip__pc) FROM loyaltycore.account WHERE ivls__vip__pc = true AND ivls__membership__pc = '"+membershipId + "'";
        newMembershipObj.ivls__noofvip__c= (await router.query_one_way(querystring)).rows[0].count;
       
        querystring = "SELECT count(id) FROM loyaltycore.account WHERE ivls__membership__pc = '"+membershipId + "'";
        newMembershipObj.ivls__noofmember__c  = (await router.query_one_way(querystring)).rows[0].count;
        
        querystring = "SELECT a.ivls__pgid__c FROM loyaltycore.account as a JOIN loyaltycore.ivls__transaction__c as t ON a.ivls__pgid__c = t.ivls__memberpgid__c WHERE (SELECT count(id) FROM loyaltycore.ivls__transaction__c WHERE DATE_PART('day', NOW()- t.ivls__transactiondate__c  ) <= "+(ActiveMemberDuration1 * 30) +") >0  AND a.ivls__membership__pc = '"+membershipId + "' GROUP BY a.ivls__pgid__c";
        newMembershipObj.ivls__activemember1__c = (await router.query_one_way(querystring)).rowCount;

        querystring = "SELECT a.ivls__pgid__c FROM loyaltycore.account as a JOIN loyaltycore.ivls__transaction__c as t ON a.ivls__pgid__c = t.ivls__memberpgid__c WHERE (SELECT count(id) FROM loyaltycore.ivls__transaction__c WHERE DATE_PART('day', NOW()- t.ivls__transactiondate__c  ) <= "+(ActiveMemberDuration2 * 30) +") >0  AND a.ivls__membership__pc = '"+membershipId + "' GROUP BY a.ivls__pgid__c";
        newMembershipObj.ivls__activemember2__c = (await router.query_one_way(querystring)).rowCount;
        
        querystring = "SELECT SUM(t.ivls__pointsearned__c) as pointsearned, SUM(t.ivls__spendingexclusive__c) as spending, SUM(t.ivls__pointsredeemed__c) as pointsredeemed FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__transactiondate__c >= '"+getSQLDateTimeFormat(FYS) +"'";
        var sumObj = (await router.query_one_way(querystring)).rows[0];
        newMembershipObj.ivls__spendingytd__c = sumObj.spending;
        newMembershipObj.ivls__pointsearnedytd__c = sumObj.pointsearned;
        newMembershipObj.ivls__pointsredeemedytd__c = sumObj.pointsredeemed;

        querystring = "SELECT SUM(ivls__pointsnet__c) as pointsnet FROM loyaltycore.ivls__transaction__c";
        newMembershipObj.ivls__accumulatedpoints__c = (await router.query_one_way(querystring)).rows[0].pointsnet;

      
        // console.log("newMembershipObj:");
        // console.log(newMembershipObj);


        var membershiptiers_list = await router.getMembershiptier();
        maxTierSFID = membershiptiers_list[0].sfid;
        for(var i=0;i<membershiptiers_list.length;i++){
            var expiryDATE = router.getExpiryDate(yearstart, membershiptiers_list[i]);
            membershiptiers_list[i].expiryDATE = expiryDATE;
        }
        var recordtype_list = await router.selectALL('recordtype');

        // querystring = "SELECT ivls__memberpgid__c as id FROM loyaltycore.ivls__transaction__c WHERE ivls__status__c = 'Open' GROUP BY ivls__memberpgid__c";
        querystring = "SELECT ivls__memberpgid__c as ivls__pgid__c FROM loyaltycore.ivls__transactiondetails__c WHERE DATE_PART('day',createddate - NOW()) = 0 AND ivls__memberpgid__c IS NOT NULL GROUP BY ivls__memberpgid__c";
        // querystring = "SELECT ivls__memberpgid__c as ivls__pgid__c FROM loyaltycore.ivls__transaction__c WHERE DATE_PART('day',lastmodifieddate - NOW()) = 0 AND ivls__memberpgid__c IS NOT NULL GROUP BY ivls__memberpgid__c";
        var member_list = (await router.query_one_way(querystring)).rows;
        if(router.isNotNull(member_list)==false|| member_list.length==0){
            console.log("No member info need to be updated");
        }

        var numberOfMember_total = member_list.length;
        var numberOfMember_processed = 0;
        var numberOfMember_error = 0;


        for (var i=0;i<member_list.length;i++){
            try{
                numberOfMember_processed = numberOfMember_processed +1;
                console.log("Current Postion:"+numberOfMember_processed+"/"+numberOfMember_total);
                var targetMemberPGID = member_list[i].ivls__pgid__c;

                var transactiondatetime = "NOW()"
                var updateMemberSQL = await updatemember.updateMemberInfo(pool, targetMemberPGID, membershipObj, membershiptiers_list, "");
                console.log("updateMemberSQL:");
                console.log(updateMemberSQL);
                if(updateMemberSQL == false){
                    continue;
                }
                querystring_list.push(updateMemberSQL);
                // if(environment == "production"){
                //     var updateResult = await router.query_one_way(updateMemberSQL);
                //     console.log(updateResult);
                // }

                // querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__memberpgid__c = "+member_list[i].ivls__pgid__c +" ORDER BY ivls__memberpgid__c ASC";
                // var transaction_list_byMember = (await router.query_one_way(querystring)).rows;
                // //no need to update this member if no transaction
                // if(transaction_list_byMember.length ==0){
                //     continue;
                // }
                
                // querystring = "SELECT * FROM loyaltycore.account WHERE ivls__pgid__c = " + member_list[i].ivls__pgid__c;
                // var oldMemberObj = (await router.query_one_way(querystring)).rows[0];
                // if(router.isNotNull(oldMemberObj.ivls__pgid__c)==false){
                //     continue;
                // }
               
                // console.log("member ivls__pgid__c:"+oldMemberObj.ivls__pgid__c);
                // console.log("transaction_list_byMember.length:"+transaction_list_byMember.length);

                
                // var newMemberObj= {};
                // newMemberObj.ivls__spendinglifetime__pc = oldMemberObj.ivls__spendinglifetime__pc;
                // newMemberObj.ivls__spendingsincelastrenewal__pc =0;
                // newMemberObj.ivls__spendingytd__pc =0;
            
                // newMemberObj.ivls__pointsearnedlifetime__pc = oldMemberObj.ivls__pointsearnedlifetime__pc;
                // newMemberObj.ivls__pointsearnedsincelastrenewal__pc =0;
                // newMemberObj.ivls__pointsearnedytd__pc =0;
            
                // newMemberObj.ivls__pointsredeemedlifetime__pc = oldMemberObj.ivls__pointsredeemedlifetime__pc;
                // newMemberObj.ivls__pointsredeemedsincelastrenewal__pc =0;
                // newMemberObj.ivls__pointsredeemedytd__pc =0;
            
                // newMemberObj.ivls__pointsredeeming__pc =0;
                // newMemberObj.ivls__pointsgraceperiod__pc =0;
                // newMemberObj.ivls__pointsbalance__pc =0;
                // newMemberObj.ivls__datelasttx__pc =0;
                // newMemberObj.ivls__datelaststorevisit__pc ='';

            
                // var receipt_obj = {};
                // for(var s=0;s<transaction_list_byMember.length;s++){
                //     var transactionObj = transaction_list_byMember[s];
                //     var receiptNo = transactionObj.ivls__receiptno__c
                //     receipt_obj[receiptNo] = transactionObj;
                // }
                // var receipt_list=[];
                // for (index in receipt_obj){
                //     receipt_list.push(receipt_obj[index]);
                // }
                // console.log("receipt_list:"+receipt_list.length) 

        
                // var numOfTran12M = 0;
                // var amtOfTran12M = 0;
                // var numVisit = 0;
                // var numOfTran3M = 0;
                // var numOfTran6M = 0;
                // var numWithinPeriod = 0;
                // var ivls__dateexpired__pc = oldMemberObj.ivls__dateexpired__pc;
                // var ivls__datetier__pc =  oldMemberObj.ivls__datetier__pc;

                // if(router.isNotNull(ivls__dateexpired__pc) ==false){
                //     console.log("ivls__dateexpired__pc IS NULL at"+oldMemberObj.ivls__pgid__c);
                //     continue;
                // }

                // if(router.isNotNull(ivls__datetier__pc) == false){
                //     console.log("ivls__datetier__pc IS NULL at"+oldMemberObj.ivls__pgid__c);
                //     continue;
                // }
                
                // var memberObj= router.getCurrentMemberInfo(oldMemberObj, membershiptiers_list);

                // var CurrentMembershipTier = memberObj.CurrentMembershipTier;
                // var currentPointsTo = 0;
                
                // if(memberObj.hasNextMembershipTier){
                //     currentPointsTo =  memberObj.NextMembershipTier.ivls__pointsfrom__c;
                // }else{
                //     currentPointsTo =  memberObj.CurrentMembershipTier.ivls__pointsfrom__c;
                // }
                // var currentPointsKeep = memberObj.CurrentMembershipTier.ivls__pointsfrom__c;
                
                // querystring = "SELECT SUM(t.ivls__pointsearned__c) as pointsearned FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c+" AND DATE_PART('day', t.ivls__transactiondate__c - a.ivls__datetier__pc ) > 0;"
                // var pointsEarnedSinceLastRenewal = (await router.query_one_way(querystring)).rows[0].pointsearned;
                // var newPointsEarnedSinceLastRenewal = Number(pointsEarnedSinceLastRenewal);
                // console.log('pointsEarnedSinceLastRenewal:'+ pointsEarnedSinceLastRenewal);


                // var newPointsToNextTier = currentPointsTo - newPointsEarnedSinceLastRenewal;
                // var newPointsTokeepTier = currentPointsKeep - newPointsEarnedSinceLastRenewal;
                // if(newPointsTokeepTier < 0){
                //     newPointsTokeepTier = 0;
                // }
                // if(newPointsToNextTier < 0){
                //     newPointsToNextTier = 0;
                // }
                // newMemberObj.ivls__pointstonexttier__c = newPointsToNextTier;
                // newMemberObj.ivls__pointstokeeptier__c = newPointsTokeepTier;
                
                // newMemberObj.ivls__pointsredeemedsincelastrenewal__pc =0
                // newMemberObj.ivls__pointsredeemedsincelastrenewal__pc =0

                // console.log("ivls__datetier__pc:"+ivls__datetier__pc);
              

               

                // for(var j=0;j<receipt_list.length;j++){
                //     var receipt_obj = receipt_list[j];

                //     //Average Spending (12M)
                //     var past12M = new Date();
                //     // past12M.setFullYear(past12M.getFullYear() - 1);
                //     past12M.setMonth(past12M.getMonth() - RFMduration);
                //     console.log("past12M:"+past12M);
                //     if(receipt_obj.ivls__transactiondate__c >= past12M){
                //         numOfTran12M = numOfTran12M +1;
                //         amtOfTran12M = amtOfTran12M + receipt_obj.ivls__totalcashamount__c;
                //     }
                    
                //     // var past3M = new Date();
                //     // past3M.setMonth(past3M.getMonth() - 3);
                //     // // console.log("past3M:"+past3M);
                //     // if(receipt_obj.ivls__transactiondate__c >= past3M){
                //     //     numOfTran3M = numOfTran3M +1;
                //     // }

                //     // var past6M = new Date();
                //     // past6M.setMonth(past6M.getMonth() - 6);
                //     // // console.log("past6M:"+past6M);
                //     // if(receipt_obj.ivls__transactiondate__c >= past6M){
                //     //     numOfTran6M = numOfTran6M +1;
                //     // }

                //     if(receipt_obj.ivls__transactiontype__c == 'POS'){//Sony Store
                //         newMemberObj.ivls__datelaststorevisit__pc = getSQLDateTimeFormat(receipt_obj.ivls__transactiondate__c) ;
                //         numVisit = numVisit +1;
                //     }

                //     newMemberObj.ivls__datelasttx__pc = getSQLDateTimeFormat(receipt_obj.ivls__transactiondate__c);
                // }

                // newMemberObj.ivls__nooftx__pc = receipt_list.length ;
                // newMemberObj.ivls__noofstorevisit__pc = numVisit;
                // newMemberObj.ivls__averagespending__pc = Math.round(amtOfTran12M / numOfTran12M * 100)/100;
                // console.log("amtOfTran12M", amtOfTran12M)
                // console.log("numOfTran12M", numOfTran12M)

                // for(var j=0;j<transaction_list_byMember.length;j++){
                //     var transactionObj = transaction_list_byMember[j];

                //     //Spending (Lifetime)
                //     newMemberObj.ivls__spendinglifetime__pc = newMemberObj.ivls__spendinglifetime__pc + transactionObj.ivls__spendingexclusive__c;
                //     //Spending since Last Renewal/Join
                //     if(transactionObj.ivls__transactiondate__c > oldMemberObj.ivls__datetier__pc){
                //         newMemberObj.ivls__spendingsincelastrenewal__pc = newMemberObj.ivls__spendingsincelastrenewal__pc + transactionObj.ivls__spendingexclusive__c;
                //     }
                    

                //     //Spending (YTD)
                //     if(transactionObj.ivls__transactiondate__c >= FYS){
                //         newMemberObj.ivls__spendingytd__pc = newMemberObj.ivls__spendingytd__pc + transactionObj.ivls__spendingexclusive__c;
                //         // newMembershipObj.ivls__spendingytd__c  = newMembershipObj.ivls__spendingytd__c  + transactionObj.ivls__spendingexclusive__c;
                //     }
                   
                //     //Points Earned (Lifetime)
                //     newMemberObj.ivls__pointsearnedlifetime__pc  = newMemberObj.ivls__pointsearnedlifetime__pc  + transactionObj.ivls__pointsearned__c;
                    
                //     //Points Earned since Last Renewal/Join
                //     if(transactionObj.ivls__transactiondate__c > oldMemberObj.ivls__datetier__pc){
                //         newMemberObj.ivls__pointsearnedsincelastrenewal__pc = newMemberObj.ivls__pointsearnedsincelastrenewal__pc + transactionObj.ivls__pointsearned__c;
                //     }
                    
                //     //Points Earned (YTD)
                //     if(transactionObj.ivls__transactiondate__c >= FYS){
                //         newMemberObj.ivls__pointsearnedytd__pc = newMemberObj.ivls__pointsearnedytd__pc + transactionObj.ivls__pointsearned__c;
                //         // newMembershipObj.ivls__pointsearnedytd__c  = newMembershipObj.ivls__pointsearnedytd__c  + transactionObj.ivls__pointsearned__c;
                //     }

                
                //     //Points Redeemed (Lifetime)
                //     newMemberObj.ivls__pointsredeemedlifetime__pc = newMemberObj.ivls__pointsredeemedlifetime__pc + transactionObj.ivls__pointsredeemed__c;


                //     //Points Redeemed since Last Renewal/Join
                //     if(transactionObj.ivls__transactiondate__c > oldMemberObj.ivls__datetier__pc){
                //         newMemberObj.ivls__pointsredeemedsincelastrenewal__pc = newMemberObj.ivls__pointsredeemedsincelastrenewal__pc + transactionObj.ivls__pointsredeemed__c;
                //     }
                    
                //     //Points Redeemed (YTD)
                //     if(transactionObj.ivls__transactiondate__c >= FYS){
                //         newMemberObj.ivls__pointsredeemedytd__pc = newMemberObj.ivls__pointsredeemedytd__pc + transactionObj.ivls__pointsredeemed__c;
                //         // newMembershipObj.ivls__pointsredeemedytd__c  = newMembershipObj.ivls__pointsredeemedytd__c  + transactionObj.ivls__pointsredeemed__c;
                //     }


                //     //Points Redeeming
                //     newMemberObj.ivls__pointsredeeming__pc =  newMemberObj.ivls__pointsredeeming__pc + transactionObj.ivls__pointsredeeming__c;
                    
                //     //Points Balance (points net)
                //     newMemberObj.ivls__pointsbalance__pc =  newMemberObj.ivls__pointsbalance__pc + transactionObj.ivls__pointsnet__c;
                //     // newMembershipObj.ivls__accumulatedpoints__c  =  newMembershipObj.ivls__accumulatedpoints__c  + transactionObj.ivls__pointsnet__c;
                    
                // }
            
            
                // // console.log("amtOfTran12M:"+amtOfTran12M);
                // // console.log("numOfTran12M:"+numOfTran12M);
                // console.log("newMemberObj:");
                // console.log(newMemberObj);
                // querystring = getSQLstr(newMemberObj, dbname, 'account', router, 'ivls__pgid__c', oldMemberObj.ivls__pgid__c);
                // console.log("update member SQL:");
                // console.log(querystring);
                // querystring_list.push(querystring);

                // numberOfMember_processed = numberOfMember_processed +1;
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log('Error on process member:'+ member_list[i].id);
                console.log('Error Message:');
                console.log(e);
                numberOfMember_error = numberOfMember_error +1;
            }
        }
        console.log("newMembershipObj:");
        console.log(newMembershipObj);
        querystring = getSQLstr(newMembershipObj, dbname, 'ivls__membership__c', router,'sfid', membershipId);
        querystring_list.push(querystring);
        console.log("querystring_list:");
        console.log(querystring_list);
        console.log("querystring_list:"+querystring_list.length);
        console.log("numberOfMember_total:"+numberOfMember_total);
        console.log("numberOfMember_processed:"+numberOfMember_processed);
        console.log("numberOfMember_error:"+numberOfMember_error);

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
    }catch (e) {
        console.log("//////////////////////////////   ERROR   //////////////////////////////") 
        console.log('Overall error:')
        console.log(e)
    }
}

function getSQLDateTimeFormat(JStime){
    var month = (Number(JStime.getMonth())+1 );
    return JStime.getFullYear() +'/'+ month+ '/'+  JStime.getDate();
}

function getSQLstr(InsertObj, dbname, tablename, router,fieldname, sfid){

    for (x in InsertObj){
        InsertObj[x] = router.AddSingleQuoteSymbol(InsertObj[x]);
    }
    InsertObj.lastmodifieddate = 'NOW()'
    querystring = 'UPDATE '+dbname+tablename+' SET ';
    var count = 0;
    var fieldsstr = ''
    for (x in InsertObj){
        if(InsertObj[x] !== '\'\''){ //is NOT '', 0, null, or undefined
            if(count == 0){
                fieldsstr =  fieldsstr + x + ' = ' + InsertObj[x];
            }else{
                fieldsstr =  fieldsstr + ', ' + x + ' = ' + InsertObj[x];
            }
            count = count +1;
        }
    }
    querystring = querystring +fieldsstr +' WHERE '+fieldname+' = '+ router.AddSingleQuoteSymbol(sfid);
    return querystring;
}

function sameDay(d1, d2) {
    return (d1.getFullYear() === d2.getFullYear() &&
      d1.getMonth() === d2.getMonth() &&
      d1.getDate() === d2.getDate());
}

