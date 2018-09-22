var pg = require('pg');
var dbname = 'loyaltycore.';
module.exports = {

    updateMemberInfo:function(pool, targetMemberPGID, membershipObj, membershiptiers_list, beforeupdatePTK){
        return updateMemberInfo (pool, targetMemberPGID, membershipObj, membershiptiers_list, beforeupdatePTK);
    },
    query_one_way:function(pool, q){
        return query_one_way(pool, q);
    },
    isNotNull:function(value){
        return isNotNull(value);
    },
    getCurrentMemberInfo :function(Account, membershiptiers_list){
        return getCurrentMemberInfo(Account, membershiptiers_list);
    },
    getSQL_Update :function(InsertObj, dbname, tablename,fieldname, sfid){
        return  getSQL_Update(InsertObj, dbname, tablename,fieldname, sfid);
    },
    AddSingleQuoteSymbol :function(str){
        return AddSingleQuoteSymbol(str);
    },
    getSQLDateTimeFormatWithSeconds:function(JStime){
        return getSQLDateTimeFormatWithSeconds (JStime);
    },
    getExpiryDate:function(pool, yearstart, transactiondate, obj){
        return getExpiryDate (pool, yearstart, transactiondate,  obj);
    },
};
function getSQLDateTimeFormatWithSeconds(JStime){
    var month = (Number(JStime.getMonth())+1 );
    return JStime.getFullYear() +'-'+ month+ '-'+  JStime.getDate() + ' ' + JStime.getHours() + ':'+ JStime.getMinutes()+ ':'+ JStime.getSeconds() +'.000' ;
}
function AddSingleQuoteSymbol(str){
    if(isNotNull(str) == false){
        if(typeof(str) == 'number'){
        
            str = 0;
        }else{   
            str = "";
        }
        
    }
    if(typeof(str) == 'number'){
        
        return str;
    }else{
        return '\'' + str + '\'';
    }
}
async function getExpiryDate(pool,yearstart,transactiondate, obj){
    // querystring = "SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = 'MySony'";
    // var membershipObj =  (await query_one_way(pool, querystring)).rows[0];
    // var yearstart = membershipObj.ivls__yearstart__c;

    var ivls__pdaterange1__c = obj.ivls__pdaterange1__c
    var ivls__pdaterange2__c = obj.ivls__pdaterange2__c
    var ivls__pexpirationdate1ddmm__c = obj.ivls__pexpirationdate1ddmm__c
    var ivls__pexpirationdate2ddmm__c = obj.ivls__pexpirationdate2ddmm__c

    // var transactiondate = new Date();
    var currentYear = (new Date()).getFullYear();
    var cutoffDate = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2018/04/01
    var expiryDATE = '';
    // console.log("transactiondate:"+transactiondate);
    // console.log("cutoffDate:"+cutoffDate);
    if( transactiondate < cutoffDate){
        //2018-03-23
        // console.log('<');
        var FYS = new Date(currentYear-1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2017-04-01
        var date1S = new Date(getYearStr(currentYear, Number(ivls__pdaterange1__c.substring(2,4))-1, ivls__pdaterange1__c.substring(0,2)));//2018-03-17
        var date1E = getYearStr(currentYear, ivls__pexpirationdate1ddmm__c.substring(2,4), ivls__pexpirationdate1ddmm__c.substring(0,2));//'2018/03/31'

        var date2S = new Date(getYearStr(currentYear, ivls__pdaterange2__c.substring(2,4), ivls__pdaterange2__c.substring(0,2)));//2018-03-31
        var date2E = getYearStr(currentYear, ivls__pexpirationdate2ddmm__c.substring(2,4), ivls__pexpirationdate2ddmm__c.substring(0,2));//'2018/04/15'

        if(FYS<transactiondate && transactiondate<date1S){
            expiryDATE = date1E;
        }else{
            expiryDATE = date2E;
        }
    }else{
        //2018-06-23
        // console.log('>');
        var FYS = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2018-04-01
        var date1S = new Date(getYearStr(currentYear+1, Number(ivls__pdaterange1__c.substring(2,4))-1, ivls__pdaterange1__c.substring(0,2)));//2019-03-17
        var date1E = getYearStr(currentYear+1, ivls__pexpirationdate1ddmm__c.substring(2,4), ivls__pexpirationdate1ddmm__c.substring(0,2));//'2019/03/31'

        var date2S = new Date(getYearStr(currentYear+1, ivls__pdaterange2__c.substring(2,4), ivls__pdaterange2__c.substring(0,2)));//2019-03-31
        var date2E = getYearStr(currentYear+1, ivls__pexpirationdate2ddmm__c.substring(2,4), ivls__pexpirationdate2ddmm__c.substring(0,2));//'2019/04/15'

        if(FYS<transactiondate && transactiondate<date1S){
            expiryDATE = date1E;
        }else{
            expiryDATE = date2E;
        }

    }
    return expiryDATE;
}
function getYearStr(year, month, day){
    return year + '/'+month + '/' +day;
}
async function query_one_way(pool, q) {
    const client = await pool.connect()
    let res
    try {
      await client.query('BEGIN')
      try {
        res = await client.query(q)
        await client.query('COMMIT')
      } catch (err) {
        await client.query('ROLLBACK')
        throw err
      }
    } finally {
      client.release()
    }
    return res
}
function getSQL_Update(InsertObj, dbname, tablename,fieldname, sfid){
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
    querystring = querystring +fieldsstr +' WHERE '+fieldname+' = '+ AddSingleQuoteSymbol(sfid);
    return querystring;
}
function isNotNull(value){
    if(value !== 0 && value !== null && value !== "" && typeof(value) !== "undefined"){
        return true;
    }else{
        return false;
    }
}
async function updateMemberInfo(pool, targetMemberPGID, membershipObj, membershiptiers_list, beforeupdatePTK){
    var querystring =  "";
   
    ////    get membership information
    // querystring = "SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = 'MySony'";
    // var membershipObj =   (await query_one_way(pool, querystring)).rows[0];
    var yearstart = membershipObj.ivls__yearstart__c;
    var membershipId = membershipObj.sfid;
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


    ////    get membershiptier information
    // querystring= 'SELECT * FROM loyaltycore.ivls__membershiptier__c ORDER BY ivls__sequence__c DESC';
    // var membershiptiers_list= (await query_one_way(pool, querystring)).rows;
    // var maxTierSFID = membershiptiers_list[0].sfid;
    // querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__memberpgid__c = "+targetMemberPGID +" ORDER BY ivls__memberpgid__c ASC";
    // var transaction_list_byMember = (await query_one_way(pool, querystring)).rows;
    // //no need to update this member if no transaction
    // if(transaction_list_byMember.length ==0){
    //     return false;
    // }
    
    




    querystring = "SELECT * FROM loyaltycore.account WHERE ivls__pgid__c = " + targetMemberPGID;
    var oldMemberObj = (await query_one_way(pool, querystring)).rows[0];
    if(isNotNull(oldMemberObj.ivls__pgid__c)==false){
        return false;
    }
    
    console.log("Current member PGID:"+oldMemberObj.ivls__pgid__c);
    // console.log("transaction_list_byMember.length:"+transaction_list_byMember.length);

    ////        Fields to update        ////
    var newMemberObj= {};
    // newMemberObj.ivls__spendinglifetime__pc = oldMemberObj.ivls__spendinglifetime__pc;
    // newMemberObj.ivls__pointsearnedlifetime__pc = oldMemberObj.ivls__pointsearnedlifetime__pc;
    // newMemberObj.ivls__pointsredeemedlifetime__pc = oldMemberObj.ivls__pointsredeemedlifetime__pc;
    // newMemberObj.ivls__datelasttx__pc =0;
    // newMemberObj.ivls__datelaststorevisit__pc ='';
    // newMemberObj.ivls__nooftx__pc = 0;
    // newMemberObj.ivls__noofstorevisit__pc = 0;
    // newMemberObj.ivls__averagespending__pc = 0;

    newMemberObj.ivls__spendingsincelastrenewal__pc =0;
    newMemberObj.ivls__pointsearnedsincelastrenewal__pc =0;
    newMemberObj.ivls__pointsredeemedsincelastrenewal__pc =0;

    newMemberObj.ivls__spendingytd__pc =0;
    newMemberObj.ivls__pointsearnedytd__pc =0;
    newMemberObj.ivls__pointsredeemedytd__pc =0;

    newMemberObj.ivls__pointsredeeming__pc =0;
    newMemberObj.ivls__pointsgraceperiod__pc =0;
    newMemberObj.ivls__pointsbalance__pc =0;
   
    newMemberObj.ivls__pointstonexttier__c = 0;
    newMemberObj.ivls__pointstonexttier__pc = 0;
    newMemberObj.ivls__pointstokeeptier__c = 0;

    
    

    // var receipt_obj = {};
    // /*  Group Transaction into Receipt  */
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

    var ivls__dateexpired__pc = oldMemberObj.ivls__dateexpired__pc;
    var ivls__datetier__pc =  oldMemberObj.ivls__datetier__pc;
    var ivls__datetiertime__c =  oldMemberObj.ivls__datetiertime__c;


    // //  checking account information, if missing skip
    // if(isNotNull(ivls__dateexpired__pc) ==false){
    //     console.log("ivls__dateexpired__pc IS NULL in "+oldMemberObj.ivls__pgid__c);
    //     return false;
    // }

    if(isNotNull(ivls__datetier__pc) == false){
        console.log("ivls__datetier__pc IS NULL in "+oldMemberObj.ivls__pgid__c);
        return false;
    }

    if(isNotNull(ivls__datetiertime__c) == false){
        console.log("ivls__datetiertime__c IS NULL in "+oldMemberObj.ivls__pgid__c);
        return false;
    }


    //restructure the memberObj, get available prev and next tier
    var memberObj= getCurrentMemberInfo(oldMemberObj, membershiptiers_list);

    var CurrentMembershipTier = memberObj.CurrentMembershipTier;
    var ivls__pdaterange1__c = CurrentMembershipTier.ivls__pdaterange1__c
    var ivls__pexpirationdate1ddmm__c = CurrentMembershipTier.ivls__pexpirationdate1ddmm__c

    var date1S = currentYear + "/" + ivls__pdaterange1__c.substring(2,4)  + "/" + ivls__pdaterange1__c.substring(0,2);//2018/03/17
    var date1E = currentYear + "/" + ivls__pexpirationdate1ddmm__c.substring(2,4)  + "/" + ivls__pexpirationdate1ddmm__c.substring(0,2);//'2018/03/31'



    // var updategraceperiodSQL = "UPDATE loyaltycore.account SET ivls__pointsgraceperiod__pc = COALESCE((SELECT SUM(ivls__pointsearned__c) as ivls__pointsgraceperiod__pc FROM loyaltycore.ivls__transaction__c WHERE ivls__transactiondate__c::date > '"+date1S+"' AND ivls__transactiondate__c::date <'"+date1E+"' AND ivls__memberpgid__c = "+targetMemberPGID+" group by ivls__memberpgid__c ), 0) WHERE ivls__pgid__c = "+targetMemberPGID;
    var updategraceperiodSQL = "UPDATE loyaltycore.account SET ivls__pointsgraceperiod__pc = COALESCE((SELECT SUM(ivls__pointsearned__c) as ivls__pointsgraceperiod__pc FROM loyaltycore.ivls__transaction__c WHERE ivls__transactiondate__c::date > '"+date1S+"' AND ivls__transactiondate__c::date <'"+date1E+"' AND ivls__memberpgid__c = "+targetMemberPGID+" group by ivls__memberpgid__c ), 0), ivls__pointslost__c =  COALESCE((SELECT SUM(ivls__pointslost__c) as ivls__pointsgraceperiod__pc FROM loyaltycore.ivls__transaction__c WHERE  ivls__memberpgid__c = "+targetMemberPGID+"), 0) WHERE ivls__pgid__c = "+targetMemberPGID;
    console.log("updategraceperiodSQL: "+targetMemberPGID);
    console.log(updategraceperiodSQL);
    var result = (await query_one_way(pool, updategraceperiodSQL));
    console.log(result);











    var currentPointsTo = 0;
    if(memberObj.hasNextMembershipTier){
        currentPointsTo =  memberObj.NextMembershipTier.ivls__pointsfrom__c;
    }else{
        currentPointsTo =  memberObj.CurrentMembershipTier.ivls__pointsfrom__c;
    }
    var currentPointsKeep = memberObj.CurrentMembershipTier.ivls__pointsfrom__c;


    querystring = "(SELECT 'type1' as f0,  COALESCE(SUM(t.ivls__pointsredeeming__c), 0) as f1, COALESCE(SUM(t.ivls__pointsnet__c), 0) as f2, 0 as f3 FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__status__c LIKE '%Complete%' AND t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c+") UNION ALL (SELECT 'type2' as f0, COALESCE(SUM(t.ivls__spendingexclusive__c), 0) as f1, COALESCE(SUM(t.ivls__pointsearned__c), 0) as f2, COALESCE(SUM(t.ivls__pointsredeemed__c), 0) as f3 FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE  t.ivls__status__c LIKE '%Complete%' AND t.ivls__transactiondate__c > '"+getSQLDateTimeFormatWithSeconds(FYS)+"' AND t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c+") UNION ALL (SELECT 'type3' as f0, COALESCE(SUM(t.ivls__pointsearned__c), 0) as f1, COALESCE(SUM(t.ivls__spendingexclusive__c), 0) as f2, COALESCE(SUM(t.ivls__pointsredeemed__c), 0) as f3 FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE  t.ivls__status__c LIKE '%Complete%' AND t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c+" AND t.ivls__transactiondate__c > a.ivls__datetiertime__c);"
    var tempObj = (await query_one_way(pool, querystring)).rows;


    var Runningobj = tempObj[0];
    newMemberObj.ivls__pointsredeeming__pc =Runningobj.f1;
    newMemberObj.ivls__pointsbalance__pc =Runningobj.f2;
    if(newMemberObj.ivls__pointsbalance__pc < 0){
        newMemberObj.ivls__pointsbalance__pc = 0;
    }


    var YTDobj = tempObj[1];
    newMemberObj.ivls__spendingytd__pc =YTDobj.f1;
    newMemberObj.ivls__pointsearnedytd__pc =YTDobj.f2;
    newMemberObj.ivls__pointsredeemedytd__pc =YTDobj.f3;


    var SLRobj = tempObj[2];
    newMemberObj.ivls__pointsearnedsincelastrenewal__pc= SLRobj.f1;
    newMemberObj.ivls__spendingsincelastrenewal__pc = SLRobj.f2;
    newMemberObj.ivls__pointsredeemedsincelastrenewal__pc  = SLRobj.f3;
    var pointsEarnedSinceLastRenewal = SLRobj.f1;
    if(pointsEarnedSinceLastRenewal < 0){
        pointsEarnedSinceLastRenewal = 0;
    }



    // querystring = "SELECT COALESCE(SUM(t.ivls__pointsredeeming__c), 0) as ivls__pointsredeeming__c, COALESCE(SUM(t.ivls__pointsnet__c), 0) as ivls__pointsnet__c FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__status__c LIKE '%Complete%' AND t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c;
    // var Runningobj = (await query_one_way(pool, querystring)).rows[0];
    // newMemberObj.ivls__pointsredeeming__pc =Runningobj.ivls__pointsredeeming__c;
    // newMemberObj.ivls__pointsbalance__pc =Runningobj.ivls__pointsnet__c;
    // if(newMemberObj.ivls__pointsbalance__pc < 0){
    //     newMemberObj.ivls__pointsbalance__pc = 0;
    // }

    // querystring = "SELECT COALESCE(SUM(t.ivls__spendingexclusive__c), 0) as ivls__spendingexclusive__c, COALESCE(SUM(t.ivls__pointsearned__c), 0) as ivls__pointsearned__c, COALESCE(SUM(t.ivls__pointsredeemed__c), 0) as ivls__pointsredeemed__c FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE  t.ivls__status__c LIKE '%Complete%' AND t.ivls__transactiondate__c > '"+getSQLDateTimeFormatWithSeconds(FYS)+"' AND t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c;
    // var YTDobj = (await query_one_way(pool, querystring)).rows[0];
    // newMemberObj.ivls__spendingytd__pc =YTDobj.ivls__spendingexclusive__c;
    // newMemberObj.ivls__pointsearnedytd__pc =YTDobj.ivls__pointsearned__c;
    // newMemberObj.ivls__pointsredeemedytd__pc =YTDobj.ivls__pointsredeemed__c;

    // querystring = "SELECT COALESCE(SUM(t.ivls__pointsearned__c), 0) as pointsearnedslr, COALESCE(SUM(t.ivls__spendingexclusive__c), 0) as spendingslr, COALESCE(SUM(t.ivls__pointsredeemed__c), 0) as redeemedslr FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE  t.ivls__status__c LIKE '%Complete%' AND t.ivls__memberpgid__c = "+oldMemberObj.ivls__pgid__c+" AND t.ivls__transactiondate__c > a.ivls__datetiertime__c;"
    // var SLRobj = (await query_one_way(pool, querystring)).rows[0];
    // newMemberObj.ivls__pointsearnedsincelastrenewal__pc= SLRobj.pointsearnedslr;
    // newMemberObj.ivls__spendingsincelastrenewal__pc = SLRobj.spendingslr;
    // newMemberObj.ivls__pointsredeemedsincelastrenewal__pc  = SLRobj.redeemedslr;
    // var pointsEarnedSinceLastRenewal = SLRobj.pointsearnedslr;
    // if(pointsEarnedSinceLastRenewal < 0){
    //     pointsEarnedSinceLastRenewal = 0;
    // }
  
    var currentPointsTo = memberObj.CurrentMembershipTier.ivls__pointsto__c;
    var currentPointsKeep = memberObj.CurrentMembershipTier.ivls__pointsfrom__c;
    var currentTierName = memberObj.CurrentMembershipTier.name;
    var nextTierName = memberObj.NextMembershipTier.name;

    if(memberObj.hasNextMembershipTier){
        currentPointsTo =  memberObj.NextMembershipTier.ivls__pointsfrom__c;
    }else{
        currentPointsTo =  memberObj.CurrentMembershipTier.ivls__pointsfrom__c;
    }
    var enoughtPoint = false;
    if(pointsEarnedSinceLastRenewal > Number(currentPointsTo)){
        enoughtPoint = true;
    }


    console.log('pointsEarnedSinceLastRenewal:'+ pointsEarnedSinceLastRenewal);
    console.log('currentPointsTo:'+ currentPointsTo);
    console.log('currentTierName:'+ currentTierName);
    console.log('nextTierName:'+ nextTierName);


    var newPointsToNextTier = currentPointsTo - pointsEarnedSinceLastRenewal;
    var newPointsTokeepTier = currentPointsKeep - pointsEarnedSinceLastRenewal;

    var levelUp = false;
    if(enoughtPoint == true ){
        if(memberObj.hasNextMembershipTier){
            levelUp = true;
        }
    }
    console.log('levelUp:'+ levelUp);
    if(levelUp){
       

       
        //// multitier upgrade ////

        // var currentSequence =  memberObj.CurrentMembershipTier.ivls__sequence__c;//10
        // var currentPoints = pointsEarnedSinceLastRenewal;//60000
        // var DESC_array = membershiptiers_list;
        // DESC_array.reverse();
        // for(var u=0;u<DESC_array.length;u++){
        //     if( DESC_array[u].ivls__sequence__c > currentSequence){//20 > 10

        //         if(DESC_array[u].sfid !== maxTierSFID){
        //             var thisTierFrom = DESC_array[u].ivls__pointsfrom__c;//20000
        //             // var thisTierTo = DESC_array[u].ivls__pointsto__c;//50000

        //             if(currentPoints > thisTierFrom){
        //                 // var diff = thisTierTo - thisTierFrom//50000-20000
        //                 currentPoints = currentPoints - thisTierFrom;
        //                 continue;
        //             }else{
        //                 // newTierSFID = DESC_array[u].sfid;
        //                 newTierObj = DESC_array[u];
        //                 break;
        //             }    
        //         }else{
        //             newTierObj = DESC_array[u];
        //         }
               
        //     }else{
        //         continue;
        //     }
        // }
        var newTierObj = {};
        newTierObj = memberObj.NextMembershipTier;
        newMemberObj.ivls__membershiptier__pc = "'"+newTierObj.sfid+"'";
        newMemberObj.ivls__dateexpired__pc = "NOW() -interval'1 day' + interval '"+newTierObj.ivls__mexpirationdate1y__c+" year'";
        newMemberObj.ivls__datetier__pc = "NOW()::date";
        newMemberObj.ivls__datetiertime__c = "NOW() -interval '8 hour'";


        // if(transactiondatetime == "NOW()"){
        //     // transactiondatetime == "NOW() - interval '8 hour'";
        //     newMemberObj.ivls__datetier__pc = "(to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS'))::timestamp";
        //     newMemberObj.ivls__datetiertime__c = "(to_char(NOW() - interval '8 hours', 'YYYY-MM-DD HH24:MI:SS'))::timestamp";
        // }else{

        //     if(isNotNull(upgradeTransactionPGID)){
        //         transactionalUpgradeObjquerystring = "UPDATE loyaltycore.account  SET ivls__datetier__pc = (SELECT ((to_char(ivls__transactiondate__c, 'YYYY-MM-DD HH24:MI:SS'))::timestamp + interval '8 hour')::date FROM loyaltycore.ivls__transaction__c WHERE id = "+upgradeTransactionPGID+" LIMIT 1), ivls__datetiertime__c= (SELECT (to_char(ivls__transactiondate__c, 'YYYY-MM-DD HH24:MI:SS'))::timestamp FROM loyaltycore.ivls__transaction__c WHERE id = "+upgradeTransactionPGID+" LIMIT 1) WHERE id in ('"+oldMemberObj.id+"');"
        //         // transactionalUpgradeObjquerystring = "UPDATE loyaltycore.account  SET ivls__datetier__pc = (SELECT ivls__transactiondate__c::date FROM loyaltycore.ivls__transaction__c WHERE id = "+upgradeTransactionPGID+" LIMIT 1), ivls__datetiertime__c= (SELECT ivls__transactiondate__c FROM loyaltycore.ivls__transaction__c WHERE id = "+upgradeTransactionPGID+" LIMIT 1) WHERE id in ('"+oldMemberObj.id+"');"
        //         console.log('transactionalUpgradeObjquerystring:');
        //         console.log(transactionalUpgradeObjquerystring);
        //         var transactionalUpgradeObj = (await query_one_way(pool, transactionalUpgradeObjquerystring));
        //         console.log('transactionalUpgradeObj:');
        //         console.log(transactionalUpgradeObj);
        //     }else{
        //         newMemberObj.ivls__datetier__pc = AddSingleQuoteSymbol(transactiondatetime);
        //         newMemberObj.ivls__datetiertime__c = AddSingleQuoteSymbol(transactiondatetime);
        //     }
        // }
       
        newPointsToNextTier = newTierObj.ivls__pointsto__c;
        newPointsTokeepTier = newTierObj.ivls__pointsfrom__c;
        newMemberObj.ivls__spendingsincelastrenewal__pc = 0;
        newMemberObj.ivls__pointsearnedsincelastrenewal__pc = 0;
        newMemberObj.ivls__pointsredeemedsincelastrenewal__pc = 0;
        newMemberObj.ivls__syscurrenttiersequence__pc = newTierObj.ivls__sequence__c;
        
    }else{
        ////Not upgrade ////
        console.log('beforeupdatePTK:'+ beforeupdatePTK);
        console.log('newPointsTokeepTier:'+ newPointsTokeepTier);
        
        if(isNotNull(beforeupdatePTK)){
            //// before earnpoints PTK is not 0, after earnpoint PTK is 0 (this moment become keep tier)
            //// extent expiry date to +12 M -1 D
            if(beforeupdatePTK !== 0 && newPointsTokeepTier<=0){
                var M12 = 1;

                if(memberObj.CurrentMembershipTier.ivls__mexpirationdate1y__c){
                    M12 = memberObj.CurrentMembershipTier.ivls__mexpirationdate1y__c;
                }
                // newMemberObj.ivls__datetier__pc = "NOW()::date";
                newMemberObj.ivls__dateexpired__pc = "NOW() -interval'1 day' + interval '"+M12+" year'";

                // querystring = "UPDATE loyaltycore.ivls__membershiphistory__c SET ivls__datetimeto__c = (to_char((NOW()-interval '1 day' + interval '1 year'),  'YYYY-MM-DD 15:59:59'))::timestamp where ivls__pgid__c = (SELECT ivls__pgid__c FROM loyaltycore.ivls__membershiphistory__c WHERE ivls__member__c =(select id from loyaltycore.account where ivls__pgid__c = "+targetMemberPGID+" LIMIT 1) order by ivls__pgid__c desc limit 1);";
                // console.log("updatememberhistorySQL:");
                // console.log(querystring);
                // var result = (await query_one_way(pool, querystring));
            }
        }
        

    }
    if(newPointsTokeepTier < 0 ){
        newPointsTokeepTier = 0;
    }
    if(newPointsToNextTier < 0 || isNotNull(newPointsToNextTier)==false){
        newPointsToNextTier = 0;
    }
   
    if(memberObj.hasNextMembershipTier== false){
        newPointsToNextTier = 0;
    }

    console.log('newPointsToNextTier:'+ newPointsToNextTier);
    console.log('newPointsTokeepTier:'+ newPointsTokeepTier);

    newMemberObj.ivls__pointstonexttier__c = isNaN(newPointsToNextTier)?0:newPointsToNextTier;
    newMemberObj.ivls__pointstonexttier__pc = isNaN(newPointsToNextTier)?0:newPointsToNextTier;
    newMemberObj.ivls__pointstokeeptier__c = isNaN(newPointsTokeepTier)?0:newPointsTokeepTier;

    
    // console.log("newMemberObj:");
    // console.log(newMemberObj);



    querystring = getSQL_Update(newMemberObj, dbname, 'account', 'ivls__pgid__c', oldMemberObj.ivls__pgid__c);
    // console.log("update member SQL:");
    // console.log(querystring);
    return querystring;
}
function getCurrentMemberInfo(Account, membershiptiers_list){
    var memberObj={
        Account:{},
        CurrentMembershipTier:{},
        NextMembershipTier:{},
        PrevMembershipTier:{},
        isInMemberList:false,
        hasNextMembershipTier:false,
        hasPrevMembershipTier:false,
    }
    
    memberObj.Account = Account;
    memberObj.isInMemberList = true;
  
    for (var j=0;j<membershiptiers_list.length;j++){
        if(membershiptiers_list[j].ivls__membership__c == memberObj.Account.ivls__membership__pc){
            if(membershiptiers_list[j].sfid == memberObj.Account.ivls__membershiptier__pc ){
                memberObj.CurrentMembershipTier = membershiptiers_list[j];
                break;
            }
        }
    }



    //get prev tier's info
    for (var j=0;j<membershiptiers_list.length;j++){
        if(membershiptiers_list[j].ivls__membership__c == memberObj.Account.ivls__membership__pc){
            if(membershiptiers_list[j].ivls__ignorepointsearn__c !== true){
                if(membershiptiers_list[j].ivls__sequence__c < memberObj.CurrentMembershipTier.ivls__sequence__c){
                    memberObj.PrevMembershipTier = membershiptiers_list[j];
                    memberObj.hasPrevMembershipTier = true;
                    break;
                }
            }
        }
    }
  
    var AES_array = [];
    for (var j=0;j<membershiptiers_list.length;j++){
        if(membershiptiers_list[j].ivls__membership__c == memberObj.Account.ivls__membership__pc){
            AES_array.push(membershiptiers_list[j].ivls__sequence__c);
        }
    }

    var DESC_array = AES_array;
    DESC_array.reverse();

    var reverseed_membershiptiers_list = [];
    for(var i=0;i<DESC_array.length;i++){
        for (var j=0;j<membershiptiers_list.length;j++){
            if(membershiptiers_list[j].ivls__sequence__c == DESC_array[i]){
                reverseed_membershiptiers_list.push(membershiptiers_list[j]);
            }
        }
    }

    for (var j=0;j<reverseed_membershiptiers_list.length;j++){
        if(reverseed_membershiptiers_list[j].ivls__membership__c == memberObj.Account.ivls__membership__pc){
            if(reverseed_membershiptiers_list[j].ivls__ignorepointsearn__c !== true){
                if(reverseed_membershiptiers_list[j].ivls__sequence__c > memberObj.CurrentMembershipTier.ivls__sequence__c){
                    memberObj.NextMembershipTier = reverseed_membershiptiers_list[j];
                    memberObj.hasNextMembershipTier = true;
                    break;
                }
            }   
        }
    }
    
    return memberObj;
}