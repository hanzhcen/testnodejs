const router = require('./router')


//define sql elements
const dbname = router.getDBname();
var tablename = '';
var querystring = '';
var querystring_list = [];


var enableGenTransaction = true;


module.exports = {

    gentransactions:function(processDate,environment, log_level){
        return gentransactions (processDate,environment, log_level);
    },
    
};
async function gentransactions (processDate,environment, log_level) {
    try{
    
        var membershiptiers_list = await router.getMembershiptier();
        var yearstart = await router.getMembershipYearStart();
        var recordtype_list = await router.selectALL('recordtype');
        var txMapping_list = await router.selectALL('ivls__txmapping__c');
       
        console.log("membershiptiers_list:"+membershiptiers_list.length) 
        console.log("yearstart:"+yearstart) 
        console.log("recordtype_list:"+recordtype_list.length)
        console.log("txMapping_list:"+txMapping_list.length) 
    
        

        //// if run specific transactionlog ////
        // querystring =  "SELECT  * FROM loyaltycore.ivls__transactionlog__c WHERE (LOWER(ivls__transactiontype__c) = 'pos' OR LOWER(ivls__transactiontype__c) = 'eshop') AND LOWER(ivls__status__c) = 'open'  AND id in  (47460) AND createddate>='2018-08-22' ORDER BY id;";
        
        
        //// if run all open transactionlog ////
        querystring =  "SELECT  * FROM loyaltycore.ivls__transactionlog__c WHERE (LOWER(ivls__transactiontype__c) = 'pos' OR LOWER(ivls__transactiontype__c) = 'eshop') AND LOWER(ivls__status__c) = 'open' ORDER BY id DESC ;";
       
       
        var transactionlog_list = (await router.query_one_way(querystring)).rows;
        console.log("transactionlog_list:"+transactionlog_list.length);
        var totalNum = transactionlog_list.length;
        var currentNum = 0;


        if(enableGenTransaction){
            for(var i=0; i<transactionlog_list.length;i++){
                var pertransactionQuery = [];
                try{
                    try{
                        //// parse JSON ////
                        var JSON_ivls__detail__c =  transactionlog_list[i].ivls__detail__c;
                        var ivls__detail__c = JSON.parse(JSON_ivls__detail__c);
                        

                        //// check whether this transactionlog is include in loyalty    ////
                        var LoyaltyInclude = ivls__detail__c.LoyaltyInclude;
                        var JSONisValid = true;
                        if(router.isNotNull(LoyaltyInclude)==false|| LoyaltyInclude !== true){
                            JSONisValid = false;
                        }
                        
                    }catch(e){
                        console.log("current position: "+currentNum+ "/"+ totalNum);
                        console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                        console.log('Error Reading JSON:transactionlog_list[i].sfid:'+ transactionlog_list[i].sfid);
                        console.log('Error Message:');
                        console.log(e);
                        querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid )));
                        querystring_list.push(querystring);

                        var tempStr = 'Error Reading JSON:transactionlog_list[i].sfid:'+ transactionlog_list[i].sfid;
                        console.log(tempStr);
                       
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [ivls__transactionlog__c, transactionlog_list[i].sfid, tempStr, e.toString()];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }

                        continue;
                    }

                    if(JSONisValid == false){
                        console.log('This transaction log JSONisValid == false:'+ transactionlog_list[i].name);
                        querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Exclusive'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid ))); 
                        querystring_list.push(querystring);
                        continue;
                    }   
                    

                    //// map JSON with txmapping ////
                    var mappingObj = router.getMappingObj(ivls__detail__c, txMapping_list, 'POS');
                    var statusTran = "Open";


                    //// given a status on transaction  ////
                    if(mappingObj.ivls__quantity__c < 0 ){
                        statusTran = "Refund Open";
                    }

                    //// display current status ////
                    currentNum = currentNum +1;
                    console.log("progress: "+currentNum+"/"+totalNum);


                    //// prevent header transactionlog with 0 QTY become transaction ////
                    if(mappingObj.ivls__quantity__c == 0 ){
                        console.log('This transaction log mappingObj.ivls__quantity__c == 0:'+ transactionlog_list[i].name);
                        querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Exclusive'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid )));
                        querystring_list.push(querystring);
                        continue;
                    }


                    var ivls__datetx__c = mappingObj.ivls__receiptdate__c;
                    var ivls__transactiontype__c =  transactionlog_list[i].ivls__transactiontype__c;


                    //// extract member's profile ////
                    var CurrentMemberSFID = mappingObj.ivls__member__c;
                    var member_list = await router.selectALL_with_parameter('account','id','=', router.AddSingleQuoteSymbol(CurrentMemberSFID));
                    // console.log('memberObj:');
                    // console.log(memberObj);


                    //// check whether this member is in Database ////
                    if(router.isNotNull(member_list)== false || member_list.length == 0||router.isNotNull(member_list[0]) == false){
                        console.log("current position: "+currentNum+ "/"+ totalNum);
                        console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                        console.log('Error Message:');
                        console.log('This member is not in our system!!!'+CurrentMemberSFID);
                        querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid )));
                        querystring_list.push(querystring);


                        var tempStr = 'This member is not in our system!!!'+CurrentMemberSFID
                        console.log(tempStr);
                       
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [ivls__transactionlog__c, transactionlog_list[i].sfid, "gentx error", tempStr];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }



                        continue;
                    }

                    //// map member profile with membershiptier ////
                    var memberObj= router.getCurrentMemberInfo(member_list[0], membershiptiers_list);
                    var CurrentMembershipTier = memberObj.CurrentMembershipTier;
                    var TierSpending = CurrentMembershipTier.ivls__spending__c;
                    var TierPoints =  CurrentMembershipTier.ivls__tpoints__c;
                
                    var targetMemberPGID =  memberObj.Account.ivls__pgid__c;
                    var targetMemberSFID =  memberObj.Account.id;


                    // ////    check blacklisted member    ////
                    // var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
                    // if(currentMemberTierIsBlacklisted !== true){
                    //     if(router.isNotNull(TierSpending) == false){
                    //         console.log("current position: "+currentNum+ "/"+ totalNum);
                    //         console.log('Missing TierSpending'+ TierSpending);
                    //         querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid ))); 
                    //         pertransactionQuery.push(querystring);
                    //         continue;
                    //     }
                    //     if(router.isNotNull(TierPoints) == false){
                    //         console.log("current position: "+currentNum+ "/"+ totalNum);
                    //         console.log('Missing TierPoints'+ TierPoints);
                    //         querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid )));
                    //         pertransactionQuery.push(querystring); 
                    //         continue;
                    //     }    
                    // }
                    


                    //// if first purchase, set ivls__datefirstpurchaseatdtp__pc = NOW() ////
                    if(router.isNotNull(memberObj.Account.ivls__datefirstpurchaseatdtp__pc) == false){
                        memberObj.Account.ivls__datefirstpurchaseatdtp__pc = new Date();
                        if(processDate == "T+1"){
                            querystring = "UPDATE loyaltycore.account  SET ivls__datefirstpurchaseatdtp__pc = NOW() - interval '1 day' WHERE ivls__pgid__c = " + targetMemberPGID;
                        }else{
                            querystring = "UPDATE loyaltycore.account  SET ivls__datefirstpurchaseatdtp__pc = NOW() WHERE ivls__pgid__c = " + targetMemberPGID;
                        }
                        console.log('DTP targetMemberSFID'+targetMemberSFID);
                        console.log(querystring);

                        if(environment == "production"){
                            var updateSTP = await router.query_one_way(querystring);
                            console.log('DTP update result:');
                            console.log(updateSTP);
                        }
                       
                      
                    }



                    //// INSERT transaction ////
                    var neededTranType = 'Spending';
                    var tablename = 'ivls__transaction__c';
                    
                    querystring = genInsertTranSQL_line(txMapping_list, mappingObj, dbname, tablename, recordtype_list, ivls__transactiontype__c, ivls__datetx__c, statusTran, targetMemberPGID, CurrentMembershipTier)
                    pertransactionQuery.push(querystring);
                    
                    querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Complete'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid )));
                    pertransactionQuery.push(querystring);
                   
                }catch(e){
                    console.log("current position: "+currentNum+ "/"+ totalNum);
                    console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                    console.log('Error Process: transactionlog_list[i].sfid:'+ transactionlog_list[i].sfid);
                    console.log('Error Message:');
                    console.log(e);
                    querystring = markStatus(dbname, 'ivls__transactionlog__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionlog_list[i].sfid )));
                    querystring_list.push(querystring);


                    var tempStr = 'Error Reading JSON:transactionlog_list[i].sfid:'+ transactionlog_list[i].sfid;
                    console.log(tempStr);
                    
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = [ivls__transactionlog__c, transactionlog_list[i].sfid, tempStr, e.toString()];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }

                }
                

                try{
                    console.log(pertransactionQuery);
                    console.log("pertransactionQuery:"+pertransactionQuery.length);
            
                    if(pertransactionQuery.length>0){
                        var totalQueryStr='';
                        for (var q=0;q<pertransactionQuery.length;q++){
                            if(q == 0){
                                totalQueryStr =  totalQueryStr + pertransactionQuery[q];
                            }else{
                                totalQueryStr =  totalQueryStr + ' ; '+ pertransactionQuery[q];
                            }
                        
                        }
            
                        if(environment == 'production'){
                            try{
                                var result= await router.query_one_way(totalQueryStr);
                                console.log('Process pertransactionQuery successful !!!');
                                console.log(result);
                            }catch (err) {
                                console.log('Process pertransactionQuery fail !!!')
                                console.log(err)
                                return false;
                            }
                        }
                    }
                }catch(e){
                    console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                    console.log('Error Process: error running pertransactionQuery:'+transactionlog_list[i].sfid);
                    console.log(pertransactionQuery);
                    console.log('Error Message:');
                    console.log(e);
                }
                
                
                
            }

            
            try{
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
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log('Error Message:');
                console.log(e);
            }
            
        }

        
        console.log("////////////////////////////// COMPLETED //////////////////////////////") 
    }catch (err) {
        console.log('Overall error ' + err)
    }

}

function markStatus(dbname, tablename, status, sfid){
    querystring = 'UPDATE '+ dbname+tablename + ' SET ivls__status__c = '+status+' WHERE sfid = ' +sfid; 
    return querystring;
}
function genInsertTranSQL_line(txMapping_list,mappingObj, dbname,tablename, recordtype_list, ivls__transactiontype__c, ivls__datetx__c, statusTran, pgid,CurrentMembershipTier){
    var neededTranType = "Spending";
    var rawObj = {};
    for(var i=0;i<txMapping_list.length;i++){
        if(txMapping_list[i].ivls__object__c =='POS'){
            var lowercaseStr = (txMapping_list[i].ivls__targetfield__c).toLowerCase();
            rawObj[lowercaseStr] = "";
        }
    }

    var querystring ='';
    var InsertObj = rawObj;
    var currentObj = mappingObj;

    for (x in InsertObj){
        for (y in currentObj){
            if(router.isNotNull(y) && x == y){
                InsertObj[x] = currentObj[y];
            }
        }
    }

    for (x in InsertObj){
        InsertObj[x] = router.AddSingleQuoteSymbol(InsertObj[x]);
    }

    InsertObj.ivls__transactiondate__c = "NOW() - interval '8 hours'";
    // InsertObj.ivls__transactiondate__c = "(to_char(NOW() - interval '8 hours', 'YYYY-MM-DD HH24:MI:SS'))::timestamp";
   
    InsertObj.createddate = 'NOW()';
    InsertObj.lastmodifieddate = 'NOW()';
    InsertObj.ivls__source__c = '\'System\'';
    InsertObj.ivls__memberpgid__c = pgid;
    
    InsertObj.ivls__status__c = router.AddSingleQuoteSymbol(statusTran);
    InsertObj.ivls__transactiontype__c = router.AddSingleQuoteSymbol(ivls__transactiontype__c);
    InsertObj.ivls__pointspool__c = router.AddSingleQuoteSymbol(neededTranType);
    InsertObj.ivls__tier__pc = router.AddSingleQuoteSymbol(CurrentMembershipTier.sfid);
    InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Transaction__c',neededTranType, recordtype_list));

    var lineTotal = 0;
    InsertObj.ivls__pointsearned__c = lineTotal;
    InsertObj.ivls__pointsnet__c = lineTotal;
    
    querystring = 'INSERT INTO '+dbname+tablename+' (';
    var count = 0;
    var fieldsstr = ''
    for (x in InsertObj ){
        if(InsertObj[x] !== '\'\''){ //is NOT '', 0, null, or undefined
            if(count == 0){
                fieldsstr =  fieldsstr + x;
            }else{
                fieldsstr =  fieldsstr + ', ' + x;
            }
            count = count +1;
        }
    }
    count = 0;
    querystring = querystring +fieldsstr +' ) VALUES ( ';
    var valuesStr = '';
    for (x in InsertObj){
        if(InsertObj[x] !== '\'\''){ //is NOT '', 0, null, or undefined
            if(count == 0){
                valuesStr =  valuesStr + InsertObj[x];
            }else{
                valuesStr =  valuesStr + ', ' + InsertObj[x];
            }
            count = count +1;
        }
    }
    querystring = querystring +valuesStr +' )';
    return querystring;
}
