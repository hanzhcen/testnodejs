const pg = require('pg');
const router = require('../lib/router.js')
const updatemember = require('../lib/updatemember.js')

//define sql elements
const dbname = router.getDBname();
var tablename = '';
var querystring = '';
var querystring_list = [];
var allopen = false;

////        SWITCH      ////
var enableTransactionPart = false;
var enablePointSchemePart = false;
var enableCampaignPart = true;
var enableRedund = false;


allopen = true;
if(allopen){
    enableTransactionPart = true;
    enablePointSchemePart = true;
    enableCampaignPart = true;
    enableRedund = true;
}

module.exports = {

    calculatepoints:function(pool, processDate,environment, log_level){
        return calculatepoints (pool, processDate,environment,log_level);
    },
    
};
async function calculatepoints (pool, processDate,environment, log_level ) {
    try{


        querystring = "SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = 'MySony'";
        var tempList = (await router.query_one_way(querystring)).rows
        if(router.isNotNull(tempList)==false||tempList.length==0){
            console.log("Cannot Find Membershp Where ivls__membershipcode__c = 'MySony'") 
            return;
        }
        var membershipObj =  tempList[0];
        var yearstart = membershipObj.ivls__yearstart__c;//0104

        
    
        var membershiptiers_list = await router.getMembershiptier();
        var recordtype_list = await router.selectALL('recordtype');
        var tempList = recordtype_list;
        if(router.isNotNull(tempList)==false||tempList.length==0){
            console.log("Cannot Find Select recordtype") 
            return;
        }
        
        querystring = "SELECT  t.ivls__product__c as sfid, (SELECT name FROM loyaltycore.ivls__product__c where sfid = t.ivls__product__c) as name, (SELECT ivls__productcategoryl1__c FROM loyaltycore.ivls__product__c where sfid = t.ivls__product__c) as ivls__productcategoryl1__c, (SELECT ivls__productcategoryl2__c FROM loyaltycore.ivls__product__c where sfid = t.ivls__product__c) as ivls__productcategoryl2__c,(SELECT ivls__productcategoryl3__c FROM loyaltycore.ivls__product__c where sfid = t.ivls__product__c) as ivls__productcategoryl3__c FROM loyaltycore.ivls__transaction__c t  WHERE t.ivls__status__c = 'Open' AND t.ivls__product__c IS NOT NULL GROUP BY t.ivls__product__c;";
        var productCate_list = (await router.query_one_way(querystring)).rows;
        
        
        //// select active pointscheme and campaign ////
        if(processDate == "T+1"){

            //select active PointScheme
            querystring =  "SELECT * FROM loyaltycore.ivls__pointsscheme__c WHERE DATE_PART('day', NOW() - ivls__effectivefrom__c ) >=1 AND (ivls__effectiveto__c IS NULL OR DATE_PART('day', NOW() - ivls__effectiveto__c ) <=1);"
            var pointsscheme_list = (await router.query_one_way(querystring)).rows;

            //select active Campaign
            querystring =  "SELECT * FROM loyaltycore.campaign WHERE ivls__stage__c = 'Approved' AND DATE_PART('day', NOW() - startdate ) >= 1 AND (enddate IS NULL OR DATE_PART('day', NOW() - enddate ) <= 1);";
            var campaign_list = (await router.query_one_way(querystring)).rows;

        }else{//default as T+0

            //select active PointScheme
            querystring =  "SELECT * FROM loyaltycore.ivls__pointsscheme__c WHERE DATE_PART('day', NOW() - ivls__effectivefrom__c ) >=0 AND (ivls__effectiveto__c IS NULL OR DATE_PART('day', NOW() - ivls__effectiveto__c ) <=0);"
            var pointsscheme_list = (await router.query_one_way(querystring)).rows;

            //select active Campaign
            querystring =  "SELECT * FROM loyaltycore.campaign WHERE ivls__stage__c = 'Approved' AND DATE_PART('day', NOW() - startdate ) >=0  AND (enddate IS NULL OR DATE_PART('day', NOW() - enddate ) <=0);";
            var campaign_list = (await router.query_one_way(querystring)).rows;

        }
        
        //// retrieve exclusive campaigns ////
        var exclusiveCampaign_list = [];
        for(var i=0;i<campaign_list.length;i++){
            var recordtypeid_1 = router.getRecordTypeId('Campaign','Exclusion', recordtype_list);
            var recordtypeid_2 = campaign_list[i].recordtypeid;
            if(recordtypeid_1 == recordtypeid_2){
                exclusiveCampaign_list.push(campaign_list[i])
            }
        }


        console.log("membershiptiers_list:"+membershiptiers_list.length) 
        console.log("yearstart:"+yearstart) 
        console.log("recordtype_list:"+recordtype_list.length)
        console.log("pointsscheme_list:"+pointsscheme_list.length) 
        console.log("campaign_list:"+campaign_list.length) 
        
        
        try{
            if(enableTransactionPart){//section1
                var part1_result = await part1(log_level, pool, yearstart, environment, membershiptiers_list, pointsscheme_list, recordtype_list, exclusiveCampaign_list, processDate, membershipObj, productCate_list);
                console.log("part1_result: "+part1_result);
            }
        }catch(e){
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'part1 error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }


        try{
            if(enablePointSchemePart){//section2
                var part2_result = await part2(log_level, pool, yearstart, environment, membershiptiers_list, pointsscheme_list, recordtype_list, processDate, membershipObj, productCate_list);
                console.log("part2_result: "+part2_result);
            }
        }catch(e){
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'part2 error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }

        try{
            if(enableCampaignPart){//section3
                var part3_result = await part3(log_level, pool, yearstart, environment, membershiptiers_list, campaign_list, recordtype_list, processDate, dbname, membershipObj, productCate_list);
                console.log("part3_result: "+part3_result);
            }
        }catch(e){
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'part3 error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }

        try{
            if(enableRedund){
                var part4_result = await part4(log_level, pool, environment, dbname,membershiptiers_list, recordtype_list, membershipObj);
                console.log("part4_result: "+part4_result);
            }
        }catch(e){
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['', '', 'part4 error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }
        }

        
        
        

        
        console.log("////////////////////////////// COMPLETED //////////////////////////////") 


    }catch (err) {
        console.log('Overall calculatepoints error ' + err)
        if(log_level == "debug"){
            var querystring_error_log = router.get_Querystring_error_log();
            var values = ['', '', 'Overall calculatepoints error', e.toString()];
            var result  = (await router.query_with_parameter(querystring_error_log, values));
            console.log(result);
        }
    }
       
   
}

async function part1(log_level, pool, yearstart, environment, membershiptiers_list, pointsscheme_list, recordtype_list, exclusiveCampaign_list, processDate, membershipObj, productCate_list){
    var querystring = "";
    var querystring_list = [];
    console.log("------------------------    SECTION 1: Basic    ------------------------");

    ////    Get Open Transaction    ////
    //for all member
    querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__status__c = 'Open' ORDER BY id ";

    //for specific member
    // querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__status__c = 'Open' and (ivls__memberpgid__c=1506 or ivls__memberpgid__c=1066 or ivls__memberpgid__c=1180 or ivls__memberpgid__c=27327 or ivls__memberpgid__c=24930 or ivls__memberpgid__c=22642) ORDER BY ivls__transactiondate__c";
    
    var transaction_list = (await router.query_one_way(querystring)).rows;
    if(router.isNotNull(transaction_list)==false || transaction_list.length == 0){
        console.log("No Open Transaction");
        return false;
    }


    // ////    Get Open Transaction's Member's Information    ////
    // querystring = "SELECT a.* FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__status__c = 'Open' ORDER BY t.ivls__transactiondate__c;";
    // var memberObj_list = (await router.query_one_way(querystring)).rows;
    // if(router.isNotNull(memberObj_list)==false || memberObj_list.length == 0){
    //     console.log("Return Cannot get open transaction's member info");
    //     return false;
    // }



    var receipt_list=[];
    //total level calculation
    var receipt_obj = {};
    for(var i=0;i<transaction_list.length;i++){
        var transactionObj = transaction_list[i];
        var receiptNo = transactionObj.ivls__receiptno__c
        receipt_obj[receiptNo] = transactionObj;
    }
    
    for (index in receipt_obj){
        receipt_list.push(receipt_obj[index]);
    }

    console.log("transaction_list:"+transaction_list.length) 
    console.log("receipt_list:"+receipt_list.length) 

    for(var r=0;r<receipt_list.length;r++){
       
        ////    pre-defiened some variables to show the result    ////
        var receipt_obj = receipt_list[r];
        console.log("@@@@@@@@@@@@@@@@@@@@@@@@   "+receipt_obj.ivls__receiptno__c+"  @@@@@@@@@@@@@@@@@@@@@@@@");
        var PointsTotal = 0;
        var PointsBasic = 0;
        var PointsBonus = 0;
 
        try{
            ////    get member information    ////
            var targetMemberPGID = receipt_obj.ivls__memberpgid__c;

            if(router.isNotNull(targetMemberPGID)==false){
                console.log("ivls__memberpgid__c in transaction is null:"+targetMemberPGID);
                querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
                console.log("querystring::");
                console.log(querystring);
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                continue;
            }

        

            var tempMemberObj = false;
            // for(var m=0;m<memberObj_list.length;m++){
            //     if(memberObj_list[m].ivls__pgid__c == targetMemberPGID){
            //         tempMemberObj = memberObj_list[m];
            //     }
            // }

            try{
                var member_list = await router.selectALL_with_parameter('account','ivls__pgid__c','=', targetMemberPGID);
                tempMemberObj = member_list[0];
            }catch(e){
                console.log("error in select member information:"+targetMemberPGID);
                console.log(e);
                querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
                console.log("querystring::");
                console.log(querystring);
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                continue;
            }


            if(tempMemberObj == false){
                console.log("Cannot match member in memberObj_list: "+targetMemberPGID);
                querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
                console.log("querystring::");
                console.log(querystring);
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                continue;
            }



            var oldPTK = "";

            try{
                querystring = "SELECT ivls__pointstokeeptier__c FROM loyaltycore.account WHERE ivls__pgid__c = " + targetMemberPGID;
                oldPTK =  ( await router.query_one_way(querystring)).rows[0].ivls__pointstokeeptier__c;
            }catch(e){
                querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
                console.log("querystring::");
                console.log(querystring);
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                continue;
            }
            


            var memberObj= router.getCurrentMemberInfo(tempMemberObj, membershiptiers_list);
            var CurrentMembershipTier = memberObj.CurrentMembershipTier;
            var currentSpending_totalLevel = receipt_obj.ivls__totalcashamount__c;//total spending

            // console.log("memberObj:") 
            // console.log(memberObj);
            var TierSpending = CurrentMembershipTier.ivls__spending__c;
            var TierPoints =  CurrentMembershipTier.ivls__tpoints__c;

            if(router.isNotNull(receipt_obj.ivls__transactiondate__c)== false){
                console.log("ivls__transactiondate__c is null :"+receipt_obj.ivls__transactiondate__c);
                querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
                console.log("querystring::");
                console.log(querystring);
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                continue;
            }
            

            ////    check blacklisted member    ////
            var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
            if(currentMemberTierIsBlacklisted == true){
                console.log("currentMemberTierIsBlacklisted:"+targetMemberPGID);
                querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Black List' WHERE ivls__status__c = 'Open' AND ivls__memberpgid__c = "+ targetMemberPGID;
                console.log("querystring::");
                console.log(querystring);
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                continue;
            }
        

            ////    calculate points by Total through PointScheme    ////
            if(pointsscheme_list.length>0){
                for(var j=0;j<pointsscheme_list.length;j++){
                    try{
                        var pointSchemeObj = pointsscheme_list[j];
                        if(pointSchemeObj.ivls__membershiptier__c !== CurrentMembershipTier.sfid){
                            continue;
                        }
                        //total
                        var recordtypeid_1 = router.getRecordTypeId('IVLS__PointsScheme__c','Total', recordtype_list);
                        if(router.isNotNull(recordtypeid_1)== false){
                            console.log("recordtype table missing Total record type of IVLS__PointsScheme__c:"+pointSchemeObj.id) 
                            continue;
                        }

                        var recordtypeid_0 = pointSchemeObj.recordtypeid;
                        if(router.isNotNull(recordtypeid_0)== false){
                            console.log("PointScheme missing recordtype:"+pointSchemeObj.id) 
                            continue;
                        }

                        if(recordtypeid_0 !== recordtypeid_1){
                            continue;
                        }

                        var pointsbucket = pointSchemeObj.ivls__pointsbucket__c;
                        if(router.isNotNull(pointsbucket)== false){
                            console.log("PointScheme missing pointsbucket:"+pointSchemeObj.id) 
                            continue;
                        }

                        
                        var referenceObject = pointSchemeObj.ivls__referenceobject__c;
                        if(router.isNotNull(referenceObject)== false){
                            console.log("PointScheme missing referenceObject:"+pointSchemeObj.id) 
                            continue;
                        }

                        if(referenceObject.toLowerCase() !== 'ivls__transaction__c'){
                            continue;
                        }

                        var referenceField = pointSchemeObj.ivls__field__c;
                        if(router.isNotNull(referenceField)== false){
                            console.log("PointScheme missing referenceField:"+pointSchemeObj.id) 
                            continue;
                        }
                        referenceField = referenceField.toLowerCase();

                        // var pointsPool ='Spending'; //Spending or A&P
                        var pointsPool = pointSchemeObj.ivls__pointspool__c;//Spending or A&P
                        if(router.isNotNull(pointsPool)== false){
                            console.log("PointScheme missing pointsPool:"+pointSchemeObj.id) 
                            continue;
                        }

                        var operator = pointSchemeObj.ivls__operator__c;
                        if(router.isNotNull(operator)== false){
                            console.log("PointScheme missing operator:"+pointSchemeObj.id) 
                            continue;
                        }

                        var isDateTime = false;
                        var datetimeArray = router.getDateTimeKeyword();
                        for(var h=0;h<datetimeArray.length;h++){
                            if(referenceField.includes(datetimeArray[h]) == true){
                                isDateTime = true;
                                break;
                            }
                        }

                        if(router.isNotNull(isDateTime) == false){
                            console.log("error in get isDateTime");   
                            continue;
                        }


                        //get originValue
                        var originValue = receipt_obj[referenceField];

                        //get targetValue
                        var targetValue = "";
                        var condition_fieldValue= pointSchemeObj.ivls__filtervalue__c ;
                        var condition_fieldFormula= pointSchemeObj.ivls__filter_formula__c ;
                        if (router.isNotNull(condition_fieldValue)){
                            targetValue = condition_fieldValue;
                        }else if (router.isNotNull(condition_fieldFormula) && router.isNotNull(condition_fieldValue) == false){
                            targetValue = getSQLtargetValue_total(condition_fieldFormula, processDate);
                        }else{
                            console.log("Cannot read ivls__filtervalue__c or ivls__filter_formula__c:"+pointSchemeObj.id);   
                            continue;
                        }


                        if(targetValue == "invalid"){
                            console.log("Cannot read formula:"+condition_fieldFormula);   
                            console.log("Cannot read formula:"+pointSchemeObj.id);   
                            continue;
                        }

                        
                        if(router.isNotNull(targetValue)== false){
                            console.log("PointScheme missing targetValue:"+pointSchemeObj.id) 
                            continue;
                        }
                        
                        //need to check is Spending!!!!!!!
                        console.log("-------------------------------------");
                        console.log("referenceObject="+referenceObject);
                        console.log("referenceField="+referenceField);
                        console.log("pointsPool="+pointsPool);   
                        console.log("isDateTime="+isDateTime);   
                        console.log("originValue="+originValue);   
                        console.log("operator="+operator);   
                        console.log("targetValue="+targetValue);   
                        console.log("-------------------------------------");
                    
                        var meetCondition = false;
                        // var today = new Date();
                        var today = transactiondatetime;
                        
                        if(isDateTime){
                            if(condition_fieldFormula == 'This Month' || condition_fieldFormula == 'Last Month' ||condition_fieldFormula == 'This Calendar Year' ){
                                if(today >= targetValue.firstDay && today <= targetValue.lastDay){
                                    meetCondition = true;
                                }
                            }else{
                                meetCondition = checkConfitionisMeet(originValue, operator, targetValue,isDateTime)
                            }
                        }else{
                            meetCondition = checkConfitionisMeet(originValue, operator, targetValue,isDateTime)
                        }

                        if(router.isNotNull(meetCondition)==false){
                            continue;
                        }

                        if(meetCondition !== true){
                            continue;
                        }   
                        
                        var calc_points= pointSchemeObj.ivls__points__c ;
                        var calc_pointsmultiple= pointSchemeObj.ivls__pointsmultiple__c ;
                    
                        console.log("calc_points="+calc_points);
                        console.log("calc_pointsmultiple="+calc_pointsmultiple);
                    

                        var earnedPoints = 0;
                        if(router.isNotNull(calc_points) == false && router.isNotNull(calc_pointsmultiple) == false){
                            console.log("PointScheme missing ivls__points__c and calc_pointsmultiple:"+pointSchemeObj.id) 
                            continue;
                        }else if (router.isNotNull(calc_points) && router.isNotNull(calc_pointsmultiple) == false){
                            earnedPoints = calc_points;
                        }else if (router.isNotNull(calc_pointsmultiple) && router.isNotNull(calc_points) == false){
                            earnedPoints = Number(calc_pointsmultiple) * currentSpending_totalLevel;
                        }else if(router.isNotNull(calc_points) && router.isNotNull(calc_pointsmultiple)){
                            earnedPoints = calc_points;
                        }

                        var lineActivityName = '';
                        var lineActivityId = '';
                        lineActivityId = pointSchemeObj.sfid;

                        if(router.isNotNull(pointSchemeObj.ivls__activity__c)){
                            lineActivityName =  pointSchemeObj.ivls__activity__c ;
                            lineActivityName = lineActivityName.replace(/'/g, "\'");
                     }

                        if(earnedPoints > 0){   
                            PointsTotal = PointsTotal + earnedPoints;
                            PointsBonus = PointsBonus + earnedPoints;
                            querystring = genInsertTranSQL_Total(pointsPool, router, receipt_obj, 'loyaltycore.','ivls__transaction__c',  CurrentMembershipTier, recordtype_list, earnedPoints, lineActivityName, targetMemberPGID);
                        
                            var querystring1 = querystring + " RETURNING id;";
                            console.log("total level insert transaction SQL :");
                            console.log(querystring1);
                        
                            
                            if(environment == "production"){
                                var client = await pool.connect();
                                try {
                                
                                    await client.query('BEGIN')
                                    var res  = await client.query(querystring1);
                                
                                    var transaction_id = [res.rows[0].id];
                                    var InsertObj = {};
                                    InsertObj.transaction_id = transaction_id;
                                    InsertObj.ivls__pointsbucket__c = pointsbucket;
                                    InsertObj.ivls__points__c = earnedPoints;
                                    InsertObj.ivls__activity__c = lineActivityName;
                                    InsertObj.ivls__ps_campain_id = lineActivityId;
                                    InsertObj.ref_type = "IVLS__PointsScheme__c";
                                    InsertObj.ivls__memberpgid__c = targetMemberPGID;
                                    InsertObj.ivls__pointspool__c = pointsPool;
                                    InsertObj.ivls__tier__pc = CurrentMembershipTier.sfid;
                                    var querystring2  = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', true);
                                
                                    await client.query(querystring2);
                                    if(environment == "production"){
                                        await client.query('COMMIT')
                                    }else{
                                        await client.query('ROLLBACK')
                                    }
                                }catch (e) {
                                    await client.query('ROLLBACK')
                                
                                    console.log("ROLLBACK=");   
                                    console.log(e);   
                                    throw e
                                }finally {
                                    client.release()
                                }
                            }


                        }
                        
                        
                        console.log("earnedPoints="+earnedPoints);   
                        console.log("========  End total level pointscheme   ======== ");   
                        
                    }catch(e){
                        console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                        console.log('Error Processing :pointsscheme_list[j].id:'+ pointsscheme_list[j].id);
                        console.log('Error Message:');
                        console.log(e);
                        continue;
                    }
                }
            }


            ////    calculate points by Line through PointScheme    ////
            for(var i=0;i<transaction_list.length;i++){
                try{
                    var transactionObj = transaction_list[i];

                
                    if(transactionObj.ivls__receiptno__c !== receipt_obj.ivls__receiptno__c){
                        continue;
                    }
                    
                    
                    var product_sfid = transactionObj.ivls__product__c;
                    var merchant = transactionObj.ivls__merchant__c;
                    var productCate = "";
                    if(router.isNotNull(product_sfid) == false ){
                        console.log("product_sfid is null::");
                        querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionObj.id )));
                        if(environment == "production"){
                            var markStatusResult = (await router.query_one_way(querystring));
                            console.log('markStatusResult :');
                            console.log(markStatusResult);
                        
                        }
                        continue;
                    }
                    
                    var  cate1 = "";
                    var  cate2 = "";
                    var  cate3 = "";
                    

                    try{
                        var productIsinList = false;
                        for(var p=0;p<productCate_list.length;p++){
                            if(productCate_list[p].sfid == product_sfid){
                                productCate = productCate_list[p];
                                productIsinList = true;
                                break;
                            }
                        }
                        if(productIsinList == false){
                            console.log("select productCate  error:") 
                            querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionObj.id )));
                            if(environment == "production"){
                                var markStatusResult = (await router.query_one_way(querystring));
                                console.log('markStatusResult :');
                                console.log(markStatusResult);
                            
                            }
                            continue;
                        }
                        // querystring = "SELECT * FROM loyaltycore.ivls__product__c WHERE sfid = '"+product_sfid+"'";
                        // productCate = (await router.query_one_way(querystring)).rows[0];
                        // if(router.isNotNull(productCate)== false){
                        //     console.log("select productCate  error:") 
                        //     continue;
                        // }
                        cate1 = productCate.ivls__productcategoryl1__c;
                        cate2 = productCate.ivls__productcategoryl2__c;
                        cate3 = productCate.ivls__productcategoryl3__c;
                    }catch(e){
                        console.log("productCate error:") 
                        console.log(e) 
                        querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transactionObj.id )));
                        if(environment == "production"){
                            var markStatusResult = (await router.query_one_way(querystring));
                            console.log('markStatusResult :');
                            console.log(markStatusResult);
                        
                        }
                        continue;
                    }
                    console.log("cate1:"+cate1) 
                    console.log("cate2:"+cate2) 
                    console.log("cate3:"+cate3) 

                    var currentTranObj={
                        "merchant":merchant,
                        "product_sfid":product_sfid,
                        "cate1":cate1,
                        "cate2":cate2,
                        "cate3":cate3
                    }

                    var isInExclusiveList = false;
                    for (var k=0;k<exclusiveCampaign_list.length;k++){
                        var targetMerchant = exclusiveCampaign_list[k].ivls__merchant__c;
                        var targetProduct = exclusiveCampaign_list[k].ivls__product__c;
                        var targetCate = exclusiveCampaign_list[k].ivls__productcategory__c;
                        var meetCondition = false;
                        console.log("targetProduct:"+targetProduct) 
                        if(router.isNotNull(targetMerchant)){
                            if(targetMerchant == merchant){
                                // console.log("membership unmatched");
                                var meetCondition = true;
                            }
                        }

                        if(router.isNotNull(targetProduct)){
                            if(targetProduct == product_sfid){
                                // console.log("membership unmatched");
                                var meetCondition = true;
                            }
                        }
                        if(router.isNotNull(targetCate)){
                            if(targetCate == cate1 || targetCate == cate2 || targetCate == cate3){
                                // console.log("membership unmatched");
                                var meetCondition = true;
                            }
                        }
                        
                        if(meetCondition == true){
                            isInExclusiveList = true;
                        }
                    }
                    console.log("isInExclusiveList:"+isInExclusiveList) 
                    
                    if(isInExclusiveList == true){
                        console.log('is within exclusive campaign:'+transactionObj.ivls__receiptno__c)
                        querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Exclusive'), router.AddSingleQuoteSymbol((transactionObj.id )));
                        if(environment == "production"){
                            var markStatusResult = (await router.query_one_way(querystring));
                            console.log('markStatusResult :');
                            console.log(markStatusResult);
                        
                        }
                        continue;
                    }

                    var currentSpending = transactionObj.ivls__spendingexclusive__c;//line spending
                    console.log("currentSpending:"+currentSpending) 
                    var lineActivityName = '';
                    var lineActivityId = '';


                    var hasPointScheme_line = false;
                    var perTransaction_PointsBasic = 0;
                    var perTransaction_PointsNonBasic = 0;
                    console.log("-------------Basic Point Scheme--------------");  
                    if(pointsscheme_list.length > 0){
                        for (var j=0;j<pointsscheme_list.length;j++){
                            var pointSchemeObj = pointsscheme_list[j];
                        
                            if(router.isNotNull(pointSchemeObj.ivls__membershiptier__c) == false ){
                                continue;
                            }

                            if(pointSchemeObj.ivls__membershiptier__c !== CurrentMembershipTier.sfid){
                                // console.log("membership unmatched");
                                continue;
                            }

                            if(router.isNotNull(pointSchemeObj.ivls__minimumspending__c)){
                                if( pointSchemeObj.ivls__minimumspending__c > currentSpending){
                                    // console.log("min unmatched");
                                    continue;
                                }
                            }
                            if(router.isNotNull(pointSchemeObj.ivls__maximumspending__c)){
                                if( pointSchemeObj.ivls__maximumspending__c > currentSpending){
                                    var scheme_currentSpending = pointSchemeObj.ivls__maximumspending__c;
                                }else{
                                    var scheme_currentSpending = currentSpending;
                                }
                            }else{
                                var scheme_currentSpending = currentSpending;
                            }

                            var recordtypeid_1 = router.getRecordTypeId('IVLS__PointsScheme__c','Line', recordtype_list);
                            var recordtypeid_0 = pointSchemeObj.recordtypeid;
                            if(recordtypeid_0 !== recordtypeid_1){
                                // console.log("recordtype unmatched");
                                continue;
                            }
                            
                            
                            var pointsbucket = pointSchemeObj.ivls__pointsbucket__c;
                            if(pointsbucket.toLowerCase() !== "basic"){
                                continue;
                            }

                            console.log("pointsbucket:");
                            console.log(pointsbucket);
                            
                            var earnedPoints = calcLinelLevelPS(router, pointSchemeObj,currentTranObj, scheme_currentSpending);
                            // var earnedPoints = calcLinelLevelPS(pointSchemeObj,currentTranObj, scheme_currentSpending);
                            
                            console.log("earnedPoints");
                            console.log(earnedPoints);
                            console.log("pointSchemeObj.id");
                            console.log(pointSchemeObj.id);
                            
                            if(earnedPoints >0){
                                
                
                                if(router.isNotNull(pointSchemeObj.ivls__activity__c)){
                                    lineActivityName =  pointSchemeObj.ivls__activity__c ;
                                    lineActivityId = pointSchemeObj.sfid;
                                    lineActivityName = lineActivityName.replace(/'/g, "''");
                                }
                                hasPointScheme_line = true;
                                perTransaction_PointsBasic =  Math.ceil(earnedPoints * Number(transactionObj.ivls__quantity__c) *  Number(TierPoints)/ Number(TierSpending));
                                break;
                                
                            }else{
                                console.log("earnedPoints <= 0");
                            }
                        }
                    }
                    
                    console.log("-------------Non-Basic Point Scheme --------------");  
                    if(pointsscheme_list.length > 0){
                        for (var j=0;j<pointsscheme_list.length;j++){
                            var pointSchemeObj = pointsscheme_list[j];
                            var lineActivityName = '';
                            var lineActivityId = '';
                            var currentPointScheme_PointsNonBasic = 0;
                            if(router.isNotNull(pointSchemeObj.ivls__membershiptier__c) == false ){
                                continue;
                            }

                            if(pointSchemeObj.ivls__membershiptier__c !== CurrentMembershipTier.sfid){
                                continue;
                            }

                            if(router.isNotNull(pointSchemeObj.ivls__minimumspending__c)){
                                if( pointSchemeObj.ivls__minimumspending__c > currentSpending){
                                    continue;
                                }
                            }
                            if(router.isNotNull(pointSchemeObj.ivls__maximumspending__c)){
                                if( pointSchemeObj.ivls__maximumspending__c > currentSpending){
                                    var scheme_currentSpending = pointSchemeObj.ivls__maximumspending__c;
                                }else{
                                    var scheme_currentSpending = currentSpending;
                                }
                            }else{
                                var scheme_currentSpending = currentSpending;
                            }

                            var recordtypeid_1 = router.getRecordTypeId('IVLS__PointsScheme__c','Line', recordtype_list);
                            var recordtypeid_0 = pointSchemeObj.recordtypeid;
                            if(recordtypeid_0 !== recordtypeid_1){
                                continue;
                            }
                            
                            //skip basic points pointscheme
                            var pointsbucket = pointSchemeObj.ivls__pointsbucket__c;
                            if(router.isNotNull(pointsbucket) == false){
                                continue;
                            }
                            if(pointsbucket.toLowerCase() == "basic"){
                                continue;
                            }
                            var earnedPoints = calcLinelLevelPS(router, pointSchemeObj,currentTranObj, scheme_currentSpending);
                            // var earnedPoints = calcLinelLevelPS(pointSchemeObj,currentTranObj, scheme_currentSpending);
                            
                            lineActivityId = pointSchemeObj.sfid;
                            if(earnedPoints > 0){
                                ////        non-basic pointsbucket      ////
                                if(router.isNotNull(pointSchemeObj.ivls__activity__c)){
                                    lineActivityName =  pointSchemeObj.ivls__activity__c ;
                                    lineActivityName = lineActivityName.replace(/'/g, "''");
                                }
                                currentPointScheme_PointsNonBasic =  Math.ceil(earnedPoints * Number(transactionObj.ivls__quantity__c));
                                PointsTotal = PointsTotal + currentPointScheme_PointsNonBasic;
                                PointsBonus = PointsBonus + currentPointScheme_PointsNonBasic;
                                perTransaction_PointsNonBasic = perTransaction_PointsNonBasic + currentPointScheme_PointsNonBasic;
                                var InsertObj = {};
                                InsertObj.transaction_id = transactionObj.id;
                                InsertObj.ivls__pointsbucket__c = pointsbucket;
                                InsertObj.ivls__points__c = currentPointScheme_PointsNonBasic;
                                InsertObj.ivls__activity__c = lineActivityName;
                                InsertObj.ivls__ps_campain_id = lineActivityId;
                                InsertObj.ivls__pointspool__c = pointSchemeObj.ivls__pointspool__c; 
                                querystring = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', true);
                                querystring_list.push(querystring); 

                                var productCatename = productCate.name;
                                productCatename = productCatename.replace(/'/g, "''");
                                querystring = "UPDATE loyaltycore.ivls__transaction__c " + " SET ivls__pointsearned__c = "+perTransaction_PointsEarned+", ivls__pointsnet__c = "+ perTransaction_PointsEarned + ",ivls__pointsbasic__c = "+perTransaction_PointsEarned+", ivls__datepointsexpiry__c = "+router.AddSingleQuoteSymbol(expiryDATE) +", ivls__activity__c = '"+productCatename+"' WHERE id = " + transactionObj.id; 
                                querystring_list.push(querystring);
                            }
                        }
                    }
                    //if no specific pointscheme for basic points
                    if(hasPointScheme_line == false){
                        console.log("hasPointScheme_line:"+hasPointScheme_line) 
                        perTransaction_PointsBasic = Math.ceil(currentSpending *  Number(transactionObj.ivls__quantity__c) * Number(TierPoints)/ Number(TierSpending));
                    }

                    var launchDate = new Date('2018/09/24');
                    var today = new Date();
                    
                    if(today < launchDate){
                        console.log("Should be 0")
                        perTransaction_PointsBasic = 0;
                    }

                    var productCatename = productCate.name;
                    productCatename = productCatename.replace(/'/g, "''");


                    console.log("perTransaction_PointsBasic:"+perTransaction_PointsBasic);
                    var perTransaction_PointsEarned = perTransaction_PointsBasic + perTransaction_PointsNonBasic;
                    var transactiondate = transactionObj.ivls__transactiondate__c;
                    var membershipTierObj = CurrentMembershipTier;
                    var expiryDATE = await updatemember.getExpiryDate(pool,yearstart, transactiondate,  membershipTierObj);
                    console.log('expiryDATE:'+expiryDATE);
                    querystring = "UPDATE loyaltycore.ivls__transaction__c " + " SET ivls__transactiondate__c = NOW() - interval '8 hours', ivls__pointsearned__c = "+perTransaction_PointsEarned+", ivls__pointsnet__c = "+ perTransaction_PointsEarned + ",ivls__pointsbasic__c = "+perTransaction_PointsEarned+", ivls__datepointsexpiry__c = "+router.AddSingleQuoteSymbol(expiryDATE) +", ivls__activities__c = '"+productCatename+"' WHERE id = " + transactionObj.id; 
                    querystring_list.push(querystring);

                    querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Complete'), transactionObj.id );
                    querystring_list.push(querystring); 
                    PointsTotal = PointsTotal + perTransaction_PointsBasic;
                    PointsBasic = PointsBasic + perTransaction_PointsBasic;

                    var InsertObj = {};
                    InsertObj.transaction_id = transactionObj.id;
                    InsertObj.ivls__pointsbucket__c = 'Basic';
                    InsertObj.ivls__points__c = perTransaction_PointsBasic;
                    // InsertObj.ivls__activity__c = lineActivityName;
                    InsertObj.ivls__ps_campain_id = lineActivityId;
                    InsertObj.ivls__memberpgid__c = targetMemberPGID;
                    InsertObj.ivls__pointspool__c = 'Spending';
                   
                    InsertObj.ivls__activity__c = productCatename;
                    
                    querystring = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', true);
                    querystring_list.push(querystring); 

                }catch(e){
                    console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                    console.log('Error Processing :transaction_list[i].id:'+ transaction_list[i].id);
                    console.log('Error Message:');
                    console.log(e);
                    querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Error'), router.AddSingleQuoteSymbol((transaction_list[i].id )));
                    querystring_list.push(querystring); 
                    continue;
                }
            }


            ////    check calculate result (Point Scheme by Total and by Line)   ////
            console.log("###########################################");
            console.log('currentSpending:'+ currentSpending);
            console.log("ReceiptNo:"+receipt_obj.ivls__receiptno__c);
            console.log('PointsTotal:'+ PointsTotal);
            console.log('PointsBasic:'+ PointsBasic);
            console.log('PointsBonus:'+ PointsBonus);
            
            console.log(querystring_list);
            console.log("querystring_list:"+querystring_list.length);
            console.log("###########################################");


            //execute querystrings per receipt
            if(querystring_list.length > 0){
                var totalQueryStr='';
                for (var q=0;q<querystring_list.length;q++){
                    if(q == 0){
                        totalQueryStr =  totalQueryStr + querystring_list[q];
                    }else{
                        totalQueryStr =  totalQueryStr + ' ; '+ querystring_list[q];
                    }
                
                }

                querystring_list = [];
                if(environment == "production"){
                    try{
                        await router.query_one_way(totalQueryStr);
                        console.log('Process querystring_list successful !!!')
                    }catch (err) {
                        console.log('Process querystring_list fail !!!')
                        console.log(err)
                        
                    }
                }
            }
            

            if(router.isNotNull(PointsTotal)&& PointsTotal>0){

                var oldMemberObj = memberObj.Account;

                var newMemberObj= {};
                newMemberObj.ivls__spendinglifetime__pc = oldMemberObj.ivls__spendinglifetime__pc + receipt_obj.ivls__totalcashamount__c;
                newMemberObj.ivls__pointsearnedlifetime__pc = oldMemberObj.ivls__pointsearnedlifetime__pc + PointsTotal;
                // newMemberObj.ivls__pointsredeemedlifetime__pc = oldMemberObj.ivls__pointsredeemedlifetime__pc;
                newMemberObj.ivls__datelasttx__pc = router.getSQLDateTimeFormatWithSeconds(receipt_obj.ivls__transactiondate__c);
                // newMemberObj.ivls__datelasttx__pc =(receipt_obj.ivls__transactiondate__c);
                // newMemberObj.ivls__datelasttx__pc =(receipt_obj.ivls__transactiondate__c).toISOString();
                if(receipt_obj.ivls__transactiontype__c == 'POS' || receipt_obj.ivls__transactiontype__c == 'eShop'){//Sony Store
                    // newMemberObj.ivls__datelaststorevisit__pc = (receipt_obj.ivls__transactiondate__c).toISOString();
                    newMemberObj.ivls__datelaststorevisit__pc =  router.getSQLDateTimeFormatWithSeconds(receipt_obj.ivls__transactiondate__c);
                    newMemberObj.ivls__noofstorevisit__pc = oldMemberObj.ivls__noofstorevisit__pc  + 1;
                }
                newMemberObj.ivls__nooftx__pc = oldMemberObj.ivls__nooftx__pc + 1;
                
                newMemberObj.ivls__averagespending__pc = newMemberObj.ivls__spendinglifetime__pc/ newMemberObj.ivls__nooftx__pc ;
                querystring = getSQLstr(newMemberObj, 'loyaltycore.', 'account', router, 'ivls__pgid__c', targetMemberPGID);
                console.log("SECTION1 PART2 updateMemberSQL:");
                console.log(querystring);
                if(environment == "production"){
                    var updateResult = await router.query_one_way(querystring);
                    console.log(updateResult);
                }


                // var ISOdate = (receipt_obj.ivls__transactiondate__c).toISOString();
                // var transactiondatetime = ISOdate;
                // var transactiondatetime = new Date(ISOdate);
                // transactiondatetime = transactiondatetime.toLocaleString();
                // transactiondatetime = router.getSQLDateTimeFormatWithSeconds(transactiondatetime);
                // var transactiondatetime = router.getSQLDateTimeFormatWithSeconds(receipt_obj.ivls__transactiondate__c);
            
                var updateMemberSQL = await updatemember.updateMemberInfo(pool, targetMemberPGID,  membershipObj, membershiptiers_list, oldPTK);
                console.log("SECTION1 PART1 AFTER updateMemberSQL:");
                console.log(updateMemberSQL);
                if(updateMemberSQL == false){
                    querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
                    console.log("querystring::");
                    console.log(querystring);
                    if(environment == "production"){
                        await router.query_one_way(querystring);
                    }


                    continue;
                }

                if(environment == "production"){
                    var updateResult = await router.query_one_way(updateMemberSQL);
                    console.log(updateResult);
                }
            }
        }catch(e){
            console.log('process receipt_list error');
            console.log(e);
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['receipt', receipt_obj.ivls__receiptno__c, 'process receipt_list error', e.toString()];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }


            querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__status__c = 'Open' AND ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
            console.log("querystring::");
            console.log(querystring);
            if(environment == "production"){
                await router.query_one_way(querystring);
            }
        }
        

       
    }
    return true;
}
async function part2(log_level, pool, yearstart, environment, membershiptiers_list, pointsscheme_list, recordtype_list, processDate, membershipObj, productCate_list){
    var querystring = "";
    var querystring_list = [];
    //////        Behaviour PointScheme       ////////
    if(pointsscheme_list.length>0){
    
        console.log("---------------------    SECTION 2: PointScheme    ---------------------");
       
        console.log("Total "+pointsscheme_list.length+" PointSchemes");   
        for(var j=0;j<pointsscheme_list.length;j++){
            try{
                var pointSchemeObj = pointsscheme_list[j];
                //behaviour pointsscheme
                var recordtypeid_1 = router.getRecordTypeId('IVLS__PointsScheme__c','Behaviour', recordtype_list);
                var recordtypeid_0 = pointSchemeObj.recordtypeid;
                if(recordtypeid_0 !== recordtypeid_1){
                    continue;
                }

                
                var pointsbucket = pointSchemeObj.ivls__pointsbucket__c;
                if(pointsbucket.toLowerCase() == "basic"){
                    continue;
                }

                var referenceObject = pointSchemeObj.ivls__referenceobject__c;
                var referenceField = pointSchemeObj.ivls__field__c;
                var pointsPool = pointSchemeObj.ivls__pointspool__c;//Spending or A&P

                referenceField =  referenceField.toLowerCase();
                referenceObject = referenceObject.toLowerCase();

                var operator = getSQLOperator(pointSchemeObj.ivls__operator__c);
                if(operator == false){
                    var tempstr = "Cannot read ivls__operator__c:"+ivls__operator__c;
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['IVLS__PointsScheme__c', pointSchemeObj.id, 'part2 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                 }
                // var isDateTime = false;
                // if((referenceField).includes("date") == true){
                //     isDateTime = true;
                // }


                var isDateTime = false;
                var datetimeArray = router.getDateTimeKeyword();
                for(var h=0;h<datetimeArray.length;h++){
                    if(referenceField.includes(datetimeArray[h]) == true){
                        isDateTime = true;
                        break;
                    }
                }

                if(router.isNotNull(isDateTime) == false){
                    var tempstr = "Cannot read translate reference field as datetime format:";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['IVLS__PointsScheme__c', pointSchemeObj.id, 'part2 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                   
                    continue;
                }

                var condition_fieldValue= pointSchemeObj.ivls__filtervalue__c ;
                var condition_fieldFormula= pointSchemeObj.ivls__filter_formula__c ;

                var targetValue = "";
                if (router.isNotNull(condition_fieldValue)){
                    targetValue = condition_fieldValue;
                }else if (router.isNotNull(condition_fieldFormula) && router.isNotNull(condition_fieldValue) == false){
                    targetValue = getSQLtargetValue(condition_fieldFormula, processDate);
                }
                if(targetValue == "invalid"){
                   
                    var tempstr = "Cannot read condition_fieldFormula:"+condition_fieldFormula;
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['IVLS__PointsScheme__c', pointSchemeObj.id, 'part2 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                   
                    continue;
                }
            
                if(router.isNotNull(referenceObject)==false){
                    var tempstr = "referenceObject is null";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['IVLS__PointsScheme__c', pointSchemeObj.id, 'part2 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }

                if(referenceObject == 'account'){
                    if(referenceField == 'ivls__datejoin__pc'){
                        //handled by other script
                        continue;
                    }
                }

            
                if(isDateTime == false){
                    if(typeof(targetValue)=="string"){
                        querystring = "SELECT * FROM loyaltycore."+referenceObject+ " WHERE " + referenceField +" "+ operator + " '"+ targetValue+"'";
                    }else{
                        querystring = "SELECT * FROM loyaltycore."+referenceObject+ " WHERE " + referenceField +" "+ operator + " "+ targetValue;
                    }
                }else{

                    if(condition_fieldFormula == 'This Month' || condition_fieldFormula == 'Last Month'  ){
                        querystring = "SELECT * FROM loyaltycore."+referenceObject +" WHERE "+ "DATE_PART('month',"+referenceField+" - "+targetValue.firstDay+" )>=0 AND DATE_PART('month',"+referenceField+" - "+ targetValue.lastDay + ") <=0" 
                    }else if(condition_fieldFormula == 'This Calendar Year'){
                        querystring = "SELECT * FROM loyaltycore."+referenceObject +" WHERE "+ "DATE_PART('year',"+referenceField+" - "+targetValue.firstDay+" )>=0 AND DATE_PART('month',"+referenceField+" - "+ targetValue.lastDay + ") <=0" 
                    }else{
                      
                        
                      
                        if(referenceObject == 'asset'){
                            querystring="SELECT * FROM loyaltycore."+referenceObject +" WHERE "+referenceField+"::date "+operator+" '"+targetValue+"' ";//'2018-08-20'";
                            ////    for specific warranty case  ////
                            // querystring =  querystring + " AND accountid in ('0010l00000TeP26AAF', '0010l00000TeOG5AAN', '0010l00000RBxUYAA1', '0010l00000TeOtmAAF', '0010l00000RBSOPAA5','0010l00000TaiHNAAZ' )";
                           
                        }else{
                            querystring="SELECT * FROM loyaltycore."+referenceObject +" WHERE "+referenceField+"::date  "+operator+" '"+targetValue+"' ";//'2018-08-20'";

                        }
                    }
                }

                console.log("meetConditionRecord_list querystring:" );
                console.log(querystring);
                
                var meetConditionRecord_list = (await router.query_one_way(querystring)).rows;
            
                
                if(router.isNotNull(meetConditionRecord_list)==false || meetConditionRecord_list.length==0){
                    console.log("No reference list meet conditions :" +pointSchemeObj.name);
                    continue;
                }

            
            
                //need to check is Spending!!!!!!!
                console.log("-------------------------------------");
                console.log("meetConditionRecord_list:" +meetConditionRecord_list.length);
                console.log("referenceObject="+referenceObject);
                console.log("referenceField="+referenceField);
                console.log("pointsPool="+pointsPool);   
                console.log("isDateTime="+isDateTime);   
                console.log("targetValue="+targetValue);   
                console.log("-------------------------------------");


                var targetMembershipTier = '';
                var targetMemberPGID = '';
                var targetMemberSFID = '';
                for(var i=0;i<meetConditionRecord_list.length;i++){
                    if(referenceObject == 'account'){   
                        targetMemberPGID = meetConditionRecord_list[i].ivls__pgid__c;
                        targetMembershipTier = meetConditionRecord_list[i].ivls__membershiptier__pc;
                    }else if (referenceObject == 'ivls__transaction__c'){
                        
                        targetMemberPGID = meetConditionRecord_list[i].ivls__memberpgid__c;
                        querystring = "SELECT ivls__membershiptier__pc FROM loyaltycore.account WHERE ivls__pgid__c = "+meetConditionRecord_list[i].ivls__memberpgid__c;
                        try{
                            targetMembershipTier = (await router.query_one_way(querystring)).rows[0].ivls__membershiptier__pc;
                        }catch(e){
                            console.log("targetMembershipTier error:") 
                            console.log(e) 
                            continue;
                        }
                    
                    }else if(referenceObject == 'asset'){
                        targetMemberSFID = meetConditionRecord_list[i].accountid;
                        // SELECT * FROM loyaltycore.asset WHERE DATE_PART('day',HKMC_Register_Date__c - '2018-07-14') = 0 AND accountid = '0010l00000TahKCAAZ'
                        querystring = "SELECT * FROM loyaltycore.account WHERE id = '"+targetMemberSFID+"' ";
                        try{
                             tempObj = (await router.query_one_way(querystring)).rows[0];
                             if(tempObj == undefined)
                             {
                                 console.log("No Member record found!"+" Member SFID is "+targetMemberSFID)//Henry add handle undefined;
                             }
                             else
                             {
                                 targetMemberPGID = tempObj.ivls__pgid__c;
                                 targetMembershipTier = tempObj.ivls__membershiptier__pc;
                             }
                        }catch(e){
                            console.log("targetMembershipTier error:") 
                            console.log(e) 
                            continue;
                        }
                       
                    }else{
                        continue;
                    }


                
                    if(router.isNotNull(targetMemberPGID)==false){
                        console.log("targetMemberPGID is null :") 
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [referenceObject, meetConditionRecord_list[i].id, 'part2 error', 'targetMemberPGID is null'];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }
                       
                        continue;
                    }
    
                    if(router.isNotNull(targetMembershipTier)==false){
                        console.log("targetMembershipTier is null :")
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [referenceObject, meetConditionRecord_list[i].id, 'part2 error', 'targetMembershipTier is null'];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }
                        
                        continue;
                    }

        
                    if(targetMembershipTier !== pointSchemeObj.ivls__membershiptier__c){
                        console.log("targetMembershipTier unmathced:") 
                        continue;
                    }

                    if(referenceObject == 'asset'){
                        var hkmc_loyalty_include__c = meetConditionRecord_list[i].hkmc_loyalty_include__c;
                        if(router.isNotNull(hkmc_loyalty_include__c)== false|| hkmc_loyalty_include__c !== true){
                            console.log("warranty loyalty include is false:"+ meetConditionRecord_list[i].name)
                            continue;
                        }
                    }


                    // //check already given points by this point scheme
                    // if(referenceObject == 'ivls__transaction__c'){
                    //     querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+pointSchemeObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID + " AND transaction_id = "+ meetConditionRecord_list[i].id;
                    // }else if(referenceObject == 'asset'){
                    //     querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.ivls__sequenceno__c = '"+meetConditionRecord_list[i].id+"'";
                    // }else {
                    //     //account
                    //     querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+pointSchemeObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID;
                    // }

                    //check already given points by this point scheme
                    if(referenceObject == 'ivls__transaction__c'){
                        querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+pointSchemeObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID + " AND transaction_id = "+ meetConditionRecord_list[i].id;
                    }else{
                        querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].ivls__pgid__c+"' AND (SELECT ivls__activity__c FROM loyaltycore.ivls__pointsscheme__c WHERE sfid = d.ivls__ps_campain_id) = '"+pointSchemeObj.ivls__activity__c+"';"
                    }

                    
                    console.log("isAlreadyGiven_list querystring:"+querystring);
                    
                    var isAlreadyGiven_list = (await router.query_one_way(querystring)).rows;
                    console.log("isAlreadyGiven_list.length:"+isAlreadyGiven_list.length);
                    // console.log(isAlreadyGiven_list);
                    
                    var isAlreadyGiven = false;
                    if(isAlreadyGiven_list.length > 0){
                        isAlreadyGiven = true;
                    }

                   
                    if(isAlreadyGiven){
                        var tempstr = "This pointscheme ("+pointSchemeObj.name+") is already given for:"+targetMemberPGID;
                        console.log(tempstr);
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [referenceObject, meetConditionRecord_list[i].id, 'part2 error', tempstr];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }
                       
                        continue;
                    }



                    // if(referenceField == 'ivls__datefirstpurchaseatdtp__pc'){
                    //     querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+pointSchemeObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID;
                    //     var isAlreadyGiven_list = (await router.query_one_way(querystring)).rows;
                    //     console.log("DTP isAlreadyGiven_list.length:"+isAlreadyGiven_list.length);
                    //     // console.log(isAlreadyGiven_list);
                        
                    //     var isAlreadyGiven = false;
                    //     if(isAlreadyGiven_list.length > 0){
                    //         isAlreadyGiven = true;
                    //     }

                    //     if(isAlreadyGiven){
                    //         console.log("This DTP is already given:"+targetMemberPGID);
                    //         continue;
                    //     }
                        
                    // }
                   

                    var member_list = await router.selectALL_with_parameter('account','ivls__pgid__c','=', targetMemberPGID);
                    var memberObj= router.getCurrentMemberInfo(member_list[0], membershiptiers_list);
                    var CurrentMembershipTier = memberObj.CurrentMembershipTier;

                   

                    ////    check blacklisted member    ////
                    var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
                    if(currentMemberTierIsBlacklisted == true){
                        var tempstr = "currentMemberTierIsBlacklisted:"+targetMemberPGID;
                        // var querystring_error_log = router.get_Querystring_error_log();
                        // var values = [referenceObject, meetConditionRecord_list[i].id, 'part2 error', tempstr];
                        // var result  = (await router.query_with_parameter(querystring_error_log, values));
                        // console.log(result);
                        continue;
                    }


                    var currentSpending_totalLevel = 0;
                    if(referenceObject == 'ivls__transaction__c'){
                        if(router.isNotNull(pointSchemeObj.ivls__merchant__c)){
                            if(meetConditionRecord_list[i].ivls__merchant__c !== pointSchemeObj.ivls__merchant__c){
                                continue;
                            }
                        }

                        currentSpending_totalLevel = meetConditionRecord_list[i].ivls__totalcashamount__c ;
                        console.log("currentSpending_totalLevel="+currentSpending_totalLevel); 
                    }

                    var calc_points= pointSchemeObj.ivls__points__c ;
                    var calc_pointsmultiple= pointSchemeObj.ivls__pointsmultiple__c ;
                    var calc_pointsbucket= pointSchemeObj.ivls__pointsbucket__c ;


                    console.log("calc_points="+calc_points);
                    console.log("calc_pointsmultiple="+calc_pointsmultiple);
                    console.log("calc_pointsbucket="+calc_pointsbucket);   

                 
                    var earnedPoints = 0;
                    if(router.isNotNull(calc_points) == false && router.isNotNull(calc_pointsmultiple) == false){
                        console.log("PointScheme missing ivls__points__c and calc_pointsmultiple:"+pointSchemeObj.id) 
                        continue;
                    }else if (router.isNotNull(calc_points) && router.isNotNull(calc_pointsmultiple) == false){
                        earnedPoints = calc_points;
                    }else if (router.isNotNull(calc_pointsmultiple) && router.isNotNull(calc_points) == false){
                        earnedPoints = Number(calc_pointsmultiple) * currentSpending_totalLevel;
                    }else if(router.isNotNull(calc_points) && router.isNotNull(calc_pointsmultiple)){
                        earnedPoints = calc_points;
                    }
                
                    console.log("========   behaviour level pointscheme   ======== ");   
                    console.log("earnedPoints="+earnedPoints);   
                    var lineActivityName = '';
                    var lineActivityId = '';
                    lineActivityId = pointSchemeObj.sfid;
                    if(router.isNotNull(pointSchemeObj.ivls__activity__c)){
                        lineActivityName =  pointSchemeObj.ivls__activity__c ;
                    }

                    if(referenceObject == 'asset'){
                        lineActivityName =  lineActivityName +" for "+meetConditionRecord_list[i].hkmc_kataban__c ;
                    }

                    if(earnedPoints > 0){
                        var transactiondate = new Date();
                        
                        var membershipTierObj = CurrentMembershipTier;
                        var expiryDATE = await updatemember.getExpiryDate(pool,yearstart, transactiondate,  membershipTierObj);
                        console.log('expiryDATE:'+expiryDATE);
                        

                        var oldPTK = "";
                        querystring = "SELECT ivls__pointstokeeptier__c FROM loyaltycore.account WHERE ivls__pgid__c = " + targetMemberPGID;
                        oldPTK =  ( await router.query_one_way(querystring)).rows[0].ivls__pointstokeeptier__c;


                        var InsertObj = {};
                        InsertObj.ivls__memberpgid__c = targetMemberPGID;
                        InsertObj.ivls__member__c = router.AddSingleQuoteSymbol(memberObj.Account.id) ;
                        // InsertObj.ivls__transactiondate__c = "NOW() - interval '8 hour'";
                        InsertObj.ivls__transactiondate__c = "(to_char(NOW() - interval '8 hours', 'YYYY-MM-DD HH24:MI:SS'))::timestamp";
                        InsertObj.createddate = 'NOW()';
                        InsertObj.lastmodifieddate = 'NOW()';
                        InsertObj.ivls__source__c = "'System'";
                        InsertObj.ivls__status__c = "'Complete'";
                        InsertObj.sourceobject = router.AddSingleQuoteSymbol(referenceObject) ;
                        InsertObj.sourceobjectid = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].ivls__pgid__c) ;
                        
                        // if(referenceObject == 'asset'){//warranty
                        //     InsertObj.ivls__sequenceno__c = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].id);//sfid
                        // }
                        InsertObj.ivls__activities__c = router.AddSingleQuoteSymbol(lineActivityName);
                        InsertObj.ivls__pointspool__c = router.AddSingleQuoteSymbol(pointsPool);
                        InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Transaction__c',pointsPool, recordtype_list));
                        InsertObj.ivls__datepointsexpiry__c = router.AddSingleQuoteSymbol(expiryDATE);
                        InsertObj.ivls__pointsearned__c = earnedPoints;
                        InsertObj.ivls__pointsnet__c = earnedPoints;
                        InsertObj.ivls__contact__c = router.AddSingleQuoteSymbol(memberObj.Account.personcontactid);
                        InsertObj.ivls__tier__pc =  router.AddSingleQuoteSymbol(CurrentMembershipTier.sfid);
                        querystring = router.getSQL_Insert(InsertObj, 'ivls__transaction__c', false);
                       
                        console.log("transaction:");
                        console.log(InsertObj);


                        var querystring1 = querystring + " RETURNING id;";
                        console.log("behaviour level insert transaction SQL :");
                        console.log(querystring1);
                      
                       
                        var client = await pool.connect();
                        try {
                        
                            await client.query('BEGIN')
                            var res  = await client.query(querystring1);
                        
                            var insertPhotoValues = [res.rows[0].id];
                            var InsertObj = {};
                            InsertObj.transaction_id = insertPhotoValues;
                            InsertObj.ivls__memberpgid__c = targetMemberPGID;
                            InsertObj.ivls__pointsbucket__c =calc_pointsbucket;
                            InsertObj.ivls__points__c = earnedPoints;
                            InsertObj.ivls__activity__c = lineActivityName;
                            InsertObj.ivls__ps_campain_id = lineActivityId;
                            InsertObj.ref_type = "IVLS__PointsScheme__c";
                            InsertObj.ivls__pointspool__c = pointsPool;
                            querystring2 = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', true);
                            console.log("behaviour level insert transaction detail SQL :");
                            console.log(querystring2);
                            console.log("transaction detail:");
                            console.log(InsertObj);
                           
                            await client.query(querystring2);
                            
                            if(environment == 'production'){
                                await client.query('COMMIT')
                            }else{
                                await client.query('ROLLBACK')
                            }
                            
                        }catch (e) {
                            await client.query('ROLLBACK')
                        
                            console.log("ROLLBACK=");   
                            console.log(e);   
                            
                            throw e
                        }finally {
                            client.release()
                        }

                        // if(referenceObject == 'ivls__transaction__c'){
                        //     var transactiondatetime = meetConditionRecord_list[i].ivls__transactiondate__c;
                        //     transactiondatetime = router.getSQLDateTimeFormatWithSeconds(transactiondatetime);
                        // }else{
                        //     var transactiondatetime = "NOW()";
                        // }
                        // var transactiondatetime = "NOW()";
                       
                        var updateMemberSQL = await updatemember.updateMemberInfo(pool, targetMemberPGID,  membershipObj, membershiptiers_list, oldPTK);
                        console.log("SECTION2 updateMemberSQL:");
                        console.log(updateMemberSQL);
                        if(updateMemberSQL == false){
                            var tempstr = "updateMemberSQL:"+updateMemberSQL;
                            if(log_level == "debug"){
                                var querystring_error_log = router.get_Querystring_error_log();
                                var values = [referenceObject, meetConditionRecord_list[i].id, 'part2 updateMemberSQL error', tempstr];
                                var result  = (await router.query_with_parameter(querystring_error_log, values));
                                console.log(result);
                            }
                           
                            continue;
                          
                        }
                        if(environment == "production"){
                            var updateResult = await router.query_one_way(updateMemberSQL);
                            console.log(updateResult);
                        }
                    }
                }   
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log('Error Processing :pointsscheme_list[j].id:'+ pointsscheme_list[j].id);
                console.log('Error Message:');
                console.log(e);
                
                
                var tempstr = 'Error Processing :pointsscheme_list[j].id:'+ pointsscheme_list[j].id;
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['IVLS__PointsScheme__c', pointsscheme_list[j].id, 'part2 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
                }
                
                continue;


            }
        }
    }
    return true;
}
async function part3(log_level, pool, yearstart, environment, membershiptiers_list, campaign_list, recordtype_list, processDate, dbname, membershipObj, productCate_list){
    var querystring = "";
    var querystring_list = [];
    //////        Campaign        ////////
    console.log("-----------------------    SECTION 3: Campaign    ----------------------");
    if(campaign_list.length>0){
        var totalNumOfCampaign = campaign_list.length;
        console.log("Total "+totalNumOfCampaign+" Campaigns");   

        //// loop campain list  ////
        for(var j=0;j<campaign_list.length;j++){
            try{
              
                var campaignObj = campaign_list[j];
                ////    earnpoints campaign    ////
                var recordtypeid_1 = router.getRecordTypeId('Campaign','EarnPoints', recordtype_list);
                var recordtypeid_0 = campaignObj.recordtypeid;
                if(recordtypeid_0 !== recordtypeid_1){
                    continue;
                }
                console.log("Currently  "+(j+1)+"/"+totalNumOfCampaign + " Campaign name:" + campaignObj.name); 
                //// validate fields
                var calculateBy = campaignObj.ivls__calculateby__c;
                var referenceObject = campaignObj.ivls__referenceobject__c;
                var referenceField = campaignObj.ivls__field__c;
                var pointsPool = campaignObj.ivls__pointspool__c;//Spending or A&P
                var condition_fieldValue= campaignObj.ivls__filtervalue__c ;
                var condition_fieldFormula= campaignObj.ivls__filterformula1__c ;
                var operator = campaignObj.ivls__operator__c 

                if(router.isNotNull(calculateBy)==false){
                    var tempstr = "calculateBy is null ";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }
                if(router.isNotNull(referenceObject)==false){ 
                    var tempstr = "referenceObject is null ";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                   
                    continue;
                }
                if(router.isNotNull(referenceField)==false){
                    var tempstr = "referenceField is null ";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }
                if(router.isNotNull(pointsPool)==false){
                    var tempstr = "pointsPool is null ";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }
                if(router.isNotNull(condition_fieldValue)==false && router.isNotNull(condition_fieldFormula)==false){  
                    var tempstr = "condition_fieldValue and condition_fieldFormula is null ";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                   
                    continue;
                }
                
            
                referenceField =  referenceField.toLowerCase();
                referenceObject = referenceObject.toLowerCase();
                operator = getSQLOperator(operator);
            
                if(operator == false){
                    var tempstr = "Cannot read ivls__operator__c:"+ivls__operator__c;
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }


                var isDateTime = false;
                var datetimeArray = router.getDateTimeKeyword();
                for(var h=0;h<datetimeArray.length;h++){
                    if(referenceField.includes(datetimeArray[h]) == true){
                        isDateTime = true;
                        break;
                    }
                }


                ////    check whether targetValue is datetime format    ////
                if(router.isNotNull(isDateTime) == false){
                    var tempstr = "error in get isDateTime";
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }
              
                
    
                var targetValue = false;
                if (router.isNotNull(condition_fieldValue)){
                    targetValue = condition_fieldValue;
                }else if (router.isNotNull(condition_fieldFormula) && router.isNotNull(condition_fieldValue) == false){
                    targetValue = getSQLtargetValue(condition_fieldFormula, processDate);
                }
                if(targetValue == "invalid"){   
                    var tempstr = "Cannot read formula:"+condition_fieldFormula;
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                    
                    continue;
                }
                


                console.log("condition_fieldFormula:"+condition_fieldFormula);   
                console.log("targetValue:");   
                console.log(targetValue);   
                console.log("isDateTime:"+isDateTime); 
                
                


                if(isDateTime == false){
                    if (referenceObject == 'ivls__transaction__c'){
                        if(processDate == "T+1"){
                            querystring = "SELECT * FROM " +dbname+referenceObject+ " WHERE " + referenceField + " "+ operator +" "+ targetValue + " AND ivls__pointsbasic__c > 0 AND DATE_PART('day', NOW() - ivls__transactiondate__c ) = 1;";
                        }else{
                            querystring = "SELECT * FROM " +dbname+referenceObject+ " WHERE " + referenceField + " "+ operator +" "+ targetValue + " AND ivls__pointsbasic__c > 0 AND DATE_PART('day', NOW() - ivls__transactiondate__c ) = 0;";
                        }
                        
                    }else{
                        querystring = "SELECT * FROM " +dbname+referenceObject+ " WHERE " + referenceField + operator + targetValue;
                    }
                }else{
                    if(condition_fieldFormula == 'This Month' || condition_fieldFormula == 'Last Month'  ){
                        // querystring = "SELECT * FROM "+dbname+referenceObject +" WHERE "+ "DATE_PART('month',"+referenceField+" - "+targetValue.firstDay+" )>=0 AND DATE_PART('month',"+referenceField+" - "+ targetValue.lastDay + ") <=0" 
                        querystring = "SELECT * FROM loyaltycore."+referenceObject+" WHERE "+referenceField+"::date >=  '"+targetValue.firstDay+"' AND "+referenceField+"::date <=  '"+targetValue.lastDay+"';";
                    }else if(condition_fieldFormula == 'This Calendar Year'){
                        // querystring = "SELECT * FROM "+dbname+referenceObject +" WHERE "+ "DATE_PART('year',"+referenceField+" - "+targetValue.firstDay+" )>=0 AND DATE_PART('month',"+referenceField+" - "+ targetValue.lastDay + ") <=0" 
                        querystring = "SELECT * FROM loyaltycore."+referenceObject+" WHERE "+referenceField+"::date >=  '"+targetValue.firstDay+"' AND "+referenceField+"::date <=  '"+targetValue.lastDay+"';";
                      
                    }else{

                        if(referenceObject == 'account'){   
                            querystring = "SELECT a.* FROM loyaltycore.account a JOIN loyaltycore.ivls__membershiptier__c t  ON a.ivls__membershiptier__pc = t.sfid WHERE a."+referenceField+"::date "+operator+" '"+targetValue+"'";//'2018-08-20'";
                            
                            //check membership and membership tier
                            if(router.isNotNull(campaignObj.ivls__tier__c)){
                                querystring += " AND t.ivls__membershiptier__pc = '" + campaignObj.ivls__tier__c+"' "
                            }else{
                                //check if within specific sequence
                                if(router.isNotNull(campaignObj.ivls__tiersequencefrom__c)){
                                    querystring += " AND t.ivls__sequence__c >=" + Number(campaignObj.ivls__tiersequencefrom__c) 
                                }
                                if(router.isNotNull(campaignObj.ivls__tiersequenceto__c)){
                                    querystring += " AND t.ivls__sequence__c <= " + Number(campaignObj.ivls__tiersequenceto__c) 
                                }
                            }
                        }else {
                            querystring = "SELECT * FROM loyaltycore."+referenceObject +" WHERE "+referenceField+"::date "+operator+" '"+targetValue+"'";//'2018-08-20'";
                        }



                        
                        
                    }
                }
                console.log("meetConditionRecord_list querystring:" +querystring);
                var meetConditionRecord_list = (await router.query_one_way(querystring)).rows;
                if(router.isNotNull(meetConditionRecord_list)==false || meetConditionRecord_list.length==0){
                    console.log("No reference list meet conditions");
                    continue;
                }
    
                console.log("meetConditionRecord_list:" +meetConditionRecord_list.length);
                // console.log(meetConditionRecord_list);
                //need to check is Spending!!!!!!!
                console.log("-------------------------------------");
                console.log("referenceObject="+referenceObject);
                console.log("referenceField="+referenceField);
                console.log("pointsPool="+pointsPool);   
                console.log("isDateTime="+isDateTime);   
                console.log("targetValue=");   
                console.log(targetValue);   
                console.log("calculateBy="+calculateBy); 
                console.log("referenceObject="+referenceObject); 
                console.log("-------------------------------------");
                


                var targetMembershipTier = '';//sfid
                var targetMemberPGID = '';


                ////    get member's information    ////
                for(var i=0;i<meetConditionRecord_list.length;i++){
                    if(referenceObject == 'account'){   
                        targetMembershipTier = meetConditionRecord_list[i].ivls__membershiptier__pc;
                        targetMemberPGID = meetConditionRecord_list[i].ivls__pgid__c;
                    }else if (referenceObject == 'ivls__transaction__c'){
                        targetMemberPGID = meetConditionRecord_list[i].ivls__memberpgid__c;
                        querystring = "SELECT ivls__membershiptier__pc FROM loyaltycore.account WHERE ivls__pgid__c = '"+meetConditionRecord_list[i].ivls__memberpgid__c+"'";
                        try{
                            targetMembershipTier = (await router.query_one_way(querystring)).rows[0].ivls__membershiptier__pc;
    
                        }catch(e){
                            console.log("targetMembershipTier error:") 
                            console.log(e) 
                            continue;
                        }
                    
                    }else {
                        continue;
                    }
                    
                    if(router.isNotNull(targetMemberPGID)==false){
                        var tempstr = "targetMemberPGID is null :";
                        console.log(tempstr) 
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [referenceObject, meetConditionRecord_list[i].id, 'part3 error', tempstr];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }
                       
                        continue;
                    }
    
                    if(router.isNotNull(targetMembershipTier)==false){
                        var tempstr = "targetMembershipTier is null :";
                        console.log(tempstr) 
                        if(log_level == "debug"){
                            var querystring_error_log = router.get_Querystring_error_log();
                            var values = [referenceObject, meetConditionRecord_list[i].id, 'part3 error', tempstr];
                            var result  = (await router.query_with_parameter(querystring_error_log, values));
                            console.log(result);
                        }
                       
                        continue;
                    }

                    if(router.isNotNull(campaignObj.ivls__tier__c)){
                        if(targetMembershipTier !== campaignObj.ivls__tier__c){
                            console.log("targetMembershipTier unmathced:") 
                            continue;
                        }
                    }
                    

                    
                
                    var member_list = await router.selectALL_with_parameter('account','ivls__pgid__c','=',targetMemberPGID);
                    var memberObj= router.getCurrentMemberInfo(member_list[0], membershiptiers_list);
                    var CurrentMembershipTier = memberObj.CurrentMembershipTier;
                    


                    ////    check blacklisted member    ////
                    var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
                    if(currentMemberTierIsBlacklisted == true){
                        var tempstr = "currentMemberTierIsBlacklisted:"+targetMemberPGID
                        console.log(tempstr) 

                        // var querystring_error_log = router.get_Querystring_error_log();
                        // var values = [referenceObject, meetConditionRecord_list[i].id, 'part3 error', tempstr];
                        // var result  = (await router.query_with_parameter(querystring_error_log, values));
                        // console.log(result);
                        continue;
                    }


                    ////    check whether this campaign is already given to the member  ////
                    if(referenceObject == 'ivls__transaction__c'){
                        // querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+campaignObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID + "AND transaction_id = "+ meetConditionRecord_list[i].id;
                        // querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].id+"' AND (SELECT ivls__activity__c FROM loyaltycore.ivls__pointsscheme__c WHERE sfid = d.ivls__ps_campain_id) = '"+campaignObj.ivls__activity__c+"';"
                        querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].id+"' AND d.ivls__ps_campain_id = '"+campaignObj.sfid+"';";
                    }else{
                        // querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+campaignObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID;
                        // querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].ivls__pgid__c+"' AND (SELECT ivls__activity__c FROM loyaltycore.ivls__pointsscheme__c WHERE sfid = d.ivls__ps_campain_id) = '"+campaignObj.ivls__activity__c+"';"
                        querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].ivls__pgid__c+"' AND d.ivls__ps_campain_id = '"+campaignObj.sfid+"';";
                    }
                    
                    var isAlreadyGiven_list = (await router.query_one_way(querystring)).rows;
                    console.log("isAlreadyGiven_list.length:"+isAlreadyGiven_list.length);
                    var isAlreadyGiven = false;
                    if(isAlreadyGiven_list.length > 0){
                        isAlreadyGiven = true;
                    }
                    if(isAlreadyGiven){
                        console.log("This campaign ("+campaignObj.name+") is already given for:"+targetMemberPGID);
                        continue;
                    }





                    ////    get spending    ////
                    var currentSpending_totalLevel = 0;
                    var currentSpending_lineLevel = 0;
                    var currentSpending = 0;

                    if(referenceObject == 'ivls__transaction__c'){
                        
                        currentSpending_totalLevel = meetConditionRecord_list[i].ivls__totalcashamount__c ;
                        currentSpending_lineLevel =  meetConditionRecord_list[i].ivls__spendingexclusive__c ;
                        console.log("currentSpending_totalLevel="+currentSpending_totalLevel); 
                        console.log("currentSpending_lineLevel="+currentSpending_lineLevel); 

                        if(calculateBy == "Line"){
                            currentSpending = currentSpending_lineLevel;
                            
                        }else{
                            currentSpending = currentSpending_totalLevel;
                        }
                    }



                    ////    if calculateby line, check merchant, product, product cate  ////
                    if(calculateBy=="Line" && referenceObject == 'ivls__transaction__c'){
                        var  cate1 = "";
                        var  cate2 = "";
                        var  cate3 = "";
                        var product_sfid = meetConditionRecord_list[i].ivls__product__c;
                        var productCate = "";
                        var merchant = meetConditionRecord_list[i].ivls__merchant__c;

                        try{

                            var productIsinList = false;
                            for(var p=0;p<productCate_list.length;p++){
                                if(productCate_list[p].sfid == product_sfid){
                                    productCate = productCate_list[p];
                                    productIsinList = true;
                                    break;
                                }
                            }
                            if(productIsinList == false){
                                console.log("select productCate  error:") 
                                // console.log('is within exclusive campaign:'+transactionObj.ivls__receiptno__c)
                                // querystring = markStatus('loyaltycore.', 'ivls__transaction__c', router.AddSingleQuoteSymbol('Exclusive'), router.AddSingleQuoteSymbol((transactionObj.id )));
                                // if(environment == "production"){
                                //     var markStatusResult = (await router.query_one_way(querystring));
                                //     console.log('markStatusResult :');
                                //     console.log(markStatusResult);
                                
                                // }
                                continue;
                            }


                            // querystring = "SELECT * FROM loyaltycore.ivls__product__c WHERE sfid = '"+product_sfid+"'";
                            // productCate = (await router.query_one_way(querystring)).rows[0];
                            // if(router.isNotNull(productCate)== false){
                            //     console.log("select productCate  error:") 
                            //     continue;
                            // }
                            cate1 = productCate.ivls__productcategoryl1__c;
                            cate2 = productCate.ivls__productcategoryl2__c;
                            cate3 = productCate.ivls__productcategoryl3__c;
                        }catch(e){
                            console.log("productCate error:") 
                            console.log(e) 
                            continue;
                        }
                        console.log("cate1:"+cate1) 
                        console.log("cate2:"+cate2) 
                        console.log("cate3:"+cate3) 
                        console.log("product_sfid:"+product_sfid) 

                        var currentTranObj={
                            "merchant":merchant,
                            "product_sfid":product_sfid,
                            "cate1":cate1,
                            "cate2":cate2,
                            "cate3":cate3
                        }

                        var targetMerchant = campaignObj.ivls__merchant__c;
                        var targetProduct = campaignObj.ivls__product__c;
                        var targetCate = campaignObj.ivls__productcategory__c;
                        var meetCondition = true;

                        if(router.isNotNull(targetMerchant)){
                            if(targetMerchant !== merchant){
                                // console.log("membership unmatched");
                                var meetCondition = false;
                            }
                        }

                        if(router.isNotNull(targetProduct)){
                            if(targetProduct !== product_sfid){
                                console.log("targetProduct !== product_sfid");
                                var meetCondition = false;
                            }
                        }
                        if(router.isNotNull(targetCate)){
                            if(targetCate !== cate1 && targetCate !== cate2 && targetCate !== cate3){
                                // console.log("membership unmatched");
                                var meetCondition = false;
                            }
                        }
                    
                        if(meetCondition == false){
                            console.log("product, merchant, cate unmatched");
                            continue;
                        }

                    }


                    if(campaignObj.ivls__applytospecificmembers_only__c == true){
                        //check the member is specific member of the campaign
                        querystring =  "SELECT campaignmember.id, campaignmember.sfid, campaignmember.campaignid as campaignid, campaignmember.contactid as contactid, campaign.startdate, campaign.enddate FROM loyaltycore.campaignmember as campaignmember JOIN loyaltycore.campaign as campaign ON campaignmember.campaignid = campaign.sfid WHERE campaign.sfid = '"+campaignObj.sfid+"'";
                        var campaignMember_list = (await router.query_one_way(querystring)).rows;

                        var isInSpecificMemberList = false;
                        for (var k=0;k<campaignMember_list.length;k++){
                            if(campaignMember_list[k].campaignid == campaignObj.sfid){
                                if(campaignMember_list[k].contactid == memberObj.Account.personcontactid){
                                    isInSpecificMemberList = true;
                                }
                            }
                        }

                        if(isInSpecificMemberList == false){
                            console.log("isInSpecificMemberList:"+isInSpecificMemberList);
                            continue;
                        }
                    }else{
                        //check membership and membership tier
                        if(router.isNotNull(campaignObj.ivls__tier__c)){
                            if(campaignObj.ivls__tier__c !== CurrentMembershipTier.sfid){
                                console.log("campaignObj.ivls__tier__c !== CurrentMembershipTier.sfi");
                                continue;
                            }
                        }else{
                            //check if within specific sequence
                            if(router.isNotNull(campaignObj.ivls__tiersequencefrom__c)){
                                if(CurrentMembershipTier.ivls__sequence__c < campaignObj.ivls__tiersequencefrom__c ){
                                    console.log("CurrentMembershipTier.ivls__sequence__c < campaignObj.ivls__tiersequencefrom__c");
                                    continue;
                                }
                            }
                            if(router.isNotNull(campaignObj.ivls__tiersequenceto__c)){
                                if(CurrentMembershipTier.ivls__sequence__c > campaignObj.ivls__tiersequenceto__c ){
                                    console.log("CurrentMembershipTier.ivls__sequence__c > campaignObj.ivls__tiersequenceto__c");
                                    continue;
                                }
                            }
                        
                        }
                        
                     
                    }
                    
                    //--------------------------------overall checking condition-----------------------//

                    //@@TBU@@ check source
                    if(router.isNotNull(campaignObj.ivls__vip__c)){
                        if(campaignObj.ivls__vip__c == true){
                            if(memberObj.Account.ivls__vip__pc !== true){
                                continue;
                            }
                        }
                    }

                    // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__gender__c, memberObj.Account.ivls__gender__pc);
                    // if(isMatched == false){
                    //     console.log("campaign gender not matched");
                    //     continue;
                    // }
                    
                    // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__interestedin__c, memberObj.Account.ivls__interestedin__pc);
                    // if(isMatched == false){
                    //     console.log("campaign interested in not matched");
                    //     continue;
                    // }
                    // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__country__c, memberObj.Account.ivls__country__pc);
                    // if(isMatched == false){
                    //     console.log("campaign country not matched");
                    //     continue;
                    // }
                    // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__region__c, memberObj.Account.ivls__region__pc);
                    // if(isMatched == false){
                    //     console.log("campaign region not matched");
                    //     continue;
                    // }

                    var dateJoin = memberObj.Account.ivls__datejoin__pc;  

                    if(router.isNotNull(campaignObj.ivls__yearfrom__c)){
                        var NowMinusYearFrom = new Date();
                        NowMinusYearFrom.setMonth(NowMinusYearFrom.getFullYear() - Number(campaignObj.ivls__yearfrom__c));
                        if(dateJoin < NowMinusYearFrom){
                            console.log("yearfrom region not matched");
                            continue;
                        }
                    }

                    if(router.isNotNull(campaignObj.ivls__yearto__c)){
                        var NowMinusYearTo = new Date();
                        NowMinusYearTo.setMonth(NowMinusYearFrom.getFullYear() - Number(campaignObj.ivls__yearto__c));
                        if(dateJoin > NowMinusYearTo){
                            console.log("yearto region not matched");
                            continue;
                        }
                    }
                    
                   
                    if(router.isNotNull(campaignObj.ivls__minimumspending__c)){
                        if(campaignObj.ivls__minimumspending__c > currentSpending){
                            continue;
                        }
                    }

                    if(router.isNotNull(campaignObj.ivls__maximumspending__c)){
                        if(campaignObj.ivls__maximumspending__c > currentSpending){
                            var campaign_currentSpending = campaignObj.ivls__maximumspending__c;
                            }else{
                            var campaign_currentSpending = currentSpending;
                        }
                    }else{
                        var campaign_currentSpending = currentSpending;
                    }

                    console.log("Line campaign_currentSpending:"+campaign_currentSpending);

                    //@@TBU@@ check day of week
                    //@@TBU@@ check time from
                    //@@TBU@@ check time to
                    //@@TBU@@ check reference object
                    //@@TBU@@ check reference object condition

                    
                    
    
                    var calc_points= campaignObj.ivls__points__c ;
                    var calc_pointsmultiple= campaignObj.ivls__pointsmultiple__c ;
                    var calc_pointsbucket= "Campaign";
                    // var calc_pointsbucket= campaignObj.ivls__pointsbucket__c ;
    
    
                    console.log("calc_points="+calc_points);
                    console.log("calc_pointsmultiple="+calc_pointsmultiple);
                    console.log("calc_pointsbucket="+calc_pointsbucket);   
    
    
                    var earnedPoints = 0;
                    if(router.isNotNull(calc_points) == false && router.isNotNull(calc_pointsmultiple) == false){
                        console.log("PointScheme missing ivls__points__c and calc_pointsmultiple:"+campaignObj.id) 
                        continue;
                    }else if (router.isNotNull(calc_points) && router.isNotNull(calc_pointsmultiple) == false){
                        earnedPoints = calc_points;
                    }else if (router.isNotNull(calc_pointsmultiple) && router.isNotNull(calc_points) == false){
                        earnedPoints = Number(calc_pointsmultiple) * campaign_currentSpending;
                    }else if(router.isNotNull(calc_points) && router.isNotNull(calc_pointsmultiple)){
                        earnedPoints = calc_points;
                    }
                
                    console.log("========   campaignSummary   ======== ");   
                    console.log("earnedPoints="+earnedPoints);   
                    var lineActivityName = '';
                    var lineActivityId = '';
    
                    if(router.isNotNull(campaignObj.name)){
                        lineActivityName =  campaignObj.name ;
                        lineActivityId = campaignObj.sfid;
                    }
    
                    if(earnedPoints > 0){

                        var transactiondate = new Date();
                        var membershipTierObj = CurrentMembershipTier;
                        var expiryDATE = await updatemember.getExpiryDate(pool,yearstart, transactiondate,  membershipTierObj);
                        console.log('expiryDATE:'+expiryDATE);


                        var oldPTK = "";
                        querystring = "SELECT ivls__pointstokeeptier__c FROM loyaltycore.account WHERE ivls__pgid__c = " + targetMemberPGID;
                        oldPTK =  ( await router.query_one_way(querystring)).rows[0].ivls__pointstokeeptier__c;


                        var InsertObj = {};
                        InsertObj.ivls__memberpgid__c = router.AddSingleQuoteSymbol(targetMemberPGID);
                        InsertObj.ivls__member__c = router.AddSingleQuoteSymbol(memberObj.Account.id) ;
                        InsertObj.ivls__transactiondate__c = "NOW() - interval '8 hour'";
                        InsertObj.createddate = 'NOW()';
                        InsertObj.lastmodifieddate = 'NOW()';
                        InsertObj.ivls__receiptdate__c = 'NOW()';
                        InsertObj.ivls__source__c = "'System'";
                        InsertObj.ivls__status__c = "'Complete'";
                        InsertObj.ivls__activities__c = router.AddSingleQuoteSymbol(lineActivityName);
                        InsertObj.sourceobject = router.AddSingleQuoteSymbol(referenceObject) ;
                        if(referenceObject == 'ivls__transaction__c'){
                            InsertObj.sourceobjectid = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].id) ;
                        }else{
                            if(router.isNotNull(meetConditionRecord_list[i].ivls__pgid__c)){
                                InsertObj.sourceobjectid = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].ivls__pgid__c) ;
                            }
                            
                        }
                      

                        InsertObj.ivls__pointspool__c = router.AddSingleQuoteSymbol(pointsPool);
                        InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Transaction__c',pointsPool, recordtype_list));
                        InsertObj.ivls__datepointsexpiry__c = router.AddSingleQuoteSymbol(expiryDATE);
                        InsertObj.ivls__pointsearned__c = earnedPoints;
                        InsertObj.ivls__pointsnet__c = earnedPoints;
                        InsertObj.ivls__contact__c = router.AddSingleQuoteSymbol(memberObj.Account.personcontactid);
                        InsertObj.ivls__tier__pc =  router.AddSingleQuoteSymbol(CurrentMembershipTier.sfid);
                        querystring = router.getSQL_Insert(InsertObj, 'ivls__transaction__c', false);
                      
                        console.log("transaction:");
                        // console.log(InsertObj);
    
    
                        var querystring1 = querystring + " RETURNING id;";
                        console.log("behaviour level insert transaction SQL :");
                        console.log(querystring1);
                      
                        if(environment == "production"){
                            var client = await pool.connect();
                            try {
                            
                                await client.query('BEGIN')
                                var res  = await client.query(querystring1);
                            
                                var insertPhotoValues = [res.rows[0].id];
                                var InsertObj = {};
                                InsertObj.transaction_id = insertPhotoValues;
                                InsertObj.ivls__memberpgid__c = targetMemberPGID;
                                InsertObj.ivls__pointsbucket__c =calc_pointsbucket;
                                InsertObj.ivls__points__c = earnedPoints;
                                InsertObj.ivls__activity__c = lineActivityName;
                                InsertObj.ivls__ps_campain_id = lineActivityId;
                                InsertObj.ref_type = "Campaign";
                                InsertObj.ivls__pointspool__c = pointsPool;
                                querystring2 = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', true);
                                console.log("behaviour level insert transaction detail SQL :");
                                console.log(querystring2);
                                console.log("transaction detail:");
                                console.log(InsertObj);
                              
                                await client.query(querystring2);

                                if(environment == 'production'){
                                    await client.query('COMMIT')
                                }else{
                                    await client.query('ROLLBACK')
                                }
                              
                                // await client.query('COMMIT')
                            }catch (e) {
                                await client.query('ROLLBACK')
                            
                                console.log("ROLLBACK:");   
                                console.log(e);   
                                throw e
                            }finally {
                                client.release()
                            }
                        }

                        // if(referenceObject == 'ivls__transaction__c'){
                        //     var transactiondatetime = meetConditionRecord_list[i].ivls__transactiondate__c;
                        //     transactiondatetime = router.getSQLDateTimeFormatWithSeconds(transactiondatetime);
                        // }else{
                        //     var transactiondatetime = "NOW()";
                        // }
                        // var transactiondatetime = "NOW()";
                       
                        var updateMemberSQL = await updatemember.updateMemberInfo(pool, targetMemberPGID,  membershipObj, membershiptiers_list, oldPTK);
                        console.log("SECTION3 updateMemberSQL:");
                        console.log(updateMemberSQL);
                        if(updateMemberSQL == false){
                            var tempstr = "updateMemberSQL fail:"+updateMemberSQL
                            console.log(tempstr) 

                            if(log_level == "debug"){
                                var querystring_error_log = router.get_Querystring_error_log();
                                var values = [referenceObject, meetConditionRecord_list[i].id, 'part3 error', tempstr];
                                var result  = (await router.query_with_parameter(querystring_error_log, values));
                                console.log(result);
                            }
                           
                            continue;
                        }
                        if(environment == "production"){
                            var updateResult = await router.query_one_way(updateMemberSQL);
                            console.log(updateResult);
                        }
                    }
                }
    
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log('Error Processing :campaign_list[j].id:'+ campaign_list[j].name);
                console.log('Error Message:');
                console.log(e);

                var tempstr = 'Error Processing :campaign_list[j].id:'+ campaign_list[j].name
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['Campaign', campaignObj.id, 'part3 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
                    continue;
                }
                
            }
        }


        // //// loop campain list  ////
        // for(var j=0;j<campaign_list.length;j++){
        //     try{
              
        //         var campaignObj = campaign_list[j];
        //         ////    earnpoints campaign    ////
        //         var recordtypeid_1 = router.getRecordTypeId('Campaign','Coupons', recordtype_list);
        //         var recordtypeid_0 = campaignObj.recordtypeid;
        //         if(recordtypeid_0 !== recordtypeid_1){
        //             continue;
        //         }
              
        //         console.log("Currently  "+(j+1)+"/"+totalNumOfCampaign + " Campaign name:" + campaignObj.name); 
        //         //// validate fields
        //         var calculateBy = campaignObj.ivls__calculateby__c;
        //         var referenceObject = campaignObj.ivls__referenceobject__c;
        //         var referenceField = campaignObj.ivls__field__c;
        //         // var pointsPool = campaignObj.ivls__pointspool__c;//Spending or A&P
        //         var condition_fieldValue= campaignObj.ivls__filtervalue__c ;
        //         var condition_fieldFormula= campaignObj.ivls__filterformula1__c ;
        //         var operator = campaignObj.ivls__operator__c 

        //         if(router.isNotNull(calculateBy)==false){
        //             console.log("calculateBy is null ");   
        //             continue;
        //         }
        //         if(router.isNotNull(referenceObject)==false){
        //             console.log("referenceObject is null ");   
        //             continue;
        //         }
        //         if(router.isNotNull(referenceField)==false){
        //             console.log("referenceField is null ");   
        //             continue;
        //         }
        //         // if(router.isNotNull(pointsPool)==false){
        //         //     console.log("pointsPool is null ");   
        //         //     continue;
        //         // }
        //         if(router.isNotNull(condition_fieldValue)==false && router.isNotNull(condition_fieldFormula)==false){
        //             console.log("condition_fieldValue and condition_fieldFormula is null ");   
        //             continue;
        //         }
        //         if(operator == false){
        //             console.log("Cannot read ivls__operator__c:"+ivls__operator__c)
        //              continue;
        //         }
            
        //         referenceField =  referenceField.toLowerCase();
        //         referenceObject = referenceObject.toLowerCase();
        //         operator = getSQLOperator(operator);
            

        //         var isDateTime = false;
        //         var datetimeArray = router.getDateTimeKeyword();
        //         for(var h=0;h<datetimeArray.length;h++){
        //             if(referenceField.includes(datetimeArray[h]) == true){
        //                 isDateTime = true;
        //                 break;
        //             }
        //         }


        //         ////    check whether targetValue is datetime format    ////
        //         if(router.isNotNull(isDateTime) == false){
        //             console.log("error in get isDateTime");   
        //             continue;
        //         }
              
                
    
        //         var targetValue = false;
        //         if (router.isNotNull(condition_fieldValue)){
        //             targetValue = condition_fieldValue;
        //         }else if (router.isNotNull(condition_fieldFormula) && router.isNotNull(condition_fieldValue) == false){
        //             targetValue = getSQLtargetValue(condition_fieldFormula, processDate);
        //         }
        //         if(targetValue == "invalid"){
        //             console.log("Cannot read formula:"+condition_fieldFormula);   
        //             continue;
        //         }
                


        //         console.log("condition_fieldFormula:"+condition_fieldFormula);   
        //         console.log("targetValue:");   
        //         console.log(targetValue);   
        //         console.log("isDateTime:"+isDateTime); 
                
                


        //         if(isDateTime == false){
        //             if (referenceObject == 'ivls__transaction__c'){
        //                 if(processDate == "T+1"){
        //                     querystring = "SELECT * FROM " +dbname+referenceObject+ " WHERE " + referenceField + " "+ operator +" "+ targetValue + " AND ivls__pointsbasic__c > 0 AND DATE_PART('day', NOW() - ivls__transactiondate__c ) = 1;";
        //                 }else{
        //                     querystring = "SELECT * FROM " +dbname+referenceObject+ " WHERE " + referenceField + " "+ operator +" "+ targetValue + " AND ivls__pointsbasic__c > 0 AND DATE_PART('day', NOW() - ivls__transactiondate__c ) = 0;";
        //                 }
                        
        //             }else{
        //                 querystring = "SELECT * FROM " +dbname+referenceObject+ " WHERE " + referenceField + operator + targetValue;
        //             }
        //         }else{
        //             if(condition_fieldFormula == 'This Month' || condition_fieldFormula == 'Last Month'  ){
        //                 // querystring = "SELECT * FROM "+dbname+referenceObject +" WHERE "+ "DATE_PART('month',"+referenceField+" - "+targetValue.firstDay+" )>=0 AND DATE_PART('month',"+referenceField+" - "+ targetValue.lastDay + ") <=0" 
        //                 querystring = "SELECT * FROM loyaltycore."+referenceObject+" WHERE "+referenceField+"::date >=  '"+targetValue.firstDay+"' AND "+referenceField+"::date <=  '"+targetValue.lastDay+"';";
        //             }else if(condition_fieldFormula == 'This Calendar Year'){
        //                 // querystring = "SELECT * FROM "+dbname+referenceObject +" WHERE "+ "DATE_PART('year',"+referenceField+" - "+targetValue.firstDay+" )>=0 AND DATE_PART('month',"+referenceField+" - "+ targetValue.lastDay + ") <=0" 
        //                 querystring = "SELECT * FROM loyaltycore."+referenceObject+" WHERE "+referenceField+"::date >=  '"+targetValue.firstDay+"' AND "+referenceField+"::date <=  '"+targetValue.lastDay+"';";
                      
        //             }else{
        //                 querystring = "SELECT * FROM loyaltycore."+referenceObject +" WHERE "+referenceField+"::date = '"+targetValue+"' ";//'2018-08-20'";
        //                 // querystring = "SELECT * FROM "+dbname+referenceObject +" WHERE "+ "DATE_PART('day',"+referenceField+" - "+targetValue+" ) = 0 ";
        //             }
        //         }
        //         console.log("meetConditionRecord_list querystring:" +querystring);
        //         var meetConditionRecord_list = (await router.query_one_way(querystring)).rows;
        //         if(router.isNotNull(meetConditionRecord_list)==false || meetConditionRecord_list.length==0){
        //             console.log("No reference list meet conditions");
        //             continue;
        //         }
    
        //         console.log("meetConditionRecord_list:" +meetConditionRecord_list.length);
        //         // console.log(meetConditionRecord_list);
        //         //need to check is Spending!!!!!!!
        //         console.log("-------------------------------------");
        //         console.log("referenceObject="+referenceObject);
        //         console.log("referenceField="+referenceField);
        //         // console.log("pointsPool="+pointsPool);   
        //         console.log("isDateTime="+isDateTime);   
        //         console.log("targetValue=");   
        //         console.log(targetValue);   
        //         console.log("calculateBy="+calculateBy); 
        //         console.log("referenceObject="+referenceObject); 
        //         console.log("-------------------------------------");
                


        //         var targetMembershipTier = '';//sfid
        //         var targetMemberPGID = '';


        //         ////    get member's information    ////
        //         for(var i=0;i<meetConditionRecord_list.length;i++){
        //             if(referenceObject == 'account'){   
        //                 targetMembershipTier = meetConditionRecord_list[i].ivls__membershiptier__pc;
        //                 targetMemberPGID = meetConditionRecord_list[i].ivls__pgid__c;
        //             }else if (referenceObject == 'ivls__transaction__c'){
        //                 targetMemberPGID = meetConditionRecord_list[i].ivls__memberpgid__c;
        //                 querystring = "SELECT ivls__membershiptier__pc FROM loyaltycore.account WHERE ivls__pgid__c = '"+meetConditionRecord_list[i].ivls__memberpgid__c+"'";
        //                 try{
        //                     targetMembershipTier = (await router.query_one_way(querystring)).rows[0].ivls__membershiptier__pc;
    
        //                 }catch(e){
        //                     console.log("targetMembershipTier error:") 
        //                     console.log(e) 
        //                     continue;
        //                 }
                    
        //             }else {
        //                 continue;
        //             }
                    
        //             if(router.isNotNull(targetMemberPGID)==false){
        //                 console.log("targetMemberPGID is null :") 
        //                 continue;
        //             }
    
        //             if(router.isNotNull(targetMembershipTier)==false){
        //                 console.log("targetMembershipTier is null :") 
        //                 continue;
        //             }

        //             if(router.isNotNull(campaignObj.ivls__tier__c)){
        //                 if(targetMembershipTier !== campaignObj.ivls__tier__c){
        //                     console.log("targetMembershipTier unmathced:") 
        //                     continue;
        //                 }
        //             }
                    

                    
                
        //             var member_list = await router.selectALL_with_parameter('account','ivls__pgid__c','=',targetMemberPGID);
        //             var memberObj= router.getCurrentMemberInfo(member_list[0], membershiptiers_list);
        //             var CurrentMembershipTier = memberObj.CurrentMembershipTier;
                    


        //             ////    check blacklisted member    ////
        //             var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
        //             if(currentMemberTierIsBlacklisted == true){
        //                 console.log("currentMemberTierIsBlacklisted:"+targetMemberPGID);
        //                 continue;
        //             }


        //             ////    check whether this campaign is already given to the member  ////
        //             if(referenceObject == 'ivls__transaction__c'){
        //                 // querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+campaignObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID + "AND transaction_id = "+ meetConditionRecord_list[i].id;
        //                 // querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].id+"' AND (SELECT ivls__activity__c FROM loyaltycore.ivls__pointsscheme__c WHERE sfid = d.ivls__ps_campain_id) = '"+campaignObj.ivls__activity__c+"';"
        //                 querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].id+"' AND d.ivls__ps_campain_id = '"+campaignObj.sfid+"';";
        //             }else{
        //                 // querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__ps_campain_id = '"+campaignObj.sfid+"' AND ivls__memberpgid__c = " + targetMemberPGID;
        //                 // querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].ivls__pgid__c+"' AND (SELECT ivls__activity__c FROM loyaltycore.ivls__pointsscheme__c WHERE sfid = d.ivls__ps_campain_id) = '"+campaignObj.ivls__activity__c+"';"
        //                 querystring = "SELECT t.id FROM loyaltycore.ivls__transactiondetails__c as d JOIN loyaltycore.ivls__transaction__c as t ON d.transaction_id = t.id WHERE t.sourceobject = '"+referenceObject+"' AND t.sourceobjectid ='"+meetConditionRecord_list[i].ivls__pgid__c+"' AND d.ivls__ps_campain_id = '"+campaignObj.sfid+"';";
        //             }
                    
        //             var isAlreadyGiven_list = (await router.query_one_way(querystring)).rows;
        //             console.log("isAlreadyGiven_list.length:"+isAlreadyGiven_list.length);
        //             var isAlreadyGiven = false;
        //             if(isAlreadyGiven_list.length > 0){
        //                 isAlreadyGiven = true;
        //             }
        //             if(isAlreadyGiven){
        //                 console.log("This campaign ("+campaignObj.name+") is already given for:"+targetMemberPGID);
        //                 continue;
        //             }





        //             ////    get spending    ////
        //             var currentSpending_totalLevel = 0;
        //             var currentSpending_lineLevel = 0;
        //             var currentSpending = 0;

        //             if(referenceObject == 'ivls__transaction__c'){
                        
        //                 currentSpending_totalLevel = meetConditionRecord_list[i].ivls__totalcashamount__c ;
        //                 currentSpending_lineLevel =  meetConditionRecord_list[i].ivls__spendingexclusive__c ;
        //                 console.log("currentSpending_totalLevel="+currentSpending_totalLevel); 
        //                 console.log("currentSpending_lineLevel="+currentSpending_lineLevel); 

        //                 if(calculateBy == "Line"){
        //                     currentSpending = currentSpending_lineLevel;
                            
        //                 }else{
        //                     currentSpending = currentSpending_totalLevel;
        //                 }
        //             }



        //             ////    if calculateby line, check merchant, product, product cate  ////
        //             if(calculateBy=="Line" && referenceObject == 'ivls__transaction__c'){
        //                 var  cate1 = "";
        //                 var  cate2 = "";
        //                 var  cate3 = "";
        //                 var product_sfid = meetConditionRecord_list[i].ivls__product__c;
        //                 var productCate = "";
        //                 var merchant = meetConditionRecord_list[i].ivls__merchant__c;

        //                 try{
        //                     querystring = "SELECT * FROM loyaltycore.ivls__product__c WHERE sfid = '"+product_sfid+"'";
        //                     productCate = (await router.query_one_way(querystring)).rows[0];
        //                     if(router.isNotNull(productCate)== false){
        //                         console.log("select productCate  error:") 
        //                         continue;
        //                     }
        //                     cate1 = productCate.ivls__productcategoryl1__c;
        //                     cate2 = productCate.ivls__productcategoryl2__c;
        //                     cate3 = productCate.ivls__productcategoryl3__c;
        //                 }catch(e){
        //                     console.log("productCate error:") 
        //                     console.log(e) 
        //                     continue;
        //                 }
        //                 console.log("cate1:"+cate1) 
        //                 console.log("cate2:"+cate2) 
        //                 console.log("cate3:"+cate3) 
        //                 console.log("product_sfid:"+product_sfid) 

        //                 var currentTranObj={
        //                     "merchant":merchant,
        //                     "product_sfid":product_sfid,
        //                     "cate1":cate1,
        //                     "cate2":cate2,
        //                     "cate3":cate3
        //                 }

        //                 var targetMerchant = campaignObj.ivls__merchant__c;
        //                 var targetProduct = campaignObj.ivls__product__c;
        //                 var targetCate = campaignObj.ivls__productcategory__c;
        //                 var meetCondition = true;

        //                 if(router.isNotNull(targetMerchant)){
        //                     if(targetMerchant !== merchant){
        //                         // console.log("membership unmatched");
        //                         var meetCondition = false;
        //                     }
        //                 }

        //                 if(router.isNotNull(targetProduct)){
        //                     if(targetProduct !== product_sfid){
        //                         console.log("targetProduct !== product_sfid");
        //                         var meetCondition = false;
        //                     }
        //                 }
        //                 if(router.isNotNull(targetCate)){
        //                     if(targetCate !== cate1 && targetCate !== cate2 && targetCate !== cate3){
        //                         // console.log("membership unmatched");
        //                         var meetCondition = false;
        //                     }
        //                 }
                    
        //                 if(meetCondition == false){
        //                     console.log("product, merchant, cate unmatched");
        //                     continue;
        //                 }

        //             }


        //             if(campaignObj.ivls__applytospecificmembers_only__c == true){
        //                 //check the member is specific member of the campaign
        //                 querystring =  "SELECT campaignmember.id, campaignmember.sfid, campaignmember.campaignid as campaignid, campaignmember.contactid as contactid, campaign.startdate, campaign.enddate FROM loyaltycore.campaignmember as campaignmember JOIN loyaltycore.campaign as campaign ON campaignmember.campaignid = campaign.sfid WHERE campaign.sfid = '"+campaignObj.sfid+"'";
        //                 var campaignMember_list = (await router.query_one_way(querystring)).rows;

        //                 var isInSpecificMemberList = false;
        //                 for (var k=0;k<campaignMember_list.length;k++){
        //                     if(campaignMember_list[k].campaignid == campaignObj.sfid){
        //                         if(campaignMember_list[k].contactid == memberObj.Account.personcontactid){
        //                             isInSpecificMemberList = true;
        //                         }
        //                     }
        //                 }

        //                 if(isInSpecificMemberList == false){
        //                     console.log("isInSpecificMemberList:"+isInSpecificMemberList);
        //                     continue;
        //                 }
        //             }else{
        //                 //check membership and membership tier
        //                 if(router.isNotNull(campaignObj.ivls__tier__c)){
        //                     if(campaignObj.ivls__tier__c !== CurrentMembershipTier.sfid){
        //                         console.log("campaignObj.ivls__tier__c !== CurrentMembershipTier.sfi");
        //                         continue;
        //                     }
        //                 }else{
        //                     //check if within specific sequence
        //                     if(router.isNotNull(campaignObj.ivls__tiersequencefrom__c)){
        //                         if(CurrentMembershipTier.ivls__sequence__c < campaignObj.ivls__tiersequencefrom__c ){
        //                             console.log("CurrentMembershipTier.ivls__sequence__c < campaignObj.ivls__tiersequencefrom__c");
        //                             continue;
        //                         }
        //                     }
        //                     if(router.isNotNull(campaignObj.ivls__tiersequenceto__c)){
        //                         if(CurrentMembershipTier.ivls__sequence__c > campaignObj.ivls__tiersequenceto__c ){
        //                             console.log("CurrentMembershipTier.ivls__sequence__c > campaignObj.ivls__tiersequenceto__c");
        //                             continue;
        //                         }
        //                     }
                        
        //                 }
                        
                     
        //             }
                    
        //             //--------------------------------overall checking condition-----------------------//

        //             //@@TBU@@ check source
        //             if(router.isNotNull(campaignObj.ivls__vip__c)){
        //                 if(campaignObj.ivls__vip__c == true){
        //                     if(memberObj.Account.ivls__vip__pc !== true){
        //                         continue;
        //                     }
        //                 }
        //             }

        //             // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__gender__c, memberObj.Account.ivls__gender__pc);
        //             // if(isMatched == false){
        //             //     console.log("campaign gender not matched");
        //             //     continue;
        //             // }
                    
        //             // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__interestedin__c, memberObj.Account.ivls__interestedin__pc);
        //             // if(isMatched == false){
        //             //     console.log("campaign interested in not matched");
        //             //     continue;
        //             // }
        //             // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__country__c, memberObj.Account.ivls__country__pc);
        //             // if(isMatched == false){
        //             //     console.log("campaign country not matched");
        //             //     continue;
        //             // }
        //             // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__region__c, memberObj.Account.ivls__region__pc);
        //             // if(isMatched == false){
        //             //     console.log("campaign region not matched");
        //             //     continue;
        //             // }

        //             var dateJoin = memberObj.Account.ivls__datejoin__pc;  

        //             if(router.isNotNull(campaignObj.ivls__yearfrom__c)){
        //                 var NowMinusYearFrom = new Date();
        //                 NowMinusYearFrom.setMonth(NowMinusYearFrom.getFullYear() - Number(campaignObj.ivls__yearfrom__c));
        //                 if(dateJoin < NowMinusYearFrom){
        //                     console.log("yearfrom region not matched");
        //                     continue;
        //                 }
        //             }

        //             if(router.isNotNull(campaignObj.ivls__yearto__c)){
        //                 var NowMinusYearTo = new Date();
        //                 NowMinusYearTo.setMonth(NowMinusYearFrom.getFullYear() - Number(campaignObj.ivls__yearto__c));
        //                 if(dateJoin > NowMinusYearTo){
        //                     console.log("yearto region not matched");
        //                     continue;
        //                 }
        //             }
                    
                   
        //             if(router.isNotNull(campaignObj.ivls__minimumspending__c)){
        //                 if(campaignObj.ivls__minimumspending__c > currentSpending){
        //                     continue;
        //                 }
        //             }
                    
        //             //@@TBU@@ check day of week
        //             //@@TBU@@ check time from
        //             //@@TBU@@ check time to
        //             //@@TBU@@ check reference object
        //             //@@TBU@@ check reference object condition

                    
                    
        //             var rewardSFID = campaignObj.ivls__reward__c;
        //             var rewardQTY = campaignObj.ivls__rewardquantity__c;
                  
        //             rewardQTY = Number(rewardQTY);
        //             if(router.isNotNull(rewardSFID)==false){
        //                 console.log("rewardSFID is null :") 
        //                 continue;
        //             }
        //             if(router.isNotNull(rewardQTY)==false){
        //                 console.log("rewardQTY is null :") 
        //                 continue;
        //             }

        //             console.log("rewardSFID="+rewardSFID);
        //             console.log("rewardQTY="+rewardQTY);
                    
        //             querystring = "SELECT * FROM loyaltycore.ivls__reward__c WHERE sfid = 'a0q0l000000cWLVAA2' LIMIT 1;";
        //             var rewardObj = (await router.query_one_way(querystring)).rows[0];
        //             var ivls__validity__c = rewardObj.ivls__validity__c;
        //             var rewardPoints = rewardObj.ivls__rewardvalue__c
        //             rewardPoints = Number(rewardPoints);
        //             if(router.isNotNull(ivls__validity__c)==false){
        //                 console.log("ivls__validity__c is null :") 
        //                 continue;
        //             }
        //             if(router.isNotNull(rewardPoints)==false){
        //                 console.log("rewardPoints is null :") 
        //                 continue;
        //             }

        //             var pointsPool = "'A&P'";

        //             for(var r=0;r<rewardQTY;r++){
        //                 try{

        //                     var InsertObj = {};
        //                     InsertObj.ivls__memberpgid__c = targetMemberPGID;
        //                     InsertObj.ivls__member__c = router.AddSingleQuoteSymbol(memberObj.Account.id) ;
        //                     InsertObj.ivls__transactiondate__c = 'NOW()';
        //                     InsertObj.createddate = 'NOW()';
        //                     InsertObj.lastmodifieddate = 'NOW()';
        //                     InsertObj.ivls__source__c = "'System'";
        //                     InsertObj.ivls__status__c = "'Complete'";
        //                     InsertObj.sourceobject = router.AddSingleQuoteSymbol(referenceObject) ;

        //                     if(referenceObject == 'ivls__transaction__c'){//warranty
        //                         InsertObj.sourceobjectid = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].id) ;
        //                     }else{
        //                         InsertObj.sourceobjectid = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].ivls__pgid__c) ;
        //                     }
                            
                            
        //                     // if(referenceObject == 'asset'){//warranty
        //                     //     InsertObj.ivls__sequenceno__c = router.AddSingleQuoteSymbol(meetConditionRecord_list[i].id);//sfid
        //                     // }
        //                     InsertObj.ivls__activities__c =  "'"+campaignObj.name+" - "+rewardObj.name+"'";
        //                     InsertObj.ivls__pointspool__c = "'A&P'";
        //                     InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Transaction__c',pointsPool, recordtype_list));
        //                     // InsertObj.ivls__datepointsexpiry__c = router.AddSingleQuoteSymbol(expiryDATE);
        //                     InsertObj.ivls__pointsearned__c = rewardPoints;
        //                     InsertObj.ivls__pointsnet__c = 0;
        //                     InsertObj.ivls__contact__c = router.AddSingleQuoteSymbol(memberObj.Account.personcontactid);
        //                     InsertObj.ivls__tier__pc =  router.AddSingleQuoteSymbol(CurrentMembershipTier.sfid);
        //                     querystring = router.getSQL_Insert(InsertObj, 'ivls__transaction__c', false);
                        
        //                     console.log("transaction:");
        //                     console.log(InsertObj);


        //                     var querystring0 = querystring + " RETURNING id;";
        //                     console.log("behaviour level insert transaction SQL :");
        //                     console.log(querystring0);



        //                     var InsertObj = {};
        //                     InsertObj.ivls__member__c = router.AddSingleQuoteSymbol(memberObj.Account.id);
        //                     InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Redemption__c','Open', recordtype_list));
        //                     InsertObj.ivls__status__c= "'Open'";
        //                     // InsertObj.ivls__reward__c= router.AddSingleQuoteSymbol('a0q0l000000nO6aAAE');
        //                     InsertObj.ivls__reward__c= router.AddSingleQuoteSymbol(rewardSFID);
                            
        //                     InsertObj.ivls__redeempoint__c= Math.abs(rewardPoints) * 1;
        //                     InsertObj.ivls__dateredeem__c= "NOW()";
        //                     InsertObj.ivls__dateexpiry__c= "NOW() + interval '"+ivls__validity__c+" days'";
        //                     // InsertObj.ivls__reason__c= "'Refund of transaction line No: "+originalTransactionObj.ivls__sequenceno__c+"'";
                            
        //                     InsertObj.createddate= "NOW()";
        //                     InsertObj.lastmodifieddate= "NOW()";
                            
        //                     querystring =  router.getSQL_Insert(InsertObj, "ivls__redemption__c", false)
        //                     var querystring1 = querystring + " RETURNING ivls__pgid__c;"
        //                     console.log("Insert redemption SQL:");
        //                     console.log(querystring1);
        //                     // if(environment == "production"){
        //                         var client = await pool.connect();
        //                         try {
                                
        //                             await client.query('BEGIN')

        //                             var res  = await client.query(querystring0);
        //                             console.log("ref1 SQL:");
        //                             console.log(res);
        //                             var transactionpgid = [res.rows[0].id];



        //                             var res  = await client.query(querystring1);
        //                             console.log("ref1 SQL:");
        //                             console.log(res);
        //                             var redemptionPGID = [res.rows[0].ivls__pgid__c];
                                
        //                             var avalableTransaction_list = await client.query("SELECT id, ivls__pointsnet__c, ivls__pointsredeemed__c,  IVLS__Member__c FROM loyaltycore.ivls__transaction__c WHERE ivls__memberpgid__c = "+targetMemberPGID+" AND ivls__pointsnet__c > 0 ORDER BY ivls__transactiondate__c ASC, ivls__pointspool__c DESC;");
        //                             for (let row of avalableTransaction_list.rows) {
                                       



        //                                 var InsertObj = {};
        //                                 InsertObj.transaction_id = row.id;
        //                                 InsertObj.ivls__memberpgid__c = targetMemberPGID;
        //                                 InsertObj.ivls__pointsbucket__c = "'Campaign'";
        //                                 InsertObj.ivls__points__c = Math.abs(rewardPoints) * 1;
        //                                 InsertObj.ivls__activity__c = "'"+campaignObj.name+" - "+rewardObj.ivls__rewarddescriptione__c+"'";
        //                                 InsertObj.ivls__ps_campain_id = "'"+campaignObj.sfid+"'";
        //                                 InsertObj.ref_type = "'Campaign'";
        //                                 InsertObj.ivls__pointspool__c = "'A&P'";
        //                                 InsertObj.createddate= "NOW()";
                                        
        //                                 // InsertObj.lastmodifieddate= "NOW()";
        //                                 querystring3 = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', false);
        //                                 console.log("behaviour level insert transaction detail SQL :");
        //                                 console.log(querystring3);
        //                                 var result = await client.query(querystring3);
        //                                 console.log("ref2 SQL:");
        //                                 console.log(result);



        //                                 var InsertObj = {};
        //                                 InsertObj.ivls__redemptionpgid__c = redemptionPGID;
        //                                 InsertObj.ivls__pointsredeem__c = Math.abs(rewardPoints) * 1;
        //                                 InsertObj.ivls__transactionpgid__c =transactionpgid;
        //                                 InsertObj.createddate= "NOW()";
        //                                 InsertObj.lastmodifieddate= "NOW()";
        //                                 querystring2 = router.getSQL_Insert(InsertObj, 'ivls__redemptiondetail__c', false);
        //                                 querystring2 = querystring2 + " RETURNING ivls__pgid__c;";
        //                                 var result = await client.query(querystring2);
        //                                 console.log("ref2 SQL:");
        //                                 console.log(result);
        //                                 console.log("insert redeemption detail SQL :");
        //                                 console.log(querystring2);
        //                                 // redemptionDetailSQL_list.push(querystring2);
                                        
        //                                 var redemptiondetailPGID = [result.rows[0].ivls__pgid__c];
                                        
            
                                        

        //                                 var InsertObj = {};
        //                                 InsertObj.transaction_id = row.id;
        //                                 InsertObj.ivls__memberpgid__c = targetMemberPGID;
        //                                 InsertObj.ivls__pointsbucket__c = "'Redeeming'";
        //                                 InsertObj.ivls__points__c = Math.abs(rewardPoints) * 1;
        //                                 InsertObj.ivls__activity__c = "'"+campaignObj.name+" - "+rewardObj.ivls__rewarddescriptione__c+"'";
        //                                 InsertObj.ref_id = redemptiondetailPGID;
        //                                 InsertObj.ref_type = "'IVLS__Redemption__c'";
        //                                 InsertObj.ivls__pointspool__c = "'A&P'";
        //                                 InsertObj.createddate= "NOW()";
                                        
        //                                 // InsertObj.lastmodifieddate= "NOW()";
        //                                 querystring4 = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', false);
        //                                 console.log("behaviour level insert transaction detail SQL :");
        //                                 console.log(querystring4);
        //                                 var result = await client.query(querystring4);
                                       
            
        //                                 console.log("ref2 SQL:");
        //                                 console.log(result);
                                        
        //                                 // redemptionDetailSQL_list.push(querystring3);
        //                                 // if(tempPoint == 0) 
        //                                 break;
        //                             }
            
                                    
                                    
        //                             if(environment == "production"){
        //                                 await client.query('COMMIT')
        //                             }else{
        //                                 await client.query('ROLLBACK')
        //                             }
            
                                    
            
        //                         }catch (e) {
        //                             await client.query('ROLLBACK')
                                
        //                             console.log("ROLLBACK=");   
        //                             console.log(e);   
        //                             throw e
        //                         }finally {
        //                             client.release()
        //                         }
        //                     // }
        //                 }catch(e){
        //                     console.log('Error On insert redemption on campaign :');
        //                     console.log(e);
        //                 }
        //             }
        //         }
    
        //     }catch(e){
        //         console.log("//////////////////////////////  WARNING  //////////////////////////////") 
        //         console.log('Error Processing :campaign_list[j].id:'+ campaign_list[j].name);
        //         console.log('Error Message:');
        //         console.log(e);
        //         continue;
        //     }
        // }

    }
    return true;
}
async function part4(log_level, pool, environment, dbname,membershiptiers_list, recordtype_list, membershipObj){
    //section4
    var querystring = "";
    var querystring_list = [];
    console.log("-----------------------    SECTION 4: REFUND    ------------------------");
    querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__status__c = 'Refund Open'  ORDER BY ivls__transactiondate__c  ";
    
    var refundTransaction_list = (await router.query_one_way(querystring)).rows;
    console.log("refundTransaction_list:"+refundTransaction_list.length);
    var totalNum = refundTransaction_list.length;
    for(var i=0;i<refundTransaction_list.length;i++){
        var currentNum = (i+1);
        console.log("refund position:"+currentNum+"/"+totalNum);
        try{
            var transactionObj = refundTransaction_list[i];
            var lineSequenceNo = transactionObj.ivls__sequenceno__c;
            var receiptNo = transactionObj.ivls__receiptno__c;
            var originalTransactionObj = {};
            var consolidatedTransactionObj = {};
            var hasPositiveQuantity = false;
            var hasNegativeQuantity = false;
            
                
            var targetMemberPGID = transactionObj.ivls__memberpgid__c;
            var targetMemberSFID = transactionObj.ivls__member__c;
            if(router.isNotNull(targetMemberPGID)== false || router.isNotNull(targetMemberSFID)== false ){
                var tempstr = 'targetMemberPGID or targetMemberSFID is null :'+ transactionObj.id;
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
    
                }
               
                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                }
                continue;
               
            }
            
            try{
                //get member information
                var member_list = await router.selectALL_with_parameter('account','ivls__pgid__c','=', targetMemberPGID);
                var memberObj= router.getCurrentMemberInfo(member_list[0], membershiptiers_list);
                var CurrentMembershipTier = memberObj.CurrentMembershipTier;
            }catch(e){

                var tempstr = 'Cannot select this member: '+ targetMemberPGID;
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);

                }
                

                console.log('Refund Error, select member faild:');
                console.log(e);
                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }
            }
           
            

            var pointsFrom = CurrentMembershipTier.ivls__pointsfrom__c;
            if(router.isNotNull(pointsFrom)== false){
                pointsFrom = 0;
                // console.log('pointsFrom is null :'+ CurrentMembershipTier.sfid);
                // continue;
            }
            try{
                querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__sequenceno__c = '"+lineSequenceNo+"' AND (ivls__status__c = 'Complete' OR  ivls__status__c = 'Refund Complete') AND ivls__quantity__c != 0 ";
                console.log('Select originalSQL 1:');
                console.log(querystring);
                var originalTransaction_list = (await router.query_one_way(querystring)).rows;
                var hasPositiveQuantity = false
                for(var j=0;j<originalTransaction_list.length;j++){
                    if(originalTransaction_list[j].ivls__quantity__c > 0){
                        hasPositiveQuantity = true;
                    }
                }
                if(hasPositiveQuantity == false){
                    querystring = "SELECT * FROM loyaltycore.ivls__transaction__c WHERE ivls__receiptno__c = '"+transactionObj.ivls__originalorderreference__c+"' AND ivls__product__c = '"+transactionObj.ivls__product__c+"' AND (ivls__status__c = 'Complete' OR  ivls__status__c = 'Refund Complete') AND ivls__quantity__c != 0 ";
                    console.log('Select originalSQL 2:');
                    console.log(querystring);
                    originalTransaction_list = (await router.query_one_way(querystring)).rows;
                }

            }catch(e){
                console.log('Refund Error, select original transaction faild:');
                console.log(e);
                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }

            }
           
            var refundedTransaction_list = [];
            var originalQTY = 0;
            var refunedQTY = 0;
            var refundingQTY = Math.abs(transactionObj.ivls__quantity__c);
            for(var j=0;j<originalTransaction_list.length;j++){
                if(originalTransaction_list[j].ivls__quantity__c > 0){
                    originalTransactionObj = originalTransaction_list[j];
                    consolidatedTransactionObj = originalTransaction_list[j];
                    hasPositiveQuantity = true;
                }

                if(originalTransaction_list[j].ivls__quantity__c < 0){
                    refundedTransaction_list.push(originalTransaction_list[j]);
                    refunedQTY = refunedQTY + Math.abs(originalTransaction_list[j].ivls__quantity__c);
                    hasNegativeQuantity = true;
                }
            }

            if(hasPositiveQuantity == false){
                var tempstr = "Cannot find matched original transaction of refund receipt:"+transactionObj.ivls__receiptno__c+" product:"+ transactionObj.ivls__product__c;
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
                }
            
                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                  
                }
                continue;
            }
            
            originalQTY = Math.abs(originalTransactionObj.ivls__quantity__c);
            console.log('originalQTY:'+ originalQTY);
            console.log('refunedQTY:'+ refunedQTY);

            //already fully refunded
            if(originalQTY == refunedQTY || ((refundingQTY +refunedQTY) > originalQTY)){
                var tempstr = 'already fully refunded:';
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);

                }
                
                
                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }
            }

            ratioRefunded = refunedQTY/originalQTY;
            console.log('ratioRefunded:'+ ratioRefunded);


            //get consolidated transaction
            var amountAlreadyRefunded =  consolidatedTransactionObj.ivls__receiptamount__c * ratioRefunded;
            var totalSpendingAlreadyRefunded =  consolidatedTransactionObj.ivls__totalcashamount__c * ratioRefunded;
            var spendingAlreadyRefunded = consolidatedTransactionObj.ivls__spendingexclusive__c * ratioRefunded;
            
            consolidatedTransactionObj.ivls__receiptamount__c = consolidatedTransactionObj.ivls__receiptamount__c - amountAlreadyRefunded;
            consolidatedTransactionObj.ivls__totalcashamount__c = consolidatedTransactionObj.ivls__totalcashamount__c - totalSpendingAlreadyRefunded;
            consolidatedTransactionObj.ivls__spendingexclusive__c = consolidatedTransactionObj.ivls__spendingexclusive__c - spendingAlreadyRefunded;
            consolidatedTransactionObj.ivls__quantity__c = consolidatedTransactionObj.ivls__quantity__c - refunedQTY;
            
            console.log('consolidatedTransactionObj.ivls__receiptamount__c:'+ consolidatedTransactionObj.ivls__receiptamount__c);
            console.log('consolidatedTransactionObj.ivls__totalcashamount__c:'+ consolidatedTransactionObj.ivls__totalcashamount__c);
            console.log('consolidatedTransactionObj.ivls__spendingexclusive__c :'+ consolidatedTransactionObj.ivls__spendingexclusive__c );
            console.log('consolidatedTransactionObj.ivls__quantity__c :'+ consolidatedTransactionObj.ivls__quantity__c );




            try{
                ////    Get pointsEarnedSinceLastRenewal    ////
                querystring = "SELECT COALESCE(SUM(t.ivls__pointsearned__c), 0) as pointsearned FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE t.ivls__memberpgid__c = "+targetMemberPGID+" AND t.ivls__transactiondate__c > a.ivls__datetiertime__c;"
                var pointsEarnedSinceLastRenewal = (await router.query_one_way(querystring)).rows[0].pointsearned;
                if(router.isNotNull(pointsEarnedSinceLastRenewal)== false){
                    pointsEarnedSinceLastRenewal = 0;
                }
                console.log('pointsEarnedSinceLastRenewal:'+ pointsEarnedSinceLastRenewal);
            }catch(e){
                console.log("Cannot select pointsEarnedSinceLastRenewal:");
                console.log(e);
                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }
            }
           

            ////    Refund Basic Points     ////
            var pointsVoidedFromBasic = 0;
            var originalPointsBasic = originalTransactionObj.ivls__pointsbasic__c;
            console.log("originalPointsBasic:"+originalPointsBasic);
            if(router.isNotNull(originalPointsBasic)==false){
                var tempstr = 'originalPointsBasic is null or zero';
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
                }
               


                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }
            }

            //check partial refund
            var partialRefund = false;
            if(Math.abs(originalTransactionObj.ivls__quantity__c)!== Math.abs(transactionObj.ivls__quantity__c)){
                partialRefund = true;
            }
            console.log("partialRefund:"+partialRefund);
            
            if(partialRefund){
                //proportional
                pointsVoidedFromBasic = originalPointsBasic /  Math.abs(originalTransactionObj.ivls__quantity__c) * Math.abs(transactionObj.ivls__quantity__c);
            }else{
                pointsVoidedFromBasic = originalPointsBasic;
            }
            console.log("pointsVoidedFromBasic:"+pointsVoidedFromBasic);



            var pointsVoidedFromPSCAM = 0;
            var hasTotalBehaviourPoints = false;
            // try{
            //     ////    Refund PointScheme and Campaign Points  ////
               
            //     querystring = "SELECT t.ivls__pointsearned__c, d.ivls__ps_campain_id, d.ivls__points__c, d.ref_type, d.id as transactionPGID, t.id as transactionDetailPGID, t.ivls__transactiondate__c, t.createddate FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.ivls__transactiondetails__c as d ON d.transaction_id = t.id WHERE d.ivls__ps_campain_id IS NOT NULL AND d.ref_type IS NOT NULL AND t.ivls__memberpgid__c = "+targetMemberPGID+" AND t.ivls__receiptno__c = '"+receiptNo+"'";
            //     var TotalBehaviourTransaction_list = (await router.query_one_way(querystring)).rows;
            //     var hasTotalBehaviourPoints = false;
            //     if(router.isNotNull(TotalBehaviourTransaction_list)==false||TotalBehaviourTransaction_list.length ==0){
            //         console.log("No total level and behaviour level points earned");
            //         hasTotalBehaviourPoints = false;
            //     }else{
            //         hasTotalBehaviourPoints = true;
            //     }
            //     console.log("hasTotalBehaviourPoints:"+hasTotalBehaviourPoints);
                
            //     if(hasTotalBehaviourPoints){
            //         for(var h=0;h<TotalBehaviourTransaction_list.length;h++){
            //             var transactionObj_totalBehaviour = TotalBehaviourTransaction_list[h];
            //             // querystring = "SELECT * FROM loyaltycore.ivls__transactiondetails__c WHERE transaction_id = "+transactionObj_totalBehaviour.id+" AND ivls__ps_campain_id IS NOT NULL AND ref_type IS NOT NULL ";
            //             // var TotalBehaviourTransaction_TransactionDetail_list = (await router.query_one_way(querystring)).rows;
            //             // if(router.isNotNull(TotalBehaviourTransaction_TransactionDetail_list)==false|| TotalBehaviourTransaction_TransactionDetail_list.length ==0){
            //             //     console.log("No matched transaction detail for TotalBehaviourTransaction:"+transactionObj_totalBehaviour.id);
            //             //     pointsVoidedFromPSCAM =  pointsVoidedFromPSCAM  + transactionObj_totalBehaviour.ivls__pointsearned__c;
            //             //     continue;
            //             // }

            //             // for(var f=0;f<TotalBehaviourTransaction_TransactionDetail_list.length;f++){
            //             //     var detailObj = TotalBehaviourTransaction_TransactionDetail_list[f];
            //             //     var ivls__ps_campain_id = detailObj.ivls__ps_campain_id;
            //             //     var ref_type = detailObj.ref_type;
            //             //     if(router.isNotNull(ivls__ps_campain_id)==false||router.isNotNull(ref_type)==false){
            //             //         console.log("ivls__ps_campain_id or ref_type is null for detailObj:"+detailObj.id);
            //             //         pointsVoidedFromPSCAM =  pointsVoidedFromPSCAM  + detailObj.ivls__points__c;
            //             //         continue;
            //             //     }
            //             //     var meetCondition = true;
            //             //     var receipt_obj = transactionObj_totalBehaviour;
            //             //     meetCondition = await router.checkPSCAM_again(detailObj, receipt_obj,memberObj, recordtype_list) ;
            //             //     console.log("meetCondition:"+meetCondition);
            //             //     if(meetCondition == false){
            //             //         pointsVoidedFromPSCAM =  pointsVoidedFromPSCAM  + detailObj.ivls__points__c;
            //             //     }
            //             // }

            //             var meetCondition = true;//default true
            //             var receipt_obj = consolidatedTransactionObj;
            //             var ivls__ps_campain_id = transactionObj_totalBehaviour.ivls__ps_campain_id;
            //             var ref_type = transactionObj_totalBehaviour.ref_type;
            //             meetCondition = await router.checkPSCAM_again(ivls__ps_campain_id, ref_type, receipt_obj,memberObj, recordtype_list) ;
            //             console.log("meetCondition:"+meetCondition);
            //             if(meetCondition == false){
            //                 pointsVoidedFromPSCAM =  pointsVoidedFromPSCAM  + transactionObj_totalBehaviour.ivls__points__c;
            //             }
            //             console.log("transactionObj_totalBehaviour:");
            //             console.log(transactionObj_totalBehaviour);
            //         }
                    
            //     }
            // }catch(e){
            //     console.log("Cannot calculate pointsVoidedFromPSCAM:");
            //     console.log(e);
            //     querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
            //     if(environment == "production"){
            //         var markStatusResult = (await router.query_one_way(querystring));
            //         console.log('markStatusResult :');
            //         console.log(markStatusResult);
            //         continue;
            //     }
            // }
            

            console.log("#########################:");
            console.log("lineSequenceNo:"+lineSequenceNo);
            console.log("pointsVoidedFromPSCAM:"+pointsVoidedFromPSCAM);
            console.log("pointsVoidedFromBasic:"+pointsVoidedFromBasic);
            

            var totalVoidedPoints = pointsVoidedFromPSCAM + pointsVoidedFromBasic;
            console.log("totalVoidedPoints:"+totalVoidedPoints);
            console.log("pointsEarnedSinceLastRenewal:"+pointsEarnedSinceLastRenewal);
            var stillEnoughtPoint = true;
            
            if(totalVoidedPoints >= pointsEarnedSinceLastRenewal){
                stillEnoughtPoint = false;
               

            }
            console.log("stillEnoughtPoint:"+stillEnoughtPoint);
            var downGrade = false;
            if(memberObj.hasPrevMembershipTier && stillEnoughtPoint==false){
                downGrade = true;
            }
            console.log("downGrade:"+downGrade);
            var originalTierObj = {};
            var originalTierSFID = "";
            

            // var newPointsNet = originalTransactionObj.ivls__pointsnet__c;
            // if(router.isNotNull(newPointsNet)==false){
            //     newPointsNet = 0;
            // }
            // newPointsNet = newPointsNet - totalVoidedPoints;
            // var querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__pointsnet__c = "+newPointsNet+" WHERE id = "+ originalTransactionObj.id; 
            // if(environment == "production"){
            //     var markStatusResult = (await router.query_one_way(querystring));
            //     console.log('UPDATE ivls__pointsnet__c:');
            //     console.log(markStatusResult);
            // }




            // var rewardRecordTypeId = router.getRecordTypeId('IVLS__Reward__c','ForRefundUse', recordtype_list);
            querystring = "SELECT * FROM loyaltycore.ivls__reward__c WHERE ivls__forrefunduse__c = true LIMIT 1";
            var rewardSFID = (await router.query_one_way(querystring)).rows[0].sfid;
            if(router.isNotNull(rewardSFID)==false){
                var tempstr = "Cannot find refund reward sfid";
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
                }
              

                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }
            }

            
            try{
                var InsertObj = {};
                InsertObj.ivls__member__c = router.AddSingleQuoteSymbol(memberObj.Account.id);
                InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Redemption__c','Open', recordtype_list));
                InsertObj.ivls__status__c= "'Open'";
                // InsertObj.ivls__reward__c= router.AddSingleQuoteSymbol('a0q0l000000nO6aAAE');
                InsertObj.ivls__reward__c= router.AddSingleQuoteSymbol(rewardSFID);
                
                InsertObj.ivls__redeempoint__c= totalVoidedPoints;
                InsertObj.ivls__dateredeem__c= "NOW()";
                InsertObj.ivls__dateexpiry__c= "NOW()";
                // InsertObj.ivls__reason__c= "'Refund of transaction line No: "+originalTransactionObj.ivls__sequenceno__c+"'";
                
                InsertObj.createddate= "NOW()";
                InsertObj.lastmodifieddate= "NOW()";
                
                querystring =  router.getSQL_Insert(InsertObj, "ivls__redemption__c", false)
                var querystring1 = querystring + " RETURNING ivls__pgid__c;"
                console.log("Insert redemption SQL:");
                console.log(querystring1);
                // if(environment == "production"){
                    var client = await pool.connect();
                    try {
                    
                        await client.query('BEGIN')
                        var res  = await client.query(querystring1);
                        console.log("ref1 SQL:");
                        console.log(res);
                        var redemptionPGID = [res.rows[0].ivls__pgid__c];

                        var tempPoint = totalVoidedPoints;
                        
                        var redemptionDetailSQL_list = [];
                        var avalableTransaction_list = await client.query("SELECT id, ivls__pointsnet__c, ivls__pointsredeemed__c,  IVLS__Member__c FROM loyaltycore.ivls__transaction__c WHERE ivls__memberpgid__c = "+targetMemberPGID+" AND ivls__pointsnet__c > 0 ORDER BY ivls__transactiondate__c ASC, ivls__pointspool__c DESC;");
                        for (let row of avalableTransaction_list.rows) {
                            
                            var currentPointsNet = row.ivls__pointsnet__c;
                            var currentPointsRedemed = row.ivls__pointsredeemed__c;

                            if(router.isNotNull(currentPointsRedemed)==false){
                                currentPointsRedemed = 0;
                            }

                            var tempRedeeming = 0;
                            if(tempPoint > currentPointsNet){
                                //not enought pointsnet in 1 transaction;
                                tempRedeeming = currentPointsNet;
                                tempPoint = tempPoint - tempRedeeming;
                            }else{
                                //enought points in 1 transaction
                                tempRedeeming = tempPoint;
                                tempPoint = 0;
                            }
                            
                            var newRedeeming = currentPointsRedemed + tempRedeeming;
                            var newPointsnet = currentPointsNet - tempRedeeming;
                        
                            var tempSQL = "UPDATE loyaltycore.ivls__transaction__c SET ivls__pointsredeemed__c =  "+newRedeeming+",  ivls__pointsnet__c = "+newPointsnet+" WHERE id = "+ row.id; 
                            var result = await client.query(tempSQL);
                            redemptionDetailSQL_list.push(tempSQL);

                            var InsertObj = {};
                            InsertObj.ivls__redemptionpgid__c = redemptionPGID;
                            InsertObj.ivls__pointsredeem__c = tempRedeeming;
                            InsertObj.ivls__transactionpgid__c =row.id;
                            InsertObj.createddate= "NOW()";
                            InsertObj.lastmodifieddate= "NOW()";
                            querystring2 = router.getSQL_Insert(InsertObj, 'ivls__redemptiondetail__c', false);
                            var result = await client.query(querystring2);
                            console.log("ref2 SQL:");
                            console.log(result);
                            console.log("insert redeemption detail SQL :");
                            console.log(querystring2);
                            redemptionDetailSQL_list.push(querystring2);


                            var InsertObj = {};
                            InsertObj.transaction_id = row.id;
                            InsertObj.ivls__memberpgid__c = targetMemberPGID;
                            InsertObj.ivls__pointsbucket__c = "'Refund'";
                            InsertObj.ivls__points__c = Math.abs(tempRedeeming)*-1;
                            InsertObj.ivls__activity__c = "'Refund of transaction: "+originalTransactionObj.name+"'";
                            InsertObj.ref_id = redemptionPGID;
                            InsertObj.ref_type = "'IVLS__Redemption__c'";
                            InsertObj.ivls__pointspool__c = "'Spending'";
                            InsertObj.createddate= "NOW()";
                            
                            // InsertObj.lastmodifieddate= "NOW()";
                            querystring3 = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', false);
                            var result = await client.query(querystring3);
                            console.log("behaviour level insert transaction detail SQL :");
                            console.log(querystring3);

                            console.log("ref2 SQL:");
                            console.log(result);
                            
                            redemptionDetailSQL_list.push(querystring3);
                            if(tempPoint == 0) 
                            break;
                        }

                        console.log("redemptionDetailSQL_list:");
                        console.log(redemptionDetailSQL_list);

                        ////    PointLost   ////
                        if(tempPoint >  0){
                            var PointLost = tempPoint;
                            console.log("has point loss!!!!!!!!!:");
                            console.log(PointLost);
                            tempPoint = 0;
                        }

                        
                        if(environment == "production"){
                            await client.query('COMMIT')
                        }else{
                            await client.query('ROLLBACK')
                        }

                        

                    }catch (e) {
                        await client.query('ROLLBACK')
                    
                        console.log("ROLLBACK=");   
                        console.log(e);   
                        throw e
                    }finally {
                        client.release()
                    }
                // }
                    
                var updateObj = {};
                updateObj.ivls__status__c = "'Refund Complete'";
                updateObj.ivls__pointsearned__c = Math.abs(totalVoidedPoints) * -1;
                updateObj.ivls__pointsredeemed__c = Math.abs(totalVoidedPoints) * -1;
                    
                // updateObj.ivls__pointsnet__c = Math.abs(totalVoidedPoints) * -1;
                updateObj.ivls__activities__c = "'Refund of "+transactionObj.ivls__sequenceno__c+"'";
                updateObj.ivls__transactiondate__c = router.AddSingleQuoteSymbol(router.getSQLDateTimeFormatWithSeconds(originalTransactionObj.ivls__transactiondate__c));

                if(PointLost > 0){
                    updateObj.ivls__pointslost__c = PointLost;
                }
                querystring = router.getSQL_Update(updateObj, dbname, 'ivls__transaction__c', 'id', transactionObj.id);
                console.log("UPDATE refund transaction querystring");   
                console.log(querystring);   

                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                var InsertObj = {};
                InsertObj.transaction_id = transactionObj.id;
                InsertObj.ivls__pointsbucket__c = "Refund";
                InsertObj.ivls__points__c = Math.abs(totalVoidedPoints) * -1;
                InsertObj.ivls__activity__c = "Refund of "+transactionObj.ivls__sequenceno__c;
                InsertObj.ref_type = "IVLS__PointsScheme__c";
                InsertObj.ivls__memberpgid__c = targetMemberPGID;
                InsertObj.ivls__pointspool__c = "Spending";
                querystring = router.getSQL_Insert(InsertObj, 'ivls__transactiondetails__c', true);
                console.log("Insert refund transaction detail querystring");   
                console.log(querystring);   
                if(environment == "production"){
                    await router.query_one_way(querystring);
                }
                
               
            }catch(e){
                console.log(e);
                var tempstr = "Error On insert transaction and redemption on refund ";
                console.log(tempstr);
                if(log_level == "debug"){
                    var querystring_error_log = router.get_Querystring_error_log();
                    var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                    var result  = (await router.query_with_parameter(querystring_error_log, values));
                    console.log(result);
    
                }
               

                querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
                if(environment == "production"){
                    var markStatusResult = (await router.query_one_way(querystring));
                    console.log('markStatusResult :');
                    console.log(markStatusResult);
                    continue;
                }
            }
            


            if(downGrade){
                querystring = "SELECT * FROM loyaltycore.ivls__membershiphistory__c WHERE ivls__datetimefrom__c IS NOT NULL AND ivls__member__c = '"+targetMemberSFID+"' ORDER BY ivls__pgid__c DESC";
                var membershiptierHistory_list = (await router.query_one_way(querystring)).rows;
                var lastRenewalDatetime = new Date();
                if(router.isNotNull(membershiptierHistory_list)==false||membershiptierHistory_list.length < 2){
                    var tempstr = "This member do not have membership history" + targetMemberSFID;
                    console.log(tempstr);
                    if(log_level == "debug"){
                        var querystring_error_log = router.get_Querystring_error_log();
                        var values = ['ivls__transaction__c', transactionObj.id, 'part4 error', tempstr];
                        var result  = (await router.query_with_parameter(querystring_error_log, values));
                        console.log(result);
                    }
                   


                    querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((transactionObj.id )));
                    if(environment == "production"){
                        var markStatusResult = (await router.query_one_way(querystring));
                        console.log('markStatusResult :');
                        console.log(markStatusResult);
                        continue;
                    }
                }else if(membershiptierHistory_list.length >= 2){
                    // console.log("This member have more than 1 membership history");
                    // lastRenewalDatetime = membershiptierHistory_list[1].ivls__datetimefrom__c;
                    // console.log("lastRenewalDatetimeUTC+8:"+lastRenewalDatetime);
                    // lastRenewalDatetime = new Date( lastRenewalDatetime.getTime() + (lastRenewalDatetime.getTimezoneOffset() * 60000));
                    // console.log("lastRenewalDatetimeUTC+0:"+ lastRenewalDatetime);

                    console.log("This member have more than 1 membership history");
                        //lastRenewalDatetime = (membershiptierHistory_list[2].ivls__datetimefrom__c);
                    lastRenewalDatetime = (membershiptierHistory_list[1].ivls__datetimefrom__c);//Henry fixed from 2 to 0 for get last record;
                    originalTierSFID = (membershiptierHistory_list[1].ivls__tier__c);
                   
                    // console.log("lastRenewalDatetimeUTC+8:"+lastRenewalDatetime);
                    // lastRenewalDatetime = new Date( lastRenewalDatetime.getTime() + (lastRenewalDatetime.getTimezoneOffset() * 60000));
                    // console.log("lastRenewalDatetimeUTC+0:"+ lastRenewalDatetime);
                    
                  

                    // console.log("lastRenewalDatetimeUTC9999:"+router.getSQLDateTimeFormatWithSeconds(lastRenewalDatetime));

                }else{
                    console.log("This member have only 1 membership history");
                    lastRenewalDatetime = memberObj.Account.createddate;
                    originalTierSFID = (membershiptierHistory_list[0].ivls__tier__c);
                }

              



                querystring = "SELECT * FROM loyaltycore.ivls__membershiptier__c WHERE sfid = '"+originalTierSFID+"' LIMIT 1";
                originalTierObj = (await router.query_one_way(querystring)).rows[0];

              

                var lastRenewalDatetimeSQLformat = router.getSQLDateTimeFormatWithSeconds(lastRenewalDatetime);

                querystring = "SELECT t.ivls__receiptno__c, t.ivls__memberpgid__c, SUM(t.ivls__pointsearned__c) as ivls__pointsearned__c, MIN(t.ivls__transactiondate__c) as ivls__transactiondate__c FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE  t.ivls__status__c LIKE '%Complete%' AND ivls__receiptno__c IS NOT NULL AND t.ivls__memberpgid__c = "+targetMemberPGID+" AND t.ivls__pointsearned__c > 0 AND t.ivls__transactiondate__c > '"+lastRenewalDatetimeSQLformat+"' GROUP BY t.ivls__receiptno__c, t.ivls__memberpgid__c ORDER BY ivls__transactiondate__c;"
                var positiveReceipt_list = (await router.query_one_way(querystring)).rows;
                console.log("positiveReceipt_list:");
                console.log(positiveReceipt_list);

                querystring = "SELECT t.ivls__originalorderreference__c as ivls__receiptno__c, t.ivls__memberpgid__c, SUM(t.ivls__pointsearned__c) as ivls__pointsearned__c, MIN(t.ivls__transactiondate__c) as ivls__transactiondate__c FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON t.ivls__memberpgid__c = a.ivls__pgid__c WHERE  t.ivls__status__c LIKE '%Complete%' AND ivls__receiptno__c IS NOT NULL AND t.ivls__memberpgid__c = "+targetMemberPGID+" AND t.ivls__pointsearned__c < 0 AND t.ivls__transactiondate__c > '"+lastRenewalDatetimeSQLformat+"' GROUP BY t.ivls__originalorderreference__c, t.ivls__memberpgid__c ORDER BY ivls__transactiondate__c;"
                console.log("netgativeReceipt_list querystring:");
                console.log(querystring);
                var netgativeReceipt_list = (await router.query_one_way(querystring)).rows;
                console.log("netgativeReceipt_list:");
                console.log(netgativeReceipt_list);

                var transactional_list = positiveReceipt_list;
                for(var t=0;t<transactional_list.length;t++){
                    for(var n=0;n<netgativeReceipt_list.length;n++){
                        if(netgativeReceipt_list[n].ivls__receiptno__c == transactional_list[t].ivls__receiptno__c){
                            transactional_list[t].ivls__pointsearned__c =  transactional_list[t].ivls__pointsearned__c  - Math.abs(netgativeReceipt_list[n].ivls__pointsearned__c);
                            if(transactional_list[t].ivls__pointsearned__c < 0){
                                transactional_list[t].ivls__pointsearned__c = 0;
                            }
                        }
                    }
                }

                querystring = "SELECT name as ivls__receiptno__c, ivls__pointsearned__c, ivls__transactiondate__c, ivls__memberpgid__c FROM loyaltycore.ivls__transaction__c WHERE ivls__receiptno__c IS NULL AND ivls__memberpgid__c = "+targetMemberPGID+" AND ivls__transactiondate__c > '"+lastRenewalDatetimeSQLformat+"' ORDER BY ivls__transactiondate__c;"
                var nonTransactional_list = (await router.query_one_way(querystring)).rows;

                var all_list = transactional_list.concat(nonTransactional_list);
               
                all_list.sort(function(x, y){
                    return x.ivls__transactiondate__c - y.ivls__transactiondate__c;
                })
                console.log("all_list:");
                console.log(all_list);

                var DESC_array = membershiptiers_list;
                DESC_array.reverse();
                var newTierObj = originalTierObj;



                // var currentPoints = 0;
                // var currentSequence = originalTierObj.ivls__sequence__c;
                // var maxTierSFID = membershiptiers_list[0].sfid;
                // var newTransactionDate = lastRenewalDatetime;//JS TIME
                // console.log("newTransactionDate0:");
                // console.log(newTransactionDate);
                // for(var n=0;n<all_list.length;n++){
                //     currentPoints = currentPoints + all_list[n].ivls__pointsearned__c;
                    
                //     for(var u=0;u<DESC_array.length;u++){
                //         if( DESC_array[u].ivls__sequence__c > currentSequence){
                //             var thisTierFrom = DESC_array[u].ivls__pointsfrom__c;//20000
                //             // var thisTierTo = DESC_array[u].ivls__pointsto__c;//50000
    
                //             if(currentPoints > thisTierFrom){
                //                 console.log("currentPoints > thisTierFrom:");
                //                 console.log("currentPoints:");
                //                 console.log(currentPoints);
                //                 console.log("thisTierFrom:"+thisTierFrom);
                //                 // var diff = thisTierTo - thisTierFrom//50000-20000
                //                 newTierObj = DESC_array[u];
                //                 newTransactionDate = all_list[n].ivls__transactiondate__c;
                //                 currentPoints = currentPoints - thisTierFrom;
                //                 continue;
                //             }else{
                //                 // newTierSFID = DESC_array[u].sfid;
                               
                //                 break;
                //             }
                            

                //         //     var thisTierFrom = DESC_array[u].ivls__pointsfrom__c;
                //         //     var thisTierTo = DESC_array[u].ivls__pointsto__c;
            
            
                //         //     if(currentPoints > thisTierFrom){
                //         //         currentPoints = currentPoints - thisTierFrom;
                //         //         newTierObj = DESC_array[u];
                //         //         newTransactionDate = all_list[n].ivls__transactiondate__c;
                //         //         continue;
                //         //     }else{
                //         //         // newTierSFID = DESC_array[u].sfid;
                //         //         // newTierObj = DESC_array[u];
                //         //         break;
                //         //     }  
                           
                //         }else{
                //             continue;
                //         }
                //     }
                // }
             
              
                // var transactiondate_array = [];
                // var ordered_all_list = [];
                // for(var a=0;a<all_list.length;a++){
                //     transactiondate_array.push(all_list[a].ivls__transactiondate__c);
                // }
                // transactiondate_array.sort();

               


                if(router.isNotNull(originalTierSFID)==false){
                    console.log("originalTierSFID is null ");
                    continue;
                }



                console.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                console.log("originalTierSFID: "+originalTierObj.name);
                console.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                

                var transactiondatetime = lastRenewalDatetime;
                transactiondatetime = router.getSQLDateTimeFormatWithSeconds(transactiondatetime);
                console.log("transactiondatetime:");
                console.log(transactiondatetime);

                console.log("hasTotalBehaviourPoints:"+hasTotalBehaviourPoints);
                var newMemberObj = router.getEvaluateObj(newTierObj);
                newMemberObj.ivls__datetier__pc = router.AddSingleQuoteSymbol(transactiondatetime);
                newMemberObj.ivls__datetiertime__c =  router.AddSingleQuoteSymbol(transactiondatetime);
                newMemberObj.ivls__syscurrenttiersequence__pc = newTierObj.ivls__sequence__c;
                querystring = router.getSQL_Update(newMemberObj, dbname, 'account', 'ivls__pgid__c', targetMemberPGID);
                console.log("refund downGrade querystring:");
                console.log(querystring);
                if(environment == "production"){
                    var updateResult = await router.query_one_way(querystring);
                    console.log("updateResult:");
                    console.log(updateResult);
                }
            }
            
            var updateMemberSQL = await updatemember.updateMemberInfo(pool,  targetMemberPGID,  membershipObj, membershiptiers_list, "");
            console.log("SECTION4 updateMemberSQL:");
            console.log(updateMemberSQL);
            if(updateMemberSQL == false){
                continue;
            }
            if(environment == "production"){
                var updateResult = await router.query_one_way(updateMemberSQL);
                console.log(updateResult);
            }
            
            console.log("#########################:");
        }catch(e){
            console.log("//////////////////////////////  WARNING  //////////////////////////////") 
            console.log('Error Processing :refundTransaction_list[i].id:'+ refundTransaction_list[i].id);
            console.log('Error Message:');
            console.log(e);

            var tempstr = 'Error Processing :refundTransaction_list[i].id:'+ refundTransaction_list[i].id;
            console.log(tempstr);
            if(log_level == "debug"){
                var querystring_error_log = router.get_Querystring_error_log();
                var values = ['ivls__transaction__c', refundTransaction_list[i].id, 'part4 error', tempstr];
                var result  = (await router.query_with_parameter(querystring_error_log, values));
                console.log(result);
            }

            querystring = markStatus(dbname, 'ivls__transaction__c', router.AddSingleQuoteSymbol('Refund Error'), router.AddSingleQuoteSymbol((refundTransaction_list[i].id )));
            if(environment == "production"){
                var markStatusResult = (await router.query_one_way(querystring));
                console.log('markStatusResult :');
                console.log(markStatusResult);
                continue;
            }
        }

        console.log(querystring_list);
        console.log("querystring_list:"+querystring_list.length);
      
        if(querystring_list.length > 0){
            var totalQueryStr='';
            for (var q=0;q<querystring_list.length;q++){
                if(q == 0){
                    totalQueryStr =  totalQueryStr + querystring_list[q];
                }else{
                    totalQueryStr =  totalQueryStr + ' ; '+ querystring_list[q];
                }
            
            }

            querystring_list = [];
            if(environment == "production"){
                try{
                    await router.query_one_way(totalQueryStr);
                    console.log('Process querystring_list successful !!!')
                }catch (err) {
                    console.log('Process querystring_list fail !!!')
                    console.log(err)
                }
            }
        }
        
    }
    return true;
}


function sameDay(d1, d2) {
    return (d1.getFullYear() === d2.getFullYear() &&
      d1.getMonth() === d2.getMonth() &&
      d1.getDate() === d2.getDate());
}
function isNotNull(value){
    if(value !== 0 && value !== null && value !== "" && typeof(value) !== "undefined"){
        return true;
    }else{
        return false;
    }
}
function checkConfitionisMeet(originValue, operator, targetValue,isDateTime){
    try{
        if(isDateTime){
            if(router.isNotNull(originValue) && router.isNotNull(targetValue)){
                if(operator == "equals"){
                    return sameDay(originValue, targetValue);
                }else{
                    return false;
                }
            }else{
                return false;
            }
            
        }else{
            if(operator == "equals"){
                return (originValue == targetValue);
            }else if (operator == "greater than"){
                return (originValue > targetValue);
            }else if (operator == "greater or equals"){
                return (originValue >= targetValue);
            }else if (operator == "less than"){
                return (originValue < targetValue);
            }else if (operator == "less or equals"){
                return (originValue <= targetValue);
            }else if (operator == "not equals"){
                return (originValue !== targetValue);
            }else if (operator == "contains"){
                return (originValue.includes(targetValue));
            }else{
                return false;
            }
        }
    }catch(e){
        console.log("Error in checkConfitionisMeet:")
        console.log(e);
        return false;
    }
}


function calcLinelLevelPS(router, pointSchemeObj,currentTranObj, scheme_currentSpending){
    if(scheme_currentSpending == 0){
        return 0;
    }
    

    var targetMerchant = pointSchemeObj.ivls__merchant__c;
    var targetProduct = pointSchemeObj.ivls__product__c;
    var targetCate = pointSchemeObj.ivls__productcategory__c;
    var meetCondition = true;

    if(router.isNotNull(targetMerchant)){
        if(targetMerchant !== currentTranObj.merchant){
            console.log("merchant unmatched");
            var meetCondition = false;
        }
    }

    if(router.isNotNull(targetProduct)){
        if(targetProduct !== currentTranObj.product_sfid){
            console.log("product_sfid unmatched");
            var meetCondition = false;
        }
    }
    if(router.isNotNull(targetCate)){
        if(targetCate !== currentTranObj.cate1 && targetCate !== currentTranObj.cate2 && targetCate !== currentTranObj.cate3 ){
            console.log("product cate unmatched");
            var meetCondition = false;
        }
    }
   
    // var conditionObj = {
    //     "Merchant":pointSchemeObj.ivls__merchant__c,
    //     "Product":pointSchemeObj.ivls__product__c,
    //     "Product_Category":pointSchemeObj.ivls__productcategory__c,
    // };
    // var meetCondition = false;
    // for (index in conditionObj){
    //     if(router.isNotNull(conditionObj[index])){
    //         for (subindex in currentTranObj){
    //             if(router.isNotNull(currentTranObj[subindex])){
    //                 if(index == subindex){
    //                     if(currentTranObj[subindex] == conditionObj[index]){
    //                         meetCondition = true;
    //                     }
    //                 }
    //             }
    //         } 
    //     }
    // }

   
   
    if(meetCondition == true){
        console.log(pointSchemeObj.sfid);
    }else{
        return 0;
    }

    var SchemeSpending = pointSchemeObj.ivls__spending__c;
    var SchemePoints = pointSchemeObj.ivls__points__c;
    
    if(router.isNotNull(SchemeSpending) == false){
        return 0;
    }
    if(router.isNotNull(SchemePoints) == false){
        return 0;
    }

    var PointSchemePSratio = SchemePoints / SchemeSpending;
    console.log("PointSchemePSratio:"+PointSchemePSratio);
    console.log("scheme_currentSpending:"+scheme_currentSpending);
    var earnedPoints = Number(scheme_currentSpending) * (SchemePoints / SchemeSpending);
   
    // PointsByScheme = PointsByScheme + earnedPoints;
   
    // earnedPoints = earnedPoints * Number(quantity)

    return earnedPoints;           
}


function getSQLOperator(operator){
    if(operator == "equals"){
        return '=';
    }else if (operator == "greater than"){
        return '>';
    }else if (operator == "greater or equals"){
        return '>='
    }else if (operator == "less than"){
        return '<';
    }else if (operator == "less or equals"){
        return '<=';
    }else if (operator == "not equals"){
        return '!=';
    }else{
        return false;
    }
}
function getSQLtargetValue(targetValue, processDate){
    var today = "NOW()";
    var date = new Date();

    
    if(processDate == "T+1"){
        today = "NOW() - interval '1 day'"
        // date = date.setDate(date.getDate() - 1);
        date = new Date(date.getTime() - 24*60*60*1000);
    }
    console.log("date:");
    console.log(date);
    if(targetValue == "True"){
        return 'true';
    }else if (targetValue == "False"){
        return 'false';
    }else if (targetValue == "Today"||targetValue == "TODAY()"){
        // return today;
        return getSQLDateTimeFormat(date);
    }else if (targetValue == "This Month"){
        var firstDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth(), 1));
        var lastDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth() + 1, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "Last Month"){
        var firstDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth()-1, 1));
        var lastDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth(), 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "This Calendar Year"){
        var firstDay = getSQLDateTimeFormat(new Date(date.getFullYear(), 0, 1));
        var lastDay = getSQLDateTimeFormat(new Date(date.getFullYear(), 11, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else{
        return "invalid";
    }
}

function getSQLtargetValue_total(targetValue, processDate){
    var date = new Date();
    if(processDate == "T+1"){
        date = new Date(date.getTime() - 24*60*60*1000);//yesterday
    }
    if(targetValue == "True"){
        return true;
    }else if (targetValue == "False"){
        return false;
    }else if (targetValue == "Today"||targetValue == "TODAY()"){
        return date;
    }else if (targetValue == "This Month"){
       
        var firstDay = (new Date(date.getFullYear(), date.getMonth(), 1));
        var lastDay = (new Date(date.getFullYear(), date.getMonth() + 1, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "Last Month"){
        
        var firstDay = (new Date(date.getFullYear(), date.getMonth()-1, 1));
        var lastDay = (new Date(date.getFullYear(), date.getMonth(), 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "This Calendar Year"){
       
        var firstDay = (new Date(date.getFullYear(), 0, 1));
        var lastDay = (new Date(date.getFullYear(), 11, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else{
        return "invalid";
    }
}
function getSQLDateTimeFormat(JStime){
    var month = (Number(JStime.getMonth())+1 );
    return JStime.getFullYear() +'/'+ month+ '/'+  JStime.getDate();
}
function markStatus(dbname, tablename, status, id){
    querystring = 'UPDATE '+ dbname+tablename + ' SET ivls__status__c = '+status+' WHERE id = ' +id; 
    return querystring;
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
        if(router.isNotNull(InsertObj[x]) && InsertObj[x] !== '\'\''){ //is NOT '', 0, null, or undefined
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

function genInsertTranSQL_Total(neededTranType, router, mappingObj, dbname,tablename,  CurrentMembershipTier, recordtype_list, lineTotal, lineActivityName, targetMemberPGID){
    var querystring ='';
    var InsertObj = router.getInsertTransactionTotalObj();
    var currentObj = mappingObj;

    for (x in InsertObj){
        for (y in currentObj){
            if(x == y){
                InsertObj[x] = currentObj[y];
            }
        }
    }
    // console.log("InsertObj")
    // console.log(InsertObj)
    for (x in InsertObj){
        InsertObj[x] = router.AddSingleQuoteSymbol(InsertObj[x]);
    }

    InsertObj.ivls__transactiondate__c = "'NOW() - interval '8 hours'";
    // InsertObj.ivls__transactiondate__c = "(to_char(NOW() - interval '8 hours', 'YYYY-MM-DD HH24:MI:SS'))::timestamp";
    InsertObj.createddate = 'NOW()';
    InsertObj.lastmodifieddate = 'NOW()';
    InsertObj.ivls__receiptdate__c = 'NOW()';
    InsertObj.ivls__source__c = '\'System\'';
    InsertObj.ivls__status__c = '\'Complete\'';
    InsertObj.ivls__memberpgid__c = targetMemberPGID;
    
    InsertObj.ivls__activities__c = router.AddSingleQuoteSymbol(lineActivityName);
    InsertObj.ivls__pointspool__c = router.AddSingleQuoteSymbol(neededTranType);
    
    InsertObj.recordtypeid = router.AddSingleQuoteSymbol(router.getRecordTypeId('IVLS__Transaction__c',neededTranType, recordtype_list));
    
    InsertObj.ivls__datepointsexpiry__c = router.AddSingleQuoteSymbol(CurrentMembershipTier.expiryDATE);
    
  
    InsertObj.ivls__pointsearned__c = lineTotal;
    InsertObj.ivls__pointsnet__c = lineTotal;
    
    querystring = 'INSERT INTO '+dbname+tablename+' (';
    var count = 0;
    var fieldsstr = ''
    for (x in InsertObj ){
        if(router.isNotNull(InsertObj[x]) && InsertObj[x] !== '\'\''){ //is NOT '', 0, null, or undefined
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
    for (y in InsertObj){
        if(router.isNotNull(InsertObj[y]) && InsertObj[y] !== '\'\''){ //is NOT '', 0, null, or undefined
            if(count == 0){
                valuesStr =  valuesStr + InsertObj[y];
            }else{
                valuesStr =  valuesStr + ', ' + InsertObj[y];
            }
            count = count +1;
        }
    }
    querystring = querystring +valuesStr +' )';
    return querystring;
}
