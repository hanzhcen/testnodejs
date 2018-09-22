
var environment = 'production';
var processDate = 'T+1';
var dbname = 'loyaltycore.';
var pg = require('pg');
var request = require('request-promise');
var fs = require('fs');
var crypto = require('asymmetric-crypto');


if(environment == 'production'){
   
    var config = {
        user: 'u8u5obf9qe75gq', //env var: PGUSER
        database: 'd3e6v86hr1aenn', //env var: PGDATABASE
        password: 'p2257769c0deea148a027e6a6ecd32297d0bb37af183447155d03243e4459ed81', //env var: PGPASSWORD
        host:  'ec2-18-233-254-125.compute-1.amazonaws.com', // Server hosting the postgres database
        port: 5432, //env var: PGPORT
        max: 10, // max number of clients in the pool
        ssl: true,
        idleTimeoutMillis: 30 // how long a client is allowed to remain idle before being closed
    };
    var pool = new pg.Pool(config);
    // const { Pool } = require('pg')
    // var pool = new pg.Pool({
    //     connectionString: process.env.DATABASE_URL,
    //     ssl: true,
    //     idleTimeoutMillis: 30
    // });

    

}else{
  
    var config = {
        user: 'u8u5obf9qe75gq', //env var: PGUSER
        database: 'd3e6v86hr1aenn', //env var: PGDATABASE
        password: 'p2257769c0deea148a027e6a6ecd32297d0bb37af183447155d03243e4459ed81', //env var: PGPASSWORD
        host:  'ec2-18-233-254-125.compute-1.amazonaws.com', // Server hosting the postgres database
        port: 5432, //env var: PGPORT
        max: 10, // max number of clients in the pool
        ssl: true,
        idleTimeoutMillis: 30 // how long a client is allowed to remain idle before being closed
    };
    var pool = new pg.Pool(config);
}


module.exports = {

   
    getEnvironment : function (){
        return environment;
    },
    getProcessDate : function (){
        return processDate;
    },
    getPool : function (){
        return pool;
    },
    getDebugMode : function (){
        try{
            var debug_mode = process.env.DEBUG_MODE;
            return debug_mode;
        }catch(e){
            return false;
        }
    },
    getDBname :function(){
        return dbname;
    },
    checkAuth :function(){
        return checkAuth();
    },
    getPOSnESHOPDetailJSONmapping:function(){
        var obj={
            "ivls__receiptamount__c":"",
            "ivls__currency__c":"",
            "ivls__member__c":"",
            "ivls__totaldiscountamount__c":"",
            "ivls__receiptdate__c":"",
            "ivls__receiptno__c":"",
            "ivls__totalcashamount__c":"",
            "ivls__shop__c":"",
            "ivls__merchant__c":"",
            "ivls__remarks__c":"",
            "ivls__totalcreditcardamount__c":"",
            "ivls__totalvoucheramount__c":"",
            "ivls__transactiondate__c":"",
            "ivls__unitprice__c":"",
            "ivls__netunitprice__c":"",
            "ivls__sequenceno__c":"",
            "ivls__product__c":"",
            "ivls__quantity__c":"",
            "ivls__discountline__c":"",
            "ivls__spending__c":"",
            "ivls__spendingexclusive__c":"",
            "ivls__totalpriceline__c":"",
            "ivls__voucheramountline__c":"",
            "ivls__promotioncode__c":"",
            "ivls__contact__c":""
        }
       
        return obj;
    },
    getInsertTransactionTotalObj:function(){
        var obj={
            "ivls__receiptamount__c":"",
            "ivls__currency__c":"",
            "ivls__member__c":"",
            "ivls__totaldiscountamount__c":"",
            "ivls__receiptdate__c":"",
            "ivls__receiptno__c":"",
            "ivls__totalcashamount__c":"",
            "ivls__shop__c":"",
            "ivls__merchant__c":"",
            "ivls__remarks__c":"",
            "ivls__totalcreditcardamount__c":"",
            "ivls__totalvoucheramount__c":"",
            "ivls__transactiondate__c":"",
            "ivls__promotioncode__c":"",
            "ivls__contact__c":"",
        }
       
        return obj;
    },
    isNotNull :function(value){
        if(value !== 0 && value !== null && value !== "" && typeof(value) !== "undefined"){
            return true;
        }else{
            return false;
        }
    },
    getRecordTypeId :function(sobjecttype,developername, recordtype_list){
        return getRecordTypeId(sobjecttype,developername, recordtype_list);
    },
    AddSingleQuoteSymbol :function(str){
        return AddSingleQuoteSymbol(str);
    },
    getExpiryDate :function(yearstart, obj){
        var ivls__pdaterange1__c = obj.ivls__pdaterange1__c
        var ivls__pdaterange2__c = obj.ivls__pdaterange2__c
        var ivls__pexpirationdate1ddmm__c = obj.ivls__pexpirationdate1ddmm__c
        var ivls__pexpirationdate2ddmm__c = obj.ivls__pexpirationdate2ddmm__c

        var today = new Date();
        var currentYear = (new Date()).getFullYear();
        var cutoffDate = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));
        var expiryDATE = '';
        // console.log("today:"+today);
        // console.log("cutoffDate:"+cutoffDate);
        if( today < cutoffDate){
            //2018-03-23
            // console.log('<');
            var FYS = new Date(currentYear-1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2017-04-01
            var date1S = new Date(getYearStr(currentYear, Number(ivls__pdaterange1__c.substring(2,4))-1, ivls__pdaterange1__c.substring(0,2)));//2018-03-17
            var date1E = getYearStr(currentYear, ivls__pexpirationdate1ddmm__c.substring(2,4), ivls__pexpirationdate1ddmm__c.substring(0,2));//'2018/03/31'

            var date2S = new Date(getYearStr(currentYear, ivls__pdaterange2__c.substring(2,4), ivls__pdaterange2__c.substring(0,2)));//2018-03-31
            var date2E = getYearStr(currentYear, ivls__pexpirationdate2ddmm__c.substring(2,4), ivls__pexpirationdate2ddmm__c.substring(0,2));//'2018/04/15'

            if(FYS<today && today<date1S){
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

            if(FYS<today && today<date1S){
                expiryDATE = date1E;
            }else{
                expiryDATE = date2E;
            }

        }
        return expiryDATE;
    },
    getCurrentMemberInfo :function(Account, membershiptiers_list){
        return getCurrentMemberInfo(Account, membershiptiers_list);
    },
    getSQL_Insert :function(InsertObj, tablename, needSingleQuote){
        return getSQL_Insert(InsertObj, tablename, needSingleQuote);
    },
    getEvaluateObj :function(CurrentMembershipTier){
        return  getEvaluateObj(CurrentMembershipTier);
    },
    getSQL_Update :function(InsertObj, dbname, tablename,fieldname, sfid){
        return  getSQL_Update(InsertObj, dbname, tablename,fieldname, sfid);
    },
   
    checkPSCAM_again:function(ivls__ps_campain_id, ref_type, receipt_obj,memberObj, recordtype_list){
        return checkPSCAM_again (ivls__ps_campain_id, ref_type, receipt_obj,memberObj, recordtype_list) ;
    },
    
    getMappingObj:function(ivls__detail__c, txMapping_list,type){
        return getMappingObj(ivls__detail__c, txMapping_list,type);
    },
    query_with_parameter:function(q, p){
        return query_with_parameter(q, p);
    },
    query_one_way:function(q){
        return query_one_way(q);
    },
    getDateTimeKeyword:function(){
        return getDateTimeKeyword();
    },
    
    getMembershiptier:function(){
        return getMembershiptier();
    },
    getMembershipYearStart:function(){
        return getMembershipYearStart();
    },
    getMembershipMembershipID:function(){
        return getMembershipMembershipID();
    },
    
    selectALL:function(tablename){
        return selectALL(tablename);
    },
    selectALL_with_parameter:function(tablename, parameterName,operater, parameterValue){
        return selectALL_with_parameter (tablename, parameterName, operater, parameterValue);
    },
    checkMultiPicklistValue:function( targetField, originField){
        return checkMultiPicklistValue ( targetField, originField);
    },
    getSQLDateTimeFormat:function(JStime){
        return getSQLDateTimeFormat (JStime);
    },
    getSQLDateTimeFormatWithSeconds:function(JStime){
        return getSQLDateTimeFormatWithSeconds (JStime);
    },
    get_server_subscription:function(){
        return get_server_subscription ();
    },
    get_Querystring_error_log:function(){
       
        return get_Querystring_error_log (); 
    },
    get_SPK:function(){
        return get_SPK (); 
    },
    get_MSK:function(){
        return get_MSK (); 
    },
    check_local_json:function(){
        return check_local_json (); 
    },
    encrypt:function(text, algorithm, password, crypto){
        return encrypt (text, algorithm, password, crypto); 
    },
    decrypt:function(text, algorithm, password, crypto){
        return decrypt (text, algorithm, password, crypto); 
    },

    
    
  
    
};
function encrypt(text, algorithm, password, crypto){
    var cipher = crypto.createCipher(algorithm,password)
    var crypted = cipher.update(text,'utf8','hex')
    crypted += cipher.final('hex');
    return crypted;
}

function decrypt(text, algorithm, password, crypto){
    var decipher = crypto.createDecipher(algorithm,password)
    var dec = decipher.update(text,'hex','utf8')
    dec += decipher.final('utf8');
    return dec;
}
function get_SPK(){
    return 'Pjoj1PaduNhNiFeSGN6LypSTGd3/+PytjcHtxmKzrjE=';
}
function get_MSK(){
    return 'efIDiTroyPXS9o0/wSKb4xwfsThyn3BCJkD3bWRgwGQfap4uTg2TgyvetDN/W8UvMg1zNB7VvsVaKeFDSZPAOA==';
}
function get_Querystring_error_log (){
    var querystring_error_log =`INSERT INTO loyaltycore.error_log (object_name, reference_id, error_msg, createddate, remarks)
    VALUES ($1, $2, $3, NOW(), $4);`;
    return querystring_error_log;
}
function checkMultiPicklistValue( targetField, originField){
    if(isNotNull(targetField)){
        try{
            var target = targetField;
            var temArray = [];
            temArray = target.split(";");
            var isMatched = false;
            if(temArray.length > 0 ){
                for(var l=0;l<temArray.length;l++){
                    if(originField == temArray[l]){
                        isMatched = true;
                    }
                }
            }
            if(isMatched == false){
                return false;
            }else{
                return true;
            }
        }catch(e){
            console.log("Error in checking campaign target value");
            console.log(e);
            return false;
        }
    }else{
        return true;
    }
}
function getDateTimeKeyword(){
    var obj=[];
    obj.push("date");
    obj.push("day");
    obj.push("month");
    obj.push("year");

    obj.push("today");
    obj.push("tomorrow");
    obj.push("yesterday");
    return obj;
}
function getSQL_Update(InsertObj, dbname, tablename,fieldname, sfid){
    // InsertObj.lastmodifieddate = 'NOW()'
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
function getSQLDateTimeFormatWithSeconds(JStime){
    var month = (Number(JStime.getMonth())+1 );
    return JStime.getFullYear() +'-'+ month+ '-'+  JStime.getDate() + ' ' + JStime.getHours() + ':'+ JStime.getMinutes()+ ':'+ JStime.getSeconds() +'.'+ JStime.getMilliseconds(); ;
}
function getSQL_Insert(InsertObj, tablename, needSingleQuote){
    if(needSingleQuote){
        for (x in InsertObj){
            InsertObj[x] = AddSingleQuoteSymbol(InsertObj[x]);
        }
    }
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
    for (y in InsertObj){
        if(InsertObj[y] !== '\'\''){ //is NOT '', 0, null, or undefined
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
function isNotNull(value){
    if(value !== 0 && value !== null && value !== "" && typeof(value) !== "undefined"){
        return true;
    }else{
        return false;
    }
}
function getEvaluateObj(CurrentMembershipTier){
    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    var year = yesterday.getFullYear();
    var month = yesterday.getMonth();
    var day = yesterday.getDate();
    year = year + Number(CurrentMembershipTier.ivls__mexpirationdate1y__c);
    month = month +1;
    var datestr = year + '/' + month + '/' + day;
    var newMemberObj = {};
    newMemberObj.ivls__membershiptier__pc =AddSingleQuoteSymbol(CurrentMembershipTier.sfid); 
    newMemberObj.ivls__dateexpired__pc =AddSingleQuoteSymbol(datestr);
    newMemberObj.ivls__datetier__pc = 'NOW()::date';
    newMemberObj.ivls__datetiertime__c = "NOW() - interval '8 hour'";
    newMemberObj.ivls__spendingsincelastrenewal__pc = 0;
    newMemberObj.ivls__pointsearnedsincelastrenewal__pc = 0;
    newMemberObj.ivls__pointsredeemedsincelastrenewal__pc = 0;
    newMemberObj.ivls__pointstonexttier__c = CurrentMembershipTier.ivls__pointsto__c;
    newMemberObj.ivls__pointstokeeptier__c = CurrentMembershipTier.ivls__pointsfrom__c;
    return newMemberObj;
}
async function get_server_subscription () {
    var token = "";
    var options = { method: 'POST',
    url: 'https://introv-lm.azurewebsites.net/v',
    headers: 
    { 'Postman-Token': '59be6f29-3c81-4104-ae86-5590706b5287',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/x-www-form-urlencoded' },
    form: 
    { app: 'sony-loyalty',
        app_key: '73mqe61nrsovszpygyqglf3xgad0dzy6' } };

    await request(options, function (error, response, body) {
        if (error) throw new Error(error);

        // console.log(body);
        token = body;
    });

    token = JSON.parse(token);
    return token;
}
async function checkAuth () {

    //// get SPK ////
    try{
        var SPK = get_SPK();
    }catch(e){
        var tempStr = "Cannot get_SPK";
        console.log(tempStr);
        var querystring_error_log = get_Querystring_error_log();
        var values = ['', '', tempStr, e.toString()];
        var result  = (await query_with_parameter(querystring_error_log, values));
        console.log(result);
        return false;
    }

    //// get MSK ////
    try{
        var MSK = get_MSK();
    }catch(e){
        var tempStr = "Cannot get_MSK";
        console.log(tempStr);
        var querystring_error_log = get_Querystring_error_log();
        var values = ['', '', tempStr, e.toString()];
        var result  = (await query_with_parameter(querystring_error_log, values));
        console.log(result);
        return false;
    }

    //// check null SPK ////
    if(isNotNull(SPK)==false){
        var tempStr = "SPK is null";
        console.log(tempStr);
        var querystring_error_log = get_Querystring_error_log();
        var values = ['', '', "Auth error", tempStr];
        var result  = (await query_with_parameter(querystring_error_log, values));
        console.log(result);
        return false;
    }

    //// check null MSK ////
    if(isNotNull(MSK)==false){
        var tempStr = "MSK is null";
        console.log(tempStr);
        var querystring_error_log = get_Querystring_error_log();
        var values = ['', '', "Auth error", tempStr];
        var result  = (await query_with_parameter(querystring_error_log, values));
        console.log(result);
        return false;
    }



    var checkexpire = false;

    //// api get server auth ////
    try{
        var server_subscription = await get_server_subscription () ;
    }catch(e){
        var tempStr = "Cannot get server_subscription";
        console.log(tempStr);
        var querystring_error_log = get_Querystring_error_log();
        var values = ['', '', tempStr, e.toString()];
        var result  = (await query_with_parameter(querystring_error_log, values));
        console.log(result);
        checkexpire = true;
    }

    if(checkexpire == false){
        //// verify server signature ////
        try{
            var verify_signature = crypto.verify(server_subscription.data, server_subscription.signature, SPK);
            // console.log(verify_signature);
        }catch(e){
            var tempStr = "Cannot verify_signature";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', "Auth error", tempStr];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            checkexpire = true;
        }
    }
   

    if(checkexpire == false){
        //// check server signature ////
        if(isNotNull(verify_signature)==false ||verify_signature !== true ){
            var tempStr = "Cannot verify_signature";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', "Auth error", tempStr];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            checkexpire = true;
        }
    }
    
    if(checkexpire == false){
        try{
            var server_decrypted_data = crypto.decrypt(server_subscription.data, server_subscription.nonce, SPK, MSK);
            server_decrypted_data = JSON.parse(server_decrypted_data);
            console.log("server_decrypted_data");
            console.log(server_decrypted_data);
        }catch(e){
            var tempStr = "Cannot decrypt server_subscription";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', "Auth error", tempStr];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            checkexpire = true;
        }    
    }
  

    // checkexpire = true;
    console.log("checkexpire"+checkexpire);
    if(checkexpire == true){
        // var local_json_is_integrity = check_local_json(SPK, MSK);
        // if(check_local_json !== true){
        //     return false;
        // }
        //// read local auth ////
        try{
            var local_subscription = require('../lib/auth.json')
        }catch(e){
            var tempStr = "Cannot get local_subscription";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', tempStr, e.toString()];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            return false;
        }
        // console.log("local_subscription");
        // console.log(local_subscription);
        //// decrypt local auth ////
        try{
            var local_decrypted_data = crypto.decrypt(local_subscription.data, local_subscription.nonce, SPK, MSK);
            local_decrypted_data = JSON.parse(local_decrypted_data);
            console.log("local_decrypted_data");
            console.log(local_decrypted_data);
        }catch(e){
            var tempStr = "Cannot decrypt local_subscription";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', "Auth error", tempStr];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            return false;
        }


        var now = new Date();

        try{
            var local_subscription_expiredate =  new Date(local_decrypted_data.expiry);
        }catch(e){
            var tempStr = "Cannot read local_decrypted_data.expiry";
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', tempStr, e.toString()];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            return false;
        }
       

        console.log("now");
        console.log(now);

        console.log("local_subscription_expiredate");
        console.log(local_subscription_expiredate);

        if(now > local_subscription_expiredate){
            console.log("Subscription expiry");
            var tempStr = "Subscription expiry";
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', tempStr, local_subscription_expiredate];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            return false;
        }else{
            console.log("Valid Subscription");
            return true;
        }

        
    }else{
        console.log("JSON.stringify(server_subscription)");
        console.log(JSON.stringify(server_subscription));
        try{
           fs.writeFile('./lib/auth.json', JSON.stringify(server_subscription), function(err) {
                if(err) {
                    return console.log(err);
                }
                console.log("auth.json was updated");

            }); 
        }catch(e){
            console.log("Write auth.json file fail");
            console.log(e);
            // return;

            var tempStr = "Write auth.json file fail";
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', tempStr, e.toString()];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
        }
        return true;
    }
}
async function check_local_json(SPK, MSK){
    try{
        //// read local auth ////
        try{
            var local_subscription = require('../lib/auth.json')
        }catch(e){
            var tempStr = "Cannot get local_subscription";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', tempStr, e.toString()];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            return false;
        }
        
        //// decrypt local auth ////
        try{
            var local_decrypted_data = crypto.decrypt(local_subscription.data, local_subscription.nonce, SPK, MSK);
            local_decrypted_data = JSON.parse(local_decrypted_data);
            console.log(local_decrypted_data);
        }catch(e){
            var tempStr = "Cannot decrypt local_subscription";
            console.log(tempStr);
            var querystring_error_log = get_Querystring_error_log();
            var values = ['', '', "Auth error", tempStr];
            var result  = (await query_with_parameter(querystring_error_log, values));
            console.log(result);
            return false;
        }

        // //// validate local format ////
        // try{
        //     var local_subscription_token = local_decrypted_data.license.validation;
        // }catch(e){
        //     var tempStr = "Cannot get local_subscription.license.validation;";
        //     var querystring_error_log = get_Querystring_error_log();
        //     var values = ['', '', tempStr, e.toString()];
        //     var result  = (await query_with_parameter(querystring_error_log, values));
        //     console.log(result);
        //     return false;
        // }

        return true;
    }catch(e){
        return false;
    }
}
async function query_with_parameter (q,p) {
    const client = await pool.connect()
    let res
    try {
      await client.query('BEGIN')
      try {
        res = await client.query(q,p)
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
async function query_one_way (q) {
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
async function getMembershiptier () {
    try{
        var querystring =  'SELECT * FROM loyaltycore.ivls__membershiptier__c ORDER BY ivls__sequence__c DESC';
        var result= await query_one_way(querystring);
        return result.rows;
    }catch (err) {
        console.log('Database ' + err)
        return false;
    }
}
async function getMembershipYearStart () {
    try{
        var querystring =  'SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = \'MySony\'';
        var result= await query_one_way(querystring);
        return result.rows[0].ivls__yearstart__c;
    }catch (err) {
        console.log('Database ' + err)
        return false;
    }
}
async function checkCAM(campaignObj, recordtype_list, memberObj) {
    try{
        // // earnpoints campaign
        var recordtypeid_1 = getRecordTypeId('Campaign','EarnPoints', recordtype_list);
        var recordtypeid_0 = campaignObj.recordtypeid;
        if(recordtypeid_0 !== recordtypeid_1){
            return false;
        }
        var calculateBy = campaignObj.ivls__calculateby__c;

        var referenceObject = campaignObj.ivls__referenceobject__c;
        var referenceField = campaignObj.ivls__field__c;
        var pointsPool = campaignObj.ivls__pointspool__c;//Spending or A&P

        if(isNotNull(referenceObject)==false){
            console.log("referenceObject is null ");   
            return false;
        }

        referenceField =  referenceField.toLowerCase();
        referenceObject = referenceObject.toLowerCase();

        
        var operator = getSQLOperator(campaignObj.ivls__operator__c);
        var isDateTime = false;
        if((referenceField).includes("date") == true){
            isDateTime = true;
        }
        var condition_fieldValue= campaignObj.ivls__filtervalue__c ;
        var condition_fieldFormula= campaignObj.ivls__filter_formula__c ;

        var targetValue = "";
        if (isNotNull(condition_fieldValue)){
            targetValue = condition_fieldValue;
        }else if (isNotNull(condition_fieldFormula) && isNotNull(condition_fieldValue) == false){
            targetValue = getSQLtargetValue(condition_fieldFormula);
        }

        if(targetValue == false){
            console.log("Cannot read formula:"+condition_fieldFormula);   
            return false;
        }
    
    
    
        //need to check is Spending!!!!!!!
        console.log("-------------------------------------");
        console.log("referenceObject="+referenceObject);
        console.log("referenceField="+referenceField);
        console.log("pointsPool="+pointsPool);   
        console.log("isDateTime="+isDateTime);   
        console.log("targetValue="+targetValue);   
        console.log("-------------------------------------");

        
        var targetMembershipTier = '';
        var targetMemberPGID = '';
        
        if(referenceObject == 'account'){   
            targetMembershipTier = transactionObj.ivls__membershiptier__pc;
            targetMemberPGID = transactionObj.id;
        }else if (referenceObject == 'ivls__transaction__c'){
            targetMemberPGID = transactionObj.ivls__memberpgid__c;
            querystring = "SELECT ivls__membershiptier__pc FROM loyaltycore.account WHERE ivls__pgid__c = '"+transactionObj.ivls__memberpgid__c+"'";
            try{
                targetMembershipTier = (await query_one_way(querystring)).rows[0].ivls__membershiptier__pc;

            }catch(e){
                console.log("targetMembershipTier error:") 
                console.log(e) 
                return false;
            }
        
        }else {
            return false;
        }
        
        
        if(isNotNull(targetMemberPGID)==false){
            return false;
        }

        if(isNotNull(targetMembershipTier)==false){
            return false;
        }

        if(targetMembershipTier !== campaignObj.ivls__tier__c){
            return false;
        }
        
    
        // var member_list = await selectALL_with_parameter('account','ivls__pgid__c','=',targetMemberPGID);
        // var memberObj= getCurrentMemberInfo(member_list[0], membershiptiers_list);
        var CurrentMembershipTier = memberObj.CurrentMembershipTier;
        
        ////    check blacklisted member    ////
        var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
        if(currentMemberTierIsBlacklisted == true){
            console.log("currentMemberTierIsBlacklisted:"+targetMemberPGID);
            return false;
        }

        var currentSpending_totalLevel = 0;
        var currentSpending_lineLevel = 0;
        var currentSpending = 0;
        if(referenceObject == 'ivls__transaction__c'){
            
            currentSpending_totalLevel = transactionObj.ivls__totalcashamount__c ;
            currentSpending_lineLevel =  transactionObj.ivls__spending__c ;
            console.log("currentSpending_totalLevel="+currentSpending_totalLevel); 
            console.log("currentSpending_lineLevel="+currentSpending_lineLevel); 

            if(calculateBy == "Line"){
                currentSpending = currentSpending_lineLevel;
                
            }else{
                currentSpending = currentSpending_totalLevel;
            }
        }


        if(calculateBy=="Line" && referenceObject == 'ivls__transaction__c'){
            var  cate1 = "";
            var  cate2 = "";
            var  cate3 = "";
            var product_sfid = transactionObj.ivls__product__c;
            var productCate = "";
            var merchant = transactionObj.ivls__merchant__c;

            try{
                querystring = "SELECT * FROM loyaltycore.ivls__product__c WHERE sfid = '"+product_sfid+"'";
                productCate = (await query_one_way(querystring)).rows[0];
                if(isNotNull(productCate)== false){
                    console.log("select productCate  error:") 
                    return false;
                }
                cate1 = productCate.ivls__productcategoryl1__c;
                cate2 = productCate.ivls__productcategoryl2__c;
                cate3 = productCate.ivls__productcategoryl3__c;
            }catch(e){
                console.log("productCate error:") 
                console.log(e) 
                return false;
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

            var targetMerchant = campaignObj.ivls__merchant__c;
            var targetProduct = campaignObj.ivls__product__c;
            var targetCate = campaignObj.ivls__productcategory__c;
            var meetCondition = true;

            if(isNotNull(targetMerchant)){
                if(targetMerchant !== merchant){
                    // console.log("membership unmatched");
                    var meetCondition = false;
                }
            }

            if(isNotNull(targetProduct)){
                if(targetProduct !== product_sfid){
                    // console.log("membership unmatched");
                    var meetCondition = false;
                }
            }
            if(isNotNull(targetCate)){
                if(targetCate !== cate1 && targetCate !== cate2 && targetCate !== cate3){
                    // console.log("membership unmatched");
                    var meetCondition = false;
                }
            }
        
            if(meetCondition == false){
                console.log("product, merchant, cate unmatched");
                return false;
            }

        }


        if(campaignObj.ivls__applytospecificmembers_only__c == true){
            //check the member is specific member of the campaign
            querystring =  "SELECT campaignmember.id, campaignmember.sfid, campaignmember.campaignid as campaignid, campaignmember.contactid as contactid, campaign.startdate, campaign.enddate FROM loyaltycore.campaignmember as campaignmember JOIN loyaltycore.campaign as campaign ON campaignmember.campaignid = campaign.sfid WHERE campaign.sfid = '"+campaignObj.sfid+"'";
            var campaignMember_list = (await query_one_way(querystring)).rows;

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
                return false;
            }
        }else{
            //check membership and membership tier
            if(isNotNull(campaignObj.ivls__tier__c)){
                if(campaignObj.ivls__tier__c !== CurrentMembershipTier.sfid){
                    console.log("campaignObj.ivls__tier__c !== CurrentMembershipTier.sfi");
                    return false;
                }
            }else{
                //check if within specific sequence
                if(isNotNull(campaignObj.ivls__tiersequencefrom__c)){
                    if(CurrentMembershipTier.ivls__sequence__c < campaignObj.ivls__tiersequencefrom__c ){
                        console.log("CurrentMembershipTier.ivls__sequence__c < campaignObj.ivls__tiersequencefrom__c");
                        return false;
                    }
                }
                if(isNotNull(campaignObj.ivls__tiersequenceto__c)){
                    if(CurrentMembershipTier.ivls__sequence__c > campaignObj.ivls__tiersequenceto__c ){
                        console.log("CurrentMembershipTier.ivls__sequence__c > campaignObj.ivls__tiersequenceto__c");
                        return false;
                    }
                }
            
            }
            
            
        }
        
        //--------------------------------overall checking condition-----------------------//

        //@@TBU@@ check source
        if(isNotNull(campaignObj.ivls__vip__c)){
            if(campaignObj.ivls__vip__c == true){
                if(memberObj.Account.ivls__vip__pc !== true){
                    return false;
                }
            }
        }

        // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__gender__c, memberObj.Account.ivls__gender__pc);
        // if(isMatched == false){
        //     console.log("campaign gender not matched");
        //     return false;
        // }
        
        // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__interestedin__c, memberObj.Account.ivls__interestedin__pc);
        // if(isMatched == false){
        //     console.log("campaign interested in not matched");
        //     return false;
        // }
        // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__country__c, memberObj.Account.ivls__country__pc);
        // if(isMatched == false){
        //     console.log("campaign country not matched");
        //     return false;
        // }
        // var isMatched = checkMultiPicklistValue(router, campaignObj.ivls__region__c, memberObj.Account.ivls__region__pc);
        // if(isMatched == false){
        //     console.log("campaign region not matched");
        //     return false;
        // }

        var dateJoin = memberObj.Account.ivls__datejoin__pc;  

        if(isNotNull(campaignObj.ivls__yearfrom__c)){
            var NowMinusYearFrom = new Date();
            NowMinusYearFrom.setMonth(NowMinusYearFrom.getFullYear() - Number(campaignObj.ivls__yearfrom__c));
            if(dateJoin < NowMinusYearFrom){
                console.log("yearfrom region not matched");
                return false;
            }
        }

        if(isNotNull(campaignObj.ivls__yearto__c)){
            var NowMinusYearTo = new Date();
            NowMinusYearTo.setMonth(NowMinusYearFrom.getFullYear() - Number(campaignObj.ivls__yearto__c));
            if(dateJoin > NowMinusYearTo){
                console.log("yearto region not matched");
                return false;
            }
        }
        
        
        if(isNotNull(campaignObj.ivls__minimumspending__c)){
            if(campaignObj.ivls__minimumspending__c > currentSpending){
                return false;
            }
        }

        if(isNotNull(campaignObj.ivls__maximumspending__c)){
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
        var calc_pointsbucket= campaignObj.ivls__pointsbucket__c ;


        console.log("calc_points="+calc_points);
        console.log("calc_pointsmultiple="+calc_pointsmultiple);
        console.log("calc_pointsbucket="+calc_pointsbucket);   


        var earnedPoints = 0;
        if(isNotNull(calc_points) == false && isNotNull(calc_pointsmultiple) == false){
            console.log("PointScheme missing ivls__points__c and calc_pointsmultiple:"+campaignObj.id) 
            return false;
        }else if (isNotNull(calc_points) && isNotNull(calc_pointsmultiple) == false){
            earnedPoints = calc_points;
        }else if (isNotNull(calc_pointsmultiple) && isNotNull(calc_points) == false){
            earnedPoints = Number(calc_pointsmultiple) * campaign_currentSpending;
        }else if(isNotNull(calc_points) && isNotNull(calc_pointsmultiple)){
            earnedPoints = calc_points;
        }
    
        console.log("========   behaviour level pointscheme   ======== ");   
        console.log("earnedPoints="+earnedPoints);   
        var lineActivityName = '';
        var lineActivityId = '';

        if(isNotNull(campaignObj.ivls__activity__c)){
            lineActivityName =  campaignObj.ivls__activity__c ;
            lineActivityId = campaignObj.sfid;
        }

        if(earnedPoints > 0){
            return earnedPoints;
        }
    }catch (err) {
        console.log('checkCAM: ' + err)
        return false;
    }
}

async function checkPS_Total(receipt_obj, pointSchemeObj, memberObj, recordtype_list) {
    try{
        ////    pre-defiened some variables to show the result    ////
        console.log("@@@@@@@@@@@@@@@@@@@@@@@@   "+receipt_obj.ivls__receiptno__c+"  @@@@@@@@@@@@@@@@@@@@@@@@");
        var PointsTotal = 0;
        var PointsBasic = 0;
        var PointsBonus = 0;

        ////    get member information    ////
        var targetMemberPGID = receipt_obj.ivls__memberpgid__c;
        if(isNotNull(targetMemberPGID)==false){
            console.log("ivls__memberpgid__c in transaction is null:"+targetMemberPGID);
            querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Error' WHERE ivls__receiptno__c = '"+ receipt_obj.ivls__receiptno__c +"'";
            console.log("querystring::");
            console.log(querystring);
            if(environment == "production"){
                await query_one_way(querystring);
            }
            return false;
        }

        
        var CurrentMembershipTier = memberObj.CurrentMembershipTier;
        var currentSpending_totalLevel = receipt_obj.ivls__totalcashamount__c;//total spending
        
        var TierSpending = CurrentMembershipTier.ivls__spending__c;
        var TierPoints =  CurrentMembershipTier.ivls__tpoints__c;

        if(isNotNull(receipt_obj.ivls__transactiondate__c)== false){
            console.log("ivls__transactiondate__c is null :"+receipt_obj.ivls__transactiondate__c);
            return false;
        }
       
       
        ////    check blacklisted member    ////
        var currentMemberTierIsBlacklisted = CurrentMembershipTier.ivls__ignorepointsearn__c;
        if(currentMemberTierIsBlacklisted == true){
            console.log("currentMemberTierIsBlacklisted:"+targetMemberPGID);
            querystring = "UPDATE loyaltycore.ivls__transaction__c SET ivls__status__c = 'Black List' WHERE ivls__status__c = 'Open' AND ivls__memberpgid__c = "+ targetMemberPGID;
            console.log("querystring::");
            console.log(querystring);
            if(environment == "production"){
                await query_one_way(querystring);
            }
            return false;
        }
       
        ////    CheckPS     ////
        try{
            if(pointSchemeObj.ivls__membershiptier__c !== CurrentMembershipTier.sfid){
                console.log("Membershiptier unmatched:"+  memberObj.Account.id) 
                return false;
            }
           
            //total
            var recordtypeid_1 = getRecordTypeId('IVLS__PointsScheme__c','Total', recordtype_list);
            if(isNotNull(recordtypeid_1)== false){
                console.log("recordtype table missing Total record type of IVLS__PointsScheme__c:"+pointSchemeObj.id) 
                return false;
            }

            var recordtypeid_0 = pointSchemeObj.recordtypeid;
            if(isNotNull(recordtypeid_0)== false){
                console.log("PointScheme missing recordtype:"+pointSchemeObj.id) 
                return false;
            }
           
            if(recordtypeid_0 !== recordtypeid_1){
                return false;
            }
           
            var pointsbucket = pointSchemeObj.ivls__pointsbucket__c;
            if(isNotNull(pointsbucket)== false){
                console.log("PointScheme missing pointsbucket:"+pointSchemeObj.id) 
                return false;
            }

            
            var referenceObject = pointSchemeObj.ivls__referenceobject__c;
            if(isNotNull(referenceObject)== false){
                console.log("PointScheme missing referenceObject:"+pointSchemeObj.id) 
                return false;
            }

            if(referenceObject.toLowerCase() !== 'ivls__transaction__c'){
                return false;
            }

            var referenceField = pointSchemeObj.ivls__field__c;
            if(isNotNull(referenceField)== false){
                console.log("PointScheme missing referenceField:"+pointSchemeObj.id) 
                return false;
            }
            referenceField = referenceField.toLowerCase();

            // var pointsPool ='Spending'; //Spending or A&P
            var pointsPool = pointSchemeObj.ivls__pointspool__c;//Spending or A&P
            if(isNotNull(pointsPool)== false){
                console.log("PointScheme missing pointsPool:"+pointSchemeObj.id) 
                return false;
            }

            var operator = pointSchemeObj.ivls__operator__c;
            if(isNotNull(operator)== false){
                console.log("PointScheme missing operator:"+pointSchemeObj.id) 
                return false;
            }

            var isDateTime = false;
            var datetimeArray = getDateTimeKeyword();
            for(var h=0;h<datetimeArray.length;h++){
                if(referenceField.includes(datetimeArray[h]) == true){
                    isDateTime = true;
                    break;
                }
            }

            if(isNotNull(isDateTime) == false){
                console.log("error in get isDateTime");   
                return false;
            }
            //get originValue
            var originValue = receipt_obj[referenceField];

            var targetValue = "";
            var condition_fieldValue= pointSchemeObj.ivls__filtervalue__c ;
            var condition_fieldFormula= pointSchemeObj.ivls__filter_formula__c ;
            if (isNotNull(condition_fieldValue)){
                targetValue = condition_fieldValue;
            }else if (isNotNull(condition_fieldFormula) && isNotNull(condition_fieldValue) == false){
                targetValue = getSQLtargetValue_total(condition_fieldFormula);
            }
            if(targetValue == false){
                console.log("Cannot read formula:"+condition_fieldFormula);   
                return false;
            }
            if(isNotNull(targetValue)== false){
                console.log("PointScheme missing targetValue:"+pointSchemeObj.id) 
                return false;
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
            var today = new Date();
            if(isDateTime){
                if(condition_fieldFormula == 'This Month' || condition_fieldFormula == 'Last Month' ||condition_fieldFormula == 'This Calendar Year' ){
                    if(today >= targetValue.firstDay &&today <= targetValue.lastDay){
                        meetCondition = true;
                    }
                }else{
                    meetCondition = checkConditionisMeet(originValue, operator, targetValue,isDateTime)
                }
            }else{
                meetCondition = checkConditionisMeet(originValue, operator, targetValue,isDateTime)
            }
            console.log("PS meetCondition="+meetCondition);   
            if(isNotNull(meetCondition)==false){ 
                return false;
            }

            if(meetCondition !== true){
               
                return false;
            }   

            var calc_points= pointSchemeObj.ivls__points__c ;
            var calc_pointsmultiple= pointSchemeObj.ivls__pointsmultiple__c ;
        
            console.log("calc_points="+calc_points);
            console.log("calc_pointsmultiple="+calc_pointsmultiple);
        

            var earnedPoints = 0;
            if(isNotNull(calc_points) == false && isNotNull(calc_pointsmultiple) == false){
                console.log("PointScheme missing ivls__points__c and calc_pointsmultiple:"+pointSchemeObj.id) 
                return false;
            }else if (isNotNull(calc_points) && isNotNull(calc_pointsmultiple) == false){
                earnedPoints = calc_points;
            }else if (isNotNull(calc_pointsmultiple) && isNotNull(calc_points) == false){
                earnedPoints = Number(calc_pointsmultiple) * currentSpending_totalLevel;
            }else if(isNotNull(calc_points) && isNotNull(calc_pointsmultiple)){
                earnedPoints = calc_points;
            }

            var lineActivityName = '';
            var lineActivityId = '';

            if(isNotNull(pointSchemeObj.ivls__activity__c)){
                lineActivityName =  pointSchemeObj.ivls__activity__c ;
                lineActivityId = pointSchemeObj.sfid;
            }
            console.log("earnedPoints="+earnedPoints);   
            if(earnedPoints > 0){   
                PointsTotal = PointsTotal + earnedPoints;
                PointsBonus = PointsBonus + earnedPoints;
                return earnedPoints;
            }else{
                return false;
            }
          
         
          
           
           
        }catch(e){
            console.log("//////////////////////////////  WARNING  //////////////////////////////") 
            console.log('Error Processing :pointSchemeObj.id:'+ pointSchemeObj.id);
            console.log('Error Message:');
            console.log(e);
            return false;
        }
    }catch (err) {
        console.log('checkPS_Total: ' + err)
        return false;
    }
}function getSQLDateTimeFormat(JStime){
    var month = (Number(JStime.getMonth())+1 );
    return JStime.getFullYear() +'/'+ month+ '/'+  JStime.getDate();
}
function getSQLtargetValue(targetValue){
    if(targetValue == "True"){
        return 'true';
    }else if (targetValue == "False"){
        return 'false';
    }else if (targetValue == "Today"||targetValue == "TODAY()"){
        return 'NOW()'
    }else if (targetValue == "This Month"){
        var date = new Date();
        var firstDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth(), 1));
        var lastDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth() + 1, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "Last Month"){
        var date = new Date();
        var firstDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth()-1, 1));
        var lastDay = getSQLDateTimeFormat(new Date(date.getFullYear(), date.getMonth(), 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "This Calendar Year"){
        var date = new Date();
        var firstDay = getSQLDateTimeFormat(new Date(date.getFullYear(), 0, 1));
        var lastDay = getSQLDateTimeFormat(new Date(date.getFullYear(), 11, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else{
        return false;
    }
}

async function checkPSCAM_again (ivls__ps_campain_id, ref_type, receipt_obj,memberObj, recordtype_list) {
    try{
        // var ivls__ps_campain_id = detailObj.ivls__ps_campain_id;
        // var ref_type = detailObj.ref_type;
        ref_type = ref_type.toLowerCase();
        var querystring =  "SELECT * FROM loyaltycore."+ref_type+" WHERE sfid = '"+ivls__ps_campain_id + "' LIMIT 1";
        var PSCAMobj= (await query_one_way(querystring)).rows[0];
        
        if(isNotNull(PSCAMobj) == false){
            return false;
        }
        if(ref_type == "ivls__pointsscheme__c"){
            var pointSchemeObj = PSCAMobj;
            var earnedPoints = await checkPS_Total(receipt_obj, pointSchemeObj, memberObj, recordtype_list);
            if(earnedPoints > 0 && isNotNull(earnedPoints)){
                return true;
            }else{
                return false;
            }
        }else if(ref_type == "campaign"){
            var campaignObj = PSCAMobj;
            var earnedPoints = await checkCAM(campaignObj, recordtype_list, memberObj) ;
            if(earnedPoints > 0 && isNotNull(earnedPoints)){
                return true;
            }else{
                return false;
            }
            
            
            return false;
        }else{
            return false;
        }

        return true;
    }catch (err) {
        console.log('Database ' + err)
        return false;
    }
}
async function getMembershipMembershipID () {
    try{
        var querystring =  'SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = \'MySony\'';
        var result= await query_one_way(querystring);
        return result.rows[0].sfid;
    }catch (err) {
        console.log('Database ' + err)
        return false;
    }
}
async function selectALL (tablename) {
    try{
        var  querystring = 'SELECT * FROM loyaltycore.'+tablename;
        var result= await query_one_way(querystring);
        return result.rows;
    }catch (err) {
        console.log('Database ' + err)
        return false;
    }
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
        return '<>';
    }else{
        return false;
    }
}
async function selectALL_with_parameter (tablename, parameterName, operater, parameterValue) {
    try{
        if(isNotNull(parameterName)==false || isNotNull(operater)==false ||isNotNull(parameterValue)==false ){
           return [];
        }
        var  querystring = 'SELECT * FROM loyaltycore.'+tablename + ' WHERE ' + parameterName + ' ' + operater + ' ' +  parameterValue;
        var result= await query_one_way(querystring);
        return result.rows;
    }catch (err) {
        console.log('Database ' + err)
        return false;
    }
}
function checkConditionisMeet(originValue, operator, targetValue,isDateTime){
    if(isDateTime){
        if(isNotNull(originValue) && isNotNull(targetValue)){
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
    
}
function checkMultiPicklistValue(targetField, originField){
    if(isNotNull(targetField)){
        try{
            var target = targetField;
            var temArray = [];
            temArray = target.split(";");
            var isMatched = false;
            if(temArray.length > 0 ){
                for(var l=0;l<temArray.length;l++){
                    if(originField == temArray[l]){
                        isMatched = true;
                    }
                }
            }
            if(isMatched == false){
                return false;
            }else{
                return true;
            }
        }catch(e){
            console.log("Error in checking campaign target value");
            console.log(e);
            return false;
        }
    }else{
        return true;
    }
}
function sameDay(d1, d2) {
    return (d1.getFullYear() === d2.getFullYear() &&
      d1.getMonth() === d2.getMonth() &&
      d1.getDate() === d2.getDate());
}
function getMappingObj(ivls__detail__c, txMapping_list, type){
    // var mappingObj = getPOSnESHOPDetailJSONmapping();
    var mappingObj = {};
    if(type.toLowerCase() == 'warranty'){
        for(x in ivls__detail__c){
            for(var h=0;h<txMapping_list.length;h++){
                if(txMapping_list[h].ivls__sourcefield__c == x && txMapping_list[h].ivls__object__c=='Warranty'){
                    var lowercaseStr = (txMapping_list[h].ivls__targetfield__c).toLowerCase();
                    mappingObj[lowercaseStr] = ivls__detail__c[x];
                }
            }
        }
    }else{
        for(x in ivls__detail__c){
            for(var h=0;h<txMapping_list.length;h++){
                if(txMapping_list[h].ivls__sourcefield__c == x && txMapping_list[h].ivls__object__c=='POS'){
                    var lowercaseStr = (txMapping_list[h].ivls__targetfield__c).toLowerCase();
                    mappingObj[lowercaseStr] = ivls__detail__c[x];
                }
            }
        }
    }

    return mappingObj;
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
function getSQLtargetValue_total(targetValue){
    if(targetValue == "True"){
        return 'true';
    }else if (targetValue == "False"){
        return 'false';
    }else if (targetValue == "Today"||targetValue == "TODAY()"){
        return 'NOW()'
    }else if (targetValue == "This Month"){
        var date = new Date();
        var firstDay = (new Date(date.getFullYear(), date.getMonth(), 1));
        var lastDay = (new Date(date.getFullYear(), date.getMonth() + 1, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "Last Month"){
        var date = new Date();
        var firstDay = (new Date(date.getFullYear(), date.getMonth()-1, 1));
        var lastDay = (new Date(date.getFullYear(), date.getMonth(), 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else if (targetValue == "This Calendar Year"){
        var date = new Date();
        var firstDay = (new Date(date.getFullYear(), 0, 1));
        var lastDay = (new Date(date.getFullYear(), 11, 0));
        var obj={
            firstDay:firstDay,
            lastDay:lastDay,
        }
        return obj;
    }else{
        return false;
    }
}
function getPOSnESHOPDetailJSONmapping(){
    var obj={
        "ivls__receiptamount__c":"",
        "ivls__currency__c":"",
        "ivls__member__c":"",
        "ivls__totaldiscountamount__c":"",
        "ivls__receiptdate__c":"",
        "ivls__receiptno__c":"",
        "ivls__totalcashamount__c":"",
        "ivls__shop__c":"",
        "ivls__merchant__c":"",
        "ivls__remarks__c":"",
        "ivls__totalcreditcardamount__c":"",
        "ivls__totalvoucheramount__c":"",
        "ivls__transactiondate__c":"",
        "ivls__unitprice__c":"",
        "ivls__netunitprice__c":"",
        "ivls__sequenceno__c":"",
        "ivls__product__c":"",
        "ivls__quantity__c":"",
        "ivls__discountline__c":"",
        "ivls__spending__c":"",
        "ivls__spendingexclusive__c":"",
        "ivls__totalpriceline__c":"",
        "ivls__voucheramountline__c":"",
    }
    return obj;
}
function getYearStr(year, month, day){
    return year + '/'+month + '/' +day;
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
function getRecordTypeId(sobjecttype,developername, recordtype_list){
    var recordtypeid = '';
    for(var i=0;i<recordtype_list.length;i++){
        if(recordtype_list[i].sobjecttype == sobjecttype && recordtype_list[i].developername == developername ){
            recordtypeid = recordtype_list[i].sfid ;
            break;
        }
    }
    return recordtypeid;
}
function genInsertTranSQL_Total(neededTranType, mappingObj, dbname,tablename,  CurrentMembershipTier, recordtype_list, lineTotal, lineActivityName, targetMemberPGID){
    var querystring ='';
    var InsertObj = getInsertTransactionTotalObj();
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
        InsertObj[x] = AddSingleQuoteSymbol(InsertObj[x]);
    }

    InsertObj.ivls__transactiondate__c = 'NOW()';
    InsertObj.createddate = 'NOW()';
    InsertObj.lastmodifieddate = 'NOW()';
    InsertObj.ivls__receiptdate__c = 'NOW()';
    InsertObj.ivls__source__c = '\'System\'';
    InsertObj.ivls__status__c = '\'Complete\'';
    InsertObj.ivls__memberpgid__c = targetMemberPGID;
    
    InsertObj.ivls__activities__c = AddSingleQuoteSymbol(lineActivityName);
    InsertObj.ivls__pointspool__c = AddSingleQuoteSymbol(neededTranType);
    
    InsertObj.recordtypeid = AddSingleQuoteSymbol(getRecordTypeId('IVLS__Transaction__c',neededTranType, recordtype_list));
    
    InsertObj.ivls__datepointsexpiry__c = AddSingleQuoteSymbol(CurrentMembershipTier.expiryDATE);
    
  
    InsertObj.ivls__pointsearned__c = lineTotal;
    InsertObj.ivls__pointsnet__c = lineTotal;
    
    querystring = 'INSERT INTO '+dbname+tablename+' (';
    var count = 0;
    var fieldsstr = ''
    for (x in InsertObj ){
        if(isNotNull(InsertObj[x]) && InsertObj[x] !== '\'\''){ //is NOT '', 0, null, or undefined
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
        if(isNotNull(InsertObj[y]) && InsertObj[y] !== '\'\''){ //is NOT '', 0, null, or undefined
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
