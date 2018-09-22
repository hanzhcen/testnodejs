const router = require('../lib/router.js')
const updatemember = require('../lib/updatemember.js')
// var pg = require('pg');
const pool = router.getPool();
var dbname = 'loyaltycore.';
var querystring = '';
console.log("//////////////////////////////   START   //////////////////////////////") 

async function main () {
    querystring = "SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = 'MySony'";
    var membershipObj =   (await router.query_one_way(querystring)).rows[0];
    querystring= 'SELECT * FROM loyaltycore.ivls__membershiptier__c ORDER BY ivls__sequence__c DESC';
    var membershiptiers_list= (await router.query_one_way(querystring)).rows;
    ////  POS ////
    //12342, HAO CHEN (0010l00000RkrCiAAJ), 1685445, Bronze, 2018-08-27 18:28:38, y
    //22664, Mei Tai Lo (0010l00000RBSMUAA5),1563633, Platinum, 
    //22665, Chi Hung Law (0010l00000RBSMbAAP),1407042, Gold, 2018-08-28 12:00:40, y
    //101, Chi Ming Wong (0010l00000RkpdmAAB),594537, Silver, 2018-08-28 16:52:03, need reset datetiertime, simonwong0115@gmail.com
    //22666, Wing Keung Kwok (0010l00000RBSMTAA5),1565775, Bronze, 2018-08-27 18:43:47, , kwokchai@mac.com
    //33529, Pak Ho Pako Lau (0010l00000Tug8iAAB), 


    // querystring = "SELECT ivls__pgid__c FROM loyaltycore.account  WHERE ivls__pointsbalance__pc > 0 order by ivls__pgid__c;"
    // var targetArray = (await router.query_one_way(querystring)).rows;
    // console.log("targetArray:"+targetArray.length);


    ////  ESHOP ////
    //12039, 616407,  0010l00000TFdj5AAD, T,  stanleymkh@gmail.com
    //32848, 1317243, 0010l00000TeP26AAF, N,  neomichell@netvigator.com
    //30436, 1187912, 0010l00000TeOG5AAN, N,  frankie1215@gmail.com
    //33456, 650629,  0010l00000RBxUYAA1, T,  wallylau831@yahoo.com.hk
    //32483, 1180083, 0010l00000TeOtmAAF, N,  calvinlui13@gmail.com
    //33457, 1181364, 0010l00000RBSOPAA5, T,  harristsam@gmail.com
    //28386, 686642,  0010l00000TaiHNAAZ, T,  vincentkwan22222@gmail.com



    ////  New ////
    //0010l00000RBSwaAAH, PAKO lau
    //Pak Ho Pako Lau, 0010l00000Tug8iAAB, 33529
    // var targetMemberPGID = 22666;//PGID
    // var targetMemberSFID = '0010l00000RBSMTAA5';
    var targetArray = [101];
    // var targetArray = [12342,22664, 22665, 101, 22666, 33529, 12039];
    // var newtargetArray = [];
    // for(var i=0;i<targetArray.length;i++){
    //   for(var j=0;j<transactiondetails_list.length;j++){
    //     if(targetArray[i] == transactiondetails_list[j].ivls__pgid__c){
    //       console.log("Find critical error:"+transactiondetails_list[j].ivls__pgid__c);
         
    //     }else{
    //       newtargetArray.push(transactiondetails_list[j].ivls__pgid__c);
    //     }
    //   }
    // }
    // var targetArray = [12039 ];//Hao Chen
    // var targetArray = [12039, 32848,30436, 33456, 32483, 33457];
    // var targetArray = [12039, 32848,30436, 33456, 32483, 33457, 28386];
    // var targetArray_SFID = ['0010l00000TFdj5AAD', '0010l00000TeP26AAF','0010l00000TeOG5AAN', '0010l00000RBxUYAA1', '0010l00000TeOtmAAF', '0010l00000RBSOPAA5', '0010l00000TaiHNAAZ'];
    for(var i=0;i<targetArray.length;i++){
      try{
        var targetMemberPGID = targetArray[i];
        console.log("targetMemberPGID:"+targetMemberPGID);
        console.log("position:"+(i+1)+"/"+targetArray.length);
        
        //delete transaction
        querystring = "DELETE FROM loyaltycore.ivls__transaction__c WHERE ivls__memberpgid__c = "+targetMemberPGID;
        (await router.query_one_way(querystring));


        //delete transaction detail
        querystring = "DELETE FROM loyaltycore.ivls__transactiondetails__c WHERE ivls__memberpgid__c = "+targetMemberPGID;
        (await router.query_one_way(querystring));

        //delete redemption detail
        querystring = "DELETE FROM loyaltycore.ivls__redemption__c WHERE ivls__member__c = (SELECT id FROM loyaltycore.account WHERE ivls__pgid__c = "+targetMemberPGID+")";
        (await router.query_one_way(querystring));

        var transactiondatetime = "NOW()";
        //reset points
        var updateMemberSQL = await updatemember.updateMemberInfo(pool, targetMemberPGID,  membershipObj, membershiptiers_list, "");
        console.log("////////////////////////////// COMPLETED //////////////////////////////") 
        console.log("updateMemberSQL:");
        console.log(updateMemberSQL);

        if(updateMemberSQL !== false){
          (await router.query_one_way(updateMemberSQL));
        }
       
        querystring = "UPDATE loyaltycore.account SET ivls__datefirstpurchaseatdtp__pc = NULL WHERE ivls__pgid__c = "+targetMemberPGID;
        var updateMemberSQL =   (await router.query_one_way(querystring));
        console.log("updateMemberSQL:");
        console.log(updateMemberSQL);

        // querystring = "UPDATE loyaltycore.account SET ivls__datetiertime__c = '2018-08-27 18:28:38', ivls__datefirstpurchaseatdtp__pc = NULL WHERE ivls__pgid__c = "+targetMemberPGID;
        // var updateMemberSQL =   (await router.query_one_way(querystring));
        // console.log("updateMemberSQL:");
        // console.log(updateMemberSQL);

        // querystring = "SELECT ivls__pgid__c FROM  loyaltycore.ivls__membershiphistory__c WHERE ivls__member__c = '0010l00000RkpdmAAB' ORDER BY name DESC LIMIT 1;"
        // var ivls__membershiphistory__c_pgid =   (await router.query_one_way(querystring)).rows[0].ivls__pgid__c;

        // querystring = "UPDATE  loyaltycore.ivls__membershiphistory__c  set ivls__datetimefrom__c = '2018-08-27 18:28:38' WHERE ivls__pgid__c = "+ivls__membershiphistory__c_pgid+";";
        // var updateMemberSQL =   (await router.query_one_way(querystring));
        // console.log("updateMemberSQL:");
        // console.log(updateMemberSQL);
      }catch(e){
        console.log("ERROR")
        console.log(e)
      }
      
    }
   
}


main()


pool.on('error', function (err, client) {
  console.error('idle client error', err.message, err.stack)
})


