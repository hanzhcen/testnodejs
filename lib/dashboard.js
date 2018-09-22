const router = require('../lib/router.js')
// const pool = router.getPool();

//define sql elements
var querystring = '';
var querystring_list = [];

//TOP
////        SWITCH      ////
var enableReport1 = false;
var enableReport2 = false;
var enableReport3 = false;
var enableReport4 = false;
var enableReport5 = false;
var enableReport6 = false;
var enableReport7 = true;
var enableReport8 = false;


var allopen = true;
if(allopen){
    var enableReport1 = true;
    var enableReport2 = true;
    var enableReport3 = true;
    var enableReport4 = true;
    var enableReport5 = true;
    var enableReport6 = true;
    var enableReport7 = true;
    var enableReport8 = true;
}


module.exports = {
    updateDashboard:function(pool, processDate,environment){
        return updateDashboard (pool, processDate,environment);
    },
    
};


async function updateDashboard (pool, processDate,environment) {
    try{

        querystring = "SELECT * FROM loyaltycore.ivls__membership__c WHERE ivls__membershipcode__c = 'MySony'";
        var tempList = (await router.query_one_way(querystring)).rows
        if(router.isNotNull(tempList)==false||tempList.length==0){
            console.log("Cannot Find Membershp Where ivls__membershipcode__c = 'MySony'") 
            return;
        }

        querystring = "SELECT * FROM loyaltycore.ivls__statistic__c WHERE name IS NOT NULL";
        var existingStatistic_list = (await router.query_one_way(querystring)).rows

        var membershipObj =  tempList[0];
        var yearstart = membershipObj.ivls__yearstart__c;//0104
     
        var baseDate = membershipObj.ivls__basedate__c;//by receipt date or by transaction date (decide when the points are earned)
        var tierBase = membershipObj.ivls__scheme__c;//upgrade by points earned or by spending
        var roundBase = membershipObj.ivls__round_base__c;//by total or by line
        
        var membershiptiers_list = await router.getMembershiptier();
        var today = new Date('2018/04/02');
        var monthArray = ["01","02","03","04","05","06","07","08","09","10","11","12"];
        if(processDate = "T+1"){
            today.setDate(today.getDate() - 1);
        }
        var currentYear = today.getFullYear();
        var cutoffDate = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));
    
      
        var FYS = '';
        var FYS_lastYear = '';
        var FYS_nextYear = '';
        if(today < cutoffDate){
            //2018-03-23
            FYS_nextYear = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2018-04-01
            FYS = new Date(currentYear-1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2017-04-01
            FYS_lastYear = new Date(currentYear-2, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2016-04-01
        }else{
            //2018-06-23
            FYS_nextYear = new Date(currentYear+1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2019-04-01
            FYS = new Date(currentYear, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2018-04-01
            FYS_lastYear = new Date(currentYear-1, Number(yearstart.substring(2,4))-1, yearstart.substring(0,2));//2017-04-01
    
        }
       
        var SQLFYS_nextYear = router.getSQLDateTimeFormat(FYS_nextYear);//2019/04/01
        var SQLFYS = router.getSQLDateTimeFormat(FYS);//2018/04/01
        var SQLFYS_lastYear = router.getSQLDateTimeFormat(FYS_lastYear);//2017/04/01

        var FYnextYear = FYS_nextYear.getFullYear() -1;//2018
        var FYTthisYear = FYS.getFullYear() -1;//2017
        var FYTnextYear = FYS_lastYear.getFullYear() -1;//2016

        var currentFiscalYear = FYS.getFullYear();
        var currentMonth = today.getMonth()+1;

        console.log("FYS_nextYear:"+FYS_nextYear) 
        console.log("FYS:"+FYS) 
        console.log("FYS_lastYear:"+FYS_lastYear) 


        console.log("SQLFYS_nextYear:"+SQLFYS_nextYear) 
        console.log("SQLFYS:"+SQLFYS) 
        console.log("SQLFYS_lastYear:"+SQLFYS_lastYear) 


        console.log("FYnextYear:"+FYnextYear) 
        console.log("FYTthisYear:"+FYTthisYear) 
        console.log("FYTnextYear:"+FYTnextYear) 

        console.log("today:"+today) 
        console.log("currentFiscalYear:"+currentFiscalYear) 
        console.log("currentMonth:"+currentMonth) 


        var todayIsYearStart = sameDay(cutoffDate, today);
        console.log("todayIsYearStart:"+todayIsYearStart);
        

        if(enableReport1){
            try{
                ////  **  Total Member  **  ////
                querystring = "SELECT Substring('"+SQLFYS+"' from 0 for 5) as year, count(*) as numberofmember, a.ivls__membershiptier__pc as tiersfid, ti.ivls__displayname__c as tiername FROM loyaltycore.account as a JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE a.ivls__datejoin__pc < '"+SQLFYS_nextYear+"' AND a.ivls__membershiptier__pc IS NOT NULL AND ti.ivls__ignorepointsearn__c != true AND ti.ivls__displayname__c IS NOT NULL GROUP BY EXTRACT(YEAR FROM NOW()), a.ivls__membershiptier__pc, ti.ivls__displayname__c;";
                console.log(querystring) 
                var totalmember_list_thisyear = (await router.query_one_way(querystring)).rows;
                console.log("totalmember_list_thisyear:"+totalmember_list_thisyear.length);
                console.log(totalmember_list_thisyear);


                var ThisRecordIsExisted = false;

                var currentYear = FYnextYear;
                var currentName = 'Total Member';

           
                for(var i=0;i<totalmember_list_thisyear.length;i++){
                    var ThisRecordIsExisted = false;
                    var numberofmember = totalmember_list_thisyear[i].numberofmember;
                    var currenTierName = totalmember_list_thisyear[i].tiername;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                    ThisRecordIsExisted = true;
                                }
                            }
                        }
                    }
                    console.log("ThisRecordIsExisted:"+ThisRecordIsExisted) 
                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__members__c =  "+numberofmember+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"'  AND ivls__tier__c = '"+currenTierName+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c, ivls__tier__c, ivls__members__c) VALUES ('"+currentName+"', '"+currentYear+"', '"+currenTierName+"', "+numberofmember+")";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 1 querystring_list:"+querystring_list.length);
           

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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 1 querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 1 querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 1")
                console.log('Error Message:');
                console.log(e);
            }
        }

        if(enableReport2){
            try{
                ////  **  Increase Member  **  ////
                //VERSION 1, WITH TIER:  querystring = "SELECT EXTRACT(YEAR FROM ivls__datejoin__pc) as year,Substring(to_char(ivls__datejoin__pc,'YYYY-MM') from 6 for 2) as month, count(*) as numberofmember, a.ivls__membershiptier__pc as tiersfid, ti.ivls__displayname__c as tiername FROM loyaltycore.account as a JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE ti.ivls__ignorepointsearn__c != true AND EXTRACT(YEAR FROM ivls__datejoin__pc) IS NOT NULL AND EXTRACT(YEAR FROM ivls__datejoin__pc) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW() -interval '1 year'),EXTRACT(YEAR FROM NOW() -interval '2 year')) GROUP BY a.ivls__membershiptier__pc, ti.ivls__displayname__c, EXTRACT(YEAR FROM ivls__datejoin__pc), to_char(ivls__datejoin__pc,'YYYY-MM') ORDER BY EXTRACT(YEAR FROM ivls__datejoin__pc) DESC, to_char(ivls__datejoin__pc,'YYYY-MM') DESC, numberofmember DESC"
                querystring = "SELECT EXTRACT(YEAR FROM ivls__datejoin__pc) as year,Substring(to_char(ivls__datejoin__pc,'YYYY-MM') from 6 for 2) as month, count(*) as numberofmember FROM loyaltycore.account as a JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE ti.ivls__ignorepointsearn__c != true AND EXTRACT(YEAR FROM ivls__datejoin__pc) IS NOT NULL AND EXTRACT(YEAR FROM ivls__datejoin__pc) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW() -interval '1 year'),EXTRACT(YEAR FROM NOW() -interval '2 year')) GROUP BY EXTRACT(YEAR FROM ivls__datejoin__pc), to_char(ivls__datejoin__pc,'YYYY-MM') ORDER BY EXTRACT(YEAR FROM ivls__datejoin__pc) DESC, to_char(ivls__datejoin__pc,'YYYY-MM') DESC, numberofmember DESC;";
                var increaseMember_list = (await router.query_one_way(querystring)).rows;
                console.log("increaseMember_list:"+increaseMember_list.length);
                console.log(increaseMember_list);

                for(var i=0;i<increaseMember_list.length;i++){
                    var currentYear = increaseMember_list[i].year;
                    var currentMonth = increaseMember_list[i].month;
                    // var currenTierName = increaseMember_list[i].tiername;
                    var numberofmember = increaseMember_list[i].numberofmember;
                    var currentName = 'Increase Member';
                    //check whether current Year's record is already created
                    var ThisRecordIsExisted = false;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currentMonth == existingStatistic_list[j].ivls__month__c){
                                    // if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                        ThisRecordIsExisted = true;
                                    // }
                                }
                            }
                        }
                    }

                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__members__c =  "+numberofmember+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"' AND ivls__month__c = '"+currentMonth+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c,ivls__month__c, ivls__members__c) VALUES ('"+currentName+"', '"+currentYear+"','"+currentMonth+"', "+numberofmember+")";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 2 querystring_list:"+querystring_list.length);
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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 2  querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 2  querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
               
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 2")
                console.log('Error Message:');
                console.log(e);
            }
        }
        

        if(enableReport3){
            try{
                ////  **  Earn Point  **  ////
                querystring = "SELECT EXTRACT(YEAR FROM t.ivls__transactiondate__c) as year, Substring(to_char(t.ivls__transactiondate__c,'YYYY-MM') from 6 for 2) as month,  SUM(t.ivls__pointsearned__c) as points, (SELECT ivls__displayname__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid =  t.ivls__tier__pc LIMIT 1) as tiername, t.ivls__pointspool__c as pointspool FROM loyaltycore.ivls__transaction__c as t JOIN loyaltycore.account as a ON a.id = t.ivls__member__c JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE t.ivls__tier__pc IS NOT NULL AND EXTRACT(YEAR FROM t.ivls__transactiondate__c) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW() -interval '1 year'),EXTRACT(YEAR FROM NOW() -interval '2 year')) GROUP BY EXTRACT(YEAR FROM t.ivls__transactiondate__c), Substring(to_char(t.ivls__transactiondate__c,'YYYY-MM') from 6 for 2),  t.ivls__tier__pc, t.ivls__pointspool__c;"
                var earnPoint_list = (await router.query_one_way(querystring)).rows;
                console.log("earnPoint_list:"+earnPoint_list.length);
                console.log(earnPoint_list);

                for(var i=0;i<earnPoint_list.length;i++){
                    var currentYear = earnPoint_list[i].year;
                    var currentMonth = earnPoint_list[i].month;
                    var currenTierName = earnPoint_list[i].tiername;
                    var currentPoints = earnPoint_list[i].points;
                    var pointspool = earnPoint_list[i].pointspool;
                    
                    var currentName = 'Earn Point';
                    var currentType = 'Earn';

                    //check whether current Year's record is already created
                    var ThisRecordIsExisted = false;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currentMonth == existingStatistic_list[j].ivls__month__c){
                                    if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                        if(pointspool == existingStatistic_list[j].ivls__category__c){
                                            ThisRecordIsExisted = true;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__point__c =  "+currentPoints+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"' AND ivls__month__c = '"+currentMonth+"' AND ivls__tier__c = '"+currenTierName+"' AND ivls__type__c = '"+currentType+"' AND ivls__category__c = '"+pointspool+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c,ivls__month__c, ivls__point__c, ivls__tier__c, ivls__type__c, ivls__category__c) VALUES ('"+currentName+"', '"+currentYear+"','"+currentMonth+"', "+currentPoints+", '"+currenTierName+"' , '"+currentType+"', '"+pointspool+"')";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 3 querystring_list:"+querystring_list.length);
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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 3  querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 3  querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
               
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 3")
                console.log('Error Message:');
                console.log(e);
            }
        }


        if(enableReport4){
            try{
                ////  **  Earn Member  **  ////
                querystring = "SELECT Substring('"+SQLFYS+"' from 0 for 5) as year, count(a.id) as numberofmember, (SELECT ivls__displayname__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid = a.ivls__membershiptier__pc LIMIT 1) as tiername FROM loyaltycore.account as a WHERE (SELECT count(t.id) FROM loyaltycore.ivls__transaction__c as t WHERE t.ivls__transactiondate__c::date >= '"+SQLFYS+"' AND t.ivls__member__c = a.id AND t.ivls__tier__pc IS NOT NULL ) >0 GROUP BY a.ivls__membershiptier__pc;"
                console.log(querystring) 
                var earnmember_list_thisyear = (await router.query_one_way(querystring)).rows;
                console.log("earnmember_list_thisyear:"+earnmember_list_thisyear.length);
                console.log(earnmember_list_thisyear);


                var ThisRecordIsExisted = false;

                var currentYear = FYnextYear;
                var currentName = 'Earn Member';
                var currentType = "Earn";
           
                for(var i=0;i<earnmember_list_thisyear.length;i++){
                    var ThisRecordIsExisted = false;
                    var numberofmember = earnmember_list_thisyear[i].numberofmember;
                    var currenTierName = earnmember_list_thisyear[i].tiername;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                    ThisRecordIsExisted = true;
                                }
                            }
                        }
                    }
                    console.log("ThisRecordIsExisted:"+ThisRecordIsExisted) 
                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__members__c =  "+numberofmember+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"'  AND ivls__tier__c = '"+currenTierName+"' AND ivls__type__c = '"+currentType+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c, ivls__tier__c, ivls__members__c, ivls__type__c) VALUES ('"+currentName+"', '"+currentYear+"', '"+currenTierName+"', "+numberofmember+", '"+currentType+"')";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 4 querystring_list:"+querystring_list.length);
           

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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 4 querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 4 querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 4")
                console.log('Error Message:');
                console.log(e);
            }
        }

        if(enableReport5){
            try{
                ////  **  Redeem Point and Time  **  ////
                querystring = "SELECT count(r.id) as count, (SELECT ivls__rewardtype__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1) as rewardtype, EXTRACT(YEAR FROM r.ivls__dateredeem__c) as year, Substring(to_char(r.ivls__dateredeem__c,'YYYY-MM') from 6 for 2) as month,  SUM(r.ivls__redeempoint__c) as points, (SELECT ivls__displayname__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid =  r.ivls__currentmembershiptier__c LIMIT 1) as tiername FROM loyaltycore.ivls__redemption__c as r JOIN loyaltycore.account as a ON a.id = r.ivls__member__c JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE r.ivls__currentmembershiptier__c  IS NOT NULL AND EXTRACT(YEAR FROM r.ivls__dateredeem__c) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW() -interval '1 year'),EXTRACT(YEAR FROM NOW() -interval '2 year')) GROUP BY EXTRACT(YEAR FROM r.ivls__dateredeem__c), Substring(to_char(r.ivls__dateredeem__c,'YYYY-MM') from 6 for 2),  r.ivls__currentmembershiptier__c, (SELECT ivls__rewardtype__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1);"
                var statis_List = (await router.query_one_way(querystring)).rows;
                console.log("statis_List:"+statis_List.length);
                console.log(statis_List);

                for(var i=0;i<statis_List.length;i++){
                    var currentYear = statis_List[i].year;
                    var currentMonth = statis_List[i].month;
                    var currenTierName = statis_List[i].tiername;
                    var currentPoints = statis_List[i].points;
                    var currentCount = statis_List[i].count;
                    var rewardtype = statis_List[i].rewardtype;
                    var currentName = 'Redeem Point and Time';
                    var currentType = 'Redeem';

                    //check whether current Year's record is already created
                    var ThisRecordIsExisted = false;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currentMonth == existingStatistic_list[j].ivls__month__c){
                                    if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                        if(rewardtype == existingStatistic_list[j].ivls__rewardtype__c){
                                            ThisRecordIsExisted = true;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__point__c =  "+currentPoints+", ivls__timesredeemed__c = "+currentCount+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"' AND ivls__month__c = '"+currentMonth+"' AND ivls__tier__c = '"+currenTierName+"' AND ivls__type__c = '"+currentType+"' AND ivls__rewardtype__c = '"+rewardtype+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c,ivls__month__c, ivls__point__c, ivls__tier__c, ivls__type__c, ivls__timesredeemed__c, ivls__rewardtype__c) VALUES ('"+currentName+"', '"+currentYear+"','"+currentMonth+"', "+currentPoints+", '"+currenTierName+"' , '"+currentType+"', "+currentCount+", '"+rewardtype+"')";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 5 querystring_list:"+querystring_list.length);
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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 5  querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 5  querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
               
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 5")
                console.log('Error Message:');
                console.log(e);
            }
        }


        if(enableReport6){
            try{
                ////  **  Redeem Member  **  ////
                querystring = "SELECT Substring('"+SQLFYS+"' from 0 for 5) as year, count(a.id) as numberofmember, (SELECT ivls__displayname__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid = a.ivls__membershiptier__pc LIMIT 1) as tiername FROM loyaltycore.account as a JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE (SELECT count(r.id) FROM loyaltycore.ivls__redemption__c as r WHERE ti.ivls__ignorepointsearn__c != true AND r.ivls__dateredeem__c::date >= '"+SQLFYS+"' AND r.ivls__member__c = a.id AND r.ivls__currentmembershiptier__c IS NOT NULL ) >0 GROUP BY a.ivls__membershiptier__pc;"
                console.log(querystring) 
                var redeemmember_list_thisyear = (await router.query_one_way(querystring)).rows;
                console.log("redeemmember_list_thisyear:"+redeemmember_list_thisyear.length);
                console.log(redeemmember_list_thisyear);


                var ThisRecordIsExisted = false;

                var currentYear = FYnextYear;
                var currentName = 'Redeem Member';
                var currentType = "Redeem";
           
                for(var i=0;i<redeemmember_list_thisyear.length;i++){
                    var ThisRecordIsExisted = false;
                    var numberofmember = redeemmember_list_thisyear[i].numberofmember;
                    var currenTierName = redeemmember_list_thisyear[i].tiername;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                    ThisRecordIsExisted = true;
                                }
                            }
                        }
                    }
                    console.log("ThisRecordIsExisted:"+ThisRecordIsExisted) 
                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__members__c =  "+numberofmember+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"'  AND ivls__tier__c = '"+currenTierName+"' AND ivls__type__c = '"+currentType+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c, ivls__tier__c, ivls__members__c, ivls__type__c) VALUES ('"+currentName+"', '"+currentYear+"', '"+currenTierName+"', "+numberofmember+", '"+currentType+"')";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 6 querystring_list:"+querystring_list.length);
           

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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 6 querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 6 querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 6")
                console.log('Error Message:');
                console.log(e);
            }
        }
        
        
        if(enableReport7){
            try{
                ////  **  Redeem Item  **  ////
                querystring = "SELECT count(r.id) as count, (SELECT ivls__facevalue__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1) as facevalue, (SELECT ivls__rewarddescriptione__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1) as rewardname, (SELECT ivls__rewardtype__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1) as rewardtype, EXTRACT(YEAR FROM r.ivls__dateredeem__c) as year,  SUM(r.ivls__redeempoint__c) as points FROM loyaltycore.ivls__redemption__c as r JOIN loyaltycore.account as a ON a.id = r.ivls__member__c JOIN loyaltycore.ivls__membershiptier__c as ti ON a.ivls__membershiptier__pc = ti.sfid WHERE r.ivls__dateredeem__c >= '2018/04/01' AND r.ivls__currentmembershiptier__c  IS NOT NULL   GROUP BY EXTRACT(YEAR FROM r.ivls__dateredeem__c),  (SELECT ivls__rewarddescriptione__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1) , (SELECT ivls__rewardtype__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1), (SELECT ivls__facevalue__c FROM loyaltycore.ivls__reward__c WHERE sfid = r.ivls__reward__c LIMIT 1) ORDER BY count(r.id) DESC,  SUM(r.ivls__redeempoint__c) DESC LIMIT 20;"
                var statis_List = (await router.query_one_way(querystring)).rows;
                console.log("statis_List:"+statis_List.length);
                console.log(statis_List);

                for(var i=0;i<statis_List.length;i++){
                    if(i==20){
                        break;
                    }
                    var currentYear = statis_List[i].year;
                    var currentPoints = statis_List[i].points;
                    var currentCount = statis_List[i].count;
                    var rewardtype = statis_List[i].rewardtype;
                    var rewardname = statis_List[i].rewardname;
                    var facevalue = statis_List[i].facevalue;
                    var sequence = (i+1).toString();
                    var currentName = 'Redeem Item';
                    var currentType = 'Redeem';

                    //check whether current Year's record is already created
                    var ThisRecordIsExisted = false;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(sequence == existingStatistic_list[j].ivls__sequence__c){
                                    ThisRecordIsExisted = true;
                                }
                            }
                        }
                    }

                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET ivls__facevalue__c = "+facevalue+", ivls__item__c = '"+rewardname+"', ivls__rewardtype__c = '"+rewardtype+"',  ivls__point__c =  "+currentPoints+", ivls__timesredeemed__c = "+currentCount+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"'  AND ivls__sequence__c = '"+sequence+"' ";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c, ivls__point__c, ivls__type__c, ivls__timesredeemed__c,ivls__item__c,  ivls__rewardtype__c, ivls__sequence__c, ivls__facevalue__c) VALUES ('"+currentName+"', '"+currentYear+"',"+currentPoints+", '"+currentType+"', "+currentCount+" , '"+rewardname+"', '"+rewardtype+"', '"+sequence+"', "+facevalue+")";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 7 querystring_list:"+querystring_list.length);
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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 7  querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 7  querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
               
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 5")
                console.log('Error Message:');
                console.log(e);
            }
        }


        if(enableReport8){
            try{
                ////  **  Sales by Tier by Year Month  **  ////ivls__forrefunduse__c
                querystring = "SELECT EXTRACT(YEAR FROM ivls__transactiondate__c) as year,Substring(to_char(ivls__transactiondate__c,'YYYY-MM') from 6 for 2) as month, SUM(ivls__spendingexclusive__c) as points, (SELECT name FROM loyaltycore.ivls__shop__c WHERE sfid = t.ivls__shop__c LIMIT 1) as shopname, (SELECT ivls__displayname__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid = t.ivls__tier__pc LIMIT 1) as tiername FROM loyaltycore.ivls__transaction__c as t WHERE (SELECT ivls__ignorepointsearn__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid = t.ivls__tier__pc LIMIT 1) != true AND (SELECT name FROM loyaltycore.ivls__shop__c WHERE sfid = t.ivls__shop__c LIMIT 1) IS NOT NULL AND EXTRACT(YEAR FROM t.ivls__transactiondate__c) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW() -interval '1 year'),EXTRACT(YEAR FROM NOW() -interval '2 year')) GROUP BY EXTRACT(YEAR FROM t.ivls__transactiondate__c), Substring(to_char(ivls__transactiondate__c,'YYYY-MM') from 6 for 2), t.ivls__tier__pc, ivls__shop__c"
                var statis_List = (await router.query_one_way(querystring)).rows;
                console.log("statis_List:"+statis_List.length);
                console.log(statis_List);

                for(var i=0;i<statis_List.length;i++){
                   
                    var currentYear = statis_List[i].year;
                    var currentMonth = statis_List[i].month;
                    var currenTierName = statis_List[i].tiername;
                    var currenShop = statis_List[i].shopname;
                    var currentPoints = statis_List[i].points;
                    var currentName = 'Sales by Tier by Year Month';
                 

                    //check whether current Year's record is already created
                    var ThisRecordIsExisted = false;
                    for(var j=0;j<existingStatistic_list.length;j++){
                        if(existingStatistic_list[j].name == currentName){
                            if(currentYear == existingStatistic_list[j].ivls__year__c){
                                if(currentMonth == existingStatistic_list[j].ivls__month__c){
                                    if(currenTierName == existingStatistic_list[j].ivls__tier__c){
                                        if(currenShop == existingStatistic_list[j].ivls__shop__c){
                                            ThisRecordIsExisted = true;
                                        }
                                    }
                                }
                                
                            }
                        }
                    }

                    if(ThisRecordIsExisted == true){
                        //UPDATE
                        querystring = "UPDATE loyaltycore.ivls__statistic__c SET  ivls__netinvoiceamount__c =  "+currentPoints+" WHERE name = '"+currentName+"'  AND ivls__year__c = '"+currentYear+"'  AND ivls__month__c = '"+currentMonth+"' AND ivls__tier__c = '"+currenTierName+"' AND ivls__shop__c = '"+currenShop+"' AND ivls__tier__c='"+currenTierName+"'";
                        querystring_list.push(querystring)
                    }else{
                        //INSERT
                        querystring = "INSERT INTO loyaltycore.ivls__statistic__c (name, ivls__year__c, ivls__month__c, ivls__shop__c, ivls__tier__c, ivls__netinvoiceamount__c) VALUES ('"+currentName+"', '"+currentYear+"','"+currentMonth+"', '"+currenShop+"', '"+currenTierName+"', "+currentPoints+")";
                        querystring_list.push(querystring)
                    }
                }
                console.log(querystring_list);
                console.log("table 8 querystring_list:"+querystring_list.length);
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
                            var result = await router.query_one_way(totalQueryStr);
                            console.log('Process table 8  querystring_list successful !!!')
                            console.log(result)
                        }catch (err) {
                            console.log('Process table 8  querystring_list fail !!!')
                            console.log(err)
                        }
                    }
                    totalQueryStr = "";
                }
               
            }catch(e){
                console.log("//////////////////////////////  WARNING  //////////////////////////////") 
                console.log("current position: report 5")
                console.log('Error Message:');
                console.log(e);
            }
        }


        querystring = "SELECT EXTRACT(YEAR FROM ivls__transactiondate__c) as year,Substring(to_char(ivls__transactiondate__c,'YYYY-MM') from 6 for 2) as month, SUM(ivls__spendingexclusive__c), (SELECT name FROM loyaltycore.ivls__shop__c WHERE sfid = t.ivls__shop__c LIMIT 1) as shopname, (SELECT ivls__displayname__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid = t.ivls__tier__pc LIMIT 1) as tiername FROM loyaltycore.ivls__transaction__c as t WHERE (SELECT ivls__ignorepointsearn__c FROM loyaltycore.ivls__membershiptier__c WHERE sfid = t.ivls__tier__pc LIMIT 1) != true AND (SELECT name FROM loyaltycore.ivls__shop__c WHERE sfid = t.ivls__shop__c LIMIT 1) IS NOT NULL AND EXTRACT(YEAR FROM t.ivls__transactiondate__c) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW() -interval '1 year'),EXTRACT(YEAR FROM NOW() -interval '2 year')) GROUP BY EXTRACT(YEAR FROM t.ivls__transactiondate__c), Substring(to_char(ivls__transactiondate__c,'YYYY-MM') from 6 for 2), t.ivls__tier__pc, ivls__shop__c;";


    

        

        console.log("////////////////////////////// COMPLETED //////////////////////////////") 
    }catch (err) {
        console.log('Overall error ' + err)
    }
}


// updateDashboard()


// pool.on('error', function (err, client) {
//   console.error('idle client error', err.message, err.stack)
// })
function sameDay(d1, d2) {
    return (d1.getFullYear() === d2.getFullYear() &&
      d1.getMonth() === d2.getMonth() &&
      d1.getDate() === d2.getDate());
}
