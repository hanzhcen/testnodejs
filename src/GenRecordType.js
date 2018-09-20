var async = require("async");
var fs = require('fs');
var recordtype;
const db = require('../lib/db.js');
var schedule = require('node-schedule');
//var dateFormat = require('dateformat');
schejob1();


function schejob1(){
    schedule.scheduleJob('*/5 * * * * *', function(){
      //var day=dateFormat(new Date(), "yyyy-mm-dd h:MM:ss");
      //console.log(day);
      try{
          fs.exists("./recordtype.json", function(exists) {
              console.log(exists ? "存在文件" : "没有文件");
              if (exists)
              {   recordtype = require("../recordtype.json");
                  Append_recordtype();}
               else
               {Gen_recordtype();}
            });
        }
        catch(error){
        console.error(error);
        }
    });
}



function Gen_recordtype() {
    db.getRecordType((err, r) => {
    	console.log('creating New file!');
    	var json='';
        for (var i = 0; i < r.rows.length; i++) {
            var rec = r.rows[i];
            var keystr=rec.sobjecttype+rec.developername;
            //console.log(keystr);
            var keyvalue='{"sobjecttype": "'+rec.sobjecttype+'","developername":"'+rec.developername+'","sfid":"'+rec.sfid+'"}'
            var objstr='"'+keystr+'":'+keyvalue;
            if (json)
            {
            	json=json+',\n\t'+objstr;
            }
            else
            {
            	json=objstr;
            }
            
        }
         //JSON.parse
         json='{\n\t'+json+'\n}';
              	  fs.writeFile("./recordtype.json",json,function(err){
				    if(err) throw err;
				  })
			
    });
}
function Append_recordtype() {
     db.getRecordType((err, r) => {
    	console.log('Appending new record!');
    	var json='';
    	var firstrow=0;
    	for (var i in r.rows)
    	{
    		var rec = r.rows[i];
    		
    		var keystr=rec.sobjecttype+rec.developername;
            var keyvalue='{"sobjecttype": "'+rec.sobjecttype+'","developername":"'+rec.developername+'","sfid":"'+rec.sfid+'"}'
            var objstr=',\n\t"'+keystr+'":'+keyvalue;
            var havenew=false;
            json=objstr
            var seekRecordtype = recordtype[keystr];
            if (!seekRecordtype)
            {
	                //console
	                fs.appendFileSync('./recordtype.json', json, function (err) {
					  if (err) throw err;
					  //console.log('Saved!');
					});
               havenew=true;
            }
    	}
        	//console.log('ddd');
            if (havenew)
            {
	        fs.appendFileSync('./recordtype.json', '\n}', function (err) {
					  if (err) throw err;
					  //console.log('Saved!');
					}); 
        }
		    fs.readFile(".//recordtype.json", 'utf8', function (err,data) {
					  if (err) {
					    return console.log(err);
					  }
					  //console.log('will be replace with ');
					  var result = data.replace(/\n},/g, ',');

					  fs.writeFileSync("./recordtype.json", result, 'utf8', function (err) {
					     if (err) return console.log(err);
					  });
					});  
        
    });

}
function statPath(filepath){
  let flag = true;
  try{
    fs.accessSync(filepath, fs.F_OK);
  }catch(e){
    flag = false;
  }
  return flag;
}
