
var request=require('request');
var fs = require('fs');
var filecontent;
var filename;
//gene header for chinese version
request.get({url:'http://www.sony.com.hk/shared/header?includeFontType=true&includeJs=true&includeCss=true&type=responsive&locale=zh_HK'}, function optionalCallback(err, httpResponse, body) {
            if (err) {
                res.send(JSON.stringify({ success: false, message: err }));
            }
            else{
                // var resultDict = JSON.parse(body);
                //console.log("@@@resultDict1: "+body);
                filecontent=body;
                //newString = oldString.replace(/\s+/g,"");
                 try{
                       filename="../views/partials/header_zh.ejs";
                        Gen_file(filename);
                    }
                    catch(error){
                    console.error(error);
                    } 
            }
        });
//gene header for english version
request.get({url:'http://www.sony.com.hk/shared/header?includeFontType=true&includeJs=true&includeCss=true&type=responsive&locale=en_HK'}, function optionalCallback(err, httpResponse, body) {
            if (err) {
                res.send(JSON.stringify({ success: false, message: err }));
            }
            else{
                // var resultDict = JSON.parse(body);
                //console.log("@@@resultDict1: "+body);
                filecontent=body;
                //newString = oldString.replace(/\s+/g,"");
                 try{
                       filename="../views/partials/header_en.ejs";
                        Gen_file(filename);
                    }
                    catch(error){
                    console.error(error);
                    } 
            }
        });

//gene footer for chinese version
request.get({url:'http://www.sony.com.hk/shared/footer?includeFontType=false&includeJs=true&includeCss=false&type=responsive&locale=zh_HK'}, function optionalCallback(err, httpResponse, body) {
            if (err) {
                res.send(JSON.stringify({ success: false, message: err }));
            }
            else{
                // var resultDict = JSON.parse(body);
                //console.log("@@@resultDict1: "+body);
                //filecontent=body.replace(/\s+/g,"");
                filecontent=body;
                 try{
                       filename="../views/partials/footer_zh.ejs";
                        Gen_file(filename);
                    }
                    catch(error){
                    console.error(error);
                    } 
            }
        });
//gene footer for english version
request.get({url:'http://www.sony.com.hk/shared/footer?includeFontType=false&includeJs=true&includeCss=false&type=responsive&locale=en_HK'}, function optionalCallback(err, httpResponse, body) {
            if (err) {
                res.send(JSON.stringify({ success: false, message: err }));
            }
            else{
                // var resultDict = JSON.parse(body);
                //console.log("@@@resultDict1: "+body);
                filecontent=body;
                //newString = oldString.replace(' , ',",");
                 try{
                       filename="../views/partials/footer_en.ejs";
				        Gen_file(filename);
			        }
			        catch(error){
			        console.error(error);
			        } 
            }
        });



function Gen_file() {
    fs.writeFile(filename, filecontent, function(err) {
    if(err) {
        return console.log(err);
    }

    console.log("The file was saved!");
}); 
}