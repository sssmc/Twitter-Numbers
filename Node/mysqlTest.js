var mysql = require('mysql');

var con = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "Climate123",
    database: "test"
});

con.connect(function(err) {
    if (err) throw err;
    con.query("SELECT * FROM tn_10_metadata", function(err,result,fields){
        if(err) throw err{

            console.log(result + fields);
        };
    });
});
