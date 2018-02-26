const csvgenerate: any = require('csv-write-stream');

import * as commandLineArgs from 'command-line-args';
import * as fs from 'fs';
import * as sqlite3 from "sqlite3";
import * as async from "async";
import * as path from "path";
import * as csvparser from "csv-parse";
const db = new sqlite3.Database(':memory:');

const optionsDefinitions:commandLineArgs.OptionDefinition[]= [
    {name:'verbose',alias:'v',type:Boolean},
    {name:'csv',alias:'c',type:String,multiple:true},
    {name:'queries',alias:'q',type:String,multiple:true},
    {name:'outfiles',alias:'o',type:String,multiple:true},
    {name:'csvdelimiter',alias:'d',defaultValue:";",type:String},
    {name:'outputdelimiter',alias:'D',defaultValue:';',type:String},
    {name:'outputnewline',alias:'N',defaultValue:'\n',type:String},
    {name:'outputheader',alias:'H',defaultValue:true,type:String},
    {name:'csvencoding',alias:'e',defaultValue:"latin1",type:String},
    {name:'queryencoding',defaultValue:"latin1",type:String}
];
const options = commandLineArgs.default(optionsDefinitions);
if(!options.csv || options.csv.length == 0){
    console.error("No csv files supplied, exiting");
    process.exit(1);
}
if(!options.queries || options.queries.length == 0){
    console.error("No queries supplied, exiting");
    process.exit(1);
}

if(!options.outfiles || options.outfiles.length != options.queries.length){
    console.error("Number of outfiles need to match number of queries");
}

loadCSVFiles(options.csv,function(err){
    if(err){
        console.error(err);
        process.exit(1);
    }
    log("Succesfully loaded all csv files");
    executeQueries(options.queries,function(err){

    });

});

function log(p:string|any){
    if(options.verbose){
        console.log(p);
    }
}

function loadCSVFiles(paths:string[],callback:async.ErrorCallback<Error>){
    async.each(paths,function(csvpath:string,callback:async.ErrorCallback<Error>){
        log("Checking if file exists:" +csvpath);
        if(!fs.existsSync(csvpath) ||fs.lstatSync(csvpath).isDirectory()){
            console.error("Could not find " + csvpath +" , exiting");
            process.exit(1);
        }
        
        const tablename = path.basename(csvpath,".csv");
        log("Success checking "  + tablename +"!");
        const abspath = path.resolve(csvpath);
        const csvContent:string = fs.readFileSync(abspath).toString(options.csvencoding);
        csvparser.default(csvContent,{delimiter:options.csvdelimiter},function(err,parsedCSV:string[][]){
            if(err){
                return callback(err);
            }
            
            log(parsedCSV[0]);
            const columnNames = parsedCSV[0];
            const columnDefinitions =columnNames.map(column => {return column + " TEXT";}).reduce((a,b)=>{return a+ ", " +b;});
            
            const tableDefinition = "CREATE TABLE " + tablename + " (" + columnDefinitions + ")";
            log(tableDefinition);
            db.run(tableDefinition,(err)=>{
                if(err){
                    console.error(err);
                    return callback(err);
                }
                async.eachOf(parsedCSV,(row:string[],index,callback:async.ErrorCallback<Error>)=>{
                    //do not enter first row as data row, as this is the header
                    if(index == 0)
                        return callback();
                    const queryString = "INSERT INTO " + tablename + " VALUES (" + row.map(_=>{return "?"}).reduce((a,b)=>{return a+ ", " +b;}) + ")";
                    log(queryString);
                    db.run(queryString,row,(err:Error|undefined)=>{
                        callback(err);
                    })
                },err=>{
                    return callback(err);
                });

                
            });        

        });
        
        

    },(err)=>{
        callback(err);
    });

  
}

function executeQueries(paths:string[],callback:async.ErrorCallback<Error>){
    async.eachOf(paths,(querypath:string,queryindex,callback:async.ErrorCallback<Error>)=>{
        if(!fs.lstatSync(querypath).isFile()){
            console.error("Could not find " + querypath+ " ,exiting");
            process.exit(1);
        }
        log("Found " + querypath);
        const query = fs.readFileSync(querypath,{encoding:null,"flag":"r"}).toString(options.queryencoding);
        log(query);
        db.all(query,(err,rows)=>{
            log(rows);
            if(rows.length == 0){
                log("Empty query result");
            }else{
               
                const outpath = options.outfiles[queryindex];
                const header = Object.keys(rows[0]);

                log(header);
                const writer = csvgenerate({
                    separator:options.outputdelimiter,
                    newline:options.outputnewline,
                    header:header,
                    sendHeaders:options.outputheader
                });
                writer.pipe(fs.createWriteStream(outpath));
                rows.forEach((row)=>{writer.write(row)});
                writer.end();


            }
            
        })
    });
}