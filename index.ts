#!/usr/bin/env node
const csvgenerate: any = require('csv-write-stream');
const cmdusage:any = require('command-line-usage');
import * as commandLineArgs from 'command-line-args';
import * as fs from 'fs';
import * as sqlite3 from "sqlite3";
import * as async from "async";
import * as path from "path";
import * as csvparser from "csv-parse";
const db = new sqlite3.Database(':memory:');

const optionsDefinitions:commandLineArgs.OptionDefinition[]= [
    {name:'verbose',alias:'v',type:Boolean,description:"Verbose Logging"},
    {name:'csv',alias:'c',type:String,multiple:true,description:"CSV files to load"},
    {name:'queries',alias:'q',type:String,multiple:true,description:"Queries to run on the data"},
    {name:'outfiles',alias:'o',type:String,multiple:true,description:"Paths were the data should be written to. One per query"},
    {name:'csvdelimiter',alias:'d',defaultValue:";",type:String,description:"Delimiter of the csv files"},
    {name:'outputdelimiter',alias:'D',defaultValue:';',type:String,description:"Delimiter of the output csv files"},
    {name:'outputnewline',alias:'N',defaultValue:'\r\n',type:String,description:"Newline of the output csv files"},
    {name:'noheader',alias:'H',defaultValue:false,type:Boolean,description:"Omit writing the header to the csv files"},
    {name:'csvencoding',alias:'e',defaultValue:"utf-8",type:String,description:"Encoding of the loaded csv files"},
    {name:'outputencoding',alias:'E',defaultValue:"utf-8",type:String,description:"Encoding of the output csv files"},
    
    {name:'queryencoding',defaultValue:"utf-8",type:String,description:"Encoding of the queries"},
    {name:'help',alias:'h',defaultValue:false,type:Boolean,description:"Show this help text"}
];

const options = commandLineArgs.default(optionsDefinitions);

if(options.help){
    const sections = [
        {
        header:"queryoncsv",
        content: "A small command line utility to query csv files using sqlite3"
        },{
            header:"Options",
            optionList:optionsDefinitions
        }
    ];
    console.log(cmdusage(sections));
    process.exit(0);
}

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
    process.exit(1);
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
            const columnUsages :{[key:string]:number} ={};
            const columnDefinitions =columnNames.map(function(column){
                
                let lastUsage:number = 0;
                let columnStr:String = column;
                if(column in columnUsages){
                    lastUsage = columnUsages[column]
                    columnStr = column + "-duplicate-" + lastUsage;
                }
                columnUsages[column] = lastUsage + 1;
                return "[" + columnStr + "]" + " TEXT";
            }).reduce((a,b)=>{return a+ ", " +b;});
            
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
                    log(row);
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
            if(err){
                console.error(err);
                return;
            }

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
                    sendHeaders:!options.noheader
                });
                writer.pipe(fs.createWriteStream(outpath,{encoding:options.outputencoding}));
                rows.forEach((row)=>{writer.write(row)});
                writer.end();


            }
            
        })
    });
}