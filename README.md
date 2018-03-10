# queryoncsv
A small tool to run sqlite3 queries on csv files

## Installation
```
npm install -g queryoncsv
```

## Usage
```
queryoncsv -c file.csv -q query.sql -o out.csv
```

```
Options

  -v, --verbose                  Verbose Logging
  -c, --csv string[]             CSV files to load
  -q, --queries string[]         Queries to run on the data
  -o, --outfiles string[]        Paths were the data should be written to. One per query
  -d, --csvdelimiter string      Delimiter of the csv files
  -D, --outputdelimiter string   Delimiter of the output csv files
  -N, --outputnewline string     Newline of the output csv files
  -H, --noheader                 Omit writing the header to the csv files
  -e, --csvencoding string       Encoding of the loaded csv files
  --queryencoding string         Encoding of the queries
  -h, --help                     Show this help text

```

All tables are named with their respective csv name e.g. test.csv -> test