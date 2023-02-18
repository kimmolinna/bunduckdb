import { open } from '@evan/duckdb';
import pl from 'nodejs-polars';

//const db = open('./example.db');
const db = open(':memory:');

const connection = db.connect();
const q1 = connection.query('PRAGMA version');
console.write(JSON.stringify(q1)+'\n');
const q2 = connection.query(`
  SET s3_region='eu-north-1';
  SET s3_access_key_id='AKIAST5RBRTIXT47I6O7';
  SET s3_secret_access_key='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'
`);
const q3 = connection.query(`
  CREATE TABLE data AS 
  SELECT
    timestamp,
    date_part('year',timestamp)::INT as year,
    date_part('month',timestamp)::INT as month,
    date_part('hour',timestamp)::INT as hour,
    timestamp::date as day,
    consumption
  FROM 
    parquet_scan('s3://linna/fingrid/home_*.parquet');
`);
const q4 = connection.query(`
  SELECT 
    year,
    month,
    round(sum(consumption),2) as consumption
  FROM data
  GROUP BY year,month
`);
const q4df = pl.DataFrame(q4);
console.write(JSON.stringify(q4)+'\n');

// for (const row of connection.stream(`
//   SELECT 
//   year,
//   month,
//   sum(consumption) as consumption
//   FROM data
//   GROUP BY year,month
// `)){
//   console.write(JSON.stringify(row)+'\n')
// }

connection.close();
db.close();