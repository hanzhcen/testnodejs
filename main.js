const pg = require(‘pg’);
const pool = new pg.Pool({
user: ‘poimenixoqxsoc’,
host: ‘ec2-54-204-2-26.compute-1.amazonaws.com’,
database: ‘dbqv4blc5q52fk’,
password: ‘ee3de0243592a9822eeba6529cff124e33e46449bdfc1960efebf02fadc6bbee’,
port: ‘5432’});
pool.query(“CREATE TABLE users(id SERIAL PRIMARY KEY, firstname VARCHAR(40) NOT NULL,
lastName VARCHAR(40) NOT NULL)”, (err, res) => {
console.log(err, res);
pool.end();
});