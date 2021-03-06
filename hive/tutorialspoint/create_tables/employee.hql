use tutorialspoint;

CREATE TABLE IF NOT EXISTS employee (
eid int, name String, salary String, destination String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'employee.txt'
OVERWRITE INTO TABLE employee;

describe employee;
select * from employee;
