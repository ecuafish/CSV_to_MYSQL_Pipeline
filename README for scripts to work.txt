This script was built on python version 2.7
In order for the script to run correctly you will need to install the following packages using pip:

pip install mysql-connector-python
pip install mysql-python
pip install sqlalchemy
pip install pandas
pip install numpy

You must also have the mysql connector/python (2.7) 8.0.15. You can install this by using Mysql Installer. 

Finally this script was run where the tables were empty. Either truncate the tables in the target database or create the tables using the following sql scripts in the target database: 

CREATE TABLE contact (
id INT(11) NOT NULL AUTO_INCREMENT,
title ENUM('Mr', 'Mrs', 'Miss', 'Ms', 'Dr'),
first_name VARCHAR(64),
last_name VARCHAR(64),
company_name VARCHAR(64),
date_of_birth DATETIME,
notes VARCHAR(255),
PRIMARY KEY(id)
);

CREATE TABLE address (
id INT(11) NOT NULL AUTO_INCREMENT,
contact_id INT(11) NOT NULL,
street1 VARCHAR(100),
street2 VARCHAR(100),
suburb VARCHAR(64),
city VARCHAR(64),
post_code VARCHAR(16),
PRIMARY KEY(id)
);

CREATE TABLE phone (
id INT(11) NOT NULL AUTO_INCREMENT,
contact_id INT(11) NOT NULL,
name VARCHAR(64),
content VARCHAR(64),
type ENUM('Home', 'Work', 'Mobile', 'Other'),
PRIMARY KEY(id)
);

