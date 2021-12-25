-- Drop Schema if Already Present
drop schema if exists `i191708_DWH` ;

-- Creating Schema
CREATE SCHEMA `i191708_DWH` ;
use `i191708_DWH`;

-- Drop Tables if Already Present  {If Want to Use}
-- drop table if exists RECORD_DAY;
-- drop table if exists SUPPLIER;
-- drop table if exists PRODUCT;
-- drop table if exists STORE;
-- drop table if exists SALES;

-- Creating Tables
CREATE TABLE `STORE` (
  `STORE_ID` VARCHAR(5) NOT NULL, 
  `STORE_NAME` VARCHAR(30) NOT NULL,
  PRIMARY KEY (STORE_ID)
);

CREATE TABLE `SUPPLIER` (
  `SUPPLIER_ID` VARCHAR(5) NOT NULL, 
  `SUPPLIER_NAME` VARCHAR(30) NOT NULL,
  PRIMARY KEY (SUPPLIER_ID)
);

CREATE TABLE `PRODUCT` (
  `PRODUCT_ID` VARCHAR(6) NOT NULL, 
  `PRODUCT_NAME` VARCHAR(30) NOT NULL,
  PRIMARY KEY (PRODUCT_ID)
);

CREATE TABLE `RECORD` (
  `DATE_ID` Date NOT NULL, 
  `DAY_NAME` VARCHAR(10) NOT NULL,
  `MONTH_NAME` VARCHAR(10) NOT NULL,
  `QUARTER_NO` int NOT NULL,
  `YEAR` int NOT NULL,
  PRIMARY KEY (DATE_ID)
);

CREATE TABLE `SALES` (
  `STORE_ID` VARCHAR(4) NOT NULL, 
  `SUPPLIER_ID` VARCHAR(5) NOT NULL, 
  `PRODUCT_ID` VARCHAR(6) NOT NULL,
  `DATE_ID` Date NOT NULL,
  `TOTAL_QUANTITY` float NOT NULL,
  `TOTAL_SALES` float NOT NULL,
  FOREIGN KEY (STORE_ID) REFERENCES STORE(STORE_ID),
  FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID),
  FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID),
  FOREIGN KEY (DATE_ID) REFERENCES RECORD(DATE_ID),
  PRIMARY KEY (`STORE_ID`, `SUPPLIER_ID`, `PRODUCT_ID`, `DATE_ID`)
);

commit;