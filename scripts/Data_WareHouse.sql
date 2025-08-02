/*
Date: 2025-07-28
Author: Najam Rizvi
Description: Creating Database DataWarehouse
*/

USE master; -- Defaulting to Master Database

/* Creating Database + Schemas (Bronze, Silver, Gold) */

Create Database Datawarehouse; 
Use Datawarehouse;

Create Schema Bronze;
go -- Separte batches when working with multiple sql statements
Create Schema Gold;
go 
Create Schema Silver;
