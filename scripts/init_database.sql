use master;
go;

-- Dro and Recreate the BDataWarehouse database
if exists (select 1 from sys.databases where name = 'BDataWarehouse')
begin 
	alter database BDataWarehouse set single_user with rollback immediate;
	drop database BDataWarehouse;
end;
go;

-- Create the BDataWarehouse database.

create database BDataWarehouse;

use BDataWarehouse;
go;

-- Create the BDataWarehouse schemas.

create schema bronze;
go;
create schema silver;
go;
create schema gold;
go;
