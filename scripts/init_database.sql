use master;

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
