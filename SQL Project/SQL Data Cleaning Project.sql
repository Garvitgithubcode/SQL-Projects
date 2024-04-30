-- SQL Project - Data Cleaning

use cleaning_dataset;

select * from layoffs;

-- Now when we are data cleaning we usually follow a few steps
-- 1. Remove Duplicate
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Colums Rows

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

CREATE TABLE layoffs_copy
LIKE layoffs;

-- 1. Remove Duplicates

select * from layoffs_copy;

INSERT layoffs_copy
SELECT * FROM layoffs;

SELECT *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off,
'date') as row_num
from layoffs_copy;

with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company,location,
industry, total_laid_off, percentage_laid_off,
'date', stage, country, funds_raised_millions) as row_num
from layoffs_copy
)
select * from duplicate_cte
where row_num > 1;

select * from layoffs_copy
where company = 'casper' ;


with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company,location,
industry, total_laid_off, percentage_laid_off,
'date', stage, country, funds_raised_millions) as row_num
from layoffs_copy
)
delete
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_copy2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_copy2
where row_num > 1;

insert into layoffs_copy2
SELECT *,
row_number() over(
partition by company,location,
industry, total_laid_off, percentage_laid_off,
'date', stage, country, funds_raised_millions) as row_num
from layoffs_copy;

set sql_safe_updates = 0;

delete 
from layoffs_copy2
where row_num > 1;

select * from layoffs_copy2;

-- Standardize Data

Select company, trim(company)
from layoffs_copy2;

update layoffs_copy2
set company = trim(company);

select * from layoffs_copy2
where industry like 'crypto%';

update layoffs_copy2
set industry = 'crypto'
where industry like 'crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_copy2
order by 1;

update layoffs_copy2
set country = trim(trailing '.' from country)
where counrty like 'united states%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_copy2;

update layoffs_copy2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_copy2
modify column `date` date;

-- 3. Null Values or blank values

select * from layoffs_copy2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_copy2
set industry = null
where industry = '';

select *
from layoffs_copy2
where industry is null
or industry = '';

select *
from layoffs_copy2
where company = 'Airbnb';

select *
from layoffs_copy2 t1
join layoffs_copy2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_copy2 t1
join layoffs_copy2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

-- 4. Remove Any Colums Rows

select *
from layoffs_copy2;  

delete
from layoffs_copy2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_copy2
drop column row_num; 

select *
from layoffs_copy2;
