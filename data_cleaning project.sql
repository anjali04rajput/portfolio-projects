create database data_cleaning;

select* from layoffs;

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert into layoffs_staging
select* 
from layoffs;
-----------------------------------------------------------------------------------------
-- remove duplicate
-- step 1. check weather duplicate exist or not

select *, row_number() 
over(partition by company,industry,location,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_check as(

select *, row_number() 
over(partition by company,industry,location,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_check
where row_num>1;

-- step 2.  deleting the duplicate data

-- step 2.1 create a new table which have all the columns which existing one have 
-- step 2.2 add a new column 'row_num' to the newly created table

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- step 2.3 insert the data of newly created column 'row_num'

insert into layoffs_staging2
select *, row_number() 
over(partition by company,industry,location,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select * from LAYoffs_staging2;

-- step 2.4 now select all the duplicate data and delete it 

select * 
from layoffs_staging2
where row_num>1;

delete
from layoffs_staging2
where row_num>1;

set sql_safe_updates=0;

select*
from layoffs_staging2;

---------------------------------------------------------------------------------------------------

-- standardizing the data

-- 1. get rid of extra spaces
select company , trim(company)
from layoffs_staging2;

update layoffs_staging2 
set company = trim(company);

-- 2. get rid of data with same meaning but written in diffrent ways like crypto , cryptocurrency

select *
from layoffs_staging2
where binary industry like 'Crypto%';

update layoffs_staging2
set industry ='Crypto'
where industry like 'crypto%';

select distinct(industry)
from layoffs_staging2;

-- get rid of some extra dot

select distinct country , trim(trailing '.' from country)
from layoffs_staging2
where binary country like 'United States%';

update layoffs_staging2
set country = trim(trailing '.' from country);

select distinct country 
from layoffs_staging2
where binary country like 'United States%';

-- formate 'date' to datetime from text

select date,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date= str_to_date(`date`,'%m/%d/%Y');

-- or

alter table layoffs_staging2
modify column date date;

select * from layoffs_staging2;


--------------------------------------------------------------------------------------


-- null values or blank values

select *
from layoffs_staging2
where industry IS null
or industry = '';

select* 
from layoffs_staging2
where company like 'Airbnb';
-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- step 1. we should set the blanks to nulls since those are typically easier to work with

update layoffs_staging2
set industry = null
where industry = '';

-- step 2. use self join and update

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
set t1.industry= t2.industry
where t1.industry is null
and t2.industry is not null;

-- get rid of some more null data that we don't need 

select total_laid_off,percentage_laid_off
from layoffs_staging2
where total_laid_off IS null
AND percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off IS null
AND percentage_laid_off is null;

--------------------------------------------------------------------------------------

-- remove unnecessary columns

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;