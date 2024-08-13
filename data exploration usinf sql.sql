create database data_project;
use data_project;

select* from covid_death;

-- select needed data

select location, date,population,new_cases,total_cases,total_deaths
from covid_death;

-- perform task to find death percentage

select location,date,population,new_cases,(total_deaths/total_cases)*100 as death_percentage
from covid_death;

-- implement where , group and having clauses

select location,date,population,new_cases,(total_deaths/total_cases)*100 as death_percentage
from covid_death
where location = 'Afghanistan'
group by location,new_cases,date,population,total_deaths,total_cases
having new_cases>200;

-- looking at total cases v/s population

select location,date, (total_cases/population)*100 as percentage_population_got_covid
from covid_death;

-- countries with higher infection count

select location, max(total_cases) as infaction_count
from covid_death
group by location
order by infaction_count desc;

-- countries with higher death_count

select location, max(total_deaths) as death_count
from covid_death
group by location
order by death_count desc;

-- lets break things down by Continents

select continent, max(total_cases) as infaction_count
from covid_death
group by continent
order by infaction_count desc;

select continent, max(total_deaths) as death_count
from covid_death
group by continent
order by death_count desc;

-- use of aggregated functions

select sum(new_cases)
from covid_death;

select avg(new_cases)
from covid_death;

select MAX(new_cases)
from covid_death;

select MIN(new_cases)
from covid_death;

select COUNT(new_cases)
from covid_death;

-- global numbers

select sum(new_cases),sum(new_deaths), (SUM(new_deaths)/SUM(new_cases))*100
from covid_death;

select date, sum(new_cases),sum(new_deaths), (SUM(new_deaths)/SUM(new_cases))*100
from covid_death
group by date;

-- new table

SELECT * FROM covid_vaccination;

-- join tables

select* 
from covid_death dea
join covid_vaccination vac
on dea.location= vac.location AND
dea.date = vac.date
order by 1,2;

-- looking at total population and vaccinations

select dea.location,dea.date,dea.population,vac.total_vaccinations
from covid_death dea
join covid_vaccination vac
on dea.location= vac.location AND
dea.date = vac.date; 

select dea.location,dea.date,dea.population,vac.total_vaccinations,
sum(total_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_people_vaccinated
from covid_death dea
join covid_vaccination vac
on dea.location= vac.location AND
dea.date = vac.date; 

-- use CTE ( Common Table Expression)

with pop_vs_vac (continent, location, date, population, total_vaccination, rolling_people_vaccinated) as (
select dea.continent,dea.location,dea.date,dea.population,vac.total_vaccinations,
sum(total_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_people_vaccinated
from covid_death dea
join covid_vaccination vac
on dea.location= vac.location AND
dea.date = vac.date
)
select * ,(rolling_people_vaccinated/population )*100
from pop_vs_vac;

-- temp table

create temporary table percent_population_vaccination
(continent varchar(255),
location varchar(255),
date datetime,
population int,
total_vaccination int,
rolling_people_vaccinated int
);
 
drop temporary table if exists percent_population_vaccination;

insert into percent_population_vaccination
select dea.continent,dea.location,dea.date,dea.population,vac.total_vaccinations,
sum(total_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_people_vaccinated
from covid_death dea
join covid_vaccination vac
on dea.location= vac.location AND
dea.date = vac.date;

select * ,(rolling_people_vaccinated/population)*100
from percent_population_vaccination;

-- creating veiw for future use

create view percent_population_vaccination as 
select dea.continent,dea.location,dea.date,dea.population,vac.total_vaccinations,
sum(total_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_people_vaccinated
from covid_death dea
join covid_vaccination vac
on dea.location= vac.location AND
dea.date = vac.date;