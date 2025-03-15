# List all venues along with their associated complex name.
select v.venue_name,c.complex_name
from venues_data v left join complex_data c
on v.complex_id=c.complex_id;

#Count the number of venues in each complex
SELECT c.complex_name, COUNT(v.venue_id) AS venue_count
FROM venues_data v 
LEFT JOIN complex_data c ON v.complex_id = c.complex_id
GROUP BY c.complex_name
ORDER BY venue_count DESC;

#Get details of venues in a specific country (e.g., Chile)
select * from venues_data
where country_name = 'chile';

# Identify all venues and their timezones
select venue_name, timezone
from venues_data;

#Find complexes that have more than one venue
select c.complex_id, c.complex_name, count(v.venue_name) as venue_count
FROM complex_data c 
right join venues_data v
on c.complex_id=v.complex_id
group by complex_id
having venue_count>1;

#List venues grouped by country

SELECT country_code, 
       COUNT(venue_id) AS venue_count, 
       GROUP_CONCAT(venue_name SEPARATOR ', ') AS venue_names
FROM venues_data
GROUP BY country_code;

#Find all venues for a specific complex (e.g., Nacional)

select v.venue_name,c.complex_name,c.complex_id
from venues_data v left join complex_data c
on v.complex_id = c.complex_id
where c.complex_name = 'nacional';

