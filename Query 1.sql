#List all competitions along with their category name
use gameanalytics;
select c1.competition_name,c2.category_name,c1.category_id
from competition_data c1 left join category_data c2
on c1.category_id=c2.category_id;

#Count the number of competitions in each category
SELECT 
    c.category_id, 
    cat.category_name, 
    COUNT(c.competition_id) AS competition_count
FROM competition_data c
JOIN category_data cat ON c.category_id = cat.category_id
GROUP BY c.category_id, cat.category_name
ORDER BY competition_count DESC;

#Find all competitions of type 'doubles'
select competition_name from competition_data
where competition_type = 'doubles' ;

#Get competitions that belong to a specific category (e.g., ITF Men)
select c1.competition_name, c2.category_name from competition_data c1
left join category_data c2
on c1.category_id=c2.category_id
where c2.category_name = 'ITF Men';

#Identify parent competitions and their sub-competitions
SELECT 
    c1.competition_name AS Parent_Competition, 
    c2.competition_name AS Sub_Competition
FROM competition_data c1
JOIN competition_data c2 ON c1.competition_id = c2.parent_id
ORDER BY c1.competition_name;

#Analyze the distribution of competition types by category
select c2.category_name ,c1.competition_type, COUNT(*) as competition_count
from competition_data c1 left join category_data c2
on c1.category_id = c2.category_id
group by c2.category_name,c1.competition_type
order by c2.category_name, competition_count DESC;

#List all competitions with no parent (top-level competitions)
SELECT competition_id, competition_name, category_id, competition_type
FROM competition_data
WHERE parent_id IS NULL;
 