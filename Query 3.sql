desc competitor_data;

# Get all competitors with their rank and points.
select b.competitor_name,a.competitor_rank,a.competitor_points
from competitor_rankings_data a right join competitor_data b
on a.competitor_id=b.competitor_id;

# Find competitors ranked in the top 5
SELECT competitor_rank, competitor_name, competitor_country, competitor_points
FROM (
    SELECT r.competitor_rank, c.competitor_name, c.competitor_country, r.competitor_points,
           ROW_NUMBER() OVER (PARTITION BY r.competitor_rank ORDER BY r.competitor_points DESC) AS row_num
    FROM competitor_rankings_data r
    JOIN competitor_data c
    ON r.competitor_id = c.competitor_id
) ranked
WHERE row_num = 1
ORDER BY competitor_rank ASC
LIMIT 5;

# List competitors with no rank movement (stable rank)
SELECT competitor_rank, competitor_name, competitor_country, competitor_points
FROM (
    SELECT r.competitor_rank, c.competitor_name, c.competitor_country, r.competitor_points,
	dense_rank() OVER (ORDER BY r.competitor_points DESC) AS stablerank
    FROM competitor_rankings_data r
    JOIN competitor_data c
    ON r.competitor_id = c.competitor_id
) ranked
WHERE stablerank <= 5  -- Only get the top 5 dense ranks
ORDER BY stablerank ASC, competitor_points DESC
limit 5;

# List competitors with no rank movement (stable rank)
SELECT 
    r.competitor_rank, 
    c.competitor_name, 
    c.competitor_country, 
    r.competitor_points,
    r.competitor_movement
FROM competitor_rankings_data r
JOIN competitor_data c
ON r.competitor_id = c.competitor_id
WHERE competitor_movement = 0  -- Only select competitors whose rank did not change
ORDER BY r.competitor_rank ASC;

#Get the total points of competitors from a specific country (e.g., Croatia)
SELECT 
    c.competitor_country, COUNT(r.competitor_points) AS total_competitors
FROM competitor_rankings_data r
JOIN competitor_data c
ON r.competitor_id = c.competitor_id
WHERE c.competitor_country = 'Croatia'
GROUP BY c.competitor_country;

#Count the number of competitors per country
select c.competitor_country,count(r.competitor_points) as total_points_of_competitor
from competitor_rankings_data r right join competitor_data c
on c.competitor_id = r.competitor_id
group by c.competitor_country
order by c.competitor_country, total_points_of_competitor;

#Find competitors with the highest points in the current week

SELECT r.competitor_rank, c.competitor_name, c.competitor_country, r.competitor_points
FROM competitor_rankings_data r
JOIN competitor_data c
ON r.competitor_id = c.competitor_id
WHERE r.competitor_points = (SELECT MAX(competitor_points) FROM competitor_rankings_data)
ORDER BY r.competitor_points DESC;


