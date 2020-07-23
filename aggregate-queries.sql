
--Count storm events by state happening at least once

SELECT state, COUNT(event_type) AS Event_Amount
FROM storm_events_modeled.Curated_StormEvents_Beam c
JOIN  (SELECT a.county_id, a.name, b.state 
FROM storm_events_modeled.Locations_Beam a
JOIN  storm_events_modeled.State b 
on a.state_fips = b.state_fips) d
on c.county_id = d.county_id
GROUP BY state
HAVING COUNT(event_type) > 1 
ORDER BY state


-- Average damaged crops per storm events that damaged crops

SELECT event_type, AVG(damage_crops) average_damaged_crops
FROM storm_events_modeled.Curated_StormEvents_Beam
GROUP BY event_type
HAVING average_damaged_crops > 0
ORDER BY average_damaged_crops

--Number of daily direct deaths caused by storms since 2011 

SELECT date, SUM(deaths_indirect) AS direct_deaths
FROM ( storm_events_modeled.Curated_StormEvents_Beam c
JOIN (SELECT a.county_id, a.name, b.state 
FROM storm_events_modeled.Locations_Beam a
JOIN  storm_events_modeled.State b 
on a.state_fips = b.state_fips) d on c.county_id = d.county_id) Join 
storm_events_modeled.Date r on c. year = r.year and c.month_name = r.month_name and c.begin_day = r.begin_day
GROUP BY date
HAVING date > ('2010-12-31')
ORDER BY date

-- Sum of each month's total direct deaths in ascending order.

SELECT month_name, SUM(deaths_direct) AS Deaths
FROM storm_events_modeled.Curated_StormEvents_Beam s
GROUP BY month_name
ORDER BY Deaths DESC

-- Sum of each states total direct deaths in ascending order.

SELECT c.state, SUM(deaths_direct) AS Deaths
FROM storm_events_modeled.Curated_StormEvents_Beam s
JOIN storm_events_modeled.Locations_Beam l
ON s.county_id = l.county_id
JOIN storm_events_modeled.State c
ON l.state_fips = c.state_fips
GROUP BY c.state
ORDER BY Deaths DESC

--Sum of each type of storms total direct deaths in ascending order

SELECT event_type, SUM(deaths_direct) AS Deaths
FROM storm_events_modeled.Curated_StormEvents_Beam s
GROUP BY event_type
ORDER BY Deaths DESC

-- Amount of storms where at least one direct death has occured 

SELECT event_type, COUNT(deaths_direct) AS Deaths
FROM storm_events_modeled.Curated_StormEvents_Beam s
WHERE deaths_direct > 0
GROUP BY event_type
ORDER BY Deaths DESC
