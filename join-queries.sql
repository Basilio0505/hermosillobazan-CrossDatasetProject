
--Similar storm events joined by matching state where they happened in the same month, and in the same county order chronollogically by state and county. 
SELECT
distinct(c.cz_name), a.month_name, (a.event_type), d.date
FROM storm_events_modeled.StormEvents a
  Inner JOIN storm_events_modeled.Locations c
  ON a.state_fips = c.state_fips and a.cz_fips = c.cz_fips
  inner Join storm_events_modeled.Date d
  on a.month_name = d.month_name
  where a.year = 2010 or a.year = 2011
  
ORDER BY d.date, a.month_name, cz_name, a.event_type


--StormEvents joined to State table to display all avalanches that caused at least one death
SELECT
state, year, month_name, begin_day, deaths_direct
FROM storm_events_modeled.StormEvents a
JOIN storm_events_modeled.State b
ON a.state_fips = b.state_fips 
WHERE event_type = 'Avalanche' AND deaths_direct >= 1
ORDER BY year, month_name, begin_day, state

--Events joined to see tornados happened in different counties within states ordered chronologically
SELECT
a.year, a.month_name, cz_name, location
FROM storm_events_modeled.StormEvents a
FULL OUTER JOIN storm_events_modeled.Locations b
ON a.cz_fips = b.cz_fips AND a.state_fips = b.state_fips
JOIN storm_events_modeled.Date c
ON a.year = c.year AND a.month_name = c.month_name AND a.begin_day = c.begin_day
WHERE event_type = 'Tornado'
ORDER BY date


--Events joined to see Droughts in Texas that happened in 2010 and caused more than 100 damage in crops in  chronollogical order and by state and county

SELECT a.event_type, a.month_name , b.cz_name, c.state, (d.date)
FROM storm_events_modeled.StormEvents a
  right JOIN storm_events_modeled.Locations b
  ON a.cz_fips = b.cz_fips
  right JOIN storm_events_modeled.State c
  ON a.state_fips = b.state_fips 
  right JOIN storm_events_modeled.Date d
  ON a.year = d.year and a.month_name = d.month_name and a.begin_day = d.begin_day
  where c.state = 'TEXAS' AND a.event_type = 'Drought' AND a.damage_crops > 100
  
ORDER BY d.date, c.state,  b.cz_name

--Similar storm events that happened in the same county the first day of every month in 2010 and 2011 order alphabetically by state, county month and event 
SELECT c.event_type, c.state, c.cz_name,c.month_name, c.begin_day
FrOM
(SELECT s.state, a.state_fips, a.cz_fips, b.cz_name, a.month_name , a.event_type event_type, begin_day
FROM storm_events_modeled.StormEvents a
left JOIN storm_events_modeled.Locations b
ON a.cz_fips = b.cz_fips and a.state_fips = b.state_fips
left JOIN storm_events_modeled.State s
on a.state_fips = s.state_fips
where a.year = 2010 and begin_day = 1) c
inner Join
(SELECT s.state, a.state_fips, a.cz_fips, b.cz_name, a.month_name , a.event_type event_type, begin_day
FROM storm_events_modeled.StormEvents a
left JOIN storm_events_modeled.Locations b
ON a.cz_fips = b.cz_fips and a.state_fips = b.state_fips
left JOIN storm_events_modeled.State s
on a.state_fips = s.state_fips
where a.year = 2011 and begin_day = 1) d
on 
c.event_type = d.event_type

Order by c.state, c.cz_name,c.month_name, c.event_type


--Left outer join to see indirect deaths in 2012 that were caused by Winter Storms order in chronollogical order then by state and day 

SELECT
 distinct(d.date), a.month_name , a.begin_day day, a.deaths_indirect, event_type, c.state
FROM storm_events_modeled.StormEvents a
  left JOIN storm_events_modeled.Locations b
  ON a.state_fips = b.state_fips and a.cz_fips = b.cz_fips
  
  left JOIN storm_events_modeled.Date d
  on a.year = d.year and a.month_name = d.month_name and a.begin_day = d.begin_day
  left JOIN storm_events_modeled.State c
  ON a.state_fips = c.state_fips
  where a.deaths_indirect > 0 and a.year = 2012 and event_type = 'Winter Storm'
