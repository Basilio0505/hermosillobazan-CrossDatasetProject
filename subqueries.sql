--This query will return all storm ids that occured in the specific location Travis County, Texas
SELECT event_id, event_type, year
FROM storm_events_modeled.Curated_StormEvents_Beam_DF s
WHERE s.county_id = (SELECT county_id 
            FROM storm_events_modeled.Locations_Beam_DF l
            JOIN storm_events_modeled.State t
            ON l.state_fips = t.state_fips
            WHERE t.state = 'TEXAS' AND l.name = 'TRAVIS')
ORDER BY year, event_type

--This Query will show the total deaths by storm type that occured in Santa Clara County, California
SELECT event_type, SUM(deaths_direct) AS Direct_Deaths
FROM storm_events_modeled.Curated_StormEvents_Beam_DF s
WHERE s.county_id = (SELECT l.county_id 
            FROM storm_events_modeled.Locations_Beam_DF l
            JOIN storm_events_modeled.State t
            ON l.state_fips = t.state_fips
            WHERE t.state = 'CALIFORNIA' AND l.name = 'SANTA CLARA')
GROUP BY event_type

--This Query will show the number of storm events for states with 1000 or more events happening
SELECT L.state, count(event_type) as Event_count 
FROM 
  (SELECT county_id,  state, name
  FROM storm_events_modeled.Locations_Beam t 
  JOIN storm_events_modeled.State s
  ON s.state_fips = t.state_fips) L
  JOIN storm_events_modeled.Curated_StormEvents_Beam_DF S 
  ON L.county_id = S.county_id
  GROUP BY state
  HAVING Event_count >= 1000
  
--This Query will show events that happened in days with more than 1500 storm events
SELECT event_type, date
FROM storm_events_modeled.Curated_StormEvents_Beam c
JOIN storm_events_modeled.Date d
ON c.year = d.year and c.month_name = d.month_name and c.begin_day = d.begin_day
WHERE date in (SELECT date
FROM storm_events_modeled.Curated_StormEvents_Beam c
JOIN storm_events_modeled.Date d
ON c.year = d.year and c.month_name = d.month_name and c.begin_day = d.begin_day
GROUP BY date HAVING count(event_type) > 1500)
ORDER BY date
