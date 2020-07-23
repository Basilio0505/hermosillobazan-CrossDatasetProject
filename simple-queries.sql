-- Deaths caused by heat in 2011 in Texas counties displayed chronollogically by month 
SELECT cz_name, month_name, deaths_direct FROM storm_events_staging.StormEvents_2010  WHERE event_type = 'Heat' and state = 'TEXAS' and deaths_direct > 0 ORDER BY begin_yearmonth

-- Occurrences of tropical storms in the United from the beginning to the end of 2011 by state ordered chronollogically
SELECT state, month_name FROM storm_events_staging.StormEvents_2011  WHERE event_type = 'Tropical Storm' ORDER BY begin_yearmonth

--States affected by tornadoes that caused more than one direct injury oredered chronologically 
SELECT state, month_name,injuries_direct FROM storm_events_staging.StormEvents_2012  WHere event_type = 'Tornado' and injuries_direct > 1 order by begin_yearmonth

--Number of damaged crops in counties in the US where dorughts during 2013 caused more than 1000 damaged crops ordered by magnitude, then by state, then by county.
SELECT damage_crops, state, cz_name FROM storm_events_staging.StormEvents_2013 WHERE event_type = 'Drought' and damage_crops > 1000 order by damage_crops, state, cz_name

-- Flights cancalled in 2013 ordered chronollogically by the date in which they were cancelled. 
Select originstatename, deststatename, flightdate from storm_events_staging.OnTimePerformance_2013 where cancelled = 1.0 order by year, month, dayofmonth
