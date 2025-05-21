CREATE OR REPLACE TABLE mpelletier.summit.enriched_counties AS
WITH county AS (
SELECT count(u.id) as sightings, c.id
FROM mpelletier.summit.counties c
JOIN mpelletier.summit.enriched_ufo_sightings u ON c.id = u.county_id
GROUP BY c.id)
SELECT EXPLODE(H3_TESSELLATEASWKB(c.geometry, 5)) as h3, c.county, sightings, c.id
FROM mpelletier.summit.counties c
JOIN county cc ON c.id = cc.id
WHERE ST_IsValid(c.geometry);