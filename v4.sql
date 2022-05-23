--longest route data
SELECT
    longitude,
    latitude,
    speed
FROM
    breadcrumb
WHERE
    trip_id = (
        SELECT
            longest_trip_id ());

--other data
SELECT
    min(tstamp) AS s,
    max(tstamp) AS e
FROM
    breadcrumb
WHERE
    trip_id = (
        SELECT
            longest_trip_id ());

