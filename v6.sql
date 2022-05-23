WITH averages AS (
    SELECT
        avg(speed),
        trip.trip_id,
        route_id
    FROM
        breadcrumb
        JOIN trip ON breadcrumb.trip_id = trip.trip_id
    WHERE
        route_id IS NOT NULL
    GROUP BY
        trip.trip_id
),
minSpeedTrip AS (
    SELECT
        avg AS speed,
        trip_id,
        route_id
    FROM
        averages
    WHERE
        avg = (
            SELECT
                min(avg)
            FROM
                averages
            WHERE
                avg > (
                    SELECT
                        min(avg)
                    FROM
                        averages)))

--- get formatted data for visualization
SELECT
    longitude,
    latitude,
    speed
FROM
    breadcrumb
WHERE
    trip_id = (
        SELECT
            trip_id
        FROM
            minSpeedTrip);

--- get other info pertaining to trip
SELECT
    tstamp,
    route_id,
    trip.trip_id
FROM
    breadcrumb
    JOIN trip ON breadcrumb.trip_id = trip.trip_id
WHERE
    trip.trip_id = (
        SELECT
            trip_id
        FROM
            minSpeedTrip);

