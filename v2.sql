SELECT
    longitude, latitude, speed
FROM
    breadcrumb
    JOIN trip ON breadcrumb.trip_id = trip.trip_id
WHERE
    route_id = 65
    AND trip.direction = 0
    AND tstamp BETWEEN '2020-10-24T12:00:00' AND '2020-10-24T14:00:00';