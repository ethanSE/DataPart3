CREATE FUNCTION longest_trip_id ()
    RETURNS integer
    AS $$
DECLARE
    longest_trip_id integer;
BEGIN
    WITH tripInterval AS (
        SELECT
            max(tstamp) AS e,
            min(tstamp) AS s,
            trip.trip_id
        FROM
            breadcrumb
            JOIN trip ON breadcrumb.trip_id = trip.trip_id
        WHERE
            route_id IS NOT NULL
        GROUP BY
            trip.trip_id)
        --get trip_id of longest trip
        SELECT
            trip_id INTO longest_trip_id
        FROM
            tripInterval
        ORDER BY
            e - s DESC
        LIMIT 1;
    RETURN longest_trip_id;
END;
$$
LANGUAGE plpgsql;

