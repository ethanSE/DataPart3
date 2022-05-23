SELECT
    max(largest - smallest),
    bar.trip_id
FROM (
    SELECT
        max(tstamp) AS largest,
        trip_id
    FROM
        breadcrumb
    GROUP BY
        trip_id) AS foo
    JOIN (
        SELECT
            min(tstamp) AS smallest,
            trip_id
        FROM
            breadcrumb
        GROUP BY
            trip_id) AS bar ON foo.trip_id = bar.trip_id;

