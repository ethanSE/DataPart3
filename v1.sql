WITH t AS (
    SELECT
        trip_id
    FROM
        breadcrumb
    WHERE
        latitude > 45.586158
        AND latitude < 45.592404
        AND longitude < - 122.541270
        AND longitude > - 122.550711
    LIMIT 1
)
SELECT
    longitude,
    latitude,
    speed
FROM
    breadcrumb
WHERE
    trip_id = (select trip_id from t);