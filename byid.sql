CREATE FUNCTION byid (id int)
    RETURNS TABLE (
        longitude real,
        latitude real,
        speed real
    )
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        bc.longitude,
        bc.latitude,
        bc.speed
    FROM
        breadcrumb AS bc
    WHERE
        bc.trip_id = id;
END;
$$
LANGUAGE plpgsql;

