SELECT new_trains.train_id
FROM new_trains
WHERE NOT EXISTS (
    SELECT 1
    FROM all_trains
    WHERE all_trains.train_id = new_trains.train_id
);