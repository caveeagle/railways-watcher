SELECT new_trains.train_id
FROM new_trains
LEFT JOIN all_trains ON all_trains.train_id = new_trains.train_id
WHERE all_trains.train_id IS NULL;