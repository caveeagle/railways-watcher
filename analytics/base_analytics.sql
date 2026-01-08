SELECT COUNT(*) FROM delays WHERE update_id = 5  AND avg_delay <> 0  # 178 from 714
SELECT MAX(avg_delay) FROM delays WHERE update_id = 5  # 44
SELECT MAX(share_delayed) FROM delays WHERE update_id = 5  # 1000

