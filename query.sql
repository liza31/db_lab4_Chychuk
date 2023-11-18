-- Query 1
-- The number of attacks in which "Kalibr" cruise missiles were used by month
-- Кількість атак з використанням крилатих ракет "Калібр" за місяцями

SELECT
    date_part('year', attacks.start_datatime)::integer AS year,
    date_part('month', attacks.start_datatime)::integer AS month,
    count(DISTINCT attacks.attack_id) AS attacks_count
FROM
    attacks
    JOIN attack_groups USING (attack_id)
    JOIN group_missiles USING (group_id)
WHERE
    group_missiles.missile_id = (SELECT missile_id FROM missiles WHERE model_name = 'Kalibr')
GROUP BY
    year, month
ORDER BY
    year, month;


-- Query 2
-- Distribution of the total number of attacks by targets
-- Розподіл загальної кількості атак за цілями

SELECT
    potential_targets.general_name as target,
    count(DISTINCT attack_id) as attacks_count
FROM
    attacks
    JOIN attack_groups USING (attack_id)
    JOIN group_targets USING (group_id)
    JOIN potential_targets USING (target_id)
GROUP BY
    target
ORDER BY
    attacks_count desc;


-- Query 3
-- Mass (total number of used missiles/drones) of attacks in which "Shahed-136/131" strike drones were used by month
-- Масовість атак з використанням баражуючих боєприпасів "Shahed-136/131" за місяцями

SELECT
    date_part('year', attacks.start_datatime)::integer AS year,
    date_part('month', attacks.start_datatime)::integer AS month,
    sum(attack_groups.units_launched) AS missiles_count
FROM
    attacks
    JOIN attack_groups USING (attack_id)
    JOIN group_missiles USING (group_id)
WHERE
    group_missiles.missile_id = (SELECT missile_id FROM missiles WHERE model_name = 'Shahed-136/131')
GROUP BY
    year, month
ORDER BY
    year, month;
