-- Populate Potential target entity table

INSERT INTO
    potential_targets (general_name)
VALUES
    ('Ukraine'),
    ('Dnipro'),
    ('Odesa oblast'),
    ('Cherkasy oblast'),
    ('Kolomyia');

-- Populate Launch place entity table

INSERT INTO
    launch_places (general_name)
VALUES
    ('Caspian Sea'),
    ('Black Sea'),
    ('Kursk'),
    ('Tula oblast'),
    ('Lipetsk oblast');

-- Populate Missile entity table

INSERT INTO
    missiles (model_name)
VALUES
    ('Shahed-136/131'),
    ('X-101/X-555'),
    ('Kalibr'),
    ('X-22'),
    ('X-59'),
    ('X-47 Kinzhal');


-- Populate Attack entity table

INSERT INTO
    attacks (start_datatime, end_datetime)
VALUES
    ('2022-10-11 07:00', '2022-10-11 13:00'),
    ('2023-01-14 15:30', '2023-01-14 15:30'),
    ('2023-03-21 19:40', '2023-03-21 19:40'),
    ('2023-06-08 20:00', '2023-06-08 20:00'),
    ('2023-08-11 09:00', '2023-08-11 10:30');

-- Populate Attack group entity table

INSERT INTO
    attack_groups (attack_id, units_launched, units_destroyed)
VALUES
    (1, 13, 13),
    (1, 28, 20),
    (2, 1, 0),
    (3, 2, 2),
    (4, 2, 0),
    (5, 4, 1);


-- Populate Attack - Potential target relation table

INSERT INTO
    group_targets (group_id, target_id)
VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (4, 3),
    (5, 4),
    (6, 5);

-- Populate Attack - Launch place relation table

INSERT INTO
    group_launch_places (group_id, place_id)
VALUES
    (2, 1),
    (2, 2),
    (3, 3),
    (4, 2),
    (5, 2),
    (6, 4),
    (6, 5);

-- Populate Attack - Missile relation table

INSERT INTO
    group_missiles (group_id, missile_id)
VALUES
    (1, 1),
    (2, 2),
    (2, 3),
    (3, 4),
    (4, 5),
    (5, 3),
    (6, 6);
