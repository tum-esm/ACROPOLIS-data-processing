from datetime import datetime, timezone

ongoing_side_by_side = datetime(2024, 3, 19, 23, 59, 59).replace(tzinfo=timezone.utc)

sbs_times = [
    (
        1,
        datetime(2024, 2, 7, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 26, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    # 2 is still deployed
    (
        3,
        datetime(2024, 1, 13, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 18, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    (
        4,
        datetime(2024, 2, 14, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        5,
        datetime(2024, 2, 7, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 25, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    (
        6,
        datetime(2024, 2, 20, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        7,
        datetime(2024, 2, 21, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        8,
        datetime(2024, 2, 13, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 3, 11, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    (
        9,
        datetime(2024, 2, 12, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        10,
        datetime(2024, 1, 13, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        11,
        datetime(2024, 1, 12, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        12,
        datetime(2023, 12, 23, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 11, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    (
        13,
        datetime(2024, 1, 13, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 1, 30, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    (
        14,
        datetime(2024, 3, 2, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        15,
        datetime(2024, 2, 21, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        16,
        datetime(2023, 12, 23, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 5, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    # 17 needs assembly
    (
        18,
        datetime(2023, 12, 23, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 5, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
    (
        19,
        datetime(2024, 3, 23, 0, 0, 0).replace(tzinfo=timezone.utc),
        ongoing_side_by_side,
    ),
    (
        20,
        datetime(2023, 12, 23, 0, 0, 0).replace(tzinfo=timezone.utc),
        datetime(2024, 2, 11, 23, 59, 59).replace(tzinfo=timezone.utc),
    ),
]
