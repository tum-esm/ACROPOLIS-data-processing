from datetime import datetime, timezone

# INFO: First two days of deployment are cut due to system warming up and adjusting to new environment

# add current_date for "end_time" if the sensor is currently deployed (i.e., no end_time specified)

datetime_format = "%Y-%m-%dT%H:%M:%S%z"
current_date = datetime.now(timezone.utc).strftime(datetime_format)

deployment_times = {
    "TUMR": {
        "sensors": [
            {
                "id": 11,
                "start_time": "2024-01-12T04:00:00+0000",
                "end_time": "2024-02-14T00:00:00+0000"
            },
            {
                "id": 4,
                "start_time": "2024-02-14T00:00:00+0000",
                "end_time": "2024-07-01T00:00:00+0000"
            },
            {
                "id": 6,
                "start_time": "2024-07-01T00:00:00+0000",
                "end_time": "2025-02-18T00:00:00+0000"
            },
            {
                "id": 3,
                "start_time": "2025-02-18T00:00:00+0000",
                "end_time": "2026-01-01T00:00:00+0000"
            }
        ],
    },
    "TUMRv2": {
        "sensors": [
            {
                "id": 3,
                "start_time": "2025-02-18T00:00:00+0000",
                "end_time": "2026-04-23T00:00:00+0000"
            }
        ],
    },
    "FELR": {
        "sensors": [
            # {
            #     "id": 7,
            #     "start_time": "2023-12-14T00:00:00+0000",
            #     "end_time": "2023-12-22T00:00:00+0000"
            # },
            {
                "id": 13,
                "start_time": "2024-02-22T00:00:00+0000",
                "end_time": "2025-10-07T00:00:00+0000"
            },
        ],
    },
    "TAUR": {
        "sensors": [
            # {
            #     "id": 8,
            #     "start_time": "2023-10-27T00:00:00+0000",
            #     "end_time": "2023-12-22T00:00:00+0000"
            # },
            {
                "id": 12,
                "start_time": "2024-02-14T00:00:00+0000",
                "end_time": "2026-01-23T00:00:00+0000"
            },
        ],
    },
    "DLRR": {
        "sensors": [
            # {
            #     "id": 14,
            #     "start_time": "2023-11-22T00:00:00+0000",
            #     "end_time": "2023-12-22T00:00:00+0000"
            # },
            {
                "id": 5,
                "start_time": "2024-02-28T00:00:00+0000",
                "end_time": "2026-04-19T00:00:00+0000"
            },
        ],
    },
    "SENR": {
        "sensors": [
            {
                "id": 1,
                "start_time": "2024-02-29T00:00:00+0000",
                "end_time": "2026-04-11T00:00:00+0000"
            },
        ],
    },
    "RDIR": {
        "sensors": [
            # {
            #     "id": 2,
            #     "start_time": "2023-09-13T00:00:00+0000",
            #     "end_time": "2023-12-22T00:00:00+0000"
            # },
            {
                "id": 8,
                "start_time": "2024-03-15T00:00:00+0000",
                "end_time": "2026-04-19T00:00:00+0000"
            },
        ],
    },
    "SCHR": {
        "sensors": [
            {
                "id": 10,
                "start_time": "2024-04-11T00:00:00+0000",
                "end_time": "2026-06-04T00:00:00+0000"
            },
        ],
    },
    "FINR": {
        "sensors": [
            # {
            #     "id": 15,
            #     "start_time": "2023-11-16T00:00:00+0000",
            #     "end_time": "2023-12-22T00:00:00+0000"
            # },
            {
                "id": 3,
                "start_time": "2024-02-22T00:00:00+0000",
                "end_time": "2024-04-03T00:00:00+0000"
            },
            {
                "id": 11,
                "start_time": "2024-04-11T00:00:00+0000",
                "end_time": "2025-11-30T00:00:00+0000"
            },
        ],
    },
    "SWMR": {
        "sensors": [
            {
                "id": 15,
                "start_time": "2024-06-14T00:00:00+0000",
                "end_time": "2026-04-19T00:00:00+0000"
            },
        ],
    },
    "MAIR": {
        "sensors": [
            # {
            #     "id": 1,
            #     "start_time": "2023-09-08T00:00:00+0000",
            #     "end_time": "2023-12-22T00:00:00+0000"
            # },
            {
                "id": 16,
                "start_time": "2024-02-08T00:00:00+0000",
                "end_time": "2025-07-21T00:00:00+0000"
            },
        ],
    },
    "MAIR2": {
        "sensors": [
            {
                "id": 16,
                "start_time": "2025-07-21T00:00:00+0000",
                "end_time": "2025-11-14T00:00:00+0000"
            },
        ],
    },
    "PASR": {
        "sensors": [
            # {
            #     "id": 5,
            #     "start_time": "2023-11-16T00:00:00+0000",
            #     "end_time": "2024-02-06T00:00:00+0000"
            # },
            {
                "id": 18,
                "start_time": "2024-02-08T00:00:00+0000",
                "end_time": "2026-04-19T00:00:00+0000"
            },
        ],
    },
    "GROR": {
        "sensors": [
            # {
            #     "id": 4,
            #     "start_time": "2023-09-22T00:00:00+0000",
            #     "end_time": "2024-02-12T00:00:00+0000"
            # },
            {
                "id": 20,
                "start_time": "2024-02-14T00:00:00+0000",
                "end_time": "2026-05-22T00:00:00+0000"
            },
        ],
    },
    "BLUT_48": {
        "sensors": [
            {
                "id": 14,
                "start_time": "2024-06-23T00:00:00+0000",
                "end_time": "2026-04-13T00:00:00+0000"
            },
        ],
    },
    "BLUT_85": {
        "sensors": [
            {
                "id": 7,
                "start_time": "2024-06-23T00:00:00+0000",
                "end_time": "2026-04-19T00:00:00+0000"
            },
        ],
    },
    "NPLR": {
        "sensors": [
            {
                "id": 9,
                "start_time": "2024-06-26T00:00:00+0000",
                "end_time": "2025-11-04T00:00:00+0000"
            },
        ],
    },
    "BOGR": {
        "sensors": [
            {
                "id": 17,
                "start_time": "2024-07-09T00:00:00+0000",
                "end_time": "2025-03-03T11:27:00+0000"
            },
            {
                "id": 2,
                "start_time": "2025-03-03T11:29:00+0000",
                "end_time": "2026-04-27T00:00:00+0000"
            },
        ],
    },
    "HARR": {
        "sensors": [
            {
                "id": 4,
                "start_time": "2024-07-30T00:00:00+0000",
                "end_time": "2026-02-21T00:00:00+0000"
            },
        ],
    },
    "BALR": {
        "sensors": [
            {
                "id": 19,
                "start_time": "2024-10-09T00:00:00+0000",
                "end_time": "2025-10-05T00:00:00+0000"
            },
        ],
    },
}
