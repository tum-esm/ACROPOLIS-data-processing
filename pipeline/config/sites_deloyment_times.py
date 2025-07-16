from datetime import datetime, timezone

# INFO: First two days of deployment are cut due to system warming up and adjusting to new environment

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
                "end_time": "2024-7-1T00:00:00+0000"
            },
            {
                "id": 6,
                "start_time": "2024-7-1T00:00:00+0000",
                "end_time": current_date
            },
        ],
    },
    "TUMRv2": {
        "sensors": [
            {
                "id": 3,
                "start_time": "2025-02-18T00:00:00+0000",
                "end_time": current_date
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
                "end_time": current_date
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
                "end_time": current_date
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
                "end_time": current_date
            },
        ],
    },
    "SENR": {
        "sensors": [
            {
                "id": 1,
                "start_time": "2024-02-29T00:00:00+0000",
                "end_time": current_date
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
                "end_time": current_date
            },
        ],
    },
    "SCHR": {
        "sensors": [
            {
                "id": 10,
                "start_time": "2024-04-11T00:00:00+0000",
                "end_time": current_date
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
                "end_time": current_date
            },
        ],
    },
    "SWMR": {
        "sensors": [
            {
                "id": 15,
                "start_time": "2024-06-14T00:00:00+0000",
                "end_time": current_date
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
                "end_time": current_date
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
                "end_time": current_date
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
                "end_time": current_date
            },
        ],
    },
    "BLUT_48": {
        "sensors": [
            {
                "id": 14,
                "start_time": "2024-06-23T00:00:00+0000",
                "end_time": current_date
            },
        ],
    },
    "BLUT_85": {
        "sensors": [
            {
                "id": 7,
                "start_time": "2024-06-23T00:00:00+0000",
                "end_time": current_date
            },
        ],
    },
    "NPLR": {
        "sensors": [
            {
                "id": 9,
                "start_time": "2024-06-26T00:00:00+0000",
                "end_time": current_date
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
                "end_time": current_date
            },
        ],
    },
    "HARR": {
        "sensors": [
            {
                "id": 4,
                "start_time": "2024-07-30T00:00:00+0000",
                "end_time": current_date
            },
        ],
    },
    "BALR": {
        "sensors": [
            {
                "id": 19,
                "start_time": "2024-10-09T00:00:00+0000",
                "end_time": current_date
            },
        ],
    },
}
