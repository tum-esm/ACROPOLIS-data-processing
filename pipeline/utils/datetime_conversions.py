from datetime import datetime, timezone


def calculate_decimal_year(date) -> float:
    year = date.year
    today = (date - datetime(year, 1, 1, 0, 0,
                             0).replace(tzinfo=timezone.utc)).total_seconds()
    seconds_total_year = (
        datetime(year, 1, 1, 0, 0, 0).replace(tzinfo=timezone.utc) -
        datetime(year - 1, 1, 1, 0, 0,
                 0).replace(tzinfo=timezone.utc)).total_seconds()

    x = ((today / seconds_total_year) + year)
    return float("{:.6f}".format(x))
