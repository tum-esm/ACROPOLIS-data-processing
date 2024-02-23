import warnings

warnings.simplefilter("ignore", category=FutureWarning)


def two_point_calibration(measured_values, true_values):
    # Check if input lists have length 2
    if len(measured_values) != 2 or len(true_values) != 2:
        raise ValueError("Both measured_values and true_values must have length 2")

    # Calculate calibration parameters (slope and intercept)
    #
    slope = (true_values[1] - true_values[0]) / (
        measured_values[1] - measured_values[0]
    )
    intercept = true_values[0] - slope * measured_values[0]

    return slope, intercept


def average_bottle(conc_list):
    if len(conc_list) == 0:
        print("Length of list to average is 0.")
        return 0

    uncut_avg = sum(conc_list) / len(conc_list)
    print(f"Uncut average: {uncut_avg}")

    if len(conc_list) > 0:
        conc_list_cut = conc_list[
            int(len(conc_list) * 0.3) : int(len(conc_list) * 0.95)
        ]
        cut_avg = sum(conc_list_cut) / len(conc_list_cut)
        print(f"Cut average: {cut_avg}")

        return cut_avg
    return 0
