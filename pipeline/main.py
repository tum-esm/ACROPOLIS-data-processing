import L1_1_acropolis_postprocessing, L1_2_timeseries_despiking, L1_3_write_csv_icos_cp, L1_4_upload_csv_icos_cp

def L1_pipeline():
    L1_1_acropolis_postprocessing.run()
    L1_2_timeseries_despiking.run()
    L1_3_write_csv_icos_cp.run()
    L1_4_upload_csv_icos_cp.run()
    
if __name__ == "__main__":
    L1_pipeline()