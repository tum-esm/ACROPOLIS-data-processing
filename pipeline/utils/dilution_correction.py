import polars as pl

from . import ambient_parameter_conversion as apc


def wet_to_dry_mole_fraction(df_wet: pl.DataFrame) -> pl.DataFrame:
    # perform dry conversion for measurement data
    return df_wet.with_columns(pl.struct(['gmp343_temperature','sht45_humidity']) \
        .map_elements(lambda x: apc.rh_to_ah(x['sht45_humidity'],apc.absolute_temperature(x['gmp343_temperature'])), return_dtype=pl.Float64)
        .alias("h2o_ah")) \
        .with_columns(pl.struct(['gmp343_temperature','sht45_humidity','bme280_pressure'])
        .map_elements(lambda x: (apc.rh_to_molar_mixing(x['sht45_humidity'],apc.absolute_temperature(x['gmp343_temperature']),x['bme280_pressure']*100))*100, return_dtype=pl.Float64) \
        .alias("h2o_v%")) \
        .with_columns(pl.struct(['gmp343_temperature','bme280_humidity','bme280_pressure'])
        .map_elements(lambda x: (apc.rh_to_molar_mixing(x['bme280_humidity'],apc.absolute_temperature(x['gmp343_temperature']),x['bme280_pressure']*100))*100, return_dtype=pl.Float64) \
        .alias("bme280_h2o_v%")) \
        .with_columns(pl.struct(['gmp343_filtered','gmp343_temperature','sht45_humidity','bme280_pressure']) \
        .map_elements(lambda x: apc.calculate_co2dry(x['gmp343_filtered'],x['gmp343_temperature'],x['sht45_humidity'],x['bme280_pressure']*100), return_dtype=pl.Float64)
        .alias("gmp343_dry"))
