"""
This module contains functions to perform
sensor-specific calculations such as conversion between units,
computation of absolute moisture from relative moisture, conversion
between molar and mass concentrations and similar operations.
It also includes some useful constants

-> Taken from Simone Bafellis <sensorutils>

Attributes
----------
T0: float
    The 0 C temperature in K
TC: float
    The critical point temperature of water
PC: float
    The critical point pressure of water
P0: float
    Reference pressure at sea level
"""
from math import log
from typing import Final

import numpy as np


"""
Constants
"""
T0: Final = 273.15
TC: Final = 647.096
PC: Final = 22.064e6
P0: Final = 1013.25e2
G0: Final = 9.80665
M: Final = 0.0289644
R_STAR: Final = 8.3144598


def absolute_temperature(t: float):
    """
    Computes the absolute temperature in K
    from a given temperature in C
    """
    return t + T0


def saturation_vapor_pressure(t: float) -> float:
    """
    Compute saturation vapor pressure of water (in pascal)
    at given absolute temperature
    Parameters
    ----------
    t: float
        Temperature in K
    """
    coef_1 = -7.85951783
    coef_2 = 1.84408259
    coef_3 = -11.7866497
    coef_4 = 22.6807411
    coef_5 = -15.9618719
    coef_6 = 1.80122502
    theta = 1 - t / TC
    pw = (
        np.exp(
            TC
            / t
            * (
                coef_1 * theta
                + coef_2 * theta**1.5
                + coef_3 * theta**3
                + coef_4 * theta**3.5
                + coef_5 * theta**4
                + coef_6 * theta**7.5
            )
        )
        * PC
    )
    return pw


def rh_to_ah(rh: float, t: float) -> float:
    """
    Conver relative to absolute humidity (in g/m^3)
    given temperature and pressure
    Parameters
    ----------
    rh: float
        Relative humidity in %
    pressure: float
        Pressure
    """
    pw = saturation_vapor_pressure(t)
    c = 2.16679
    return c * pw / t * rh / 100


def rh_to_molar_mixing(rh: float, t: float, p: float) -> float:
    """
    Convert the give relative humidity (in 100%)
    to a molar mixing ratio (in ppm)
    Parameters
    ----------
    rh: float
        The relative humidity
    t: float
        The absolute temperature in K
    p: float
        Pressure in Pa
    """
    return saturation_vapor_pressure(t) * rh / 100 * 1 / p * 100


def molar_mixing_to_rh(ppm: float, t: float, p: float) -> float:
    """
    Convert the given molar mixing ratio to
    relative humidity at the given pressure and temperature
    """
    return ppm / saturation_vapor_pressure(t) * p


def dry_to_wet_molar_mixing(conc: float, h2o: float) -> float:
    """
    Convert the dry molar mixing ratio to wet molar mixing ratio given
    the water mixing ratio (both in ppm)
    """
    return conc * (1 - h2o / 100)


def normalisation_constant(p: float, t: float) -> float:
    """
    Computes the STP normalisation constant used to normalise CO2 levels
    to standard temperature and pressure
    """
    return p / P0 * T0 / t


def normalise_concentration(c: float, p: float, t: float) -> float:
    """
    Normalises the concentration to STP (From PPM to number concentration)
    """
    return c * normalisation_constant(p, t)


def calculate_co2dry(
    co2wet: float, temperature: float, humidity: float, pressure: float
):
    """Return dry co2 value
    ToDo: bring it to standard temperature and pressure for comparison
    Args:
        co2wet float: CO2 wet
        temperature float: Temperature in °C
        rh float: The relative humidity
        p float: Pressure in Pa

    Returns:
        _type_: _description_
    """
    xh2o = rh_to_molar_mixing(
        rh=(humidity / 100), t=absolute_temperature(temperature), p=pressure
    )
    return co2wet / (1 - xh2o)
