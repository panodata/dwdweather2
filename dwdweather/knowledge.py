# -*- coding: utf-8 -*-
# (c) 2014 Marian Steinbach, MIT licensed
# (c) 2018-2019 Andreas Motl, MIT licensed
import logging
from collections import OrderedDict

log = logging.getLogger(__name__)


class DwdCdcKnowledge(object):
    """
    Knowledge about the data layout on the DWD Climate Data Centers (CDC) server.
    """

    class climate:

        # The different measurements for climate data
        measurements = [
            {"key": "KL", "name": "daily_observations", "folder": "kl"},
            {"key": "TU", "name": "air_temperature"},
            {"key": "CS", "name": "cloud_type"},
            {"key": "N",  "name": "cloudiness"},
            {"key": "TD", "name": "dew_point"},
            {"key": "TX", "name": "extreme_temperature"},
            {"key": "FX", "name": "extreme_wind"},
            {"key": "RR", "name": "precipitation"},
            {"key": "P0", "name": "pressure"},
            {"key": "EB", "name": "soil_temperature"},
            {"key": "ST", "name": "solar"},
            {"key": "SD", "name": "sun"},
            {"key": "VV", "name": "visibility"},
            {"key": "FF", "name": "wind"},
            {"key": "F",  "name": "wind_synop"},
        ]

        # The different resolutions for climate data
        class resolutions:

            """
            Quality information
            The quality level "Qualitätsniveau" (QN) given here applies
            to the respective columns and describes the method of quality control.

            Quality level (column header: QN_X)::

                 1 only formal control during decoding and import
                 2 controlled with individually defined criteria
                 3 automatic control and correction (with QUALIMET and QCSY)
                 5 historic, subjective procedures
                 7 second control, not yet corrected
                 8 quality control, outside ROUTINE
                 9 quality control, not all parameters corrected
                10 quality control finished, all corrections finished

            Erroneous or suspicious values are identified and set to -999.
            """

            # Temporal resolution: daily
            class daily:

                # Which data set / resolution subfolder to use.
                __folder__ = "daily"

                # Which format does the timestamp of this resolution have?
                __timestamp_format__ = "%Y%m%d"

                """
                ==================
                Daily observations
                ==================

                Recent daily station observations (temperature, pressure, precipitation,
                sunshine duration, etc.) for Germany, quality control not completed yet.

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  daily
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/DESCRIPTION_obsgermany_climate_daily_kl_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1781 - 31.12.2017
                    - Temporal resolution:  daily
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/DESCRIPTION_obsgermany_climate_daily_kl_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDD
                    QN_3                quality level of next columns   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    FX                  daily maximum of wind gust      m/s
                    FM                  daily mean of wind velocity     m/s
                    QN_4                quality level of next columns   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    RSK                 daily precipitation height      mm
                    RSKF                precipitation form              0, 1, 4, 6, 7, 8, 9
                    SDK                 daily sunshine duration         h
                    SHK_TAG             daily snow depth                cm
                    NM                  daily mean of cloud cover       1/8
                    VPM                 daily mean of vapor pressure    hPa
                    PM                  daily mean of pressure          hPa
                    TMK                 daily mean of temperature       °C
                    UPM                 daily mean of relative humidity %
                    TXK                 daily maximum of temperature
                                        at 2m height                    °C
                    TNK                 daily minimum of temperature
                                        at 2m height                    °C
                    TGK                 daily minimum of air temperature
                                        at 5cm above ground             °C
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                Form of precipitation:

                    0   No precipitation (conventional or automatic measurement),
                        relates to WMO code 10
                    1   Only rain (before 1979)
                    4   Unknown form of recorded precipitation
                    6   Only rain; only liquid precipitation at automatic stations,
                        relates to WMO code 11
                    7   Only snow; only solid precipitation at automatic stations,
                        relates to WMO code 12
                    8   Rain and snow (and/or "Schneeregen"); liquid and solid precipitation
                        at automatic stations, relates to WMO code 13
                    9   Error or missing value or no automatic determination of precipitation form,
                        relates to WMO code 15

                """
                daily_observations = (
                    ("daily_quality_level_3", "int"),
                    ("wind_gust_max", "real"),
                    ("wind_velocity_mean", "real"),
                    ("daily_quality_level_4", "int"),
                    ("precipitation_height", "real"),
                    ("precipitation_form", "int"),
                    ("sunshine_duration", "real"),
                    ("snow_depth", "real"),
                    ("cloud_cover", "real"),
                    ("vapor_pressure", "real"),
                    ("pressure", "real"),
                    ("temperature", "real"),
                    ("humidity", "real"),
                    ("temperature_max_200", "real"),
                    ("temperature_min_200", "real"),
                    ("temperature_min_005", "real"),
                )

                """
                ================
                Soil temperature
                ================

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  daily
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/soil_temperature/recent/DESCRIPTION_obsgermany_climate_daily_soil_temperature_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1949 - 31.12.2017
                    - Temporal resolution:  daily
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/soil_temperature/historical/DESCRIPTION_obsgermany_climate_daily_soil_temperature_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_2                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    V_TE002             Soil temperature in   2 cm depth °C
                    V_TE005             Soil temperature in   5 cm depth °C
                    V_TE010             Soil temperature in  10 cm depth °C
                    V_TE020             Soil temperature in  20 cm depth °C
                    V_TE050             Soil temperature in  50 cm depth °C
                    V_TE100             Soil temperature in 100 cm depth °C
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                soil_temperature = (
                    ("soil_temperature_quality_level", "int"),  # Quality level
                    ("soil_temperature_002", "real"),  # Soil temperature 2cm
                    ("soil_temperature_005", "real"),  # Soil temperature 5cm
                    ("soil_temperature_010", "real"),  # Soil temperature 10cm
                    ("soil_temperature_020", "real"),  # Soil temperature 20cm
                    ("soil_temperature_050", "real"),  # Soil temperature 50cm
                    #("soil_temperature_100", "real"),  # Soil temperature 100cm
                )

                """
                =====
                Solar
                =====

                Documentation
                -------------

                - Description:          Daily station observations of solar incoming (total/diffuse) and longwave downward radiation for Germany
                - Temporal coverage:    01.01.1937 - month before last month
                - Temporal resolution:  daily
                - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/solar/DESCRIPTION_obsgermany_climate_daily_solar_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_592              Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    ATMO_STRAHL         Hourly sum of longwave          J/cm^2
                                        downward radiation
                    FD_STRAHL           Hourly sum of diffuse           J/cm^2
                                        solar radiation
                    FG_STRAHL           Hourly sum of solar             J/cm^2
                                        incoming radiation
                    SD_STRAHL           Hourly sum of                   min
                                        sunshine duration
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                solar = (
                    ("solar_quality_level", "int"),         # Quality information
                    ("solar_atmosphere", "real"),           # Hourly sum of longwave downward radiation
                    ("solar_dhi", "real"),                  # Hourly sum of Diffuse Horizontal Irradiance (DHI)
                    ("solar_ghi", "real"),                  # Hourly sum of Global Horizontal Irradiance (GHI)
                    ("solar_sunshine", "real"),             # Hourly sum of sunshine duration
                )

            # Temporal resolution: hourly
            class hourly:

                # Which data set / resolution subfolder to use.
                __folder__ = "hourly"

                # Which format does the timestamp of this resolution have?
                __timestamp_format__ = "%Y%m%d%H"

                """
                ===============
                Air temperature
                ===============

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/DESCRIPTION_obsgermany_climate_hourly_tu_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1893 - 31.12.2016
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/DESCRIPTION_obsgermany_climate_hourly_tu_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_9                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    TT_TU               Air temperature 2m              °C
                    RF_TU               Relative humidity 2m            %
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                air_temperature = (
                    ("air_temperature_quality_level", "int"),   # Quality level
                    ("air_temperature_200", "real"),            # Air temperature 2m
                    ("relative_humidity_200", "real"),          # Relative humidity 2m
                )

                """
                ================
                Soil temperature
                ================

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  several times a day
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/soil_temperature/recent/DESCRIPTION_obsgermany_climate_hourly_soil_temperature_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1949 - 31.12.2016
                    - Temporal resolution:  several times a day
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/soil_temperature/historical/DESCRIPTION_obsgermany_climate_hourly_soil_temperature_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_2                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    V_TE002             Soil temperature in   2 cm depth °C
                    V_TE005             Soil temperature in   5 cm depth °C
                    V_TE010             Soil temperature in  10 cm depth °C
                    V_TE020             Soil temperature in  20 cm depth °C
                    V_TE050             Soil temperature in  50 cm depth °C
                    V_TE100             Soil temperature in 100 cm depth °C
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                soil_temperature = (
                    ("soil_temperature_quality_level", "int"),  # Quality level
                    ("soil_temperature_002", "real"),  # Soil temperature 2cm
                    ("soil_temperature_005", "real"),  # Soil temperature 5cm
                    ("soil_temperature_010", "real"),  # Soil temperature 10cm
                    ("soil_temperature_020", "real"),  # Soil temperature 20cm
                    ("soil_temperature_050", "real"),  # Soil temperature 50cm
                    ("soil_temperature_100", "real"),  # Soil temperature 100cm
                )

                """
                =============
                Precipitation
                =============

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/DESCRIPTION_obsgermany_climate_hourly_precipitation_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.09.1995 - 31.12.2016
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/DESCRIPTION_obsgermany_climate_hourly_precipitation_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_8                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    R1                  Hourly precipitation height     mm
                    RS_IND              Precipitation indicator         0 no precipitation
                                                                        1 precipitation has fallen
                    WRTR                Form of precipitation           WR-code
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                The WRTR form of precipitation is only given at certain times, in accordance with SYNOP definition.
                Refer to daily values for more information on precipitation type. The classification of precipitation 
                type in the daily values differs from the classification for the hourly values.

                For the hourly values, the W_R definition (see Table 55, VUB 2 Band D, 2013) is used::

                    0   No fallen precipitation or too little deposition
                        (e.g., dew or frost) to form a precipitation height larger than 0.0
                    1   Precipitation height only due to deposition
                        (dew or frost) or if it cannot decided how large the part from deposition is
                    2   Precipitation height only due to liquid deposition
                    3   Precipitation height only due to solid precipitation
                    6   Precipitation height due to fallen liquid precipitation, may also include deposition of any kind
                    7   Precipitation height due to fallen solid precipitation, may also include deposition of any kind
                    8   Fallen precipitation in liquid and solid form
                    9   No precipitation measurement, form of precipitation cannot be determined.
                """
                precipitation = (
                    ("precipitation_quality_level", "int"),  # Quality level
                    ("precipitation_height", "real"),
                    ("precipitation_fallen", "bool"),
                    ("precipitation_form", "int"),
                )

                """
                ===
                Sun
                ===

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/recent/DESCRIPTION_obsgermany_climate_hourly_sun_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1893 - 31.12.2016
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/historical/DESCRIPTION_obsgermany_climate_hourly_sun_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_7                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    SD_SO               Hourly sunshine duration        min
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                sun = (
                    ("sun_quality_level", "int"),  # Quality level
                    ("sun_duration", "real"),  # Hourly sunshine duration
                )

                """
                ========
                Pressure
                ========

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/pressure/recent/DESCRIPTION_obsgermany_climate_hourly_pressure_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1949 - 31.12.2016
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/pressure/historical/DESCRIPTION_obsgermany_climate_hourly_pressure_historical_en.pdf


                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_8, QN_9          Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    P                   Mean sea level pressure         hPA
                    P0                  Pressure at station height      hPA
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                pressure = (
                    ("pressure_quality_level", "int"),  # Quality level
                    ("pressure_msl", "real"),           # Mean sea level pressure
                    ("pressure_station", "real"),       # Pressure at station height
                )

                """
                ====
                Wind
                ====

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/DESCRIPTION_obsgermany_climate_hourly_wind_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1893 - 31.12.2016
                    - Temporal resolution:  hourly
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/DESCRIPTION_obsgermany_climate_hourly_wind_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_3                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    F                   Mean wind speed                 m/s
                    D                   Mean wind direction             degrees
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                Nowadays, hourly wind speed and wind direction is given as the average of
                the six 10min intervals measured in the previous hour
                (e.g., at UTC 11, the average windspeed and average wind direction during UTC10-UTC11 is given).

                """
                wind = (
                    ("wind_quality_level", "int"),  # Quality level
                    ("wind_speed", "real"),  # Mean wind speed
                    ("wind_direction", "int"),  # Mean wind direction
                )

                """
                ==========
                Cloudiness
                ==========

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  several times a day
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent/DESCRIPTION_obsgermany_climate_hourly_cloudiness_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1949 - 31.12.2016
                    - Temporal resolution:  several times a day
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical/DESCRIPTION_obsgermany_climate_hourly_cloudiness_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_8                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    V_N_I               How measurement is taken        P: by human person
                                                                        I: by instrument
                    V_N                 Total cloud cover               1/8
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                cloudiness = (
                    ("cloudiness_quality_level", "int"),  # Quality level
                    ("cloudiness_source", "str"),  # How measurement is taken
                    ("cloudiness_total_cover", "int"),  # Total cloud cover
                )

                """
                ==========
                Visibility
                ==========

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  several times a day
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/visibility/recent/DESCRIPTION_obsgermany_climate_hourly_visibility_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1949 - 31.12.2016
                    - Temporal resolution:  several times a day
                    - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/visibility/historical/DESCRIPTION_obsgermany_climate_hourly_visibility_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_8                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    V_VV_I              How measurement is taken        P: by human person
                                                                        I: by instrument
                    V_VV                Visibility                      m
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                visibility = (
                    ("visibility_quality_level", "int"),  # Quality level
                    ("visibility_source", "str"),  # How measurement is taken
                    ("visibility_value", "int"),  # Visibility
                )

                """
                =====
                Solar
                =====

                Documentation
                -------------

                - Description:          Hourly station observations of solar incoming (total/diffuse) and longwave downward radiation for Germany
                - Temporal coverage:    01.01.1937 - month before last month
                - Temporal resolution:  hourly
                - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/solar/DESCRIPTION_obsgermany_climate_hourly_solar_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_592              Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    ATMO_STRAHL         Hourly sum of longwave          J/cm^2
                                        downward radiation
                    FD_STRAHL           Hourly sum of diffuse           J/cm^2
                                        solar radiation
                    FG_STRAHL           Hourly sum of solar             J/cm^2
                                        incoming radiation
                    SD_STRAHL           Hourly sum of                   min
                                        sunshine duration
                    ZENITH              Solar zenith angle at mid       degree
                                        of interval
                    MESS_DATUM_WOZ      End of interval in local        YYYYMMDDHH:mm
                                        true solar time
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                solar = (
                    ("solar_quality_level", "int"),         # Quality information
                    ("solar_atmosphere", "real"),           # Hourly sum of longwave downward radiation
                    ("solar_dhi", "real"),                  # Hourly sum of Diffuse Horizontal Irradiance (DHI)
                    ("solar_ghi", "real"),                  # Hourly sum of Global Horizontal Irradiance (GHI)
                    ("solar_sunshine", "int"),              # Hourly sum of sunshine duration
                    ("solar_zenith", "real"),               # Solar zenith angle at mid of interval
                    ("solar_end_of_interval", "datetime"),  # End of interval in local true solar time
                )

            # Temporal resolution: 10 minutes
            class minutes_10:

                # Which data set / resolution subfolder to use.
                __folder__ = "10_minutes"

                # Which format does the timestamp of this resolution have?
                __timestamp_format__ = "%Y%m%d%H%M"

                """
                ===============
                Air temperature
                ===============

                Documentation
                -------------

                - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/meta_data/

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN                  Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    PP_10               Pressure at station height      hPA
                    TT_10               Air temperature 2m              °C
                    TM5_10              Air temperature 5cm             °C
                    RF_10               Relative humidity 2m            %
                    TD_10               Dew point temperature 2m        °C
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                air_temperature = (
                    ("air_temperature_quality_level", "int"),       # Quality level
                    ("pressure_station", "real"),                   # Pressure at station height
                    ("air_temperature_200", "real"),                # Air temperature 2m
                    ("air_temperature_005", "real"),                # Air temperature 5cm
                    ("relative_humidity_200", "real"),              # Relative humidity 2m
                    ("dewpoint_temperature_200", "real"),           # Dew point temperature 2m
                )
                """
                =====
                Solar
                =====

                Documentation
                -------------

                - Description:          Recent 10-minute station observations of solar incoming radiation, longwavedownward radiation and sunshine duration for Germany
                - Temporal coverage:    rolling: 500 days before yesterday - month before last month
                - Temporal resolution:  10 minutes sampling interval
                - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/solar/recent/DESCRIPTION_obsgermany_climate_10min_solar_recent_en.pdf

                Fields
                ------
                ::

                    Field               Description                               Format or unit
                    STATIONS_ID         Station identification number             Integer
                    MESS_DATUM          Measurement time                          YYYYMMDDHH
                    QN                  Quality level                             Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    DS_10               10min-sum of diffuse solar radiation      J/cm^2
                    GS_10               10min-sum of solar incoming radiation     J/cm^2
                    SD_10               10min-sum of sunshine duration            h
                    LS_10               10min-sum of longwave downward radiation  J/cm^2
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                solar = (
                    ("solar_quality_level", "int"),         # Quality information
                    ("solar_dhi", "real"),                  # 10 minutes sum of Diffuse Horizontal Irradiance (DHI)
                    ("solar_ghi", "real"),                  # 10 minutes sum of Global Horizontal Irradiance (GHI)
                    ("solar_sunshine", "real"),             # 10 minutes sum of sunshine duration
                    ("solar_atmosphere", "real"),           # 10 minutes sum of longwave downward radiation
                )

        @classmethod
        def get_resolutions(cls):
            resolutions_map = OrderedDict()
            resolutions = DwdCdcKnowledge.as_dict(cls.resolutions)
            for name, class_ in resolutions.items():
                folder = class_.__folder__
                resolutions_map[folder] = class_
            return resolutions_map

        @classmethod
        def get_resolution_by_name(cls, resolution):
            resolutions_map = cls.get_resolutions()
            return resolutions_map.get(resolution)

    @classmethod
    def as_dict(cls, what):
        content = {}
        for entry in dir(what):
            if entry.startswith("__"):
                continue
            content[entry] = getattr(what, entry)
        return content
