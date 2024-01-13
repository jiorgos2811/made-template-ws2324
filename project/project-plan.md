# Project Plan

## Title
Urban Air Quality in Motion: Unraveling Traffic's Effect on Berlin's Air Quality in 2023

## Main Question
1. How big is the impact of traffic to the air quality of Berlin?

## Description
The declining quality of air in modern cities is an important problem that needs to be adressed. The project is motivated by the critical need to understand and mitigate the adverse health effects of air pollution in urban centers, especially those linked to vehicular emissions. The project will include exploratory data analysis to visualize and understand patterns, correlation analysis to identify pollutants most affected by traffic, and time series analysis to discern temporal trends. The results can give insights into the specific impact of traffic on air quality. This way we can provide local authorities and environmental organizations with data-driven insights for the implementation of measures aimed at reducing pollution and improving the quality of life for Berlin's residents.

## Datasources

### Datasource1: Verkehrsdetektion Berlin 
* Metadata URL: https://mobilithek.info/offers/-8644654323912256067
* Data URL: https://api.viz.berlin.de/daten/verkehrsdetektion?path=2023%2FMessquerschnitte+%28fahrtrichtungsbezogen%29%2F
* Data Type: CSV

The data from the Berlin traffic detection system is provided on a monthly basis as hourly values from the lane-accurate traffic detectors. 
Each csv archive contains the following data fields:

detid_15 - ID of the detector (15-digit number).
tag - date
stunde- hour of the day for which the measured values were determined (8 => 08:00 - 08:59).
qualitaet - indicates the percentage of faultless measurement intervals for the hour: 1.0 = 100%.
q_kfz_det_hr - Number of all motor vehicles in the hour.
v_kfz_det_hr - Average speed [km/h] over all motor vehicles in the hour.
q_pkw_det_hr - Number of all passenger cars per hour.
v_pkw_det_hr - Average speed [km/h] of all cars per hour.
q_lkw_det_hr - Number of all trucks per hour.
v_lkw_det_hr - Average speed [km/h] of all trucks per hour.

### Datasource2: Luftgütemessdaten
* Metadata URL: https://www.govdata.de/web/guest/suchen/-/details/luftgutemessdaten
* Data URL: https://luftdaten.berlin.de/pollution/pm10?stationgroup=all&period=1h&timespan=lastweek
* Data Type: CSV

This dataset includes information relevant to air pollutants with respect to the 39th BImSchV and is crucial for assessing air quality in Berlin. It encompasses gaseous air pollutants (nitrogen oxides, ozone, carbon monoxide, benzene, toluene, and sulfur dioxide) as well as particulate air pollutants (PM10 and PM2.5).

## Work Packages

1. Data Collection and Pipeline creation [#1][i1]
2. Data Preprocessing [#2][i2]
3. Data Analysis [#3][i3]
4. Automated Tests [#4][i4]

[i1]: https://github.com/jiorgos2811/made-template-ws2324/issues/1
[i2]: https://github.com/jiorgos2811/made-template-ws2324/issues/2
[i3]: https://github.com/jiorgos2811/made-template-ws2324/issues/3
[i4]: https://github.com/jiorgos2811/made-template-ws2324/issues/4
