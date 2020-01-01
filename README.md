# The surreal weather repo

Answers the question "How much weather will there be in the future?".


     /=//==//=/  \
    |=||==||=|    |
    |=||==||=|~-, |
    |=||==||=|^.`;|
     \=\\==\\=\`=.:
      `"""""""`^-,`.
               `.~,'
              ',~^:,
              `.^;`.
               ^-.~=;.
                  `.^.:`.
                  

## Data sources

* [European climate assessment & data set](https://www.ecad.eu//dailydata/index.php)
* [European center for medium range weather forecast (ECMWF)](https://www.ecmwf.int/en/forecasts/datasets)
* [Met office](https://www.metoffice.gov.uk/services/data/met-office-data-for-reuse) (requires contacting)
* [Deutscher Wetterdienst](https://www.dwd.de/DE/leistungen/klimadatendeutschland/klimadatendeutschland.html)
    * [locations of weather stations](https://www.dwd.de/DE/leistungen/klimadatendeutschland/statliste/statlex_html.html?view=nasPublication&nn=16102)
* [BigQuery](https://cloud.google.com/blog/products/gcp/global-historical-daily-weather-data-now-available-in-bigquery) 

## Other references

* [talk on public climate data](https://fahrplan.events.ccc.de/congress/2019/Fahrplan/events/10571.html) @ chaos computer congress 36 (german only) => [pdf with links](https://fahrplan.events.ccc.de/congress/2019/Fahrplan/system/event_attachments/attachments/000/004/052/original/Ressources.pdf)

## Geo plotting libraries

* geopandas:
    * [medium article](https://towardsdatascience.com/geopandas-101-plot-any-data-with-a-latitude-and-longitude-on-a-map-98e01944b972)
    * [official documentation](http://geopandas.org/index.html) 
    
## Analytics setup

* Installing MySQL: [getting started](https://dev.mysql.com/doc/mysql-getting-started/en/)
* Installing [Power BI Desktop](https://powerbi.microsoft.com/en-us/desktop/)
* Executing
```shell script
pip install mysql-connector-python
pip install SQLAlchemy
```
* Python & MySQL: 
    * [w3schools](https://www.w3schools.com/python/python_mysql_getstarted.asp)
    * SQLAlchemy & MySQL: [official documentation](https://docs.sqlalchemy.org/en/13/dialects/mysql.html), [quick how to blog](https://pythondata.com/quick-tip-sqlalchemy-for-mysql-and-pandas/) 


--> MySQL workbench to monitor the data base + Power BI to inspect the generated data set