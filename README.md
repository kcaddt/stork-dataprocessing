#### geo-data processing
##### Requirements  

Spark needs to be installed (as `preprocess.py` is written using spark python API).
The script runs in a matter of seconds on a (relatively) recent computer.

It might seems like an overkill to use spark  on small dataset but it guaranties scalability if the inputs data gets bigger.

##### Informations  
 I  added `iso-3166.csv` to get the matching table for the countries

The following files are not included in the repository:
 - `config.ini` *(`ApplicationID` and `APIKey` for `upload.py`)*
 - `inputs/alternateNamesV2.txt` *(wasn't commited due to Github file size limitations )*

##### Run   
Once you have installed Spark and the other packages of the `requirements.txt` file, first run

```
spark-submit preprocess.py
```
and then
```
python upload.py
```
