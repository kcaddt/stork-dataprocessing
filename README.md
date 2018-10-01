#### geo-data processing
##### Requirements  

Spark needs to be installed (as `preprocess.py` is written using spark python API).
The script runs in a matter of seconds on a (relatively) recent computer.

It might seems like an overkill to use spark on small dataset but I am more familiar with spark rather  pandas  these days. It also guaranties scalability if the inputs data gets bigger (even if one might argue against it).

##### Informations  
 I  added `iso-3166.csv` to get the matching table for the countries

The following files are not included in the repository:
 - `config.ini` *(`ApplicationID` and `APIKey` for `upload.py`)*
 - `inputs/alternateNamesV2.txt` *(wasn't commited due to Github file size limitations )*

##### Remarks about postal codes
Writing this documentation, I remembered I choose to limit the postal codes to 10. While not being necessary, this choice was motivated by the fact that some cities have up to 200 postal codes. I could not bring myself to just groupBy the postal codes by `geonameid` and then to do the join. I wanted to leverage the `isPreferredName` parameter from `alternateNamesV2.txt` to end up with a more relevant dataset. So after doing that I didn't want to upload the remaining cities with 250 postal codes, hence the limitation of 10.

This being said, I realize it is an arguable choice considering it does not goes towards data completeness.

##### Run   
Once you have installed Spark and the other packages of the `requirements.txt` file, first run

```
spark-submit preprocess.py
```
and then
```
python upload.py
```
