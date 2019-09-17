# Firefox Desktop Support Dashboard 

This folder contains scripts and other files necessary to collect and process the data presented in the Firefox Desktop Support Dashboard. This README provides an in-depth description of the data collected, how this is done and how the data is processed and enhanced before it is stored in the dashboard ready Google Big Query tables.  

The dashboard draws on data from a variety of data sources: CSAT (SurveyGizmo), Google Trends, Twitter, Mozilla Support (Kitsune) and Google Analytics. Each of these requires slightly different data processing and this is described in detail below. Common for all data sources is that the end results are saved in a series of Google Big Query tables which the final dashboard draws on.     

## Data source overview

In this section we give an overview what data is collected and how it is processed. For each data source we also describe what types of insights it provides.  

![All sources](data_sources.png)


### Google Trends 

From Google Trends we collect all related queries for the term 'Firefox'. This is done weekly and for the following regions in the world ['', 'US', 'DE', 'IN', 'FR', 'RU', 'IT', 'BR', 'PL', 'CN', 'NL', 'JP', 'ES', 'ID']. 
The data is collected once a week and the related queries are accompanied by a search increase score. This score is a percentage increase in search volume in this week compared to the previous week. Thus the 'Firefox monitor alert' at 8800 % means that there is an 8800 % increase in search volume of this term compared to the previous week. As such the number presented are relative and the google trends pseudo api has no way to determine absolute search volume and comparisons across different versions. 

For tidligt? 

### Twitter

The other module takes care of getting all twitter mentions that we are interested in. This module takes care of reading these tables, filtering out all non-english tweets, classifying these and running a sentiment analysis on each tweet. This enhanced data is then save in seperate gbq tables for use in the dashboard. 

Tweets are filtered out if the google language api returns a less than 80 % chance of the content being non-english. The tweets are then run through a sentiment analysis use the google sentiment api. This returns two scores which are combined in a simple Positive, Negative, Neutral class. 

Finally the tweets are classified by topic. This is done using a regex match on the topics found in the file ...
This is done dynamically so when the file changes in the repo this affects the topics at runtime.  

All this data is saved in dedicated gbq tables for use in the dashboard. 

### Kitsune api

From kitsune we get the SUMO questions. 
Again we filter out non-english questions. Also using the language api. 
The topics are reused from kitsune. 

Again we use the sentiment api to determine a Positive, Negative or Neutral sentiment. 


### Google Analytics

From google analytics we get the page views and exit rates from the support forum for questions and kb articles. 

### Firefox Customer Satisfaction Score

From here we get the results of the CSAT. 

## Data processing 

### Google Trends 

### Twitter

This data is collected using the pytrends module. This is an unofficial API ...

### Google Analytics

### Firefox Customer Satisfaction Score


Uses Google Analytics Reporting API v4 to pull dimensions and metrics for the Google Analytics SUMO report.

https://developers.google.com/analytics/devguides/reporting/core/v4/rest/

Make sure Analytics Reporting API is enabled in the GCP running the code.
A valid service account should be permissioned to pull data from the SUMO report from the Google Analytics side.
GoogleAnalytics/create_ga_tables.py creates Google Analytics BiqQuery tables with schema definition.
GoogleAnalytics/get_ga_data.py pulls data for a given range. The data is written to local csv files in /tmp folder, and pushed to google storage gs://<sumo-bucket>/googleanalytics/. The google storage files are uploaded to BigQuery dataset sumo table ga_*. After upload, the files are moved to the /processed subfolder.  Some of the data pulls hit daily data limits so it is recommend to run data pulls in one month chunks. 



## Installing / Getting started

The scripts are intended to be run on a Google Cloud Project with necessary account permissions. 

Assumes Google storage folder structure:
```shell
gs:// <sumo-bucket>  
    / googleanalytics => where google analytics data files are initially placed
    / googleanalytics / processed => where processed google analytics data files are placed after being uploaded to BigQuery
    / googleplaystore => where google  data files are initially placed [deprecated]
    / googleplaystore / processed => where processed google analytics data files are placed after being uploaded to BigQuery [deprecated]
    / tmp => model param files, aggregation files in subfolder by model pararm
gs:// <data-bucket> => location of parquet input data files
```

```shell
packagemanager install awesome-project
awesome-project start
awesome-project "Do something!"  # prints "Nah."
```

Here you should say what actually happens when you execute the code above.

### Initial Configuration

Some projects require initial configuration (e.g. access tokens or keys, `npm i`).
This is the section where you would document those requirements.

## Developing

Here's a brief intro about what a developer must do in order to start developing
the project further:

```shell
git clone https://github.com/your/awesome-project.git
cd awesome-project/
packagemanager install
```

And state what happens step-by-step.

### Units Tests

```shell
python setup.py test
```
Sigh, maybe someday.


## Features

What's all the bells and whistles this project can perform?
* What's the main functionality
* You can also do another thing
* If you get really randy, you can even do this


#### Argument 1
Type: `String`  
Default: `'default value'`

State what an argument does and how you can use it. If needed, you can provide
an example below.

Example:
```bash
awesome-project "Some other value"  # Prints "You're nailing this readme!"
```

#### Argument 2
Type: `Number|Boolean`  
Default: 100

Copy-paste as many of these as you need.

## Caveats 
We have to wait a lot when using the apis 


## Links

Even though this information can be found inside the project on machine-readable
format like in a .json file, it's good to include a summary of most useful
links to humans using your project. You can include links like:

- Project homepage: https://your.github.com/awesome-project/
- Repository: https://github.com/your/awesome-project/
- Issue tracker: https://github.com/your/awesome-project/issues
  - In case of sensitive bugs like security vulnerabilities, please contact
    my@email.com directly instead of using issue tracker. We value your effort
    to improve the security and privacy of this project!
- Related projects:
  - Your other project: https://github.com/your/other-project/
  - Someone else's project: https://github.com/someones/awesome-project/


## Licensing
Licensed under ... For details, see the LICENSE file.


![SUMO logo](https://github.com/ophie200/sumo/blob/master/images/SUMO-logo.png)
