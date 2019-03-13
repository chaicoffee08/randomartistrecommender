# randomartistrecommender
Random Similar Artist Recommender

This project builds on Chapter 3 of Advanced Analytics with Spark by Josh Wills, Sandy Ryza, Sean Owen, and Uri Laserson.  Updated for Spark 2.x and the DataFrames API. 

Download the complete audioscrobbler dataset from this link :
https://drive.google.com/drive/folders/14IbXISOKv8bJXUsoP45PPW6fV6vq1J8R?usp=sharing

Place the files 'user_artist_data.txt', 'artist_data.txt' and 'artist_alias.txt' from the folder 'Audioscrobbler_Dataset_Full' in your HDFS configuration at hdfs:///user/ds/user_artist_data.txt, hdfs:///user/ds/artist_data.txt and hdfs:///user/ds/artist_alias.txt. If you don't have a HDFS configuration you can download the sandbox at https://hortonworks.com/products/sandbox/

To build the project jar, you will need Maven. If you don't have it configured for your system, you can download it from https://maven.apache.org. Then, cd into the project directory 'audioscrobbler_recommender/ch03-recommender-chaiedit' and type the command 'mvn package'. This should give you the jar file 'ch03-recommender-chaiedit-2.0.0-jar-with-dependencies.jar' in the 'target' directory. Copy this to your HDFS file system and run the command:

spark-submit --class com.chai.datascience.recommender.RunRecommender --master yarn --deploy-mode client --driver-memory 4g ch03-recommender-chaiedit-2.0.0-jar-with-dependencies.jar 

This should launch the application.

The application works in three steps:

1) Finding a random artist from the dataset
2) Building an ALS model with the dataset
3) Finding the top five similar artists

Check the screenshots folder to check out an example of running the application and some of the artists trending in 2005! 

