# randomartistrecommender
Random Similar Artist Recommender

This project builds on chapter 3 of Advanced Analytics with Spark by Josh Wills, Sandy Ryza, Sean Owen, and Uri Laserson. Updated for Spark 2.X and the DataFrames API. 

Place the files 'user_artist_data.txt', 'artist_data.txt' and 'artist_alias.txt' from the folder 'Audioscrobbler_Dataset_Full' in your HDFS configuration at hdfs:///user/ds/user_artist_data.txt, dfs:///user/ds/artist_data.txt and dfs:///user/ds/artist_alias.txt. If you don't have a HDFS configuration you can download the sandbox at https://hortonworks.com/products/sandbox/

cd into the project directory ch03-recommender-chaiedit and type the command 'mvn package'. This should give you the jar file ch03-recommender-chaiedit-2.0.0-jar-with-dependencies.jar in the target directory. copy this to your HDFS file system and run the command :

spark-submit --class com.chai.datascience.recommender.RunRecommender --master yarn --deploy-mode client --driver-memory 4g ch03-recommender-chaiedit-2.0.0-jar-with-dependencies.jar 

This should launch the application!

Check the screenshots folder to check out some of the artists trending in 2005.

