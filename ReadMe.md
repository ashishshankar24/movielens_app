Assumptions:
This application is developed assuming the file will be delivered to the source location on daily basis.

Terminologies:
Conformed zone: Where the final tables are stored
Source location: Where the input csv will be received

Execution command:

Update the config file with input path and conformed path.

   spark-submit --files <config_location>\app_config.json <code_location>\main.py mode (full/delta)

   spark-submit --files C:\Users\Ashish\PycharmProjects\Movielens\src\config\app_config.json C:\Users\Ashish\PycharmProjects\Movielens\src\conformed\main.py full_

There are two mode of execution
1) Full mode -> 
   1) This is one time execution after a prod deployment or any major change in the business logic which require fresh data need to be loaded.
   2) Full load of the data available in source location to destination 

2) Delta mode ->
   1) This mode of execution should be used daily.
   2) Performs delta operation on the tables created

Deployments:
The current application is created keeping in mind as First Iteration of Development.

    Iteration 1 features:
        1) Extraction of Ratings and Movies file .
        2) Transformation of split_genres and rating average calculator
        3) Unit test cases

    Upcoming features:
        1) Tags file extraction and load to conformed zone.
        2) Performance enhancements.