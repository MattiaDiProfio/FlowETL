# Datasets factsheet

Here you will find the following information about all files within the `/datasets` folder. This file also contains the ground truth (GT) for all datasets

### Structured files

1. **Chess Games**

    - source file name - chess_games_source.csv
    - target file name - chess_games_target.csv
    - objects count - 20058
    - domain - board games, sports analytics
    - link - https://www.kaggle.com/datasets/datasnaek/chess
    - columns - `id,rated,created_at,last_move_at,turns,victory_status,winner,increment_code,white_id,white_rating,black_id,black_rating,moves,opening_eco,opening_name,opening_ply` 
    - transformations outline 
        - id : game_id
        - rated : is_rated
        - created_at : start_time
        - last_moved_at : end_time
        - turns : turns_taken
        - victory_status : victory_status
        - winner : winner
        - white_id : w_id
        - write_rating : w_rating
        - black_id : b_id
        - black_rating : b_rating
        - moves : moves_sequence
        - opening_name : opening_strategy_name
        - created : n/a
        - dropped : last_move_at, increment_code, opening_eco, opening_ply
        - is_rated : TRUE/FALSE mapped to 1/0
        - winner : black/white mapped to b/w
        - moves_sequence : moves formatted as "move1-move2-...-moveN"

2. **E-commerce Transactions**

    - source file name - ecommerce_transactions_source.csv
    - target file name - ecommerce_transactions_target.csv
    - objects count - 541909
    - domain - business, finance
    - link - https://www.kaggle.com/datasets/aliessamali/ecommerce
    - columns - `InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country`
    - transformations outline 
        - InvoiceNo : invoice_number
        - StockCode : stock_code
        - Description : desc
        - Quantity : qty
        - InvoiceDate : invoice_date 
        - UnitPrice : unit_price
        - CustomerID : customer_id
        - Country : country
        - created : n/a
        - dropped : n/a
        - desc : convert to lowercase
        - invoice_date : format to dd-mm-yy hh-m-s
        - country : convert to lower case

3. **Financial Compliance Audits**

    - source file name - financial_compliance_source.csv
    - target file name - financial_compliance_target.csv
    - objects count - 100
    - domain - finance, legal
    - link - https://www.kaggle.com/datasets/atharvasoundankar/big-4-financial-risk-insights-2020-2025
    - columns - `Year,Firm_Name,Total_Audit_Engagements,High_Risk_Cases,Compliance_Violations,Fraud_Cases_Detected,Industry_Affected,Total_Revenue_Impact,AI_Used_for_Auditing,Employee_Workload,Audit_Effectiveness_Score,Client_Satisfaction_Score`
    - transformations outline 
        - Year : year
        - Firm_Name : firm_name
        - Total_Audit_Engagements : total_audit_engagement
        - High_Risk_Cases : high_risk_cases_count
        - Compliance_Violations : compliance_violations_count
        - Fraud_Cases_Detected : fraud_cases_count
        - Industry_Affected : industry_affected
        - Total_Revenue_Impact : revenue_impact_millions
        - AI_Used_for_Auditing : ai_used
        - Employee_Workload : employee_workload_percent
        - Audit_Effectiveness_Score : audit_impact_score
        - Client_Satisfaction_Score : client_satisfaction_score
        - dropped : n/a
        - created : overall_score
        - ai_used : Yes/No mapped to 1/0
        - overall_score : average of audit_impact_score and client_satisfaction_score


4. **Fictional Netflix Users Profiles**

    - source file name - netflix_users_source.csv
    - target file name - netflix_users_target.csv
    - objects count - 25000
    - domain - entertainment
    - link - https://www.kaggle.com/datasets/smayanj/netflix-users-database
    - data dictionary - `User_ID,Name,Age,Country,Subscription_Type,Watch_Time_Hours,Favorite_Genre,Last_Login`
    - transformations outline
        - User_ID : id
        - Age : age
        - Country : country_name
        - Subscription_Type : subscription
        - Watch_Time_Hours : watch_time_hours
        - Favorite_Genre : favourite_genre
        - Last_Login : last_access
        - dropped : Name
        - created : first_name, last_name
        - Name is split into two new columns, first_name and last_name
        - watch_time_hours is rounded up to the nearest 10
        - last_access is formatted as dd-month_name-yy

5. **Pixar Films**

    - source file name - pixar_films_source.csv
    - target file name - pixar_films_target.csv
    - objects count - 28
    - domain - entertainment
    - link - https://www.kaggle.com/datasets/willianoliveiragibin/pixar-films
    - data dictionary - `ID,film,film_rating,cinema_score,release_date,run_time,budget,box_office_us_canada,box_office_other,box_office_worldwide,rotten_tomatoes_score,rotten_tomatoes_counts,metacritic_score,metacritic_counts,imdb_score,imdb_counts`
    - transformations outline 
        - ID : film_id
        - film : film_name
        - film_rating : watch_rating
        - cinema_score : cinema_score
        - release_date : released_date
        - run_time : run_time_minutes
        - budget : budget_millions
        - box_office_worldwide : total_box_office
        - rotten_tomatoes_score : rotten_tomatoes_score
        - rotten_tomatoes_counts : rotten_tomatoes_counts_millions
        - metacritic_score : metacritic_score
        - metacritic_counts : metacritic_counts_millions
        - imdb_score : imdb_score
        - imdb_counts : imdb_counts_millions
        - created : n/a
        - dropped : box_office_us_canada, box_office_other
        - released_date is formatted yyy-month_name-dd
        - budget_millions is the original budget expressed in millions
        - rotten_tomatoes_counts, metacritic_counts, imdb_counts are expressed in millions

6. **Smartwatch Health Readings**

    - source file name - smartwatch_health_data_source.csv
    - target file name - smartwatch_health_data_target.csv
    - objects count - 10000
    - domain - health and fitness
    - link - https://www.kaggle.com/datasets/mohammedarfathr/smartwatch-health-data-uncleaned
    - data dictionary - `User ID,Heart Rate (BPM),Blood Oxygen Level (%),Step Count,Sleep Duration (hours),Activity Level,Stress Level`
    - transformations outline
        - User ID : id
        - Heart Rate (BPM) : heart_rate_bpm
        - Blood Oxygen Level (%) : blood_oxygen_level_% 
        - Step Count : step_count
        - Sleep Duration (hours) : hours_slept
        - Activity Level : activity_level
        - Stress Level : stress_level
        - created : n/a
        - dropped : n/a
        - heart_rate_bpm, blood_oxygen_level_%, hours_slept : all rounded to 1 decimal place
        - step_count : rounded to nearest integer
        - activity_level : correct any spelling mistakes and conver to lower case
        - stress level : map values 1-3 to low, 4-7 medium, 8+ high

7. **Amazon Stock Price**

    - source file name - amazon_stock_data_source.csv
    - target file name - amazon_stock_data_target.csv
    - objects count - 6321
    - domain - finance
    - link - https://www.kaggle.com/datasets/henryshan/amazon-com-inc-amzn
    - data dictionary - `date,open,high,low,close,adj_close,volume`
    - transformations outline
        - date : date
        - open : open_price
        - high : daily_high
        - low : daily_low
        - close : close_price
        - volume : trade_volume_millions
        - dropped : adj_close
        - created : n/a
        - date is formatted to yyyy/mm/dd
        - open_price, daily_high, daily_low, close_price are all rounded to 3 decimal places
        - trade_volume_millions is expressed in millions    


### Unstructured files

1. **Amazon Reviews**

    - source file name - amazon_reviews_source.json
    - target file name - amazon_reviews_target.json
    - objects count - 696 (reduced from original 194438)
    - domain - retail
    - link - https://www.kaggle.com/datasets/abdallahwagih/amazon-reviews
    - keys - `[reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime, reviewTime]`
    - transformations outline
        - reviewerID : reviewer_id
        - asin : amazon_id
        - reviewerName : user_name,
        - reviewTime : review_date, 
        - overall : overall_review_score
        - created : n/a
        - dropped : helpful, reviewText, unixReviewTime
        - review_date is formatted as dd-mm-yyyy
        - summary is lowercased
    

2. **Flight Routes**

    - source file name - flight_routes_source.json
    - target file name - flight_routes_target.json
    - objects count - 908
    - domain - logistics
    - link - https://www.kaggle.com/datasets/jacekpardyak/openflights?select=routes.json
    - keys - `[src, dst, src_lat, src_lon, dst_lat, dst_lon]`
    - transformations outline
        - src : source
        - dst : destination
        - src_lat : source_latitude
        - src_lon : source_longitude
        - dst_lat : destination_latitude
        - dst_lon : destination_longitude
        - created : source_airport_code, destination_airport_code
        - dropped : n/a
        - source, destination only contain the first word of the string
        - source_airport_code, destination_airport_code contain the airport code for source and destination  

3. **News Categories**

    - source file name - news_categories_source.json
    - target file name - news_categories_target.json
    - objects count - 212 (reduced from original 200k+)
    - domain - media
    - link - https://www.kaggle.com/datasets/rmisra/news-category-dataset
    - keys - `[category, headline, authors, link, short_description, date]`
    - transformations outline
        - category : category
        - headline : headline
        - authors : authors
        - link : article_link
        - date : published_date
        - dropped : short_description
        - created : n/a
        - category is lowercased
        - date is formatted as dd-month_name-yyyy

4. **Food Recipes**

    - source file name - recipes_source.json
    - target file name - recipes_target.json
    - objects count - 335
    - domain - nutrition
    - link - https://www.kaggle.com/datasets/kaggle/recipe-ingredients-dataset
    - keys - `[id,cuisine,ingredients]`
    - transformations outline
        - id : recipe_id
        - cuisine : cuisine
        - ingredients : ingredients_list
        - dropped : n/a
        - created : n/a
        - ingredients is merged into a string "x, y, z, ..."    

5. **Social Media Posts**

    - source file name - social_media_posts_source.json
    - target file name - social_media_posts_target.json
    - objects count - 60
    - domain - media
    - link - https://www.kaggle.com/datasets/prishatank/post-generator-dataset
    - keys outline
        - text : post_content
        - engagement : user_engagement
        - language : post_language
        - tags : post_tags
        - tone : tone
        - dropped : line_count
        - created : post_character_count
        - post_character_count is the length of the text field
        - tags is joined into a string, separated by commas
        - tone is lowercased 

6. **Student Grades**

    - source file name - students_grades_source.json
    - target file name - students_grades_target.json
    - objects count - approx. 5000
    - domain -  education
    - link - https://www.kaggle.com/datasets/mahmoudelhemaly/students-grading-dataset?select=Students_Grading_Dataset.json
    - keys - `[Student_ID, First_Name, Last_Name, Email, Gender, Age, Department, Attendance (%), Midterm_Score, Final_Score, Assignments_Avg, Quizzes_Avg, Participation_Score, Projects_Score, Total_Score, Grade, Study_Hours_per_Week, Extracurricular_Activities, Internet_Access_at_Home, Parent_Education_Level, Family_Income_Level, Stress_Level (1-10), Sleep_Hours_per_Night]`
    - transformations outline
        - Student_ID : student_id
        - Email : student_email
        - Gender : student_gender
        - Age : student_age
        - Department : dept
        - Attendance (%) : attendance_percent
        - Midterm_Score : midterm_score
        - Final_Score : final_score
        - Assignments_Avg : assignments_avg
        - Quizzes_Avg : quizzes_avg
        - Projects_Score : projects_score
        - Total_Score : student_total_score
        - Grade : grade
        - Study_Hours_per_Week : weekly_study_hours
        - Extracurricular_Activities : takes_extracurriculars
        - Internet_Access_at_Home : has_home_internet
        - Parent_Education_Level
        - Family_Income_Level
        - Stress_Level (1-10)
        - Sleep_Hours_per_Night
        - dropped : first_name, last_name, Participation_Score
        - created : full_name
        - full_name joins student first name and lastname
        - has_home_internet, takes_extracurriculars maps from Yes/No to 1/0