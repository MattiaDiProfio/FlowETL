# Datasets factsheet

Here you will find the following information about all files within the `/evaluation` folder. This file also contains the ground truth (GT) for all datasets.

### Structured files (CSV)

1. **Chess Games**

    - source file name - chess_games_source.csv  
    - target file name - chess_games_target.csv  
    - objects count - 20058  
    - domain - board games, sports analytics  
    - link - https://www.kaggle.com/datasets/datasnaek/chess  
    - transformations outline  

    | Source                                         | Target                      | Transformation                                                    |
    |-----------------------------------------------|-----------------------------|---------------------------------------------------------------------|
    | id                                             | game_id                     |                                                                     |
    | rated                                          | is_rated                    | map TRUE/FALSE to 1/0                                              |
    | created_at                                     | start_time                  |                                                                     |
    | last_moved_at                                  | end_time                    |                                                                     |
    | turns                                          | turns_taken                 |                                                                     |
    | victory_status                                 | victory_status              |                                                                     |
    | winner                                         | winner                      | map "black"/"white" to "b"/"w"                                     |
    | white_id                                       | w_id                        |                                                                     |
    | write_rating                                   | w_rating                    |                                                                     |
    | black_id                                       | b_id                        |                                                                     |
    | black_rating                                   | b_rating                    |                                                                     |
    | moves                                          | moves_sequence              | merge into dash-separated string                                   |
    | opening_name                                   | opening_strategy_name       |                                                                     |
    | last_move_at, increment_code, opening_eco, opening_ply |                       |                                                              |

2. **E‑commerce Transactions**

    - source file name - ecommerce_transactions_source.csv  
    - target file name - ecommerce_transactions_target.csv  
    - objects count - 541909  
    - domain - business, finance  
    - link - https://www.kaggle.com/datasets/aliessamali/ecommerce  
    - transformations outline  

    | Source      | Target         | Transformation                   |
    |-------------|----------------|----------------------------------|
    | InvoiceNo   | invoice_number |                                  |
    | StockCode   | stock_code     |                                  |
    | Description | desc           |                                  |
    | Quantity    | qty            |                                  |
    | InvoiceDate | invoice_date   | format to dd-mm-yy hh-m-s        |
    | UnitPrice   | unit_price     |                                  |
    | CustomerID  | customer_id    |                                  |
    | Country     | country        | convert to lower case            |
    | desc        | desc           | convert to lowercase             |

3. **Financial Compliance Audits**

    - source file name - financial_compliance_source.csv  
    - target file name - financial_compliance_target.csv  
    - objects count - 100  
    - domain - finance, legal  
    - link - https://www.kaggle.com/datasets/atharvasoundankar/big-4-financial-risk-insights-2020-2025  
    - transformations outline  

    | Source                   | Target                       | Transformation                                             |
    |--------------------------|------------------------------|------------------------------------------------------------|
    | Year                     | year                         |                                                            |
    | Firm_Name                | firm_name                    |                                                            |
    | Total_Audit_Engagements  | total_audit_engagement       |                                                            |
    | High_Risk_Cases          | high_risk_cases_count        |                                                            |
    | Compliance_Violations    | compliance_violations_count  |                                                            |
    | Fraud_Cases_Detected     | fraud_cases_count            |                                                            |
    | Industry_Affected        | industry_affected            |                                                            |
    | Total_Revenue_Impact     | revenue_impact_millions      |                                                            |
    | AI_Used_for_Auditing     | ai_used                      | map Yes/No to 1/0                                          |
    | Employee_Workload        | employee_workload_percent    |                                                            |
    | Audit_Effectiveness_Score| audit_impact_score           |                                                            |
    | Client_Satisfaction_Score| client_satisfaction_score    |                                                            |
    |                          | overall_score                | average of audit_impact_score and client_satisfaction_score|

4. **Fictional Netflix Users Profiles**

    - source file name - netflix_users_source.csv  
    - target file name - netflix_users_target.csv  
    - objects count - 25000  
    - domain - entertainment  
    - link - https://www.kaggle.com/datasets/smayanj/netflix-users-database  
    - transformations outline  

    | Source            | Target                   | Transformation                                 |
    |-------------------|--------------------------|------------------------------------------------|
    | User_ID           | id                       |                                                |
    | Age               | age                      |                                                |
    | Country           | country_name             |                                                |
    | Subscription_Type | subscription             |                                                |
    | Watch_Time_Hours  | watch_time_hours         | round up to the nearest 10                     |
    | Favorite_Genre    | favourite_genre          |                                                |
    | Last_Login        | last_access              | format as dd-month_name-yy                     |
    | Name              | first_name, last_name    | split Name into first_name and last_name       |

5. **Pixar Films**

    - source file name - pixar_films_source.csv  
    - target file name - pixar_films_target.csv  
    - objects count - 28  
    - domain - entertainment  
    - link - https://www.kaggle.com/datasets/willianoliveiragibin/pixar-films  
    - transformations outline  

    | Source                  | Target                            | Transformation                                    |
    |-------------------------|-----------------------------------|---------------------------------------------------|
    | ID                      | film_id                           |                                                   |
    | film                    | film_name                         |                                                   |
    | film_rating             | watch_rating                      |                                                   |
    | cinema_score            | cinema_score                      |                                                   |
    | release_date            | released_date                     | format as yyyy-month_name-dd                     |
    | run_time                | run_time_minutes                  |                                                   |
    | budget                  | budget_millions                   | convert to millions                               |
    | box_office_worldwide    | total_box_office                  |                                                   |
    | rotten_tomatoes_score   | rotten_tomatoes_score             |                                                   |
    | rotten_tomatoes_counts  | rotten_tomatoes_counts_millions   | express in millions                               |
    | metacritic_score        | metacritic_score                  |                                                   |
    | metacritic_counts       | metacritic_counts_millions        | express in millions                               |
    | imdb_score              | imdb_score                        |                                                   |
    | imdb_counts             | imdb_counts_millions              | express in millions                               |
    | box_office_us_canada, box_office_other |                |                                                   |

6. **Smartwatch Health Readings**

    - source file name - smartwatch_health_data_source.csv  
    - target file name - smartwatch_health_data_target.csv  
    - objects count - 10000  
    - domain - health and fitness  
    - link - https://www.kaggle.com/datasets/mohammedarfathr/smartwatch-health-data-uncleaned  
    - transformations outline  

    | Source                       | Target                   | Transformation                                             |
    |------------------------------|--------------------------|------------------------------------------------------------|
    | User ID                      | id                       |                                                            |
    | Heart Rate (BPM)             | heart_rate_bpm           | round to 1 decimal place                                   |
    | Blood Oxygen Level (%)       | blood_oxygen_level_%     | round to 1 decimal place                                   |
    | Step Count                   | step_count               | round to nearest integer                                   |
    | Sleep Duration (hours)       | hours_slept              | round to 1 decimal place                                   |
    | Activity Level               | activity_level           | correct spelling mistakes, convert to lower case           |
    | Stress Level                 | stress_level             | map 1–3 → low, 4–7 → medium, 8+ → high                     |

7. **Amazon Stock Price**

    - source file name - amazon_stock_data_source.csv  
    - target file name - amazon_stock_data_target.csv  
    - objects count - 6321  
    - domain - finance  
    - link - https://www.kaggle.com/datasets/henryshan/amazon-com-inc-amzn  
    - transformations outline  

    | Source    | Target                   | Transformation                     |
    |-----------|--------------------------|------------------------------------|
    | date      | date                     | format to yyyy/mm/dd               |
    | open      | open_price               | round to 3 decimal places          |
    | high      | daily_high               | round to 3 decimal places          |
    | low       | daily_low                | round to 3 decimal places          |
    | close     | close_price              | round to 3 decimal places          |
    | volume    | trade_volume_millions    | express in millions                |
    | adj_close |                          |                                    |

### Unstructured files (JSON)

1. **Amazon Reviews**

    - source file name - amazon_reviews_source.json  
    - target file name - amazon_reviews_target.json  
    - objects count - 696 (reduced from original 194438)  
    - domain - retail  
    - link - https://www.kaggle.com/datasets/abdallahwagih/amazon-reviews  
    - transformations outline  

    | Source       | Target                | Transformation                     |
    |--------------|-----------------------|------------------------------------|
    | reviewerID   | reviewer_id           |                                    |
    | asin         | amazon_id             |                                    |
    | reviewerName | user_name             |                                    |
    | reviewTime   | review_date           | format as dd-mm-yyyy               |
    | overall      | overall_review_score  |                                    |
    | summary      | summary               | convert to lowercase               |
    | helpful, reviewText, unixReviewTime |            |                                    |

2. **Flight Routes**

    - source file name - flight_routes_source.json  
    - target file name - flight_routes_target.json  
    - objects count - 908  
    - domain - logistics  
    - link - https://www.kaggle.com/datasets/jacekpardyak/openflights?select=routes.json  
    - transformations outline  

    | Source     | Target                       | Transformation                               |
    |------------|------------------------------|----------------------------------------------|
    | src        | source                       | extract first word of the string             |
    | dst        | destination                  | extract first word of the string             |
    | src_lat    | source_latitude              |                                              |
    | src_lon    | source_longitude             |                                              |
    | dst_lat    | destination_latitude         |                                              |
    | dst_lon    | destination_longitude        |                                              |
    |            | source_airport_code          | extract airport code from source             |
    |            | destination_airport_code     | extract airport code from destination        |

3. **News Categories**

    - source file name - news_categories_source.json  
    - target file name - news_categories_target.json  
    - objects count - 212 (reduced from original 200k+)  
    - domain - media  
    - link - https://www.kaggle.com/datasets/rmisra/news-category-dataset  
    - transformations outline  

    | Source            | Target            | Transformation                 |
    |-------------------|-------------------|--------------------------------|
    | category          | category          | convert to lowercase           |
    | headline          | headline          |                                |
    | authors           | authors           |                                |
    | link              | article_link      |                                |
    | date              | published_date    | format as dd-month_name-yyyy   |
    | short_description |                   |                                |

4. **Food Recipes**

    - source file name - recipes_source.json  
    - target file name - recipes_target.json  
    - objects count - 335  
    - domain - nutrition  
    - link - https://www.kaggle.com/datasets/kaggle/recipe-ingredients-dataset  
    - transformations outline  

    | Source      | Target             | Transformation                      |
    |-------------|--------------------|-------------------------------------|
    | id          | recipe_id          |                                     |
    | cuisine     | cuisine            |                                     |
    | ingredients | ingredients_list   | merge into a comma-separated string|

5. **Social Media Posts**

    - source file name - social_media_posts_source.json  
    - target file name - social_media_posts_target.json  
    - objects count - 60  
    - domain - media  
    - link - https://www.kaggle.com/datasets/prishatank/post-generator-dataset  
    - transformations outline  

    | Source      | Target                | Transformation                             |
    |-------------|-----------------------|--------------------------------------------|
    | text        | post_content          |                                            |
    | engagement  | user_engagement       |                                            |
    | language    | post_language         |                                            |
    | tags        | post_tags             | join into a string, separated by commas    |
    | tone        | tone                  | convert to lowercase                       |
    | line_count  |                       |                                            |
    |             | post_character_count  | set as length of post_content field        |

6. **Student Grades**

    - source file name - students_grades_source.json  
    - target file name - students_grades_target.json  
    - objects count - approx. 5000  
    - domain - education  
    - link - https://www.kaggle.com/datasets/mahmoudelhemaly/students-grading-dataset?select=Students_Grading_Dataset.json  
    - transformations outline  

    | Source                     | Target                       | Transformation                                             |
    |----------------------------|------------------------------|------------------------------------------------------------|
    | Student_ID                 | student_id                   |                                                            |
    | Email                      | student_email                |                                                            |
    | Gender                     | student_gender               |                                                            |
    | Age                        | student_age                  |                                                            |
    | Department                 | dept                         |                                                            |
    | Attendance (%)             | attendance_percent           |                                                            |
    | Midterm_Score              | midterm_score                |                                                            |
    | Final_Score                | final_score                  |                                                            |
    | Assignments_Avg            | assignments_avg              |                                                            |
    | Quizzes_Avg                | quizzes_avg                  |                                                            |
    | Projects_Score             | projects_score               |                                                            |
    | Total_Score                | student_total_score          |                                                            |
    | Grade                      | grade                        |                                                            |
    | Study_Hours_per_Week       | weekly_study_hours           |                                                            |
    | Extracurricular_Activities | takes_extracurriculars       | map Yes/No to 1/0                                           |
    | Internet_Access_at_Home    | has_home_internet            | map Yes/No to 1/0                                           |
    | Parent_Education_Level     | Parent_Education_Level       |                                                            |
    | Family_Income_Level        | Family_Income_Level          |                                                            |
    | Stress_Level (1-10)        | Stress_Level (1-10)          |                                                            |
    | Sleep_Hours_per_Night      | Sleep_Hours_per_Night        |                                                            |
    | Participation_Score        |                              |                                                            |
    | first_name, last_name      | full_name                    | join first name and last name to create full_name          |
