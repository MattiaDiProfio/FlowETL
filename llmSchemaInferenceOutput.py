[
    {
        "fileName": "amazon_stock_data.csv",
        "llmSchemaMap" : { 
            ('date',): ('date',), 
            ('open',): ('open_price',), 
            ('high',): ('daily_high',), 
            ('low',): ('daily_low',), 
            ('close',): ('close_price',), 
            ('volume',): ('trade_volume_millions',), 
            ('adj_close',): () 
        }
    },
    {
        "fileName" : "chess_games.csv",
        "llmSchemaMap" : { 
            ('id',): ('game_id',),
            ('rated',): ('is_rated',), 
            ('created_at',): ('start_time',), 
            ('last_move_at',): ('end_time',), 
            ('turns',): ('turns_taken',), 
            ('victory_status',): ('victory_status',), 
            ('winner',): ('winner',), 
            ('white_id',): ('w_id',), 
            ('white_rating',): ('w_rating',), 
            ('black_id',): ('b_id',), 
            ('black_rating',): ('b_rating',), 
            ('moves',): ('moves_sequence',), 
            ('opening_name',): ('opening_strategy_name',) 
        }
    },
    {
        "fileName" : "ecommerce_transactions.csv",
        "llmSchemaMap" : {
            ('InvoiceNo',): ('invoice_number',),
            ('StockCode',): ('stock_code',),
            ('Description',): ('desc',),
            ('Quantity',): ('qty',),
            ('InvoiceDate',): ('invoice_date',),
            ('UnitPrice',): ('unit_price',),
            ('CustomerID',): ('customer_id',),
            ('Country',): ('country',)
        }
    },
    {
        "fileName" : "financial_compliance.csv",
        "llmSchemaMap" : {
            ('Year',): ('year',),
            ('Firm_Name',): ('firm_name',),
            ('Total_Audit_Engagements',): ('total_audit_engagement',),
            ('High_Risk_Cases',): ('high_risk_cases_count',),
            ('Compliance_Violations',): ('compliance_violations_count',),
            ('Fraud_Cases_Detected',): ('fraud_cases_count',),
            ('Industry_Affected',): ('industry_affected',),
            ('Total_Revenue_Impact',): ('revenue_impact_millions',),
            ('AI_Used_for_Auditing',): ('ai_used',),
            ('Employee_Workload',): ('employee_workload_percent',),
            ('Audit_Effectiveness_Score',): ('audit_impact_score',),
            ('Client_Satisfaction_Score',): ('client_satisfaction_score',),
            (): ('overall_score',)
        }
    },
    {
        "fileName" : "netflix_users.csv",
        "llmSchemaMap" : {
            ('User_ID',): ('id',),
            ('Name',): ('first_name', 'last_name'),
            ('Age',): ('age',),
            ('Country',): ('country_name',),
            ('Subscription_Type',): ('subscription',),
            ('Watch_Time_Hours',): ('watch_time_hours',),
            ('Favorite_Genre',): ('favourite_genre',),
            ('Last_Login',): ('last_access',)
        }
    },
    {
        "fileName" : "pixar_films.csv",
        "llmSchemaMap" : { 
            ('ID',): ('film_id',), 
            ('film',): ('film_name',), 
            ('film_rating',): ('watch_rating',), 
            ('cinema_score',): ('cinema_score',), 
            ('release_date',): ('released_date',), 
            ('run_time',): ('run_time_minutes',), 
            ('budget',): ('budget_millions',), 
            ('box_office_us_canada', 'box_office_other', 'box_office_worldwide'): ('total_box_office',), 
            ('rotten_tomatoes_score',): ('rotten_tomatoes_score',), 
            ('rotten_tomatoes_counts',): ('rotten_tomatoes_counts_millions',), 
            ('metacritic_score',): ('metacritic_score',), 
            ('metacritic_counts',): ('metacritic_counts_millions',), 
            ('imdb_score',): ('imdb_score',), 
            ('imdb_counts',): ('imdb_counts_millions',) 
        }
    },
    {
        "fileName" : "smartwatch_health_data.csv",
        "llmSchemaMap" : { 
            ('User ID',): ('id',), 
            ('Heart Rate (BPM)',): ('heart_rate_bpm',), 
            ('Blood Oxygen Level (%)',): ('blood_oxygen_level_%',), 
            ('Step Count',): ('step_count',), 
            ('Sleep Duration (hours)',): ('hours_slept',), 
            ('Activity Level',): ('activity_level',), 
            ('Stress Level',): ('stress_level',) 
        }
    },
    {
        "fileName" : "amazon_reviews.json",
        "llmSchemaMap" : {
            ('asin',): ('amazon_id',),
            ('overall',): ('overall_review_score',),
            ('reviewerName',): ('user_name',),
            ('reviewTime',): ('review_date',),
            ('summary',): ('summary',),
            ('reviewerID',): ('reviewer_id',),
            ('unixReviewTime', 'reviewText', 'helpful'): ()
        }
    },
    {
        "fileName" : "flight_routes.json",
        "llmSchemaMap" : { 
            ('dst', ): ('destination_airport_code', ), 
            ('dst', ): ('destination', ), 
            ('dst_lat', ): ('destination_latitude', ), 
            ('dst_lon', ): ('destination_longitude', ), 
            ('src', ): ('source_airport_code', ), 
            ('src', ): ('source', ), 
            ('src_lat', ): ('source_latitude', ), 
            ('src_lon', ): ('source_longitude', )
        }
    },
    {
        "fileName" : "news_categories.json",
        "llmSchemaMap" : { 
            ('short_description',): (None, None), 
            ('category',): ('category', None), 
            ('date',): ('published_date', None), 
            ('link',): ('article_link', None), 
            ('headline',): ('headline', None), 
            ('authors',): ('authors', None) 
        }     
    },
    {
        "fileName" : "recipes.json",
        "llmSchemaMap" : { 
            ('cuisine',) : ('cuisine',), 
            ('id',) : ('recipe_id',), 
            ('ingredients',) : ('ingredients_list',) 
        }
    },
    {
        "fileName" : "social_media_posts.json",
        "llmSchemaMap" : 	{ 
            ('tags',): ('post_tags',), 
            ('text',): ('post_content',), 
            ('engagement',): ('user_engagement',), 
            ('tone',): ('tone',), 
            ('language',): ('post_language',), 
            ('line_count',): ('post_character_count',)
        }
    },
    {
        "fileName" : "students_grades.json",
        "llmSchemaMap" : {
            ('Sleep_Hours_per_Night',): ('Sleep_Hours_per_Night',),
            ('Grade',): ('grade',),
            ('First_Name', 'Last_Name'): ('full_name',),
            ('Total_Score',): ('student_total_score',),
            ('Study_Hours_per_Week',): ('weekly_study_hours',),
            ('Age',): ('student_age',),
            ('Extracurricular_Activities',): ('takes_extracurriculars',),
            ('Internet_Access_at_Home',): ('has_home_internet',),
            ('Email',): ('student_email',),
            ('Student_ID',): ('student_id',),
            ('Attendance (%)',): ('attendance_percent',),
            ('Family_Income_Level',): ('Family_Income_Level',),
            ('Midterm_Score',): ('midterm_score',),
            ('Parent_Education_Level',): ('Parent_Education_Level',),
            ('Projects_Score',): ('projects_score',),
            ('Assignments_Avg',): ('assignments_avg',),
            ('Final_Score',): ('final_score',),
            ('Gender',): ('student_gender',),
            ('Stress_Level (1-10)',): ('Stress_Level (1-10)',),
            ('Department',): ('dept',),
            ('Quizzes_Avg',): ('quizzes_avg',)
        }
    }
]