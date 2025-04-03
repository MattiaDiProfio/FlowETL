import numpy as np
import json
import csv
import bonobo
from bonoboutils import to_internal, infer_schema, compute_dq, compute_etl_metrics
import time 

def amazonstock(key, ir, schema, path, start_time):
    from datetime import datetime
    transformed_ir = []
    # format the headers 
    transformed_headers = ["date", "open_price", "daily_high", "daily_low", "close_price", "trade_volume_millions"]
    transformed_ir.append(transformed_headers)
    for row in ir[1:]:
        date, open, high, low, close, adjclose, volume = row
        # apply transformations
        dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%S%z").strftime("%Y/%m/%d") if date != "NA" else date
        volume = str(float(volume)/1000000)
        open = round(float(open), 3)
        high = round(float(high), 3)
        low = round(float(low), 3)
        close = round(float(close), 3)
        transformed_ir.append([dt, open, high, low, close, volume])
    schema = infer_schema(transformed_ir) # reinfer schema since columns could have changed
    return key, transformed_ir, schema, path, start_time

def chessgames(key, ir, schema, path, start_time):
    transformed_ir = []
    
    transformed_headers = [
        "game_id", "is_rated", "start_time", "end_time", "turns_taken", "victory_status", "winner",
        "w_id", "w_rating", "b_id", "b_rating", "moves_sequence", "opening_strategy_name"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        id, rated, created_at, last_move_at, turns, victory_status, winner, increment_code, white_id, white_rating, black_id, black_rating, moves, opening_eco, opening_name, opening_ply = row
        
        is_rated = 1 if rated.lower() == "true" else 0
        winner = "w" if winner.lower() == "white" else "b"
        moves_sequence = "-".join(moves.split())
        
        transformed_row = [
            id, is_rated, created_at, last_move_at, turns, victory_status, winner,
            white_id, white_rating, black_id, black_rating, moves_sequence, opening_name
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def financialcompliance(key, ir, schema, path, start_time):
    transformed_ir = []
    
    transformed_headers = [
        "year", "firm_name", "total_audit_engagement", "high_risk_cases_count",
        "compliance_violations_count", "fraud_cases_count", "industry_affected",
        "revenue_impact_millions", "ai_used", "employee_workload_percent",
        "audit_impact_score", "client_satisfaction_score", "overall_score"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        Year, Firm_Name, Total_Audit_Engagements, High_Risk_Cases, Compliance_Violations, Fraud_Cases_Detected, Industry_Affected, Total_Revenue_Impact, AI_Used_for_Auditing, Employee_Workload, Audit_Effectiveness_Score, Client_Satisfaction_Score = row
        
        ai_used = 1 if str(AI_Used_for_Auditing).lower() == "yes" else 0
        overall_score = (float(Audit_Effectiveness_Score) + float(Client_Satisfaction_Score)) / 2
        
        transformed_row = [
            Year, Firm_Name, Total_Audit_Engagements, High_Risk_Cases, Compliance_Violations,
            Fraud_Cases_Detected, Industry_Affected, Total_Revenue_Impact, ai_used,
            Employee_Workload, Audit_Effectiveness_Score, Client_Satisfaction_Score, overall_score
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def netflixusers(key, ir, schema, path, start_time):
    from datetime import datetime

    transformed_ir = []
    
    transformed_headers = [
        "id", "first_name", "last_name", "age", "country_name", "subscription",
        "watch_time_hours", "favourite_genre", "last_access"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        User_ID, Name, Age, Country, Subscription_Type, Watch_Time_Hours, Favorite_Genre, Last_Login = row
        
        first_name, last_name = Name.split(" ", 1) if " " in Name else (Name, "")
        watch_time_hours = (float(Watch_Time_Hours) + 9) // 10 * 10
        last_access = datetime.strptime(Last_Login, "%Y-%m-%d").strftime("%d-%B-%y") if Last_Login != "NA" else "NA"
        
        transformed_row = [
            User_ID, first_name, last_name, Age, Country, Subscription_Type,
            watch_time_hours, Favorite_Genre, last_access
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def pixarfilms(key, ir, schema, path, start_time):
    
    from datetime import datetime

    transformed_ir = []
    
    transformed_headers = [
        "film_id", "film_name", "watch_rating", "cinema_score", "released_date",
        "run_time_minutes", "budget_millions", "total_box_office",
        "rotten_tomatoes_score", "rotten_tomatoes_counts_millions",
        "metacritic_score", "metacritic_counts_millions",
        "imdb_score", "imdb_counts_millions"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        ID, film, film_rating, cinema_score, release_date, run_time, budget, box_office_us_canada, box_office_other, box_office_worldwide, rotten_tomatoes_score, rotten_tomatoes_counts, metacritic_score, metacritic_counts, imdb_score, imdb_counts = row
        
        released_date = datetime.strptime(release_date, "%Y-%m-%d").strftime("%Y-%B-%d") if release_date != "NA" else "NA"
        budget_millions = float(budget) / 1_000_000
        rotten_tomatoes_counts_millions = float(rotten_tomatoes_counts) / 1_000_000
        metacritic_counts_millions = float(metacritic_counts) / 1_000_000
        imdb_counts_millions = float(imdb_counts) / 1_000_000
        
        transformed_row = [
            ID, film, film_rating, cinema_score, released_date, run_time,
            budget_millions, box_office_worldwide, rotten_tomatoes_score,
            rotten_tomatoes_counts_millions, metacritic_score, metacritic_counts_millions,
            imdb_score, imdb_counts_millions
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def smartwatchdata(key, ir, schema, path, start_time):
    transformed_ir = []
    
    transformed_headers = [
        "id", "heart_rate_bpm", "blood_oxygen_level_%", "step_count",
        "hours_slept", "activity_level", "stress_level"
    ]
    transformed_ir.append(transformed_headers)
    
    def map_stress_level(value):
        value = safe_float(value)
        if value == "NA": return "NA"
        if value >= 8:
            return "high"
        elif 4 <= value <= 7:
            return "medium"
        else:
            return "low"
    
    def safe_float(value, decimals=1):
        try:
            return round(float(value), decimals)
        except ValueError:
            return "NA"
    
    for row in ir[1:]:
        User_ID, Heart_Rate, Blood_Oxygen_Level, Step_Count, Sleep_Duration, Activity_Level, Stress_Level = row
        
        heart_rate_bpm = safe_float(Heart_Rate)
        blood_oxygen_level = safe_float(Blood_Oxygen_Level)
        step_count = safe_float(Step_Count)
        hours_slept = safe_float(Sleep_Duration)
        activity_level = Activity_Level.lower().strip()
        stress_level = map_stress_level(Stress_Level)
        
        transformed_row = [
            User_ID, heart_rate_bpm, blood_oxygen_level, step_count,
            hours_slept, activity_level, stress_level
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def ecommercetransactions(key, ir, schema, path, start_time):
    from datetime import datetime
    transformed_ir = []
    
    transformed_headers = [
        "invoice_number", "stock_code", "desc", "qty", "invoice_date",
        "unit_price", "customer_id", "country"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country = row
        
        desc = Description.lower()
        invoice_date = datetime.strptime(InvoiceDate, "%Y-%m-%d %H:%M:%S").strftime("%d-%B-%y") if InvoiceDate != "NA" else "NA"
        country = Country.lower()
        
        transformed_row = [
            InvoiceNo, StockCode, desc, Quantity, invoice_date, UnitPrice, CustomerID, country
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def studentgrades(key, ir, schema, path, start_time):
    transformed_ir = []
    
    transformed_headers = [
        "student_id", "student_email", "student_gender", "student_age", "dept", "attendance_percent", 
        "midterm_score", "final_score", "assignments_avg", "quizzes_avg", "projects_score", 
        "student_total_score", "grade", "weekly_study_hours", "takes_extracurriculars", 
        "has_home_internet", "parent_education_level", "family_income_level", "stress_level", 
        "sleep_hours_per_night", "full_name"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        student_id, email, gender, age, dept, attendance_percent, midterm_score, final_score, assignments_avg, quizzes_avg, projects_score, student_total_score, grade, weekly_study_hours, takes_extracurriculars, has_home_internet, parent_education_level, family_income_level, stress_level, sleep_hours_per_night, first_name, last_name, participation_score = row
        
        full_name = f"{first_name} {last_name}" if first_name != "NA" and last_name != "NA" else "NA"
        takes_extracurriculars = 1 if str(takes_extracurriculars).lower() == "yes" else 0
        has_home_internet = 1 if str(has_home_internet).lower() == "yes" else 0
        
        transformed_row = [
            student_id, email, gender, age, dept, attendance_percent, 
            midterm_score, final_score, assignments_avg, quizzes_avg, projects_score, 
            student_total_score, grade, weekly_study_hours, takes_extracurriculars, 
            has_home_internet, parent_education_level, family_income_level, stress_level, 
            sleep_hours_per_night, full_name
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def newscategories(key, ir, schema, path, start_time):
    transformed_ir = []
    from datetime import datetime
    transformed_headers = ["category", "headline", "authors", "article_link", "published_date"]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        article_link, headline, category, short_description, authors, date = row
        
        category = category.lower() if category != "NA" else "NA"
        published_date = datetime.strptime(date, "%Y-%m-%d").strftime("%d-%B-%Y") if date != "NA" else "NA"
        transformed_row = [category, headline, authors, article_link, published_date]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def socialmediaposts(key, ir, schema, path, start_time):
    transformed_ir = []
    transformed_headers = ["post_content", "user_engagement", "post_language", "post_tags", "tone", "post_character_count"]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        post_content, user_engagement, post_language, post_tags, tone, line_count = row
        post_character_count = len(post_content) if post_content != "NA" else 0
        tags = ", ".join(post_tags) if isinstance(post_tags, list) else post_tags
        tone = tone.lower() if tone != "NA" else "NA"
        transformed_row = [ post_content, user_engagement, post_language, tags, tone, post_character_count]
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def recipes(key, ir, schema, path, start_time):
    transformed_ir = []
    
    transformed_headers = [
        "recipe_id", "cuisine", "ingredients_list"
    ]
    transformed_ir.append(transformed_headers)
    
    for row in ir[1:]:
        recipe_id, cuisine, ingredients = row
        
        ingredients_list = ", ".join(ingredients) if isinstance(ingredients, list) else ingredients
        
        transformed_row = [
            recipe_id, cuisine, ingredients_list
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time

def flightroutes(key, ir, schema, path, start_time):
    transformed_ir = []
    
    transformed_headers = [
        "source", "destination", "source_latitude", "source_longitude", 
        "destination_latitude", "destination_longitude", 
        "destination_airport_code", "source_airport_code"
    ]
    
    transformed_ir.append(transformed_headers)
    
    header_mapping = {header: idx for idx, header in enumerate(ir[0])}
    
    for row in ir[1:]:
        
        src_lat = row[header_mapping.get('src_lat')]
        src_lon = row[header_mapping.get('src_lon')]
        dst_lat = row[header_mapping.get('dst_lat')]
        dst_lon = row[header_mapping.get('dst_lon')]
        source = row[header_mapping.get('src')]
        destination = row[header_mapping.get('dst')]
        
        source_parts = source.split(" ")
        destination_parts = destination.split(" ")
        
        source_name = " ".join(source_parts[:-1])
        source_airport_code = source_parts[-1]
        
        destination_name = " ".join(destination_parts[:-1])
        destination_airport_code = destination_parts[-1]
        
        transformed_row = [
            source_name, destination_name, src_lat, src_lon, dst_lat, dst_lon, 
            destination_airport_code, source_airport_code
        ]
        
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time


def amazonreviews(key, ir, schema, path, start_time):
    import datetime
    
    transformed_ir = []
    transformed_headers = ["reviewer_id", "amazon_id", "user_name", "review_date", "overall_review_score", "summary"]
    transformed_ir.append(transformed_headers)
    header_mapping = {header: idx for idx, header in enumerate(ir[0])}
    for row in ir[1:]:
        reviewerID = row[header_mapping.get("reviewerID")]
        asin = row[header_mapping.get("asin")]
        reviewerName = row[header_mapping.get("reviewerName")]
        reviewTime = row[header_mapping.get("reviewTime")]
        overall = row[header_mapping.get("overall")]
        reviewText = row[header_mapping.get("reviewText")]
        
        review_date = datetime.datetime.strptime(reviewTime, "%m %d, %Y").strftime("%d-%m-%Y") if reviewTime != "NA" else "NA"
        summary = reviewText.lower().strip() if reviewText != "NA" else "NA"
        
        transformed_row = [reviewerID, asin, reviewerName, review_date, overall, summary]
        transformed_ir.append(transformed_row)
    
    schema = infer_schema(transformed_ir)
    return key, transformed_ir, schema, path, start_time



def load(key, ir):

    # extract the filename from path
    filename = path.split("\\")[-1]
    file_ext = filename.split(".")[-1]

    if file_ext == "csv":
        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(ir) 
        
    elif file_ext == "json":

        reconstructed_objs = [] # the list of transformed objects
        keys = ir[0]
        for row in ir[1:]:
            obj = {}
            for key, value in zip(keys, row):
                if value != '_ext_':
                    obj[key] = value
            reconstructed_objs.append(obj)

        # read over json file, making sure to keep non-transformed attributes
        with open(path, 'r') as file:
            dictionary = json.load(file)

        if key and key in dictionary:
            dictionary[key] = reconstructed_objs

        with open("filename" + filename, "w") as file:
            json.dump(dictionary, file, indent=4)

    else:
        print("Error!")

def extract(path):
    start_time = time.time()
    key, ir = to_internal(path)
    schema = infer_schema(ir)
    yield key, ir, schema, path, start_time

def handle_missing_values(key, ir, schema, path, start_time):
    for i in range(1, len(ir)):
        for j in range(len(ir[i])):
            celltype = schema[ir[0][j]]
            if ir[i][j] is None or ir[i][j] == '':
                if celltype == 'number': ir[i][j] = 0.0
                elif celltype == 'boolean': ir[i][j] = False
                else: ir[i][j] = 'NA' 
    return key, ir, schema, path, start_time

def handle_duplicates(key, ir, schema, path, start_time):
    unique_rows = []
    seen = set()
    for row in ir:
        row_key = json.dumps(row)
        if row_key not in seen:
            unique_rows.append(row)
            seen.add(row_key)
    return key, unique_rows, schema, path, start_time

def handle_outliers(key, ir, schema, path, start_time):
    threshold = 3
    for column_indx, column_name in enumerate(ir[0]):
        if schema[column_name] == 'number':
            column = [float(row[column_indx]) for row in ir[1:]]
            median = np.median(column)
            mad = np.median(np.abs(column - median))
            outlier_mask = np.abs(column - median) > (threshold * mad)
            outlier_indices = np.where(outlier_mask)[0]
            for idx in outlier_indices:
                ir[idx + 1][column_indx] = float(median)
    return key, ir, schema, path, start_time

def compute_metrics(key, ir, schema, path, start_time):
    dq, metrics = round(compute_dq(ir, schema), 2), compute_etl_metrics(ir, schema)
    elapsed_time = time.time() - start_time
    print(f"File: {path} - Time: {elapsed_time:.2f}s - Data Quality: {dq}, Metrics: {metrics}")
    return key, ir


def get_graph(path):
    graph = bonobo.Graph()
    graph.add_chain(extract(path), handle_missing_values, handle_duplicates, handle_outliers, amazonreviews, compute_metrics, load)
    return graph

def get_services():
    return {}

if __name__ == '__main__':
    path = "..\\datasets\\source\\json\\amazon_reviews_source.json"
    start_time = time.time()
    bonobo.run(get_graph(path), services=get_services())
    total_time = time.time() - start_time
    print(f"Total processing time: {total_time:.2f}s")