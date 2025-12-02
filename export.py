import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import zipfile
from datetime import datetime
import time

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# Start timer
start_time = time.time()

# -----------------------------
# PostgreSQL connection config
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

# -----------------------------
# Query 1
# -----------------------------
QUERY_1 = """  
WITH t AS (
      SELECT 
          sgm.member_uid,
          sgm.member_id,
          sgm.is_president,
          sgm.is_cashier,
          sgm.is_secretary,
          v.group_name
      FROM vwgroupregister v
      INNER JOIN core.sacp_groups_members sgm 
      ON v.id::text = sgm.group_uid
)
SELECT 
    v2.id,
    v2.division as "Divison",
    v2.district as "District",
    v2.upazila as "Upazila",
    v2.union_name as "Union",
    v2.village_beneficiary as "Village",
    g.value_label::json->>'English' AS "Group type",
    concat(v2.b_first_name::text , ' ' , v2.b_last_name::text , ' ' , v2.b_middle_name::text) as "Name",
    v2.fathersname as "Father/Husband Name",
    v2.nid as "Nid",
    v2.gender_label "Sex",
    e.value_label::json->>'English' AS "Education label",
    m.value_label::json->>'English' AS "Marital status",
    eth.value_label::json->>'English' AS "Ethnicity",
    t.group_name as "Group name",
    v2.land_size_cultivable as "Cultivable land",
    v2.land_size_cultivated as "Cultivated Land",
    v2.crops_cultivated_last_year as "Crops status land cultivated in the last year",
    v2.animal_kept_last_year as "Animals kept in the household during last year",
    v2.fisheries_kept_last_year as "Fisheries kept in the household during last year",
    fr.value_label::json->>'English' AS "Forestry water Farmer kept in the infrastructure in the household group during last year",
    wi.value_label::json->>'English' AS "Water Farmer kept in the infrastructure",
    blf.value_label::json->>'English' AS "Lead Farmer",
    atr.value_label::json->>'English' AS "Training on Agriculture",
    aes.value_label::json->>'English' AS "Agriculture as main earning source",
    t.member_id as "Bid",
    v2.b_mobile_number as Mobile
FROM beneficiary v2
LEFT JOIN t ON v2.id::text = t.member_uid
LEFT JOIN xform_extracted g 
    ON g.xform_id = 528 AND g.field_name = 'm_info/farmer_group' AND g.value_text = v2.group_type_id
LEFT JOIN xform_extracted e 
    ON e.xform_id = 528 AND e.field_name = 'm_info/b_info/education' AND e.value_text = v2.education_id
LEFT JOIN xform_extracted m 
    ON m.xform_id = 528 AND m.field_name = 'm_info/b_info/marital_status' AND m.value_text = v2.marital_status_id
LEFT JOIN xform_extracted eth 
    ON eth.xform_id = 528 AND eth.field_name = 'm_info/b_info/ethnicity' AND eth.value_text = v2.ethnicity
LEFT JOIN xform_extracted fr 
    ON fr.xform_id = 528 AND fr.field_name = 'm_info/forestry' AND fr.value_text = v2.forestry_kept_last_year_id
LEFT JOIN xform_extracted wi 
    ON wi.xform_id = 528 AND wi.field_name = 'm_info/water_infrastructure' AND wi.value_text = v2.house_hold_access_to_water_infrastructure_id
LEFT JOIN xform_extracted blf 
    ON blf.xform_id = 528 AND blf.field_name = 'm_info/b_lead_farmer' AND blf.value_text = v2.is_beneficiary_lead_farmer_id
LEFT JOIN xform_extracted atr 
    ON atr.xform_id = 528 AND atr.field_name = 'm_info/receive_training' AND atr.value_text = v2.any_training_on_agriculture_id
LEFT JOIN xform_extracted aes 
    ON aes.xform_id = 528 AND aes.field_name = 'm_info/main_earning_source_agriclture' AND aes.value_text = v2.agriculture_main_earning_source_id;
"""

# -----------------------------
# Query 2
# -----------------------------
QUERY_2 = """  
SELECT
    pa.id,
    pa."uuid",
    pa.project_id,
    pa.project_name,
    pa.agency_id,
    pa.agency_name,
    pa.broad_activity_id,
    pa.broad_activity_name,
    pa.sub_activity_id,
    pa.sub_activity_name,
    pa.sub_activity_name_id,
    pa.sub_activity_type_name,
    pa.location_id,
    pa.location_name,
    pa.division_id,
    pa.division_name,
    pa.district_id,
    pa.district_name,
    pa.upazila_id,
    pa.upazila_name,
    pa.event_start_date,
    pa.demonstration_date,
    pa.input_distributation_date,
    pa.publication_start_date,
    pa.monitor_visit_date,
    pa.field_trial_date,
    pa.group_date,
    pa.tag_name,
    pa.user_id,
    pa.created_at,
    pa.updated_at,
    pa.deleted_at,
    pa.demo_type,
    pa."xml",
    pa.monitor_end_date,

    COUNT(DISTINCT b.id) AS total_members,
    COUNT(DISTINCT CASE WHEN b.gender_label = 'Male' THEN b.id END) AS male_members,
    COUNT(DISTINCT CASE WHEN b.gender_label = 'Female' THEN b.id END) AS female_members,
    COUNT(DISTINCT CASE WHEN b.age::bigint between 15 AND 34 THEN b.id END) AS youth_members,
    COUNT(DISTINCT CASE WHEN LOWER(b.house_hold_gender) = 'female' THEN b.id END) AS women_headed_households,
    COUNT(DISTINCT CASE WHEN b.ethnicity IS NOT NULL AND b.ethnicity != '1' THEN b.id END) AS ethnic_members

FROM core.project_activity pa 
JOIN core.sacp_project_members spm 
    ON pa.id = spm.project_uid::bigint
LEFT JOIN beneficiary b 
    ON spm.member_uid::bigint = b.id
LEFT JOIN core.sacp_groups_members sgm 
    ON sgm.member_uid::bigint = spm.member_uid::bigint

GROUP BY
    pa.id,
    pa."uuid",
    pa.project_id,
    pa.project_name,
    pa.agency_id,
    pa.agency_name,
    pa.broad_activity_id,
    pa.broad_activity_name,
    pa.sub_activity_id,
    pa.sub_activity_name,
    pa.sub_activity_name_id,
    pa.sub_activity_type_name,
    pa.location_id,
    pa.location_name,
    pa.division_id,
    pa.division_name,
    pa.district_id,
    pa.district_name,
    pa.upazila_id,
    pa.upazila_name,
    pa.event_start_date,
    pa.demonstration_date,
    pa.input_distributation_date,
    pa.publication_start_date,
    pa.monitor_visit_date,
    pa.field_trial_date,
    pa.group_date,
    pa.tag_name,
    pa.user_id,
    pa.created_at,
    pa.updated_at,
    pa.deleted_at,
    pa.demo_type,
    pa."xml",
    pa.monitor_end_date

ORDER BY pa.id DESC; 
"""

# -----------------------------
# Export function
# -----------------------------
def export_queries():
    # Create result folder
    result_folder = "result"
    os.makedirs(result_folder, exist_ok=True)

    # File paths
    today = datetime.now().strftime("%Y-%m-%d")
    file1 = os.path.join(result_folder, f"Beneficiary_{today}.xlsx")
    file2 = os.path.join(result_folder, f"Project_activity_{today}.xlsx")
    zip_filename = os.path.join(result_folder, f"SACP_exports_{today}.zip")

    # Connect to PostgreSQL
    print("Connecting to PostgreSQL...")
    dsn = (
        f"dbname={DB_CONFIG['database']} "
        f"user={DB_CONFIG['user']} "
        f"password='{DB_CONFIG['password']}' "
        f"host={DB_CONFIG['host']} "
        f"port={DB_CONFIG['port']}"
    )
    conn = psycopg2.connect(dsn)
    print("Connected.")

    # -----------------------------
    # Run Query 1
    # -----------------------------
    print("\nRunning Query 1...")
    q1_start = time.time()
    df1 = pd.read_sql_query(QUERY_1, conn)
    df1.to_excel(file1, index=False)
    q1_end = time.time()
    print(f"Query 1 execution time: {q1_end - q1_start:.2f} sec")
    print(f"Query 1 rows exported: {len(df1)}")

    # -----------------------------
    # Run Query 2
    # -----------------------------
    print("\nRunning Query 2...")
    q2_start = time.time()
    df2 = pd.read_sql_query(QUERY_2, conn)
    df2.to_excel(file2, index=False)
    q2_end = time.time()
    print(f"Query 2 execution time: {q2_end - q2_start:.2f} sec")
    print(f"Query 2 rows exported: {len(df2)}")

    # Close connection
    conn.close()
    print("\nExcel files exported.")

    # -----------------------------
    # Create ZIP
    # -----------------------------
    print("Creating ZIP...")
    with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file1, arcname=os.path.basename(file1))
        zipf.write(file2, arcname=os.path.basename(file2))
    print(f"ZIP created: {zip_filename}")

    # -----------------------------
    # Total execution time
    # -----------------------------
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = total_time % 60
    print(f"\n⏱ TOTAL EXECUTION TIME: {minutes} min {seconds:.2f} sec")
    print("Done.")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    export_queries()
