WITH t AS (
          SELECT
              sgm.member_uid,
              sgm.member_id,
              sgm.is_president,
              sgm.is_cashier,
              sgm.is_secretary,
              v.group_name
          FROM
              vwgroupregister v
          INNER JOIN
              core.sacp_groups_members sgm
          ON
              v.id::text = sgm.group_uid
      )
      SELECT
          v2.id,
          v2.village_beneficiary as  "Village",
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
          fr.value_label::json->>'English' AS  "Forestry water Farmer kept in the infrastructure in the household group during last year",
          wi.value_label::json->>'English' AS "Water Farmer kept in the infrastructure",
          blf.value_label::json->>'English' AS "Lead Farmer",
          atr.value_label::json->>'English' AS "Training on Agriculture",
          aes.value_label::json->>'English' AS "Agriculture as main earning source",
          t.member_id as "Bid",
          v2.b_mobile_number as Mobile
      FROM
          beneficiary v2
      LEFT JOIN
          t
      ON
          v2.id::text = t.member_uid
      LEFT JOIN
          xform_extracted g
      ON
          g.xform_id = 528 AND g.field_name = 'm_info/farmer_group' AND g.value_text = v2.group_type_id
      LEFT JOIN
          xform_extracted e
      ON
          e.xform_id = 528 AND e.field_name = 'm_info/b_info/education' AND e.value_text = v2.education_id
      LEFT JOIN
          xform_extracted m
      ON
          m.xform_id = 528 AND m.field_name = 'm_info/b_info/marital_status' AND m.value_text = v2.marital_status_id
      LEFT JOIN
          xform_extracted eth
      ON
          eth.xform_id = 528 AND eth.field_name = 'm_info/b_info/ethnicity' AND eth.value_text = v2.ethnicity
      LEFT JOIN
          xform_extracted hhg
      ON
          hhg.xform_id = 528 AND hhg.field_name = 'm_info/b_info/hh_gender' AND hhg.value_text = v2.house_hold_gender
      LEFT JOIN
          xform_extracted lu
      ON
          lu.xform_id = 528 AND lu.field_name = 'm_info/l_size/land_unit' AND lu.value_text = v2.land_unit_id
      LEFT JOIN
          xform_extracted fr
      ON
          fr.xform_id = 528 AND fr.field_name = 'm_info/forestry' AND fr.value_text = v2.forestry_kept_last_year_id
      LEFT JOIN
          xform_extracted wi
      ON
          wi.xform_id = 528 AND wi.field_name = 'm_info/water_infrastructure' AND wi.value_text = v2.house_hold_access_to_water_infrastructure_id
      LEFT JOIN
          xform_extracted blf
      ON
          blf.xform_id = 528 AND blf.field_name = 'm_info/b_lead_farmer' AND blf.value_text = v2.is_beneficiary_lead_farmer_id
      LEFT JOIN
          xform_extracted atr
      ON
          atr.xform_id = 528 AND atr.field_name = 'm_info/receive_training' AND atr.value_text = v2.any_training_on_agriculture_id
      LEFT JOIN
          xform_extracted aes
      ON
          aes.xform_id = 528 AND aes.field_name = 'm_info/main_earning_source_agriclture' AND aes.value_text = v2.agriculture_main_earning_source_id