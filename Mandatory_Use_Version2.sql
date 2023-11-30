--This new update improves over the current logic by:
--(1) using NPI to connect searches with dispensations instead of DEA
--(2) Making adjustment to the full and partial searches matching to patients name and DOB

DROP TABLE IF EXISTS mandatory_use_version2;
CREATE temp TABLE mandatory_use_version2 diststyle ALL sortkey(id) AS
SELECT d.id,
        max(case when (req.created_at::date - d.written_at) between -90 and 0 then 'Y' else 'N' end) search_w_in_90,
        max(case when (req.created_at::date - d.written_at) between -30 and 0 then 'Y' else 'N' end) search_w_in_30,
        max(case when (req.created_at::date - d.written_at) between -15 and 0 then 'Y' else 'N' end) search_w_in_15,
        max(case when (req.created_at::date - d.written_at) between -10 and 0 then 'Y' else 'N' end) search_w_in_10,
        max(case when (req.created_at::date - d.written_at) between -9  and 0 then 'Y' else 'N' end) search_w_in_9,
        max(case when (req.created_at::date - d.written_at) between -8  and 0 then 'Y' else 'N' end) search_w_in_8,
        max(case when (req.created_at::date - d.written_at) between -7  and 0 then 'Y' else 'N' end) search_w_in_7,
        max(case when (req.created_at::date - d.written_at) between -6  and 0 then 'Y' else 'N' end) search_w_in_6,
        max(case when (req.created_at::date - d.written_at) between -5  and 0 then 'Y' else 'N' end) search_w_in_5,
        max(case when (req.created_at::date - d.written_at) between -4  and 0 then 'Y' else 'N' end) search_w_in_4,
        max(case when (req.created_at::date - d.written_at) between -3  and 0 then 'Y' else 'N' end) search_w_in_3,
        max(case when (req.created_at::date - d.written_at) between -2  and 0 then 'Y' else 'N' end) search_w_in_2,
        max(case when (req.created_at::date - d.written_at) between -1  and 0 then 'Y' else 'N' end) search_w_in_1,
        max(case when (req.created_at::date - d.written_at) = 0 then 'Y' else 'N' end) search_w_in_0

FROM  (SELECT u.id,p.national_provider_id as npi
        FROM farm.users u
        inner join dea_numbers dea on dea.user_id =u.id
        inner join  hbi_lexisnexis_ext.ln_individual_dea ln   on ln.dea= dea.dea_number
        inner join  hbi_lexisnexis_ext.ln_individual_npi n   on ln.hms_piid = n.hms_piid
        inner join prescribers p on p.national_provider_id = n.npi::varchar(15)
        where u.provider = 'identity' and u.registration_status ='approved'
        group by 1,2
                        union
        SELECT u.id,p.national_provider_id as npi
        FROM farm.users u
        inner join prescribers p on p.national_provider_id = u.national_provider_id
        where u.provider = 'identity' and u.registration_status ='approved'
        group by 1,2) u

INNER JOIN (select  CASE when p.national_provider_id is Null THEN n.npi::varchar(15) else p.national_provider_id END as national_provider_id,
        written_at,pa.first_name,pa.last_name,pa.birthdate,d.id,ci.consolidation_identifier
        from farm.prescribers p
        left join hbi_lexisnexis_ext.ln_individual_dea ln on ln.dea = p.dea_number
        left join hbi_lexisnexis_ext.ln_individual_npi n on ln.hms_piid = n.hms_piid
        inner join farm.dispensations d  on d.id=p.dispensation_id
        inner join farm.prescriptions pr on d.id=pr.dispensation_id
        inner join farm.patients pa on  d.patient_id  = pa.id
        inner join farm.drugs dr on d.id=dr.dispensation_id
        left join hbi_analytics.dim_drug_master op on dr.narx_national_drug_code = op.ndc
        inner join farm.consolidation_identifiers ci on pa.id  = ci.patient_id and ci.consolidation_ruleset_id = (select id from farm.consolidation_rulesets where status = 'active' limit 1)
        where op.drug_dea_schedule IN ('2','3','4','5')
        group by 1,2,3,4,5,6,7) d on d.national_provider_id = u.npi
INNER JOIN farm.rx_search_requests req on u.id=COALESCE(req.delegator_id, req.requestor_id)
WHERE
     datediff(day, req.created_at::date, d.written_at) BETWEEN 0 and 90
     and (((d.first_name ilike req.first_name) or (req.partial_first_name=true and d.first_name ilike '%'||req.first_name||'%')
                           or DIFFERENCE(d.first_name,req.first_name)=4)
                                                    or
         ((d.last_name ilike req.last_name) or (req.partial_last_name=true and d.last_name ilike '%'||req.last_name||'%')
                           or DIFFERENCE(d.last_name,req.last_name)=4))
    and (d.birthdate = req.birthdate)
    and req.request_status = 'complete' and req.requestor_type = 'User'
    and (req.approval_status is null or req.approval_status in ('complete','needs_consolidation','pending','rejected'))
    --and  d.written_at::timestamp between '2022-06-01' and '2023-07-31'
    and d.written_at::date >= (date_trunc('month', current_date) - interval '{} month')::date
Group BY d.id;

--The remaining part were just duplicate of existing logic

DROP TABLE IF EXISTS opioid_naive_version2;
CREATE temp TABLE opioid_naive_version2 AS
        (SELECT ci.consolidation_identifier, d.id AS dispensation_id, d.filled_at,
        lag(filled_at+(days_supply-1)) OVER (PARTITION BY ci.consolidation_identifier ORDER BY filled_at,(filled_at+(days_supply-1)),d.id) AS last_rx_end
        FROM farm.dispensations d
        INNER JOIN farm.patients pa
                ON d.patient_id = pa.id
        INNER JOIN farm.consolidation_identifiers ci
                ON pa.id = ci.patient_id
        INNER JOIN farm.consolidation_rulesets cr
                ON cr.id = ci.consolidation_ruleset_id AND cr.status = 'active'
        INNER JOIN farm.drugs dr
                ON d.id=dr.dispensation_id
        INNER JOIN hbi_analytics.dim_drug_master op  ON dr.narx_national_drug_code = op.ndc
        WHERE drug_opioid_yn = 'Y'
                AND cdc_schdl IN ('2', '3', '4')
                AND d.updated_at::date >= '2017-01-01'
                AND d.days_supply <> 399004187 -- Added 20200401 because of a MI record with a days_supply value of 399,004,187 (disp id 133119775)
        GROUP BY ci.consolidation_identifier, d.filled_at, d.days_supply, d.id);

DROP TABLE IF EXISTS mu_calendar_version2;
CREATE TEMP TABLE mu_calendar_version2 AS
    SELECT d.id
    FROM  farm.dispensations d
        INNER JOIN farm.prescriptions pr on d.id=pr.dispensation_id
        AND pr.written_at::date >= (date_trunc('month', current_date) - interval '{} month')::date
    WHERE d.filled_at::date >= (date_trunc('month', current_date) - interval '{} month')::date;

DROP TABLE IF EXISTS hyper_mandatory_use_version2;
CREATE temp TABLE hyper_mandatory_use_version2 AS
    SELECT pd.dispensation_id,                                                  -- Dispensation ID; field 0
    CONCAT(NVL(cast(pd.total_mme as varchar(20)), ''), concat('\002',           -- Total MME; field 1
    CONCAT(NVL(cast(pd.daily_mme as varchar(20)), ''), concat('\002',           -- Daily MME; field 2
    CONCAT(NVL(cast(pd.Days_Supply as varchar(20)), ''), concat('\002',         -- Days Supply; field 3
    CONCAT(NVL(to_char(c.disp_cal_date, 'YYYY-MM-DD'), ''), concat('\002',      -- Filled Date; field 4
    CONCAT(NVL(d.drug_active_ingrd, ''), concat('\002',                         -- Drug Active Ingrd; field 5
    CONCAT(NVL(d.drug_ahs_yn, ''), concat('\002',                               -- Drug AHS YN; field 6
    CONCAT(NVL(d.drug_brand_name, ''), concat('\002',                           -- Drug Brand Name; field 7
    CONCAT(NVL(cast(d.drug_cdc_stimulant as varchar(20)), ''), concat('\002',   -- Drug CDC Stimulant; field 8
    CONCAT(NVL(d.drug_ndc, ''), concat('\002',                                  -- Drug NDC; field 9
    CONCAT(NVL(d.drug_opioid_yn, ''), concat('\002',                            -- Drug Opioid YN; field 10
    CONCAT(NVL(db.drug_buprenorphine_yn, ''), concat('\002',                    -- Drug Buprenorphine YN; field 11
    CONCAT(NVL(dc.drug_ahfs_desc, ''), concat('\002',                           -- Drug AHFS Desc; field 12
    CONCAT(NVL(ds.drug_schdl, ''), concat('\002',                               -- Drug Schdl; field 13
    CONCAT(NVL(pt.patient_consolidation_id, ''), concat('\002',                 -- Patient Consolidation ID; field 14
    CONCAT(NVL(pt.patient_first_name, ''), concat('\002',                       -- Patient First Name; field 15
    CONCAT(NVL(pt.patient_last_name, ''), concat('\002',                        -- Patient Last Name; field 16
    CONCAT(NVL(to_char(pt.patient_birthdate, 'YYYY-MM-DD'), ''), concat('\002', -- Patient Birthdate; field 17
    CONCAT(NVL(pr.prescriber_dea, ''), concat('\002',                           -- Prescriber DEA; field 18
    CONCAT(NVL(pr.prescriber_first_name, ''), concat('\002',                    -- Prescriber First Name; field 19
    CONCAT(NVL(pr.prescriber_last_name, ''), concat('\002',                     -- Prescriber Last Name; field 20
    CONCAT(NVL(pr.prescriber_npi, ''), concat('\002',                           -- Prescriber NPI; field 21
    CONCAT(NVL(pr.prescriber_pdmp_active_yn, ''), concat('\002',                -- Prescriber PDMP Active YN; field 22
    CONCAT(NVL(pr.prescriber_role_desc, ''), concat('\002',                     -- Prescriber Role Desc; field 23
    CONCAT(NVL(pr.prescriber_spec_lvl2, ''), concat('\002',                     -- Prescriber Spec Lvl2; field 24
    CONCAT(NVL(pr.prescriber_spec_lvl3, ''), concat('\002',                     -- Prescriber Spec Lvl3; field 25
    CONCAT(NVL(pr.prescriber_Add_line1, ''), concat('\002',                     -- Prescriber Add Line1; field 26
    CONCAT(NVL(pr.prescriber_city, ''), concat('\002',                          -- Prescriber City; field 27
    CONCAT(NVL(pr.prescriber_state, ''), concat('\002',                         -- Prescriber State; field 28
    CONCAT(NVL(pr.prescriber_zip, ''), concat('\002',                           -- Prescriber Zip; field 29
    CONCAT(NVL(cast(pd.refill_number as varchar(20)), ''), concat('\002',       -- Refill Number; field 30
    CONCAT(NVL(rx.prescription_number, ''), concat('\002',                      -- Prescription Number; field 31
    CONCAT(NVL(to_char(rx.written_at, 'YYYY-MM-DD'), ''), concat('\002',        -- Written At; field 32
    CONCAT(NVL(mu.search_w_in_90, ''), concat('\002',                           -- Search W In 90; field 33
    CONCAT(NVL(mu.search_w_in_30, ''), concat('\002',                           -- Search W In 30; field 34
    CONCAT(NVL(mu.search_w_in_15, ''), concat('\002',                           -- Search W In 15; field 35
    CONCAT(NVL(mu.search_w_in_10, ''), concat('\002',                           -- Search W In 10; field 36
    CONCAT(NVL(mu.search_w_in_9, ''), concat('\002',                            -- Search W In 9; field 37
    CONCAT(NVL(mu.search_w_in_8, ''), concat('\002',                            -- Search W In 8; field 38
    CONCAT(NVL(mu.search_w_in_7, ''), concat('\002',                            -- Search W In 7; field 39
    CONCAT(NVL(mu.search_w_in_6, ''), concat('\002',                            -- Search W In 6; field 40
    CONCAT(NVL(mu.search_w_in_5, ''), concat('\002',                            -- Search W In 5; field 41
    CONCAT(NVL(mu.search_w_in_4, ''), concat('\002',                            -- Search W In 4; field 42
    CONCAT(NVL(mu.search_w_in_3, ''), concat('\002',                            -- Search W In 3; field 43
    CONCAT(NVL(mu.search_w_in_2, ''), concat('\002',                            -- Search W In 2; field 44
    CONCAT(NVL(mu.search_w_in_1, ''), concat('\002',                            -- Search W In 1; field 45
    CONCAT(NVL(mu.search_w_in_0, ''), concat('\002',                            -- Search W In 0; field 46
    CONCAT(NVL(to_char(o.filled_at, 'YYYY-MM-DD'), ''), concat('\002',          -- Filled At; field 47
    NVL(to_char(o.last_rx_end, 'YYYY-MM-DD'), '') )))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) as mandatory_use_txt --Last Rx End; field 48

    FROM hbi_analytics.fact_ah_patient_dispensations pd
    INNER JOIN mu_calendar_version2 cal
            ON cal.id = pd.dispensation_id
    INNER JOIN hbi_analytics.dim_ah_calendar c
            ON c.disp_cal_key=pd.disp_cal_key
    INNER JOIN hbi_analytics.dim_ah_drug d
            ON d.drug_key=pd.drug_key
    INNER JOIN hbi_analytics.dim_ah_drug_buprenorphine db
            ON db.drug_buprenorphine_key=pd.drug_buprenorphine_key
    INNER JOIN hbi_analytics.dim_ah_drug_class dc
            ON dc.drug_class_key=pd.drug_class_key
    INNER JOIN hbi_analytics.dim_ah_drug_schdl ds
            ON ds.drug_schdl_key=pd.drug_schdl_key
    INNER JOIN hbi_analytics.dim_ah_patient pt
            ON pt.patient_key=pd.patient_key
    INNER JOIN hbi_analytics.dim_ah_prescriber pr
            ON pr.prescriber_key=pd.prescriber_key
    INNER JOIN hbi_analytics.dim_ah_prescription rx
            ON rx.dispensation_id=pd.dispensation_id
    LEFT JOIN mandatory_use_version2 mu
            ON mu.id=pd.dispensation_id
    LEFT JOIN opioid_naive_version2 o
            ON o.dispensation_id=pd.dispensation_id

;