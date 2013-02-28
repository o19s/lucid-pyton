from lSdaCollection import LucidSdaCollection
import json


q = {"query": {
    "fq": ["annotations_bahSource_value:dailymed"],
      "facet.field": [
        "annotations_mesh_condition_id_value",
        "annotations_ic_name_value",
        "annotations_org_name_value",
        "annotations_study_type_value",
        "annotations_source_value",
        "annotations_mfr_sndr_value",
        "annotations_keyword_value",
        "annotations_overall_status_value",
        "annotations_route_value",
        "annotations_program_officer_name",
        "annotations_pi_names_value",
        "annotations_organizations_value",
        "annotations_drugname_value",
        "annotations_pt_value",
        "annotations_condition_value",
        "annotations_ed_inst_type_value",
        "annotations_org_dept_value",
        "annotations_project_terms_value",
        "annotations_phase_value",
        "annotations_activeIngredients_value",
        "annotations_bahSource_value",
        "annotations_cidPharma_value"
      ],
      "hl": True,
      "hl.fl": "text",
      "fl": "id,annotations_bahSource_value,text,annotations_meshAction_value,annotations_cidPharma_value,annotations_sanitizedUrlId_value,annotations_organizations_value,annotations_activeIngredients_value,annotations_drugname_value,annotations_route_value,annotations_mfr_sndr_value,annotations_pt_value,annotations_ic_name_value,annotations_org_name_value,annotations_pi_names_value,annotations_program_officer_name_value,annotations_ed_inst_type_value,annotations_org_dept_value,annotations_project_terms_value,annotations_mesh_condition_id_value,annotations_study_type_value,annotations_source_value,annotations_keyword_value,annotations_condition_value,annotations_overall_status_value,annotations_phase_value",
      "rows": 3,
      "qt": "/lucid",
      "start": 0,
      "q": "drug",
      "hl.fragsize": 200,
      "qf": "text^1",
      "facet": True
    }}


coll = LucidSdaCollection("bah13", autoCreate=False)
#coll.enableClickTracking()
print json.dumps(coll.docR(q)["QUERY"]["json"]["response"]["docs"])
