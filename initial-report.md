# BCIT Brightspace Data Sets – Metadata and Join Guide

 This document summarizes the main ETL output tables derived from Brightspace Data Sets.
 It is intended as public-facing metadata / data dictionary content for analysts and researchers.

 ---

 ## Dataset catalogue

 ### 1. Content Objects (`contentdata.contentobjects`)

 - **Source / path**: `etl-output/contentdata/contentobjects`
 - **Grain**: One row per content object (module, topic, file, external link, activity) in a course.
 - **Approx. size**: ~11.8M rows, 25 columns.
 - **Key fields**: `content_object_id` (near-unique key), `org_unit_id`, `title`,
   `content_object_type`, `parent_content_object_id`, `location`, `start_date`, `end_date`,
   `due_date`, `tool_id`, `object_id1`–`object_id3`, `is_hidden`, `sort_order`, `depth`,
   `created_by`, `last_modified_by`, `deleted_by`, `deleted_date`, `ai_utilization`.
 - **Quality notes**: Core identifiers and titles are almost fully populated. Availability and
   tool-linkage fields are sparsely populated but parse reliably.

 ### 2. Content Service (`contentservice`)

 - **Source / path**: `etl-output/contentservice`
 - **Grain**: One row per audio/video content service revision.
 - **Approx. size**: ~35.8k rows, 10 columns.
 - **Key fields**: `content_id`, `revision_id`, `revision_number`, `type`, `source`,
   `revision_size`, `duration`, `required_transcoding`, `required_transcribing`, `last_modified`.
 - **Quality notes**: No missingness reported; numeric fields parse cleanly. Outliers reflect very
   large or long media assets.

 ### 3. Discussions Forums (`discussions.discussionforums`)

 - **Source / path**: `etl-output/discussions/discussionforums`
 - **Grain**: One row per discussion forum per org unit.
 - **Approx. size**: ~151.9k rows, 17 columns.
 - **Key fields**: `org_unit_id`, `forum_id`, `name`, `description`, `must_post_to_participate`,
   `allow_anon`, `is_hidden`, `requires_approval`, `sort_order`, `is_deleted`, `deleted_date`,
   `deleted_by_user_id`, `result_id`, `start_date`, `start_date_availability_type`, `end_date`,
   `end_date_availability_type`.
 - **Quality notes**: IDs and names are complete. Descriptions and availability fields are missing
   for many forums, especially older or auto-created ones.

 ### 4. Grade Objects (`grades.gradeobjects`)

 - **Source / path**: `etl-output/grades/gradeobjects`
 - **Grain**: One row per grade object (grade item, category, final grade, etc.) in a course.
 - **Approx. size**: ~1.60M rows, 31 columns.
 - **Key fields**: `grade_object_id` (unique key), `org_unit_id`, `parent_grade_object_id`, `name`,
   `short_name`, `type_name`, `grade_object_type_id`, `max_points`, `weight`, `grade_scheme_id`,
   `can_exceed_max_grade`, `exclude_from_final_grade_calc`, `num_lowest_grades_to_drop`,
   `num_highest_grades_to_drop`, `weight_distribution_type`, `tool_name`, `tool_id`,
   `associated_tool_item_id`, `start_date`, `end_date`, `created_date`, `last_modified`,
   `is_deleted`, `deleted_date`, `deleted_by_user_id`, `result_id`, `version`.
 - **Quality notes**: Grade object IDs and most configuration flags are fully populated. Optional
   fields such as grade schemes and some dates have substantial missingness by design.

 ### 5. Organizational Units (`organizationalunits.organizationalunits`)

 - **Source / path**: `etl-output/organizationalunits/organizationalunits`
 - **Grain**: One row per org unit (courses, course offerings, groups, programs, terms, etc.).
 - **Approx. size**: ~1.15M rows, 14 columns.
 - **Key fields**: `org_unit_id` (primary identifier), `organization`, `type`, `name`, `code`,
   `start_date`, `end_date`, `is_active`, `created_date`, `is_deleted`, `deleted_date`,
   `recycled_date`, `version`, `org_unit_type_id`.
 - **Quality notes**: Most org units have names and institutional codes. Start/end dates are
   present primarily for dated offerings; deletion and recycle dates are sparse but available.

 ### 6. Quizzes (`quizzes.quizobjects`)

 - **Source / path**: `etl-output/quizzes/quizobjects`
 - **Grain**: One row per quiz definition per org unit.
 - **Approx. size**: ~701.7k rows, 37 columns.
 - **Key fields**: `quiz_id`, `quiz_name`, `quiz_description`, `quiz_category`, `is_active`,
   `org_unit_id`, `start_date`, `end_date`, `due_date`, `creation_date`, `created_by`,
   `last_modified`, `last_modified_by`, `grade_object_id`, `overall_score_calculation`,
   `quiz_score_denominator`, `has_password`, `ip_restricted`, `time_limit`, `time_limit_enforced`,
   `attempts_allowed`, `prevent_moving_backwards`, `allow_hints`, `notification_email`,
   `disable_pager_access`, `display_in_calendar`, `is_attempt_rldb`, `is_subview_rldb`,
   `sort_order`, `category_id`, `result_id`, `is_retake_incorrect_only`, `paging_type_id`,
   `is_synchronous`, `deduction_percentage`, `ai_study_support`, `hide_question_points`.
 - **Quality notes**: IDs, names and key configuration fields are well-populated. Descriptions,
   categories, due dates, and some advanced configuration fields are often missing.

 ### 7. Release Conditions (`releaseconditions.releaseconditionsobjects`)

 - **Source / path**: `etl-output/releaseconditions/releaseconditionsobjects`
 - **Grain**: One row per release condition rule.
 - **Approx. size**: ~171.7k rows, 14 columns.
 - **Key fields**: `pre_requisite_id`, `result_id`, `org_unit_id`, `name`, `is_negative_condition`,
   `pre_requisite_tool_id`, `id1`, `id2`, `result_tool_id`, `uses_percentage`,
   `operator_type_desc`, `version`, `guid1`, `guid2`.
 - **Quality notes**: Core identifiers and tool IDs are fully populated. GUID columns are unused in
   the current export and are entirely null.

 ### 8. Role Details (`roledetails.roledetails`)

 - **Source / path**: `etl-output/roledetails/roledetails`
 - **Grain**: One row per role configuration per org unit (very small table).
 - **Approx. size**: 57 rows, 31 columns.
 - **Key fields**: `org_unit_id`, `role_id`, `role_name`, `description`, `is_cascading`,
   `in_class_list`, `class_list_role_name`, `class_list_show_groups`, `class_list_show_sections`,
   `class_list_display_role`, `access_inactive_co`, `has_special_access`,
   `add_to_course_offering_groups`, `can_be_auto_enrolled_into_groups`,
   `add_to_course_offering_sections`, `can_be_auto_enrolled_into_sections`, `access_past_courses`,
   `access_future_courses`, `sort_order`, `show_in_content`, `show_in_discussion_assess`,
   `show_in_discussion_stats`, `show_in_grades`, `show_in_attendance`,
   `allow_self_enroll_in_groups`, `show_in_registration`, `show_in_user_progress`, `role_alias`,
   `role_code`, `last_modified_date`, `deleted_by`.
 - **Quality notes**: Many fields are sparsely populated due to the small number of roles and org
   units represented, but present values parse cleanly.

 ---

 ## How the datasets can be joined

 The following patterns describe common and recommended joins for downstream analysis:

 - **Course / org-unit context**  
  All activity-level tables (`contentdata.contentobjects`, `discussions.discussionforums`,
  `grades.gradeobjects`, `quizzes.quizobjects`, `releaseconditions.releaseconditionsobjects`,
  `roledetails.roledetails`) can be joined to
  `organizationalunits.organizationalunits` on `org_unit_id` to add course codes, names, types,
  and active date ranges.

 - **Content ↔ Grades / Quizzes / Other tools**  
  `contentdata.contentobjects` records that represent activities expose `tool_id` and
  `object_id1`–`object_id3`. Combined with Brightspace tool documentation, these keys can be used
  to link content items to the underlying tool-specific tables such as quizzes or grade objects.

 - **Grades ↔ Quizzes / Discussions / Other tools**  
  `grades.gradeobjects` includes `tool_name`, `tool_id`, and `associated_tool_item_id`. These
  fields allow linking grade items to the underlying tool entities (e.g., quizzes, assignments,
  discussions) when the corresponding tool data sets are available (such as `quizzes.quizobjects`).

 - **Quizzes ↔ Grades**  
  Many quiz configurations are associated with grade items. This relationship is represented via
  `quizzes.quizobjects.grade_object_id` and, from the grades side, via
  `grades.gradeobjects.associated_tool_item_id` and `tool_name`/`tool_id`.

 - **Release conditions ↔ Content / Quizzes / Grades**  
  `releaseconditions.releaseconditionsobjects` uses `pre_requisite_tool_id`, `id1`/`id2`, and
  `result_tool_id`/`result_id` to reference specific objects in other tools (content topics, quizzes,
  grade items, etc.). These columns can be combined with tool-specific keys to reconstruct
  conditional release rules.

 - **Role configuration overlays**  
  `roledetails.roledetails` can be joined to `organizationalunits.organizationalunits` on
  `org_unit_id` and then layered onto analyses from other tables to understand how role
  configuration (e.g., instructor vs. student vs. assistant roles) might influence content
  structure, assessment design, or access patterns.

 Together, these joins support multi-table analyses such as:

 - Comparing course structures (content trees) and assessment designs (grade objects, quizzes)
   across programs and terms.
 - Relating release conditions to content, quizzes, and grades to study conditional access
   patterns.
 - Examining how role configurations vary across org units and how that correlates with the use of
   specific tools or grading schemes.
