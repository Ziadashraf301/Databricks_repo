CREATE OR REFRESH STREAMING TABLE lead_status_transitions AS
SELECT 
  Field, 
  OldValue, 
  NewValue, 
  COUNT(*) AS total,
  COLLECT_LIST(LeadId) AS lead_ids,
  current_timestamp() AS created_at
FROM STREAM(workspace.salesforce.leadhistory)
GROUP BY Field, OldValue, NewValue
ORDER BY total DESC