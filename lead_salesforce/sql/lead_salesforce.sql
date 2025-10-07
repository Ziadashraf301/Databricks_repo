CREATE OR REFRESH STREAMING TABLE lead_metrics AS
SELECT
  window.start AS metric_timestamp,
  SUM(CASE WHEN Status IN ('New', 'New Lead') THEN 1 ELSE 0 END) AS new_count,
  SUM(CASE WHEN Status IN ('Contacted', 'Re-engagement') THEN 1 ELSE 0 END) AS contacted_count,
  SUM(CASE WHEN Status = 'Nurturing' THEN 1 ELSE 0 END) AS nurturing_count,
  SUM(CASE WHEN Status IN ('Qualified', 'Proposal Sent', 'Negotiation') THEN 1 ELSE 0 END) AS qualified_count,
  SUM(CASE WHEN Status IN ('On Hold', 'Closed Won', 'Closed Lost', 'Unqualified', 'Disqualified') THEN 1 ELSE 0 END) AS unqualified_count,
  SUM(CASE WHEN IsConverted = TRUE THEN 1 ELSE 0 END) AS is_converted_count
FROM STREAM(lead)
GROUP BY window(lead.LastModifiedDate, '1 hour')