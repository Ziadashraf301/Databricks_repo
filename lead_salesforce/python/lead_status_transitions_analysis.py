# ===========================================
# Lead Status Transition Analysis (Markov Chain)
# ===========================================
# This script reads Salesforce lead status transitions from a Spark table,
# calculates the probability of moving from one status to another,
# visualizes the transition matrix as a heatmap,
# and saves both the visualization and final Spark table.
# ===========================================

# ---- Imports ----
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ---- Step 1: Load Transitions Data ----
# Read from Databricks table containing lead status changes
transitions = (
    spark.read.table("workspace.salesforce.lead_status_transitions")
    .filter("Field = 'Status'")              # Keep only status change records
    .select("OldValue", "NewValue", "total") # Select relevant columns
)

# ---- Step 2: Aggregate Transition Counts ----
# Count total transitions between each pair of statuses
transition_counts = (
    transitions
    .groupBy("OldValue", "NewValue")
    .agg(F.sum("total").alias("count"))
)

# ---- Step 3: Compute Totals per Source Status ----
state_totals = (
    transition_counts
    .groupBy("OldValue")
    .agg(F.sum("count").alias("total"))
)

# ---- Step 4: Calculate Transition Probabilities ----
# For each 'from_status' → 'to_status', compute probability = count / total
transition_probs = (
    transition_counts
    .join(state_totals, "OldValue")  
    .withColumn("probability", F.col("count") / F.col("total"))
    .select(
        F.col("OldValue").alias("from_status"),
        F.col("NewValue").alias("to_status"),
        "count",
        F.round("probability", 4).alias("probability")
    )
    .orderBy("from_status", F.desc("probability"))
)


# ---- Step 5: Save Final Table ----
# Save the resulting probabilities as a managed Spark table for future use
transition_probs.write.mode("overwrite").saveAsTable("workspace.salesforce.lead_status_transition_probs")

# ---- Step 6: Convert to Pandas for Visualization ----
prob_matrix_df = transition_probs.toPandas()

# ---- Step 7: Create Pivot Table for Heatmap ----
pivot = (
    prob_matrix_df.pivot(index='from_status', columns='to_status', values='probability')
    .fillna(0)
)

# ---- Step 8: Plot Heatmap ----
plt.figure(figsize=(14, 10))
sns.heatmap(
    pivot,
    annot=True,
    fmt='.3f',
    cmap='YlOrRd',
    cbar_kws={'label': 'Transition Probability'},
    linewidths=0.5
)
plt.title('Lead Status Transition Probability Matrix (Markov Chain)', fontsize=16, pad=20)
plt.xlabel('To Status', fontsize=12)
plt.ylabel('From Status', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()

# ---- Step 9: Save Heatmap as PNG ----
output_path = os.path.join(os.getcwd(), "lead_status_transition_heatmap.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')

# ---- tep 10: Print Save Confirmation ----
print(f"✅ Transition probability table saved as 'workspace.salesforce.lead_status_transition_probs'")
print(f"✅ Heatmap image saved to: {output_path}")