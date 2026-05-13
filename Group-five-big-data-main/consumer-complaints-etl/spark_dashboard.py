import os
import sys
import webbrowser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

# Set environment variables
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["SPARK_HOME"] = r"C:\Users\Hybrid Technology\Downloads\spark-4.1.1-bin-hadoop3\spark-4.1.1-bin-hadoop3"
os.environ["HADOOP_HOME"] = os.environ["SPARK_HOME"]
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PATH"] = hadoop_bin + ";" + os.environ.get("PATH", "")

print("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("Consumer_Complaints_Spark_Dashboard") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session created successfully!")
print(f"Spark version: {spark.version}")

# Load consumer complaints data
print("Loading consumer complaints data...")
complaints_df = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/Consumer_Complaints.csv")
print(f"Loaded {complaints_df.count():,} complaints")

# Load population data
print("Loading population data...")
population_df = spark.read.option("multiline", True).json("data/raw/us-states-population.json")
print(f"Loaded population data for {population_df.count():,} states")

# Load state coordinates
print("Loading state coordinates...")
coords_df = spark.read.parquet("data/raw/US_States_Long_Lat.parquet")
print(f"Loaded coordinates for {coords_df.count():,} states")

# Process data - count complaints by state
print("Processing data...")
complaints_by_state = complaints_df.groupBy("State").agg(count("*").alias("complaints_count"))

# Join with population data
# First, let's check the schema of population data
population_df.printSchema()

# Join with coordinates
coords_df.printSchema()

# For simplicity, let's just work with complaints by state for now
complaints_by_state = complaints_by_state.orderBy(col("complaints_count").desc())

# Collect data for dashboard
print("Collecting data for dashboard...")
complaints_data = complaints_by_state.collect()

# Convert to list of dictionaries
states_data = []
for row in complaints_data:
    states_data.append({
        "state": row["State"],
        "complaints": row["complaints_count"]
    })

# Calculate statistics
total_complaints = sum(s["complaints"] for s in states_data)
total_states = len(states_data)
top_state = states_data[0] if states_data else None
top_state_name = top_state['state'] if top_state else 'N/A'
top_state_complaints = f"{top_state['complaints']:,}" if top_state else '0'

print(f"Total complaints: {total_complaints:,}")
print(f"Total states: {total_states}")
print(f"Top state: {top_state_name} with {top_state_complaints} complaints")

# Generate HTML dashboard
print("Generating HTML dashboard...")

# Generate bar chart HTML
top10 = states_data[:10]
max_c = top10[0]["complaints"]
bar_html = ""
for s in top10:
    pct = int(s["complaints"] / max_c * 100)
    bar_html += f"""
    <div class="bar-row">
      <span class="bar-label">{s['state']}</span>
      <div class="bar-track">
        <div class="bar-fill" style="width:{pct}%;background:#3b82f6">{s['complaints']:,}</div>
      </div>
    </div>"""

# Generate table HTML
table_html = ""
for i, s in enumerate(states_data, 1):
    table_html += f"""<tr>
      <td><span class="badge">{i}</span></td>
      <td><b>{s['state']}</b></td>
      <td>{s['complaints']:,}</td>
    </tr>"""

html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Consumer Complaints Spark Dashboard</title>
<style>
  body {{
    font-family: Arial, sans-serif;
    background: #0f172a;
    color: #e2e8f0;
    margin: 0;
    padding: 20px;
  }}
  h1 {{
    text-align: center;
    color: #38bdf8;
    font-size: 28px;
    margin-bottom: 5px;
  }}
  p.sub {{
    text-align: center;
    color: #94a3b8;
    margin-bottom: 30px;
  }}
  .cards {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-bottom: 30px;
  }}
  .card {{
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
    text-align: center;
  }}
  .card h2 {{
    font-size: 32px;
    color: #38bdf8;
    margin: 0;
  }}
  .card p {{
    color: #94a3b8;
    margin: 5px 0 0;
    font-size: 14px;
  }}
  .chart-box {{
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 30px;
  }}
  .chart-box h3 {{
    color: #38bdf8;
    margin: 0 0 15px;
    font-size: 16px;
  }}
  .bar-row {{
    display: flex;
    align-items: center;
    margin-bottom: 8px;
    gap: 8px;
  }}
  .bar-label {{
    width: 50px;
    font-size: 12px;
    color: #94a3b8;
    text-align: right;
  }}
  .bar-track {{
    flex: 1;
    background: #0f172a;
    border-radius: 4px;
    height: 22px;
  }}
  .bar-fill {{
    height: 22px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    padding-left: 8px;
    font-size: 11px;
    color: white;
    min-width: 40px;
  }}
  .table-box {{
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
  }}
  .table-box h3 {{
    color: #38bdf8;
    margin: 0 0 15px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  th {{
    background: #0f172a;
    color: #38bdf8;
    padding: 10px;
    text-align: left;
  }}
  td {{
    padding: 9px 10px;
    border-bottom: 1px solid #334155;
    color: #cbd5e1;
  }}
  tr:hover td {{
    background: #273449;
  }}
  .badge {{
    background: #1d4ed8;
    color: white;
    padding: 2px 8px;
    border-radius: 20px;
    font-size: 11px;
  }}
  .spark-info {{
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 30px;
    text-align: center;
  }}
  .spark-info h3 {{
    color: #38bdf8;
    margin: 0 0 10px;
  }}
</style>
</head>
<body>

<h1>Consumer Complaints Spark Dashboard</h1>
<p class="sub">US Consumer Financial Complaints by State — Powered by Apache Spark</p>

<div class="spark-info">
  <h3>Spark Information</h3>
  <p>Spark Version: {spark.version} | Master: local[*] | App Name: Consumer_Complaints_Spark_Dashboard</p>
</div>

<div class="cards">
  <div class="card">
    <h2>{total_complaints:,}</h2>
    <p>Total Complaints</p>
  </div>
  <div class="card">
    <h2>{total_states}</h2>
    <p>States Covered</p>
  </div>
  <div class="card">
    <h2>{top_state_name}</h2>
    <p>Most Complaints ({top_state_complaints})</p>
  </div>
</div>

<div class="chart-box">
  <h3>Top 10 States by Total Complaints</h3>
  {bar_html}
</div>

<div class="table-box">
  <h3>Full Data Table — All States</h3>
  <table>
    <tr>
      <th>#</th>
      <th>State</th>
      <th>Complaints</th>
    </tr>
    {table_html}
  </table>
</div>

</body>
</html>"""

# Write HTML file
output_file = "spark_dashboard.html"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"Dashboard created: {output_file}")

# Stop Spark session
spark.stop()
print("Spark session stopped.")

# Open in browser
print("Opening dashboard in browser...")
webbrowser.open('file:///' + os.path.abspath(output_file))
