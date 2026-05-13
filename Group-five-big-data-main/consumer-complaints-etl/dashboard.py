import duckdb
import os
import webbrowser

conn = duckdb.connect('output/consumer_analytics.duckdb')
rows = conn.execute('''
    SELECT state_abbrv, state_name, complaints_count, pop_2014,
           ROUND(complaints_count * 1.0 / pop_2014, 6) AS complaints_per_capita,
           latitude, longitude
    FROM consumer_insights
    ORDER BY complaints_count DESC
''').fetchall()
conn.close()

states = [{"abbr": r[0], "name": r[1], "complaints": r[2],
           "pop": r[3], "per_capita": r[4], "lat": r[5], "lon": r[6]} for r in rows]

total_complaints = sum(s['complaints'] for s in states)
total_states = len(states)
top_state = states[0]
top_percapita = max(states, key=lambda x: x['per_capita'])
top10 = states[:10]
max_c = top10[0]['complaints']
top_pc = sorted(states, key=lambda x: x['per_capita'], reverse=True)[:10]
max_pc = top_pc[0]['per_capita']

bar1 = ""
for s in top10:
    pct = int(s['complaints'] / max_c * 100)
    bar1 += f"""
    <div class="bar-row">
      <span class="bar-label">{s['abbr']}</span>
      <div class="bar-track">
        <div class="bar-fill" style="width:{pct}%;background:#3b82f6">{s['complaints']:,}</div>
      </div>
    </div>"""

bar2 = ""
for s in top_pc:
    pct = int(s['per_capita'] / max_pc * 100)
    bar2 += f"""
    <div class="bar-row">
      <span class="bar-label">{s['abbr']}</span>
      <div class="bar-track">
        <div class="bar-fill" style="width:{pct}%;background:#8b5cf6">{s['per_capita']:.4f}</div>
      </div>
    </div>"""

table_rows = ""
for i, s in enumerate(states, 1):
    table_rows += f"""<tr>
      <td><span class="badge">{i}</span></td>
      <td><b>{s['abbr']}</b></td>
      <td>{s['name']}</td>
      <td>{s['complaints']:,}</td>
      <td>{s['pop']:,}</td>
      <td>{s['per_capita']:.6f}</td>
      <td>{s['lat']}</td>
      <td>{s['lon']}</td>
    </tr>"""

html = """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Consumer Complaints Dashboard</title>
<style>
  body {
    font-family: Arial, sans-serif;
    background: #0f172a;
    color: #e2e8f0;
    margin: 0;
    padding: 20px;
  }
  h1 {
    text-align: center;
    color: #38bdf8;
    font-size: 28px;
    margin-bottom: 5px;
  }
  p.sub {
    text-align: center;
    color: #94a3b8;
    margin-bottom: 30px;
  }
  .cards {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 30px;
  }
  .card {
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
    text-align: center;
  }
  .card h2 {
    font-size: 32px;
    color: #38bdf8;
    margin: 0;
  }
  .card p {
    color: #94a3b8;
    margin: 5px 0 0;
    font-size: 14px;
  }
  .charts {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 30px;
  }
  .chart-box {
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
  }
  .chart-box h3 {
    color: #38bdf8;
    margin: 0 0 15px;
    font-size: 16px;
  }
  .bar-row {
    display: flex;
    align-items: center;
    margin-bottom: 8px;
    gap: 8px;
  }
  .bar-label {
    width: 30px;
    font-size: 12px;
    color: #94a3b8;
    text-align: right;
  }
  .bar-track {
    flex: 1;
    background: #0f172a;
    border-radius: 4px;
    height: 22px;
  }
  .bar-fill {
    height: 22px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    padding-left: 8px;
    font-size: 11px;
    color: white;
    min-width: 40px;
  }
  .table-box {
    background: #1e293b;
    border-radius: 10px;
    padding: 20px;
  }
  .table-box h3 {
    color: #38bdf8;
    margin: 0 0 15px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  th {
    background: #0f172a;
    color: #38bdf8;
    padding: 10px;
    text-align: left;
  }
  td {
    padding: 9px 10px;
    border-bottom: 1px solid #334155;
    color: #cbd5e1;
  }
  tr:hover td {
    background: #273449;
  }
  .badge {
    background: #1d4ed8;
    color: white;
    padding: 2px 8px;
    border-radius: 20px;
    font-size: 11px;
  }
</style>
</head>
<body>

<h1>Consumer Complaints Dashboard</h1>
<p class="sub">US Consumer Financial Complaints by State — 51 States Analyzed</p>

<div class="cards">
  <div class="card">
    <h2>""" + f"{total_complaints:,}" + """</h2>
    <p>Total Complaints</p>
  </div>
  <div class="card">
    <h2>""" + f"{total_states}" + """</h2>
    <p>States Covered</p>
  </div>
  <div class="card">
    <h2>""" + f"{top_state['abbr']}" + """</h2>
    <p>Most Complaints (""" + f"{top_state['complaints']:,}" + """)</p>
  </div>
  <div class="card">
    <h2>""" + f"{top_percapita['abbr']}" + """</h2>
    <p>Highest Per Capita</p>
  </div>
</div>

<div class="charts">
  <div class="chart-box">
    <h3>Top 10 States by Total Complaints</h3>
    """ + bar1 + """
  </div>
  <div class="chart-box">
    <h3>Top 10 States by Complaints Per Capita</h3>
    """ + bar2 + """
  </div>
</div>

<div class="table-box">
  <h3>Full Data Table — All 51 States</h3>
  <table>
    <tr>
      <th>#</th>
      <th>State</th>
      <th>Full Name</th>
      <th>Complaints</th>
      <th>Population</th>
      <th>Per Capita</th>
      <th>Latitude</th>
      <th>Longitude</th>
    </tr>
    """ + table_rows + """
  </table>
</div>

</body>
</html>"""

with open('dashboard.html', 'w', encoding='utf-8') as f:
    f.write(html)

print('Dashboard created successfully!')
print('Opening in browser...')
webbrowser.open('file:///' + os.path.abspath('dashboard.html'))