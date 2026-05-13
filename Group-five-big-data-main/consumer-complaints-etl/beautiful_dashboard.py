import os
import json
import warnings
import webbrowser
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

print("🚀 Creating Power BI Style Dashboard...")

# ====================== SPARK SETUP ======================
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["SPARK_HOME"] = r"C:\Users\Hybrid Technology\Downloads\spark-4.1.1-bin-hadoop3\spark-4.1.1-bin-hadoop3"
os.environ["HADOOP_HOME"] = os.environ["SPARK_HOME"]
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("Consumer_Complaints_PowerBI") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ====================== LOAD DATA ======================
df = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/Consumer_Complaints.csv")

total_complaints = df.count()
total_states     = df.select("State").distinct().count()

top_states_df = (
    df.groupBy("State")
      .agg(count("*").alias("complaints"))
      .orderBy(col("complaints").desc())
)

all_states_rows = top_states_df.collect()
states     = [row["State"] or "Unknown" for row in all_states_rows]
complaints = [int(row["complaints"])      for row in all_states_rows]

spark.stop()

# ====================== EXTRA METADATA (pop / lat / region) ======================
# Merged so the map & scatter work without a second CSV.
META = {
    "AL":("Alabama",4849377,32.80,-86.79,"South"),
    "AK":("Alaska",736732,64.20,-153.49,"West"),
    "AZ":("Arizona",6731484,33.73,-111.43,"West"),
    "AR":("Arkansas",2966369,34.97,-92.37,"South"),
    "CA":("California",38802500,36.78,-119.42,"West"),
    "CO":("Colorado",5355866,39.06,-105.31,"West"),
    "CT":("Connecticut",3596677,41.60,-72.67,"Northeast"),
    "DE":("Delaware",935614,38.99,-75.51,"Northeast"),
    "DC":("Dist. of Columbia",658893,38.90,-77.03,"Northeast"),
    "FL":("Florida",19893297,27.99,-81.76,"South"),
    "GA":("Georgia",10097343,32.16,-82.90,"South"),
    "HI":("Hawaii",1419561,19.90,-155.58,"West"),
    "ID":("Idaho",1634464,44.24,-114.48,"West"),
    "IL":("Illinois",12880580,40.35,-88.99,"Midwest"),
    "IN":("Indiana",6596855,39.85,-86.26,"Midwest"),
    "IA":("Iowa",3107126,42.03,-93.58,"Midwest"),
    "KS":("Kansas",2904021,38.53,-96.73,"Midwest"),
    "KY":("Kentucky",4413457,37.73,-84.29,"South"),
    "LA":("Louisiana",4649676,31.17,-91.87,"South"),
    "ME":("Maine",1330089,44.69,-69.38,"Northeast"),
    "MD":("Maryland",5976407,39.05,-76.64,"South"),
    "MA":("Massachusetts",6745408,42.41,-71.38,"Northeast"),
    "MI":("Michigan",9909877,43.33,-84.54,"Midwest"),
    "MN":("Minnesota",5457173,46.39,-94.63,"Midwest"),
    "MS":("Mississippi",2994079,32.74,-89.68,"South"),
    "MO":("Missouri",6063589,38.46,-92.29,"Midwest"),
    "MT":("Montana",1023579,46.88,-110.36,"West"),
    "NE":("Nebraska",1881503,41.49,-99.90,"Midwest"),
    "NV":("Nevada",2839099,38.31,-117.06,"West"),
    "NH":("New Hampshire",1326813,43.19,-71.57,"Northeast"),
    "NJ":("New Jersey",8938175,40.06,-74.41,"Northeast"),
    "NM":("New Mexico",2085572,34.84,-106.25,"West"),
    "NY":("New York",19746227,40.71,-74.01,"Northeast"),
    "NC":("North Carolina",9943964,35.63,-79.81,"South"),
    "ND":("North Dakota",739482,47.53,-99.78,"Midwest"),
    "OH":("Ohio",11594163,40.39,-82.76,"Midwest"),
    "OK":("Oklahoma",3878051,35.57,-96.93,"South"),
    "OR":("Oregon",3970239,43.94,-120.56,"West"),
    "PA":("Pennsylvania",12787209,40.59,-77.21,"Northeast"),
    "RI":("Rhode Island",1055173,41.68,-71.56,"Northeast"),
    "SC":("South Carolina",4832482,33.84,-80.90,"South"),
    "SD":("South Dakota",853175,44.44,-100.23,"Midwest"),
    "TN":("Tennessee",6549352,35.74,-86.69,"South"),
    "TX":("Texas",26956958,31.97,-99.90,"South"),
    "UT":("Utah",2942902,39.32,-111.09,"West"),
    "VT":("Vermont",626562,44.05,-72.71,"Northeast"),
    "VA":("Virginia",8326289,37.43,-78.66,"South"),
    "WA":("Washington",7061530,47.75,-120.74,"West"),
    "WV":("West Virginia",1850326,38.49,-80.95,"South"),
    "WI":("Wisconsin",5757564,44.27,-89.62,"Midwest"),
    "WY":("Wyoming",584153,42.76,-107.30,"West"),
}

# Build enriched JS-ready list
enriched = []
for abbr, cnt in zip(states, complaints):
    m = META.get(abbr, (abbr, 0, 0.0, 0.0, "Other"))
    enriched.append({
        "abbr": abbr, "name": m[0], "complaints": cnt,
        "pop": m[1], "lat": m[2], "lon": m[3], "region": m[4]
    })

js_data = json.dumps(enriched)

# ====================== HTML ======================
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Consumer Complaints Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0;}}
    body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f0f2f5;color:#222;}}

    /* Header */
    .hdr{{background:#1e3a5f;color:#fff;padding:10px 20px;display:flex;align-items:center;justify-content:space-between;}}
    .hdr h1{{font-size:18px;font-weight:600;}}
    .hdr span{{font-size:11px;color:#a0c4e8;}}

    /* Toolbar */
    .toolbar{{padding:8px 16px;display:flex;gap:7px;flex-wrap:wrap;background:#fff;border-bottom:1px solid #dce3eb;align-items:center;}}
    .btn{{font-size:11px;padding:4px 13px;border:1.5px solid #2e75b6;background:#fff;color:#2e75b6;
           border-radius:3px;cursor:pointer;transition:all .15s;font-family:inherit;}}
    .btn:hover{{background:#d6e8f7;}}
    .btn.active{{background:#1e3a5f;color:#fff;border-color:#1e3a5f;}}
    .btn.red{{border-color:#c0392b;color:#c0392b;}}
    .btn.red:hover{{background:#fde;}}
    .toolbar-label{{font-size:11px;color:#888;margin-left:8px;}}

    /* Layout */
    .db-body{{display:grid;grid-template-columns:200px 1fr 1fr;gap:11px;padding:12px;}}

    /* Cards */
    .card{{background:#fff;border:1px solid #d1d9e0;border-radius:4px;padding:10px 12px;}}
    .ct{{font-size:11px;font-weight:700;color:#1e3a5f;border-bottom:1.5px solid #2e75b6;
         padding-bottom:5px;margin-bottom:7px;}}
    .cs{{font-size:10px;color:#888;margin-bottom:5px;}}

    /* KPI column */
    .kpi-col{{grid-column:1;grid-row:1;display:flex;flex-direction:column;gap:8px;}}
    .kpi{{background:#fff;border:1px solid #d1d9e0;border-left:4px solid #1e3a5f;
           padding:10px 14px;border-radius:3px;}}
    .kpi.g{{border-left-color:#217346;}} .kpi.b{{border-left-color:#2e75b6;}} .kpi.t{{border-left-color:#1abc9c;}}
    .kv{{font-size:26px;font-weight:700;color:#1e3a5f;line-height:1.1;}}
    .kpi.g .kv{{color:#217346;}} .kpi.b .kv{{color:#2e75b6;}} .kpi.t .kv{{color:#1abc9c;}}
    .kl{{font-size:10px;color:#666;margin-top:2px;}}

    /* Grid positions */
    .bar-card{{grid-column:2;grid-row:1;}}
    .sct-card{{grid-column:3;grid-row:1;}}
    .map-card{{grid-column:1/3;grid-row:2;}}
    .dnt-card{{grid-column:3;grid-row:2;}}
    .chk-card{{grid-column:1/3;grid-row:3;}}
    .sid-card{{grid-column:3;grid-row:3;max-height:330px;overflow-y:auto;}}

    /* State checkboxes */
    .chk-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;max-height:170px;overflow-y:auto;}}
    .chk-row{{display:flex;align-items:center;gap:4px;padding:3px 4px;font-size:11px;
               cursor:pointer;border-radius:2px;transition:background .1s;}}
    .chk-row:hover{{background:#e8eef7;}}
    .chk-row input{{cursor:pointer;accent-color:#1e3a5f;}}

    /* Sidebar */
    .sid-row{{display:flex;align-items:center;gap:6px;padding:4px 2px;
               border-bottom:1px solid #f0f2f5;font-size:11px;cursor:pointer;}}
    .sid-row:hover{{background:#e8eef7;}}
    .swatch{{width:10px;height:10px;border-radius:1px;flex-shrink:0;}}
    .sid-n{{flex:1;color:#333;}} .sid-c{{color:#2e75b6;font-weight:600;}}

    /* Donut legend */
    .dleg{{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:5px;}}
    .dleg-i{{display:flex;align-items:center;gap:3px;font-size:10px;color:#555;}}
  </style>
</head>
<body>

<!-- Header -->
<div class="hdr">
  <h1>&#128202; Consumer Complaints Dashboard</h1>
  <span id="hdr-info">Total: {total_complaints:,} complaints &nbsp;|&nbsp; {total_states} States &nbsp;|&nbsp; Powered by Apache Spark 4.1.1</span>
</div>

<!-- Filter Toolbar -->
<div class="toolbar">
  <span class="toolbar-label">Filter by:</span>
  <button class="btn active" onclick="applyFilter('all',this)">All States</button>
  <button class="btn" onclick="applyFilter('West',this)">West</button>
  <button class="btn" onclick="applyFilter('South',this)">South</button>
  <button class="btn" onclick="applyFilter('Northeast',this)">Northeast</button>
  <button class="btn" onclick="applyFilter('Midwest',this)">Midwest</button>
  <button class="btn" onclick="applyFilter('top10',this)">&#9650; Top 10</button>
  <button class="btn" onclick="applyFilter('bot10',this)">&#9660; Bottom 10</button>
  <button class="btn red" onclick="applyFilter('all',document.querySelector('.btn.active'))">&#10006; Reset</button>
  <span id="sel-label" style="font-size:11px;color:#888;margin-left:auto;"></span>
</div>

<!-- Dashboard -->
<div class="db-body">

  <!-- KPIs -->
  <div class="kpi-col">
    <div class="kpi b"><div class="kv" id="k-pop">—</div><div class="kl">Sum of pop_2014</div></div>
    <div class="kpi">  <div class="kv" id="k-top">—</div><div class="kl">First state_abbrv</div></div>
    <div class="kpi g"><div class="kv" id="k-cnt">—</div><div class="kl">Sum of complaints_count</div></div>
    <div class="kpi t"><div class="kv" id="k-lat">—</div><div class="kl">Sum of latitude</div></div>
  </div>

  <!-- Bar Chart -->
  <div class="card bar-card">
    <div class="ct">Sum of complaints_count</div>
    <div class="cs">by state_name (Top 15 of selection)</div>
    <div id="bar" style="height:290px;"></div>
  </div>

  <!-- Scatter -->
  <div class="card sct-card">
    <div class="ct">Sum of pop_2014 &amp; complaints_count</div>
    <div class="cs">by state_name — bubble size = complaints</div>
    <div id="sct" style="height:290px;"></div>
  </div>

  <!-- Map -->
  <div class="card map-card">
    <div class="ct">Complaints by State — United States</div>
    <div class="cs">choropleth — click a state to isolate it</div>
    <div id="map" style="height:280px;"></div>
  </div>

  <!-- Donut -->
  <div class="card dnt-card">
    <div class="ct">Sum of complaints_count</div>
    <div class="cs">by state name</div>
    <div class="dleg" id="dleg"></div>
    <div id="dnt" style="height:230px;"></div>
  </div>

  <!-- State Checkboxes -->
  <div class="card chk-card">
    <div class="ct">state_name &nbsp;
      <span id="chk-badge" style="background:#1e3a5f;color:#fff;padding:1px 7px;border-radius:10px;font-size:10px;font-weight:400;"></span>
    </div>
    <div class="chk-grid" id="chk-grid"></div>
  </div>

  <!-- Sidebar -->
  <div class="card sid-card">
    <div class="ct">state_name</div>
    <div id="sidebar"></div>
  </div>

</div>

<script>
// ── Data injected by Python ──────────────────────────────────────────────────
const ALL = {js_data};

// ── State ────────────────────────────────────────────────────────────────────
let sel = new Set(ALL.map(s => s.abbr));
const C = ['#1e3a5f','#2e75b6','#217346','#c55a11','#7030a0','#0070c0','#00b050','#d9534f','#954f72','#e6ac00','#2e4057','#17a589'];

function fmtK(n){{ return n>=1e6?(n/1e6).toFixed(1)+'M':n>=1000?(n/1000).toFixed(0)+'K':String(n); }}
function fd(){{ return ALL.filter(s=>sel.has(s.abbr)); }}

// ── Filter buttons ────────────────────────────────────────────────────────────
function applyFilter(type, btn){{
  document.querySelectorAll('.btn').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  const srt = [...ALL].sort((a,b)=>b.complaints-a.complaints);
  if(type==='all')          sel = new Set(ALL.map(s=>s.abbr));
  else if(type==='top10')   sel = new Set(srt.slice(0,10).map(s=>s.abbr));
  else if(type==='bot10')   sel = new Set(srt.slice(-10).map(s=>s.abbr));
  else                      sel = new Set(ALL.filter(s=>s.region===type).map(s=>s.abbr));
  renderAll();
}}

// ── Solo a state (click sidebar or map) ──────────────────────────────────────
function soloState(abbr){{
  if(sel.size===1 && sel.has(abbr)) sel = new Set(ALL.map(s=>s.abbr));
  else sel = new Set([abbr]);
  renderAll();
}}

function toggleState(abbr){{
  sel.has(abbr) ? sel.delete(abbr) : sel.add(abbr);
  renderAll(false);
}}

// ── KPIs ──────────────────────────────────────────────────────────────────────
function renderKPIs(){{
  const d=fd();
  const top=d.reduce((a,b)=>a.complaints>b.complaints?a:b, d[0]||{{}});
  document.getElementById('k-pop').textContent = fmtK(d.reduce((a,b)=>a+b.pop,0));
  document.getElementById('k-top').textContent = top.abbr||'—';
  document.getElementById('k-cnt').textContent = fmtK(d.reduce((a,b)=>a+b.complaints,0));
  document.getElementById('k-lat').textContent = d.reduce((a,b)=>a+b.lat,0).toFixed(2);
  document.getElementById('sel-label').textContent = d.length + ' states selected';
}}

// ── Bar chart ─────────────────────────────────────────────────────────────────
function renderBar(){{
  const d=fd().sort((a,b)=>b.complaints-a.complaints).slice(0,15);
  Plotly.react('bar',[{{
    type:'bar', orientation:'h',
    x:d.map(s=>s.complaints), y:d.map(s=>s.name),
    text:d.map(s=>s.complaints), textposition:'inside', insidetextanchor:'middle',
    marker:{{color:d.map((_,i)=>i===0?'#1e3a5f':'#2e75b6')}},
    hovertemplate:'<b>%{{y}}</b><br>%{{x}} complaints<extra></extra>'
  }}],{{
    paper_bgcolor:'#fff', plot_bgcolor:'#fff',
    margin:{{l:130,r:30,t:10,b:30}},
    font:{{family:'Segoe UI',size:11}},
    xaxis:{{gridcolor:'#e8edf2',zeroline:false}},
    yaxis:{{autorange:'reversed',tickfont:{{size:11}}}}
  }},{{responsive:true}});
}}

// ── Scatter ───────────────────────────────────────────────────────────────────
function renderScatter(){{
  const d=fd().sort((a,b)=>b.complaints-a.complaints).slice(0,15);
  Plotly.react('sct',[{{
    type:'scatter', mode:'markers+text',
    x:d.map(s=>s.pop/1e6), y:d.map(s=>s.complaints),
    text:d.map(s=>s.abbr), textposition:'top center',
    textfont:{{size:10,color:'#333'}},
    marker:{{
      size:d.map(s=>Math.max(10,Math.sqrt(s.complaints)*1.8)),
      color:d.map((_,i)=>C[i%C.length]),
      line:{{color:'#fff',width:1}}
    }},
    hovertemplate:'<b>%{{text}}</b><br>Pop: %{{x:.1f}}M<br>Complaints: %{{y}}<extra></extra>'
  }}],{{
    paper_bgcolor:'#fff', plot_bgcolor:'#fff',
    margin:{{l:50,r:20,t:10,b:50}},
    font:{{family:'Segoe UI',size:10}},
    xaxis:{{title:'Sum of pop_2014 (M)',gridcolor:'#e8edf2',zeroline:false}},
    yaxis:{{title:'complaints',gridcolor:'#e8edf2',zeroline:false}}
  }},{{responsive:true}});
}}

// ── Choropleth Map ────────────────────────────────────────────────────────────
function renderMap(){{
  const d=fd();
  const mapDiv = document.getElementById('map');
  Plotly.react('map',[{{
    type:'choropleth', locationmode:'USA-states',
    locations:d.map(s=>s.abbr), z:d.map(s=>s.complaints),
    text:d.map(s=>s.name+'<br>'+s.complaints.toLocaleString()+' complaints'),
    hoverinfo:'text',
    colorscale:[[0,'#deebf7'],[0.25,'#9ecae1'],[0.5,'#4292c6'],[0.75,'#2171b5'],[1,'#1e3a5f']],
    colorbar:{{title:'Complaints',thickness:12,titlefont:{{size:10}},tickfont:{{size:10}}}},
    marker:{{line:{{color:'#fff',width:0.5}}}}
  }}],{{
    geo:{{scope:'usa',showlakes:true,lakecolor:'#cde',bgcolor:'#fff',landcolor:'#f0f2f5'}},
    paper_bgcolor:'#fff',
    margin:{{l:0,r:0,t:0,b:0}},
    font:{{family:'Segoe UI'}}
  }},{{responsive:true}});

  // Click a state on the map → solo it
  mapDiv.removeAllListeners && mapDiv.removeAllListeners('plotly_click');
  mapDiv.on('plotly_click', function(data){{
    if(data.points && data.points[0]){{
      soloState(data.points[0].location);
    }}
  }});
}}

// ── Donut ─────────────────────────────────────────────────────────────────────
function renderDonut(){{
  const d=fd().sort((a,b)=>b.complaints-a.complaints);
  const top6=d.slice(0,6), rest=d.slice(6).reduce((a,b)=>a+b.complaints,0);
  const labels=[...top6.map(s=>s.name),...(rest>0?['Others']:[])];
  const values=[...top6.map(s=>s.complaints),...(rest>0?[rest]:[])];
  const colors=[...C.slice(0,top6.length),'#aaa'];
  const total=values.reduce((a,b)=>a+b,0);

  document.getElementById('dleg').innerHTML = labels.map((l,i)=>
    `<span class="dleg-i">
      <span style="width:9px;height:9px;border-radius:2px;background:${{colors[i]}};display:inline-block;"></span>
      ${{l}} ${{Math.round(values[i]/total*100)}}%
    </span>`).join('');

  Plotly.react('dnt',[{{
    type:'pie', hole:0.62,
    labels, values, marker:{{colors}},
    textinfo:'none',
    hovertemplate:'<b>%{{label}}</b><br>%{{value}} complaints (%{{percent}})<extra></extra>'
  }}],{{
    paper_bgcolor:'#fff',
    margin:{{l:10,r:10,t:10,b:10}},
    showlegend:false,
    annotations:[{{
      text:'<b>'+fmtK(total)+'</b><br><span style="font-size:10px">complaints</span>',
      x:0.5,y:0.5,showarrow:false,font:{{size:14,color:'#1e3a5f'}}
    }}]
  }},{{responsive:true}});
}}

// ── Checkboxes ────────────────────────────────────────────────────────────────
function renderCheckboxes(){{
  const sorted=[...ALL].sort((a,b)=>a.name.localeCompare(b.name));
  document.getElementById('chk-grid').innerHTML = sorted.map(s=>`
    <label class="chk-row">
      <input type="checkbox" ${{sel.has(s.abbr)?'checked':''}} onchange="toggleState('${{s.abbr}}')">
      <span>${{s.abbr}}</span>
    </label>`).join('');
  document.getElementById('chk-badge').textContent = ALL.length + ' states';
}}

// ── Sidebar ───────────────────────────────────────────────────────────────────
function renderSidebar(){{
  const sorted=[...ALL].sort((a,b)=>a.name.localeCompare(b.name));
  const topAbbr=fd().sort((a,b)=>b.complaints-a.complaints)[0]?.abbr;
  document.getElementById('sidebar').innerHTML = sorted.map(s=>`
    <div class="sid-row" onclick="soloState('${{s.abbr}}')">
      <span class="swatch" style="background:${{
        s.abbr===topAbbr?'#1e3a5f':sel.has(s.abbr)?'#2e75b6':'#ddd'
      }};"></span>
      <span class="sid-n">${{s.name}}</span>
      <span class="sid-c">${{s.complaints.toLocaleString()}}</span>
    </div>`).join('');
}}

// ── Render all ────────────────────────────────────────────────────────────────
function renderAll(rebuildChk=true){{
  renderKPIs();
  renderBar();
  renderScatter();
  renderMap();
  renderDonut();
  if(rebuildChk) renderCheckboxes();
  renderSidebar();
}}

renderAll();
</script>
</body>
</html>"""

# ====================== SAVE & OPEN ======================
output_path = "powerbi_style_dashboard.html"
with open(output_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"✅ Power BI Style Dashboard Created Successfully!")
print(f"📂 File: {os.path.abspath(output_path)}")
webbrowser.open("file:///" + os.path.abspath(output_path).replace("\\", "/"))