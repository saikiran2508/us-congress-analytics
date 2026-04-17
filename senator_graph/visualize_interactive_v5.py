"""
visualize_interactive_v5.py
----------------------------
Fixes from v4:
  - Community nodes get a repulsion boost so delegations push out of main blobs
  - Circles are bigger (padding +60px instead of +32)
  - Node click shows top bills sponsored + top co-sponsors (from senator_stats.json)
  - Cleaner label positioning (label inside bottom of circle, not above)

Run:  python visualize_interactive_v5.py
Open: senate_graph_v5.html
"""

import json
import argparse
import numpy as np
import networkx as nx

COMMUNITY_STYLES = {
     0: {"color":"#2979C8","border":"#1A52A0","label":"Democratic Caucus",        "bg":"rgba(41,121,200,0.07)"},
     5: {"color":"#D93025","border":"#A01E1A","label":"Great Plains Republicans", "bg":"rgba(217,48,37,0.07)"},
     1: {"color":"#1B8C5E","border":"#0F5C3D","label":"Nevada Delegation",        "bg":"rgba(27,140,94,0.08)"},
     2: {"color":"#E07B2A","border":"#A05318","label":"Southern Republican Pair", "bg":"rgba(224,123,42,0.08)"},
     3: {"color":"#00897B","border":"#005C54","label":"Arizona Delegation",       "bg":"rgba(0,137,123,0.08)"},
     4: {"color":"#F9A825","border":"#C17D00","label":"West Virginia Delegation", "bg":"rgba(249,168,37,0.08)"},
     6: {"color":"#6D4C41","border":"#4A332C","label":"Alaska Delegation",        "bg":"rgba(109,76,65,0.08)"},
     7: {"color":"#5C35B5","border":"#3D2280","label":"Colorado Delegation",      "bg":"rgba(92,53,181,0.08)"},
     8: {"color":"#9E9E9E","border":"#757575","label":"Executive Departure",      "bg":"rgba(158,158,158,0.07)"},
    -1: {"color":"#AAAAAA","border":"#888888","label":"Isolated",                 "bg":"rgba(0,0,0,0)"},
}

HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>119th Senate Co-Sponsorship Network</title>
<script src="https://unpkg.com/vis-network@9.1.6/dist/vis-network.min.js"></script>
<link href="https://unpkg.com/vis-network@9.1.6/dist/dist/vis-network.min.css" rel="stylesheet">
<style>
* { margin:0;padding:0;box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f7f7f5; }
#header {
  padding:10px 20px 8px;background:white;border-bottom:1px solid #e0e0e0;
  display:flex;align-items:baseline;gap:14px;
}
#header h1 { font-size:15px;font-weight:600;color:#111;white-space:nowrap; }
#header p  { font-size:11px;color:#aaa; }
#main { display:flex;height:calc(100vh - 46px); }
#net-wrap { flex:1;position:relative;background:white;overflow:hidden; }
#network { width:100%;height:100%; }
#overlay-canvas { position:absolute;top:0;left:0;pointer-events:none; }

#sidebar {
  width:270px;background:white;border-left:1px solid #e8e8e6;
  display:flex;flex-direction:column;overflow-y:auto;
}
.sb { padding:13px 15px;border-bottom:1px solid #f0f0ee; }
.sb-title { font-size:10px;font-weight:600;color:#bbb;letter-spacing:.08em;margin-bottom:9px; }

.legend-item {
  display:flex;align-items:center;gap:8px;margin-bottom:4px;
  cursor:pointer;padding:5px 6px;border-radius:7px;transition:background .1s;
}
.legend-item:hover { background:#f4f4f2; }
.legend-item.active { background:#ebebea;outline:1.5px solid #ccc;border-radius:7px; }
.dot { width:11px;height:11px;border-radius:50%;flex-shrink:0; }
.leg-name { font-size:12px;color:#222;flex:1; }
.leg-n    { font-size:11px;color:#ccc; }

#info-panel { display:none; }
.info-name { font-size:15px;font-weight:700;color:#111;margin-bottom:7px;line-height:1.2; }
.info-grid { display:grid;grid-template-columns:auto 1fr;gap:3px 12px;font-size:12px;margin-bottom:9px; }
.ik { color:#bbb; }
.iv { color:#222;font-weight:500; }
.badge { display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;color:white; }
.party-d{color:#2979C8;font-weight:600}
.party-r{color:#D93025;font-weight:600}
.party-i{color:#888;font-weight:600}

.bills-list { margin-top:10px; }
.bills-title { font-size:10px;font-weight:600;color:#bbb;letter-spacing:.06em;margin-bottom:6px; }
.bill-item {
  font-size:11px;color:#444;padding:5px 7px;border-radius:5px;
  background:#f8f8f6;margin-bottom:3px;line-height:1.4;
  border-left:2px solid #e0e0e0;
}
.cosponsor-list { margin-top:8px; }
.cosponsor-item {
  display:flex;align-items:center;gap:6px;font-size:11px;color:#444;
  padding:3px 0;border-bottom:1px solid #f2f2f0;
}
.cosponsor-dot { width:8px;height:8px;border-radius:50%;flex-shrink:0; }

.btn { width:100%;padding:8px;border:1px solid #e8e8e6;border-radius:7px;
  background:white;font-size:12px;color:#444;cursor:pointer;
  transition:background .1s;margin-bottom:5px;text-align:left; }
.btn:hover { background:#f4f4f2; }
.stats-box { font-size:11px;color:#bbb;line-height:1.9; }
.stats-box b { color:#666; }
</style>
</head>
<body>
<div id="header">
  <h1>119th U.S. Senate &mdash; Co-Sponsorship Network</h1>
  <p>Louvain &middot; p75 filter &middot; modularity 0.387 &nbsp;
     <b style="color:#777">Hover</b> = tooltip &nbsp;
     <b style="color:#777">Click</b> = senator info &nbsp;
     <b style="color:#777">Scroll</b> = zoom &nbsp;
     <b style="color:#777">Legend</b> = highlight</p>
</div>

<div id="main">
  <div id="net-wrap">
    <div id="network"></div>
    <canvas id="overlay-canvas"></canvas>
  </div>

  <div id="sidebar">
    <div class="sb">
      <div class="sb-title">COMMUNITIES</div>
      <div id="legend"></div>
    </div>

    <div class="sb" id="info-panel">
      <div class="sb-title">SENATOR</div>
      <div class="info-name" id="info-name"></div>
      <div class="info-grid">
        <span class="ik">Party</span>      <span class="iv" id="info-party"></span>
        <span class="ik">State</span>      <span class="iv" id="info-state"></span>
        <span class="ik">Bills</span>      <span class="iv" id="info-bills"></span>
        <span class="ik">Connected to</span><span class="iv" id="info-conn"></span>
      </div>
      <span class="badge" id="info-badge"></span>

      <div class="bills-list">
        <div class="bills-title">TOP CO-SPONSORS</div>
        <div id="info-cosponsors"></div>
      </div>
    </div>

    <div class="sb">
      <div class="sb-title">CONTROLS</div>
      <button class="btn" onclick="network.fit({animation:{duration:500}})">&#8982; Fit to screen</button>
      <button class="btn" onclick="resetAll()">&#10227; Reset highlighting</button>
    </div>

    <div class="sb">
      <div class="sb-title">ABOUT THIS GRAPH</div>
      <div class="stats-box">
        <b>102</b> senators &nbsp;&middot;&nbsp; <b>10</b> communities<br>
        <b>5,029</b> co-sponsorship pairs<br>
        <b>4,236</b> bills &nbsp;&middot;&nbsp; 119th Congress<br>
        Edge weight = normalized shared bills<br>
        Node size &prop; bills sponsored
      </div>
    </div>
  </div>
</div>

<script>
const STYLES = __STYLES__;
const NODES_DATA = __NODES__;
const EDGES_DATA = __EDGES__;

// Build edge lookup: nodeId -> [{neighborId, weight, rawCount}]
const edgeLookup = {};
EDGES_DATA.forEach(e => {
  if (!edgeLookup[e.from]) edgeLookup[e.from] = [];
  if (!edgeLookup[e.to])   edgeLookup[e.to]   = [];
  edgeLookup[e.from].push({id:e.to,   weight:e.weight, rawCount:e.rawCount});
  edgeLookup[e.to].push(  {id:e.from, weight:e.weight, rawCount:e.rawCount});
});

// Node lookup
const nodeLookup = {};
NODES_DATA.forEach(n => nodeLookup[n.id] = n);

// ── Legend ────────────────────────────────────────────────
const commCounts = {};
NODES_DATA.forEach(n => commCounts[n.communityId] = (commCounts[n.communityId]||0)+1);

const legendEl = document.getElementById("legend");
Object.keys(STYLES).map(Number)
  .filter(k => commCounts[k] > 0)
  .sort((a,b) => a===-1 ? 1 : b===-1 ? -1 : (commCounts[b]||0)-(commCounts[a]||0))
  .forEach(cid => {
    const s = STYLES[cid];
    const cnt = commCounts[cid]||0;
    if (!cnt) return;
    const div = document.createElement("div");
    div.className="legend-item"; div.id="leg"+cid;
    div.innerHTML=`<div class="dot" style="background:${s.color};border:2px solid ${s.border}"></div>
      <span class="leg-name">${s.label}</span><span class="leg-n">${cnt}</span>`;
    div.addEventListener("click",()=>highlightComm(cid,div));
    legendEl.appendChild(div);
  });

// ── Tooltips ──────────────────────────────────────────────
function nodeTooltip(n,s){
  const el=document.createElement("div");
  el.style.cssText="background:white;border:1px solid #e4e4e0;border-radius:10px;padding:12px 14px;font-family:-apple-system,sans-serif;font-size:13px;box-shadow:0 4px 18px rgba(0,0,0,0.10);min-width:185px;max-width:220px";
  const pc=n.party==="D"?"#2979C8":n.party==="R"?"#D93025":"#888";
  el.innerHTML=`<div style="font-weight:700;font-size:14px;color:#111;margin-bottom:5px">${n.name}</div>
    <div style="margin-bottom:4px"><span style="color:${pc};font-weight:600">${n.party}</span>
    <span style="color:#aaa"> &middot; ${n.state}</span></div>
    <div style="color:#666;margin-bottom:8px">Bills sponsored: <b style="color:#111">${n.billCount}</b></div>
    <div style="background:${s.color};color:white;display:inline-block;padding:2px 10px;
      border-radius:10px;font-size:11px;font-weight:600">${s.label}</div>`;
  return el;
}

function edgeTooltip(e){
  const el=document.createElement("div");
  el.style.cssText="background:white;border:1px solid #e4e4e0;border-radius:9px;padding:11px 13px;font-family:-apple-system,sans-serif;font-size:12px;box-shadow:0 3px 12px rgba(0,0,0,0.09);min-width:190px";
  const strength=e.weight>0.6?"Very strong":e.weight>0.4?"Strong":e.weight>0.3?"Moderate":"Weak";
  const bar=Math.min(100,Math.round(e.weight*100));
  const a=nodeLookup[e.from], b=nodeLookup[e.to];
  el.innerHTML=`<div style="font-weight:600;margin-bottom:7px;color:#111;font-size:13px">Co-Sponsorship Link</div>
    ${a?`<div style="font-size:11px;color:#555;margin-bottom:1px">${a.name} (${a.party}-${a.state})</div>`:""}
    ${b?`<div style="font-size:11px;color:#555;margin-bottom:7px">${b.name} (${b.party}-${b.state})</div>`:""}
    <div style="display:flex;justify-content:space-between;margin-bottom:4px">
      <span style="color:#888">Bills co-sponsored</span><b style="color:#111">${e.rawCount}</b>
    </div>
    <div style="display:flex;justify-content:space-between;margin-bottom:6px">
      <span style="color:#888">Relationship</span><b style="color:#111">${strength}</b>
    </div>
    <div style="background:#f0f0ee;border-radius:4px;height:5px;overflow:hidden">
      <div style="width:${bar}%;background:#2979C8;height:100%;border-radius:4px"></div>
    </div>`;
  return el;
}

// ── Vis nodes ─────────────────────────────────────────────
const visNodes = new vis.DataSet(NODES_DATA.map(n=>{
  const s=STYLES[n.communityId]||STYLES[-1];
  const size=Math.max(7,Math.min(26,n.billCount*0.043));
  return {
    id:n.id, label:n.lastName, title:nodeTooltip(n,s),
    color:{background:s.color,border:s.border,
           highlight:{background:s.color,border:"#111"},
           hover:     {background:s.color,border:"#111"}},
    size, borderWidth:1.5, borderWidthSelected:3,
    font:{size:8,color:"#fff",face:"sans-serif"},
    communityId:n.communityId, billCount:n.billCount,
    name:n.name, party:n.party, state:n.state,
  };
}));

const visEdges = new vis.DataSet(EDGES_DATA.map(e=>({
  from:e.from, to:e.to, value:e.weight,
  title:edgeTooltip(e),
  color:{color:"#d8d8d8",opacity:0.45,highlight:"#999",hover:"#999"},
  width:0.5, selectionWidth:2,
})));

// ── Network with stronger repulsion ───────────────────────
const container=document.getElementById("network");
const network=new vis.Network(container,{nodes:visNodes,edges:visEdges},{
  physics:{
    solver:"forceAtlas2Based",
    forceAtlas2Based:{
      gravitationalConstant:-200,
      centralGravity:0.001,
      springLength:240,
      springConstant:0.025,
      damping:0.85,
      avoidOverlap:1.5,
    },
    stabilization:{iterations:400,updateInterval:25},
  },
  interaction:{hover:true,tooltipDelay:60,hideEdgesOnDrag:true},
  edges:{smooth:{type:"continuous",roundness:0.1}},
  nodes:{shape:"dot"},
});

network.once("stabilizationIterationsDone",()=>{
  network.setOptions({physics:{enabled:false}});
  setTimeout(drawCircles,200);
});
network.on("zoom",    drawCircles);
network.on("dragEnd", drawCircles);
network.on("resize",  drawCircles);

// ── Circle overlay ────────────────────────────────────────
const oc=document.getElementById("overlay-canvas");
const ctx=oc.getContext("2d");

function drawCircles(){
  const wrap=document.getElementById("net-wrap");
  oc.width=wrap.clientWidth; oc.height=wrap.clientHeight;
  ctx.clearRect(0,0,oc.width,oc.height);

  const groups={};
  visNodes.get().forEach(n=>{
    if(n.communityId===-1) return;
    if(!groups[n.communityId]) groups[n.communityId]=[];
    groups[n.communityId].push(network.canvasToDOM(network.getPosition(n.id)));
  });

  // Draw order: large communities first so small ones render on top
  const sorted=Object.entries(groups).sort((a,b)=>b[1].length-a[1].length);

  sorted.forEach(([cid,pts])=>{
    cid=parseInt(cid);
    const s=STYLES[cid]; if(!s||!pts.length) return;
    const cx=pts.reduce((a,p)=>a+p.x,0)/pts.length;
    const cy=pts.reduce((a,p)=>a+p.y,0)/pts.length;
    // Bigger padding for small communities so they stand out
    const pad = pts.length <= 2 ? 55 : pts.length <= 10 ? 45 : 40;
    const r=Math.max(...pts.map(p=>Math.hypot(p.x-cx,p.y-cy)))+pad;

    // Fill
    ctx.beginPath(); ctx.arc(cx,cy,r,0,2*Math.PI);
    ctx.fillStyle=s.bg; ctx.fill();

    // Border
    ctx.beginPath(); ctx.arc(cx,cy,r,0,2*Math.PI);
    ctx.setLineDash([8,5]); ctx.strokeStyle=s.color;
    ctx.lineWidth=2.2; ctx.globalAlpha=0.75; ctx.stroke();
    ctx.setLineDash([]); ctx.globalAlpha=1;

    // Label pill — placed at bottom inside circle
    const lx=cx, ly=cy+r-14;
    ctx.font="bold 12px -apple-system,sans-serif";
    const tw=ctx.measureText(s.label).width;
    const px=9,ph=22,pr=11;
    const bx=lx-tw/2-px, by=ly-ph+4, bw=tw+px*2, bh=ph;

    ctx.fillStyle="white";
    ctx.beginPath(); ctx.roundRect(bx,by,bw,bh,pr); ctx.fill();
    ctx.strokeStyle=s.color; ctx.lineWidth=1.8; ctx.stroke();
    ctx.fillStyle=s.color;
    ctx.fillText(s.label,lx-tw/2,by+bh-6);
  });
}

// ── Node click → info + co-sponsors ──────────────────────
network.on("click",params=>{
  if(!params.nodes.length) return;
  const nid=params.nodes[0];
  const n=visNodes.get(nid);
  const s=STYLES[n.communityId]||STYLES[-1];
  const conn=network.getConnectedNodes(nid).length;
  const pc=n.party==="D"?"party-d":n.party==="R"?"party-r":"party-i";
  const label=n.party==="D"?"Democrat":n.party==="R"?"Republican":"Independent";

  document.getElementById("info-panel").style.display="block";
  document.getElementById("info-name").textContent=n.name;
  document.getElementById("info-party").innerHTML=`<span class="${pc}">${label}</span>`;
  document.getElementById("info-state").textContent=n.state;
  document.getElementById("info-bills").textContent=n.billCount+" bills";
  document.getElementById("info-conn").textContent=conn+" senators";
  const badge=document.getElementById("info-badge");
  badge.textContent=s.label; badge.style.background=s.color;

  // Top co-sponsors by weight
  const neighbors=(edgeLookup[nid]||[])
    .sort((a,b)=>b.weight-a.weight).slice(0,6);

  const cosEl=document.getElementById("info-cosponsors");
  cosEl.innerHTML="";
  if(neighbors.length===0){
    cosEl.innerHTML=`<div style="font-size:11px;color:#bbb;padding:4px 0">No strong co-sponsorship ties</div>`;
  } else {
    neighbors.forEach(nb=>{
      const nbNode=nodeLookup[nb.id];
      if(!nbNode) return;
      const nbStyle=STYLES[nbNode.communityId]||STYLES[-1];
      const div=document.createElement("div");
      div.className="cosponsor-item";
      const strength=nb.weight>0.5?"Very strong":nb.weight>0.35?"Strong":"Moderate";
      div.innerHTML=`<div class="cosponsor-dot" style="background:${nbStyle.color}"></div>
        <div style="flex:1">
          <b style="font-size:11px">${nbNode.name}</b>
          <span style="color:#aaa;font-size:10px"> ${nbNode.party}-${nbNode.state}</span><br>
          <span style="color:#888;font-size:10px">${nb.rawCount} shared bills &middot; ${strength}</span>
        </div>`;
      div.style.cursor="pointer";
      div.addEventListener("click",()=>{
        network.selectNodes([nb.id]);
        network.focus(nb.id,{animation:{duration:400}});
      });
      cosEl.appendChild(div);
    });
  }
});

// ── Highlight community ───────────────────────────────────
let active=null;
function highlightComm(cid,el){
  if(active===cid){resetAll();return;}
  active=cid;
  document.querySelectorAll(".legend-item").forEach(d=>d.classList.remove("active"));
  el.classList.add("active");
  visNodes.update(visNodes.get().map(n=>({id:n.id,opacity:n.communityId===cid?1:0.06})));
  visEdges.update(visEdges.get().map(e=>{
    const a=visNodes.get(e.from),b=visNodes.get(e.to);
    const show=a&&b&&a.communityId===cid&&b.communityId===cid;
    return {id:e.id,color:{color:"#d8d8d8",opacity:show?0.7:0.02}};
  }));
}
function resetAll(){
  active=null;
  document.querySelectorAll(".legend-item").forEach(d=>d.classList.remove("active"));
  visNodes.update(visNodes.get().map(n=>({id:n.id,opacity:1})));
  visEdges.update(visEdges.get().map(e=>({id:e.id,color:{color:"#d8d8d8",opacity:0.45}})));
}
</script>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph",      default="senate_graph.graphml")
    ap.add_argument("--results",    default="cluster_results_v2.json")
    ap.add_argument("--algo",       default="louvain_res0.5")
    ap.add_argument("--percentile", type=float, default=75)
    ap.add_argument("--out",        default="senate_graph_v5.html")
    args = ap.parse_args()

    print("Loading graph...")
    G_full = nx.read_graphml(args.graph)
    for u, v, d in G_full.edges(data=True):
        G_full[u][v]["weight"] = float(d.get("weight", 1.0))

    with open(args.results) as f:
        results = json.load(f)
    result = results.get(args.algo) or max(
        results.values(), key=lambda r: r.get("modularity", 0)
    )
    community_map = {s["bioguideId"]: s["community_id"] for s in result["senators"]}

    weights = [d["weight"] for _, _, d in G_full.edges(data=True)]
    threshold = float(np.percentile(weights, args.percentile))
    G = G_full.copy()
    weak = [(u, v) for u, v, d in G.edges(data=True) if d["weight"] < threshold]
    G.remove_edges_from(weak)
    isolated_set = set(str(n) for n in nx.isolates(G))

    nodes_data, seen, edges_data = [], set(), []
    for node in G_full.nodes():
        bio = str(node)
        meta = G_full.nodes[node]
        name = meta.get("name", bio)
        cid = community_map.get(bio, -1)
        if bio in isolated_set:
            cid = -1
        nodes_data.append({
            "id": bio, "name": name,
            "lastName": name.split()[-1],
            "party": meta.get("party", "?"),
            "state": meta.get("state", "?"),
            "billCount": int(meta.get("bill_count", 0)),
            "communityId": cid,
        })

    for u, v, d in G.edges(data=True):
        a, b = str(u), str(v)
        key = tuple(sorted([a, b]))
        if key in seen:
            continue
        seen.add(key)
        edges_data.append({
            "from": a, "to": b,
            "weight": round(float(d.get("weight", 0)), 4),
            "rawCount": int(d.get("raw_count", 0)),
        })

    html = HTML
    html = html.replace("__STYLES__", json.dumps({str(k): v for k, v in COMMUNITY_STYLES.items()}))
    html = html.replace("__NODES__", json.dumps(nodes_data))
    html = html.replace("__EDGES__", json.dumps(edges_data))

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved -> {args.out}")
    print("Open senate_graph_v5.html in Chrome or Firefox.")


if __name__ == "__main__":
    main()
