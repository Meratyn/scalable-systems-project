window.PERF_DATA = {
"bench":{
"4day":[{"p":1,"median":81.32,"min":79.01,"max":82.82,"runs":3},{"p":2,"median":62.24,"min":54.92,"max":62.85,"runs":3},{"p":4,"median":47.66,"min":43.89,"max":50.01,"runs":3},{"p":8,"median":49.55,"min":48.56,"max":58.10,"runs":3}],
"7day":[{"p":1,"median":108.80,"min":97.59,"max":110.52,"runs":3},{"p":2,"median":77.44,"min":70.20,"max":87.51,"runs":3},{"p":4,"median":65.99,"min":62.35,"max":85.66,"runs":3},{"p":8,"median":57.31,"min":55.25,"max":73.15,"runs":3}]
},
"ablation":[
{"config":"baseline","median":50.82,"rel":1.000,"cpu":785,"memGB":2.75,"runs":3,"check":"ref"},
{"config":"reference-parser","median":108.48,"rel":2.135,"cpu":679,"memGB":2.79,"runs":3,"check":"IDENTICAL"},
{"config":"g1-gc","median":59.12,"rel":1.163,"cpu":822,"memGB":4.69,"runs":3,"check":"IDENTICAL"},
{"config":"legacy-source","median":81.07,"rel":1.595,"cpu":588,"memGB":2.68,"runs":3,"check":"IDENTICAL"},
{"config":"8-chunks-per-day","median":49.78,"rel":0.980,"cpu":986,"memGB":2.79,"runs":3,"check":"IDENTICAL"},
{"config":"object-reuse","median":49.87,"rel":0.981,"cpu":819,"memGB":2.74,"runs":3,"check":"IDENTICAL"}
],
"excluded":["baseline:2 (583.70 s)"]
};
