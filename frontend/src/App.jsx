import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { motion, AnimatePresence } from 'framer-motion';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { Database, Cpu, Activity, BarChart3, ShieldCheck, Power, ChevronLeft, ChevronRight, Upload, Map as MapIcon, Settings, Trash2, CheckCircle, Search, Lock, PieChart as PieIcon, LineChart as LineIcon, MessageSquare, X, Send, GitCompare, Wand2 } from 'lucide-react';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Cell, AreaChart, Area, LineChart, Line, CartesianGrid } from 'recharts';
import { Chart } from "react-google-charts";

function MapController({ center, zoom }) {
  const map = useMap();
  useEffect(() => { 
      if (center && center.length === 2 && center[0] != null && center[1] != null) { 
          map.flyTo(center, zoom, { animate: true, duration: 1.5 }); 
      }
  }, [center[0], center[1], zoom, map]);
  return null;
}

export default function MainApp() {
  const [auth, setAuth] = useState(false);
  const [mode, setMode] = useState('login');
  const [creds, setCreds] = useState({ u: '', p: '' });
  const [view, setView] = useState('dash');
  const [chart, setChart] = useState('bar');
  
  const [info, setInfo] = useState({ active: 'None', files: [], history: [], cols: [] });
  const [rows, setRows] = useState([]);
  const [mapData, setMapData] = useState([]);
  const [page, setPage] = useState(1);
  const [maxPage, setMaxPage] = useState(1);
  const [search, setSearch] = useState("");
  
  const [viz, setViz] = useState([]);
  const [vizCompare, setVizCompare] = useState([]);
  const [activeCol, setActiveCol] = useState("");
  const [compareCol, setCompareCol] = useState("");
  const [isComparing, setIsComparing] = useState(false);
  const [rowFilter, setRowFilter] = useState(""); 
  
  const [mapSearch, setMapSearch] = useState("");
  const [mapCenter, setMapCenter] = useState([20.59, 78.96]);
  const [mapZoom, setMapZoom] = useState(5);
  
  const [aiOpen, setAiOpen] = useState(false);
  const [aiChat, setAiChat] = useState([{role: 'ai', text: 'I am Sentinel AI. All systems are online. How can I assist with your data today?'}]);
  const [aiInput, setAiInput] = useState("");
  const chatEndRef = useRef(null);
  const API = (import.meta.env.VITE_API_BASE_URL || "https://sentinel-v2-0-platform.vercel.app/api").replace(/\/$/, "");

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [aiChat, aiOpen]);

  const sync = async () => {
    try {
        const res = await axios.get(`${API}/info`);
        const newInfo = res.data || { active: 'None', files: [], history: [], cols: [] };
        setInfo(newInfo);
        
        if (newInfo.active !== 'None') {
            let currentActive = activeCol;
            if (!currentActive && newInfo.cols?.length > 0) {
                const bestCols = newInfo.cols.filter(c => ['venue', 'winner', 'city', 'team1', 'player_of_match'].includes(c.toLowerCase()));
                currentActive = bestCols.length > 0 ? bestCols[0] : newInfo.cols[0];
                setActiveCol(currentActive);
            }
            
            if (view === 'data') {
                const d = await axios.get(`${API}/data?page=${page}&query=${search}`);
                setRows(d.data?.rows || []); 
                setMaxPage(d.data?.total || 1);
            }
            if (view === 'map') {
                const m = await axios.get(`${API}/map_data`);
                setMapData(m.data || []);
            }
        }
    } catch (err) { console.error("Sync error"); }
  };

  useEffect(() => { if (auth) sync(); }, [auth, view, page, search]);

  useEffect(() => {
      if (auth && activeCol && info.active !== 'None') {
          axios.post(`${API}/viz`, { col: activeCol }).then(res => setViz(res.data || []));
      }
  }, [auth, activeCol, info.active]);

  useEffect(() => {
      if (auth && isComparing && compareCol && info.active !== 'None') {
          axios.post(`${API}/viz`, { col: compareCol }).then(res => setVizCompare(res.data || []));
      }
  }, [auth, isComparing, compareCol, info.active]);

  const handleAuth = async () => {
    if (!creds.u || !creds.p) { alert("Please enter ID and KEY."); return; }
    try {
        const route = mode === 'login' ? 'login' : 'register';
        await axios.post(`${API}/${route}`, { username: creds.u, password: creds.p });
        if (mode === 'register') { 
            alert("Registration successful! Logging you in..."); 
            setMode('login'); setAuth(true); 
        } else { setAuth(true); }
    } catch (error) { alert("Auth Failed."); }
  };

  const uploadFile = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const fd = new FormData(); 
    fd.append('file', file);
    try {
        await axios.post(`${API}/upload`, fd);
        setActiveCol(""); setCompareCol(""); setIsComparing(false); setRowFilter("");
        setView('dash');
        await sync(); 
    } catch (err) { 
        alert("Upload Failed."); 
    } finally {
        e.target.value = null; 
    }
  };

  const handleCleanData = async () => {
      try {
          const res = await axios.post(`${API}/clean`);
          alert(`Cleaned! Removed ${res.data.removed} corrupted rows.`);
          await sync();
      } catch (err) { alert("Failed to clean."); }
  };

  const handleReuse = async (name) => {
    await axios.post(`${API}/reuse`, { filename: name });
    setActiveCol(""); setCompareCol(""); setIsComparing(false); setRowFilter("");
    setView('dash');
    await sync(); 
  };

  const handlePurge = async (name) => {
    await axios.delete(`${API}/purge/${name}`);
    await sync();
    if(info.active === name) setView('dash');
  };

  const executeMapSearch = async (searchQuery) => {
    const query = searchQuery.toLowerCase().trim();
    if (!query) return;

    setMapSearch(query); 

    const found = mapData.find(m => m.name.toLowerCase().includes(query));
    if (found) {
        setMapCenter([found.lat, found.lng]);
        setMapZoom(14); 
        return;
    } 
    
    try {
        const response = await axios.get(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}`);
        if (response.data && response.data.length > 0) {
            const { lat, lon } = response.data[0];
            setMapCenter([parseFloat(lat), parseFloat(lon)]);
            setMapZoom(12); 
        } else {
            console.log(`Geocoding failed for: ${query}`);
        }
    } catch (error) {
        console.error("Network error while searching for the location.");
    }
  };

  const handleExport = () => {
      if (!viz || viz.length === 0) {
          setAiChat(prev => [...prev, {role: 'ai', text: "There is no data currently visualized to export."}]);
          return;
      }
      let csvContent = "data:text/csv;charset=utf-8,Category,Value\n";
      viz.forEach(row => {
          csvContent += `"${row.name}","${row.value}"\n`;
      });
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `Sentinel_Export_${activeCol || 'Data'}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
  };

  const handleSendAi = async () => {
    if (!aiInput.trim()) return;
    const userMessage = aiInput;
    const userMessageLower = userMessage.toLowerCase().trim();
    
    const newChat = [...aiChat, {role: 'user', text: userMessage}];
    setAiChat(newChat); 
    setAiInput("");
    
    try {
        let success = false;
        let aiResponseText = "";

        try {
            const response = await axios.post(`${API}/ai`, { q: userMessage });
            aiResponseText = response.data.ans || "";
            success = true; 
        } catch (err) {
            console.warn("Backend API Failed.", err);
        }

        let aiDecision = null;

        if (success) {
            const jsonMatch = aiResponseText.match(/\{[\s\S]*\}/);
            if (jsonMatch) {
                try {
                    aiDecision = JSON.parse(jsonMatch[0]);
                } catch (e) {
                    success = false; 
                }
            } else {
                success = false; 
            }
        }

        if (!success || !aiDecision) {
            aiDecision = { text: aiResponseText || "I've processed your request and updated the interface.", view: null, location: null, column: null, chart: null, export: false };

            const isGreeting = ['hi', 'hello', 'hey', 'sup', 'yo', 'greetings', 'hii', 'hiii'].some(g => userMessageLower === g || userMessageLower.startsWith(g + ' '));
            
            if (isGreeting) {
                aiDecision.text = aiResponseText || "Hello! I am online and ready. What data would you like to explore today?";
            }
            else if (userMessageLower.match(/(map|location|where|stadium|city|town)/)) {
                aiDecision.view = 'map';
                const cities = ['mumbai', 'delhi', 'bangalore', 'chennai', 'kolkata', 'hyderabad', 'ahmedabad', 'pune', 'jaipur', 'wankhede', 'kotla', 'chinnaswamy'];
                const foundCity = cities.find(c => userMessageLower.includes(c));
                aiDecision.location = foundCity || 'mumbai';
                aiDecision.text = `Zooming into ${aiDecision.location} on the interactive map for you now.`;
            }
            else if (userMessageLower.match(/(export|download|save|csv)/)) {
                aiDecision.export = true;
                aiDecision.text = "Your data is ready. I am generating the CSV download now.";
            }
            else if (userMessageLower.match(/(table|raw data|spreadsheet|rows|grid)/)) {
                aiDecision.view = 'data';
                aiDecision.text = "I've switched to the raw data table view so you can inspect the individual rows.";
            }
            else {
                aiDecision.view = 'dash';
                
                if (userMessageLower.includes('pie')) aiDecision.chart = 'pie';
                else if (userMessageLower.includes('line')) aiDecision.chart = 'line';
                else if (userMessageLower.includes('area')) aiDecision.chart = 'area';
                else aiDecision.chart = 'bar';

                let prettyName = "the data";
                if (userMessageLower.match(/(win|won|victor|champion)/)) { aiDecision.column = 'winner'; prettyName = "match winners"; }
                else if (userMessageLower.match(/(player|man of the match|mvp|best)/)) { aiDecision.column = 'player_of_match'; prettyName = "top players"; }
                else if (userMessageLower.match(/(venue|stadium|ground)/)) { aiDecision.column = 'venue'; prettyName = "stadium venues"; }
                else if (userMessageLower.match(/(toss|coin|decide)/)) { aiDecision.column = 'toss_decision'; prettyName = "toss decisions"; }
                else if (userMessageLower.match(/(city|town)/)) { aiDecision.column = 'city'; prettyName = "cities"; }
                else if (userMessageLower.match(/(team|squad)/)) { aiDecision.column = 'team1'; prettyName = "teams"; }
                else if (info.cols && info.cols.length > 0) aiDecision.column = info.cols[0];

                aiDecision.text = aiResponseText || `I have analyzed the dataset and generated a ${aiDecision.chart} chart displaying the ${prettyName} statistics for you.`;
            }
        }

        setAiChat(prev => [...prev, {role: 'ai', text: aiDecision.text}]);
        
        let currentView = view;
        const isGraphRequest = userMessageLower.match(/(graph|chart|pie|bar|plot|visualize|show me|who|what)/);
        
        if (isGraphRequest && (!aiDecision.view || aiDecision.view === 'data')) {
            aiDecision.view = 'dash';
        }

        if (aiDecision.view && ['dash', 'data', 'map', 'history'].includes(aiDecision.view)) {
            setView(aiDecision.view);
            currentView = aiDecision.view;
        }

        if (currentView === 'map' && aiDecision.location) {
            executeMapSearch(aiDecision.location);
        }

        if (currentView === 'dash' || !aiDecision.view) {
            let viewChanged = false;
            
            if (aiDecision.column) {
                const matchedCol = info.cols?.find(c => c.toLowerCase() === aiDecision.column?.toLowerCase());
                if (matchedCol && matchedCol !== activeCol) {
                    setActiveCol(matchedCol);
                    viewChanged = true;
                }
            }
            
            if (aiDecision.chart && ['bar', 'pie', 'line', 'area'].includes(aiDecision.chart) && aiDecision.chart !== chart) {
                setChart(aiDecision.chart);
                viewChanged = true;
            }
            if (viewChanged && currentView !== 'dash') {
                setView('dash'); 
            }
        }

        if (aiDecision.export) {
            setTimeout(handleExport, 800);
        }

    } catch (error) {
        setAiChat(prev => [...prev, {role: 'ai', text: `Sorry, I encountered a brief system error: ${error.message}` }]);
    }
  };

  const filteredViz = viz.filter(item => item && item.name && item.name.toString().toLowerCase().includes(rowFilter.toLowerCase())).slice(0, 15);
  const filteredCompare = vizCompare.filter(item => item && item.name && item.name.toString().toLowerCase().includes(rowFilter.toLowerCase())).slice(0, 15);

  const truncateLabel = (label) => {
      if (!label) return "";
      const str = String(label);
      return str.length > 12 ? str.substring(0, 10) + "..." : str;
  };

  const renderChart = (data) => {
      if (!data || data.length === 0) {
          return <div className="h-full w-full flex flex-col items-center justify-center text-slate-500 font-bold uppercase tracking-widest text-xs gap-4"><Activity className="animate-pulse text-cyan-500" size={32} /> Awaiting Data...</div>;
      }

      const googleChartData = [
          ["Category", "Value"],
          ...data.map(item => [truncateLabel(item.name), item.value])
      ];

      const googleOptions = {
          is3D: true,
          backgroundColor: 'transparent',
          legend: { textStyle: { color: '#94a3b8', fontSize: 11, fontName: 'monospace' }, alignment: 'center' },
          pieSliceTextStyle: { color: '#ffffff', fontSize: 11, fontName: 'monospace' },
          chartArea: { width: '90%', height: '80%' },
          colors: ['#06b6d4', '#a855f7', '#10b981', '#f43f5e', '#f59e0b', '#3b82f6', '#ec4899', '#84cc16']
      };

      return (
      <div className="flex-1 w-full h-full relative min-h-0 min-w-0">
          <div className="absolute inset-0 flex items-center justify-center">
              
              {chart === 'pie' ? (
                  <Chart
                      chartType="PieChart"
                      data={googleChartData}
                      options={googleOptions}
                      width={"100%"}
                      height={"100%"}
                  />
              ) : (
                  <ResponsiveContainer width="100%" height="100%">
                      {chart === 'bar' ? (
                          <BarChart data={data} margin={{ bottom: 35, top: 10, right: 10, left: -20 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                              <XAxis dataKey="name" fontSize={10} stroke="#94a3b8" tickFormatter={truncateLabel} interval={0} angle={-30} textAnchor="end" />
                              <YAxis fontSize={10} stroke="#94a3b8" />
                              <Tooltip cursor={{fill: '#0f172a', opacity: 0.5}} contentStyle={{background:'#020617', border:'1px solid #22d3ee', borderRadius:'10px'}} />
                              <Bar dataKey="value" filter="url(#shadow3D)" radius={[8,8,0,0]}>
                                  {data.map((entry, index) => {
                                      const gradients = ["url(#colorCyan)", "url(#colorPurple)", "url(#colorEmerald)", "url(#colorRose)", "url(#colorAmber)"];
                                      return <Cell key={`cell-${index}`} fill={gradients[index % gradients.length]} />
                                  })}
                              </Bar>
                          </BarChart>
                      ) : chart === 'area' ? (
                          <AreaChart data={data} margin={{ bottom: 35, top: 10, right: 10, left: -20 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                              <XAxis dataKey="name" fontSize={10} stroke="#94a3b8" tickFormatter={truncateLabel} interval={0} angle={-30} textAnchor="end" />
                              <YAxis fontSize={10} stroke="#94a3b8" />
                              <Tooltip contentStyle={{background:'#020617', border:'1px solid #10b981', borderRadius:'10px'}}/>
                              <Area type="monotone" dataKey="value" stroke="#10b981" fill="url(#colorEmerald)" strokeWidth={3} filter="url(#glow3D)"/>
                          </AreaChart>
                      ) : (
                          <LineChart data={data} margin={{ bottom: 35, top: 10, right: 10, left: -20 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                              <XAxis dataKey="name" fontSize={10} stroke="#94a3b8" tickFormatter={truncateLabel} interval={0} angle={-30} textAnchor="end" />
                              <YAxis fontSize={10} stroke="#94a3b8" />
                              <Tooltip contentStyle={{background:'#020617', border:'1px solid #f43f5e', borderRadius:'10px'}}/>
                              <Line type="monotone" dataKey="value" stroke="#f43f5e" strokeWidth={4} filter="url(#shadow3D)" dot={{r:5, fill:'#f43f5e', strokeWidth: 2, stroke: '#fff'}} activeDot={{r:7}}/>
                          </LineChart>
                      )}
                  </ResponsiveContainer>
              )}
          </div>
      </div>
      );
  };

  const safeFiles = Array.isArray(info?.files) ? info.files : [];
  const safeHistory = Array.isArray(info?.history) ? info.history : [];

  if (!auth) return (
    <div className="h-screen w-screen bg-[#020617] flex items-center justify-center font-mono text-white p-6 overflow-hidden">
      <div className="bg-white/[0.02] border border-cyan-500/20 rounded-[40px] p-12 w-full max-w-sm backdrop-blur-3xl shadow-2xl">
        <ShieldCheck className="text-cyan-400 mb-6 mx-auto" size={50} />
        <h1 className="text-xl font-black text-center mb-8 uppercase italic tracking-widest">Sentinel Access</h1>
        <div className="space-y-4">
          <input type="text" placeholder="Username" className="w-full bg-black/40 border border-white/10 rounded-2xl p-4 text-sm outline-none" value={creds.u} onChange={e=>setCreds({...creds, u:e.target.value})} />
          <input type="password" placeholder="Password" className="w-full bg-black/40 border border-white/10 rounded-2xl p-4 text-sm outline-none" value={creds.p} onChange={e=>setCreds({...creds, p:e.target.value})} />
          <button onClick={handleAuth} className="w-full bg-cyan-500 text-black font-black py-4 rounded-2xl text-xs uppercase hover:bg-cyan-400 transition-all">{mode === 'login' ? 'Login' : 'Register'}</button>
          <p className="text-[10px] text-center text-slate-500 cursor-pointer mt-4 uppercase font-bold" onClick={()=>setMode(mode==='login'?'register':'login')}>
            {mode === 'login' ? 'Create an account' : 'Already have an account? Login'}
          </p>
        </div>
      </div>
    </div>
  );

  return (
    <div className="h-screen w-screen bg-[#020617] text-slate-100 font-mono flex overflow-hidden">
      
      <svg style={{ height: 0, width: 0, position: 'absolute' }}>
        <defs>
            <linearGradient id="colorCyan" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#06b6d4" stopOpacity={0.9}/><stop offset="95%" stopColor="#083344" stopOpacity={0.9}/></linearGradient>
            <linearGradient id="colorPurple" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#a855f7" stopOpacity={0.9}/><stop offset="95%" stopColor="#3b0764" stopOpacity={0.9}/></linearGradient>
            <linearGradient id="colorEmerald" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#10b981" stopOpacity={0.9}/><stop offset="95%" stopColor="#064e3b" stopOpacity={0.9}/></linearGradient>
            <linearGradient id="colorRose" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#f43f5e" stopOpacity={0.9}/><stop offset="95%" stopColor="#4c0519" stopOpacity={0.9}/></linearGradient>
            <linearGradient id="colorAmber" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#f59e0b" stopOpacity={0.9}/><stop offset="95%" stopColor="#78350f" stopOpacity={0.9}/></linearGradient>
            <filter id="shadow3D" x="-20%" y="-20%" width="140%" height="140%">
                <feDropShadow dx="4" dy="6" stdDeviation="4" floodColor="#000000" floodOpacity="0.8"/>
                <feDropShadow dx="-1" dy="-1" stdDeviation="2" floodColor="#ffffff" floodOpacity="0.2"/>
            </filter>
            <filter id="glow3D" x="-20%" y="-20%" width="140%" height="140%">
                <feDropShadow dx="0" dy="0" stdDeviation="6" floodColor="#34d399" floodOpacity="0.8"/>
            </filter>
        </defs>
      </svg>

      <aside className="w-24 h-full border-r border-white/5 bg-black/40 backdrop-blur-xl flex flex-col items-center py-8 gap-10 z-50 shrink-0">
        <Cpu className="text-cyan-400 shrink-0" size={32} />
        <Database onClick={()=>setView('dash')} className={view==='dash'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
        <BarChart3 onClick={()=>setView('data')} className={view==='data'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
        <MapIcon onClick={()=>setView('map')} className={view==='map'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
        <Settings onClick={()=>setView('history')} className={view==='history'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
        
        <div className="mt-auto flex flex-col gap-8 items-center shrink-0">
            <label className="cursor-pointer group flex flex-col items-center">
                <input type="file" className="hidden" onChange={uploadFile} accept=".csv,.xlsx,.xls" />
                <Upload className="text-cyan-400 group-hover:scale-125 transition-all drop-shadow-[0_0_10px_rgba(34,211,238,0.8)]" size={24} />
            </label>
            <Power onClick={()=>setAuth(false)} className="text-red-900 hover:text-red-500 transition-colors" cursor-pointer />
        </div>
      </aside>

      <main className="flex-1 h-full p-4 md:p-6 flex flex-col overflow-hidden relative">
        <header className="shrink-0 flex justify-between items-end mb-4 italic uppercase">
          <div>
            <h1 className="text-3xl font-black tracking-tighter text-cyan-500">SENTINEL V2.0</h1>
            <p className="text-[9px] text-white/40 tracking-[0.5em]">Dataset: {info?.active || 'None'}</p>
          </div>
          <div className="text-[10px] font-bold text-green-400 animate-pulse uppercase flex items-center gap-2">
            ● System Active
          </div>
        </header>

        {info?.active === 'None' ? (
            <div className="flex-1 min-h-0 flex flex-col items-center justify-center opacity-60 bg-white/[0.01] border border-white/5 rounded-[40px] transition-all">
                <Lock size={80} className="mb-6 text-slate-600" />
                <h2 className="text-sm font-black uppercase tracking-[1em]">System Locked</h2>
                <label className="mt-8 px-10 py-3 border border-cyan-500 text-cyan-400 rounded-2xl font-black uppercase hover:bg-cyan-500 hover:text-black transition-all shadow-[0_0_20px_rgba(6,182,212,0.2)] cursor-pointer">
                    Upload Dataset to Unlock
                    <input type="file" className="hidden" onChange={uploadFile} accept=".csv,.xlsx,.xls" />
                </label>
            </div>
        ) : (
            <div className="flex-1 min-h-0 flex flex-col gap-4 overflow-hidden">
                
                {view === 'dash' && (
                    <div className="flex-1 min-h-0 flex flex-col gap-4 overflow-hidden">
                        <div className="shrink-0 flex justify-between items-center bg-black/40 border border-white/5 p-4 rounded-3xl shadow-lg">
                            <div className="flex items-center gap-4">
                                <select value={activeCol} onChange={(e) => setActiveCol(e.target.value)} className="bg-black/60 border border-cyan-500/30 text-cyan-400 text-[10px] uppercase font-black px-4 py-2 rounded-xl outline-none cursor-pointer max-w-xs">
                                    <option value="">-- Primary Metric --</option>
                                    {info.cols?.map(c => <option key={c} value={c}>{c}</option>)}
                                </select>
                                
                                {isComparing && (
                                    <select value={compareCol} onChange={(e) => setCompareCol(e.target.value)} className="bg-black/60 border border-purple-500/30 text-purple-400 text-[10px] uppercase font-black px-4 py-2 rounded-xl outline-none cursor-pointer max-w-xs">
                                        <option value="">-- Compare Metric --</option>
                                        {info.cols?.map(c => <option key={c} value={c}>{c}</option>)}
                                    </select>
                                )}

                                <button onClick={() => setIsComparing(!isComparing)} className={`px-4 py-2 flex items-center gap-2 rounded-xl text-[10px] font-black uppercase transition-all ${isComparing ? 'bg-cyan-500 text-black' : 'bg-black/60 text-slate-500 border border-white/10 hover:text-cyan-400'}`}>
                                    <GitCompare size={14}/> {isComparing ? 'Exit Compare' : 'Compare'}
                                </button>
                                
                                <button onClick={handleCleanData} className="px-4 py-2 flex items-center gap-2 rounded-xl text-[10px] font-black uppercase transition-all bg-black/60 text-yellow-500 border border-yellow-500/30 hover:bg-yellow-500 hover:text-black">
                                    <Wand2 size={14}/> Clean
                                </button>
                            </div>

                            <div className="flex gap-4 bg-black/60 p-2 rounded-xl border border-white/10">
                                <BarChart3 onClick={()=>setChart('bar')} className={chart==='bar'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
                                <PieIcon onClick={()=>setChart('pie')} className={chart==='pie'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
                                <LineIcon onClick={()=>setChart('line')} className={chart==='line'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
                                <Activity onClick={()=>setChart('area')} className={chart==='area'?'text-cyan-400 scale-110':'text-slate-600 hover:text-white'} cursor-pointer transition-all />
                            </div>
                        </div>

                        <div className="shrink-0 flex items-center gap-4 bg-black/40 border border-white/5 p-4 rounded-3xl">
                            <Search size={16} className="text-cyan-500" />
                            <input type="text" placeholder="Search specific row data to visualize (e.g., 'Mumbai')..." className="bg-transparent border-none outline-none text-[11px] text-white uppercase font-bold w-full" value={rowFilter} onChange={e => setRowFilter(e.target.value)} />
                        </div>

                        <div className="flex-1 min-h-0 bg-black/40 border border-white/5 rounded-[40px] p-6 shadow-2xl flex flex-col">
                            {isComparing ? (
                                <div className="flex-1 min-h-0 flex flex-row gap-6 w-full">
                                    <div className="flex-1 min-h-0 min-w-0 flex flex-col border-r border-white/10 pr-6">
                                        <span className="shrink-0 text-[11px] text-cyan-500 font-black uppercase mb-2 text-center block tracking-widest">{activeCol || 'Primary'}</span>
                                        {renderChart(filteredViz)}
                                    </div>
                                    <div className="flex-1 min-h-0 min-w-0 flex flex-col pl-2">
                                        <span className="shrink-0 text-[11px] text-purple-400 font-black uppercase mb-2 text-center block tracking-widest">{compareCol || 'Secondary'}</span>
                                        {renderChart(filteredCompare)}
                                    </div>
                                </div>
                            ) : (
                                <div className="flex-1 min-h-0 min-w-0 flex flex-col items-center">
                                    <span className="shrink-0 text-[12px] text-cyan-500 font-black uppercase mb-2 text-center tracking-widest block drop-shadow-[0_0_10px_rgba(6,182,212,0.8)]">{activeCol || 'Primary Metric'}</span>
                                    {renderChart(filteredViz)}
                                </div>
                            )}
                        </div>
                    </div>
                )}

                {view === 'data' && (
                    <div className="flex-1 min-h-0 flex flex-col bg-black/40 border border-white/5 rounded-[40px] p-6 shadow-2xl">
                        <div className="shrink-0 flex bg-white/[0.02] border border-white/10 rounded-2xl items-center px-4 mb-4">
                            <Search size={18} className="text-slate-600 mr-3" />
                            <input type="text" placeholder="FILTER DATA..." className="bg-transparent border-none outline-none py-4 text-[11px] text-white w-full uppercase" onChange={e=>{setSearch(e.target.value); setPage(1)}} />
                        </div>
                        <div className="flex-1 min-h-0 overflow-auto bg-white/[0.01] border border-white/5 rounded-2xl custom-scroll">
                            <table className="w-full text-[10px] text-left border-collapse min-w-[1200px]">
                                <thead className="sticky top-0 bg-[#020617] text-cyan-400 uppercase border-b border-white/10 z-20">
                                    <tr>{info.cols?.map(k=><th key={k} className="p-4 bg-[#020617] whitespace-nowrap">{k}</th>)}</tr>
                                </thead>
                                <tbody className="divide-y divide-white/5">
                                    {rows?.map((r,i)=><tr key={i} className="hover:bg-cyan-500/5 group transition-all">
                                        {Object.values(r).map((v,j)=><td key={j} className="p-4 text-slate-500 group-hover:text-white transition-all whitespace-nowrap max-w-xs truncate" title={v}>{v}</td>)}
                                    </tr>)}
                                </tbody>
                            </table>
                        </div>
                        <div className="shrink-0 flex justify-between items-center bg-black/60 p-4 rounded-2xl border border-white/5 shadow-lg mt-4">
                            <button disabled={page===1} onClick={()=>setPage(p=>p-1)} className="text-[11px] font-black uppercase disabled:opacity-20 hover:text-cyan-400 flex items-center gap-2 transition-all"><ChevronLeft size={16}/> Prev</button>
                            <div className="text-[11px] font-bold text-slate-600 uppercase">Page {page} of {maxPage}</div>
                            <button disabled={page===maxPage} onClick={()=>setPage(p=>p+1)} className="text-[11px] font-black uppercase disabled:opacity-20 hover:text-cyan-400 flex items-center gap-2 transition-all">Next <ChevronRight size={16}/></button>
                        </div>
                    </div>
                )}

                {view === 'map' && (
                    <div className="flex-1 min-h-0 bg-black/60 border border-white/5 rounded-[40px] overflow-hidden relative shadow-[0_0_50px_rgba(0,0,0,0.8)]">
                        <div className="absolute top-6 left-6 z-[1000] flex bg-black/80 p-2 rounded-xl border border-white/10 shadow-[0_0_20px_rgba(0,0,0,0.8)]">
                            <Search size={16} className="text-cyan-400 m-2" />
                            <input 
                                type="text" 
                                placeholder="Search ANY City or State..." 
                                className="bg-transparent border-none outline-none text-[10px] text-white uppercase font-bold w-48" 
                                value={mapSearch} 
                                onChange={e=>setMapSearch(e.target.value)} 
                                onKeyDown={e=>{ if(e.key==='Enter') executeMapSearch(mapSearch) }} 
                            />
                        </div>
                        <MapContainer center={mapCenter} zoom={mapZoom} minZoom={3} maxZoom={18} style={{ height: '100%', width: '100%', background: '#000' }}>
                            <TileLayer url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}" attribution="&copy; Google Maps" />
                            <MapController center={mapCenter} zoom={mapZoom} />
                            {mapData?.map((m, i) => {
                               if (m?.lat == null || m?.lng == null) return null;
                                return (
                                    <Marker 
                                        key={i} 
                                        position={[m.lat, m.lng]} 
                                        eventHandlers={{
                                            click: () => {
                                                setMapCenter([m.lat, m.lng]);
                                                setMapZoom(16); 
                                            }
                                        }}
                                    >
                                        <Popup>
                                            <div className="font-bold text-xs uppercase text-slate-800 text-center">
                                                {m.name} <br/>
                                                <span className="text-[9px] text-cyan-600">Double-Click to Dive</span>
                                            </div>
                                        </Popup>
                                    </Marker>
                                )
                            })}
                        </MapContainer>
                    </div>
                )}

                {view === 'history' && (
                    <div className="flex-1 min-h-0 grid grid-cols-2 gap-6">
                        <div className="bg-black/40 border border-white/5 rounded-[40px] p-8 overflow-y-auto shadow-2xl custom-scroll">
                            <h3 className="text-xs font-black text-cyan-400 uppercase tracking-widest mb-6 border-b border-white/5 pb-3">Stored Datasets</h3>
                            {safeFiles.map(f => (
                                <div key={f} className={`p-4 rounded-2xl border mb-3 flex justify-between items-center transition-all ${info.active===f?'border-cyan-400 bg-cyan-400/5 shadow-[0_0_20px_rgba(6,182,212,0.1)]':'border-white/5 bg-white/5'}`}>
                                    <span className="text-[10px] uppercase font-black text-slate-300">{f}</span>
                                    <div className="flex gap-3">
                                        <CheckCircle onClick={()=>handleReuse(f)} size={18} className="text-slate-600 hover:text-cyan-400 cursor-pointer" />
                                        <Trash2 onClick={()=>handlePurge(f)} size={18} className="text-slate-700 hover:text-red-500 cursor-pointer" />
                                    </div>
                                </div>
                            ))}
                        </div>
                        <div className="bg-black/40 border border-white/5 rounded-[40px] p-8 overflow-y-auto flex flex-col shadow-2xl custom-scroll">
                            <div className="flex justify-between items-center mb-6 border-b border-white/5 pb-3">
                                <h3 className="text-xs font-black text-cyan-400 uppercase tracking-widest">System Logs</h3>
                                <button onClick={async()=>{await axios.delete(`${API}/clear_history`); sync();}} className="text-[9px] bg-red-900/20 text-red-500 px-4 py-2 rounded-xl uppercase font-black hover:bg-red-500 hover:text-white transition-all">Clear Logs</button>
                            </div>
                            {safeHistory.map((h, i) => (
                                <div key={i} className="border-l-4 border-cyan-500/20 pl-4 py-3 mb-2 bg-white/[0.02] rounded-r-2xl">
                                    <p className="text-[10px] text-white uppercase font-black">{h?.[0]}</p>
                                    <p className="text-[8px] text-slate-500 uppercase mt-1">{h?.[1]}</p>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>
        )}

        {/* --- FLOATING AI BUBBLE --- */}
        {auth && info?.active !== 'None' && (
            <div className="absolute bottom-6 right-6 z-[9999] flex flex-col items-end">
                <AnimatePresence>
                    {aiOpen && (
                        <motion.div initial={{opacity:0, y:20, scale:0.9}} animate={{opacity:1, y:0, scale:1}} exit={{opacity:0, y:20, scale:0.9}} className="mb-4 w-80 h-96 bg-black/90 border border-cyan-500/30 rounded-3xl shadow-[0_0_30px_rgba(6,182,212,0.3)] backdrop-blur-xl flex flex-col overflow-hidden">
                            <div className="bg-cyan-500/10 p-4 border-b border-cyan-500/20 flex justify-between items-center">
                                <span className="text-[10px] font-black text-cyan-400 uppercase tracking-widest">AI Interface</span>
                                <X size={16} className="text-slate-500 cursor-pointer hover:text-red-500" onClick={()=>setAiOpen(false)} />
                            </div>
                            <div className="flex-1 overflow-y-auto p-4 flex flex-col gap-4 custom-scroll">
                                {aiChat.map((msg, i) => (
                                    <div key={i} className={`p-3 rounded-2xl text-[10px] max-w-[80%] ${msg.role === 'ai' ? 'bg-cyan-500/20 text-cyan-100 self-start rounded-tl-none border border-cyan-500/30' : 'bg-white/10 text-white self-end rounded-tr-none'}`}>
                                        {msg.text}
                                    </div>
                                ))}
                                <div ref={chatEndRef} />
                            </div>
                            <div className="p-3 bg-black/60 border-t border-white/10 flex items-center gap-2">
                                <input type="text" placeholder="Ask AI..." className="flex-1 bg-transparent border-none outline-none text-[10px] text-white" value={aiInput} onChange={e=>setAiInput(e.target.value)} onKeyDown={e=>{if(e.key==='Enter')handleSendAi()}} />
                                <button onClick={handleSendAi} className="text-cyan-500 hover:text-cyan-300"><Send size={16}/></button>
                            </div>
                        </motion.div>
                    )}
                </AnimatePresence>
                <button onClick={() => setAiOpen(!aiOpen)} className="w-14 h-14 bg-cyan-500 rounded-full flex items-center justify-center shadow-[0_0_20px_#22d3ee] hover:scale-110 transition-transform">
                    <MessageSquare size={24} className="text-black" />
                </button>
            </div>
        )}
      </main>
    </div>
  );
}

export function AppWrapper() {
  return <MainApp />
}
