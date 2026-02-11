import { useState, useMemo } from "react";
import { BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Treemap } from "recharts";

const C = {
  navy:'#0F172A', dark:'#1E293B', slate:'#334155', med:'#64748B', light:'#94A3B8',
  bg:'#F8FAFC', white:'#FFFFFF', amber:'#D97706', amberL:'#FEF3C7',
  teal:'#0D9488', tealL:'#CCFBF1', green:'#059669', greenL:'#D1FAE5',
  red:'#DC2626', redL:'#FEE2E2', blue:'#2563EB', blueL:'#DBEAFE',
  purple:'#7C3AED', purpleL:'#EDE9FE', cyan:'#0891B2',
};
const LAYER_COLORS = { bronze:'#92400E', silver:'#0D9488', mdm:'#7C3AED', gold:'#2563EB', clickstream:'#0891B2', fraud:'#DC2626', partners:'#D97706', realtime:'#059669', source:'#64748B' };
const PII_COLORS = { PII:'#DC2626', SPII:'#D97706', CONFIDENTIAL:'#7C3AED', PUBLIC:'#059669' };
const CHART_COLORS = [C.blue, C.teal, C.amber, C.purple, C.red, C.green, C.cyan, '#EC4899'];

// ─── Catalogue Data (from engine output) ───
const TABLES = [
  { name:'dim_customer', layer:'gold', rows:2000, cols:29, size:'546 KB', quality:97.8, pii:{PII:8,SPII:4,CONFIDENTIAL:3,PUBLIC:14}, tags:['gold','dimension','customer','entity','contains_pii'], owner:'Data Governance', refresh:'Every 4 hours', sla:'< 4 hours' },
  { name:'dim_account', layer:'gold', rows:3867, cols:16, size:'514 KB', quality:98.2, pii:{PII:0,SPII:0,CONFIDENTIAL:6,PUBLIC:10}, tags:['gold','dimension','financial'], owner:'Finance Operations', refresh:'Every 4 hours', sla:'< 4 hours' },
  { name:'dim_product', layer:'gold', rows:19, cols:12, size:'1.6 KB', quality:99.5, pii:{PUBLIC:12}, tags:['gold','dimension'], owner:'Product Team', refresh:'On change', sla:'< 1 hour' },
  { name:'dim_date', layer:'gold', rows:1095, cols:11, size:'63 KB', quality:100, pii:{PUBLIC:11}, tags:['gold','dimension'], owner:'Data Engineering', refresh:'Yearly', sla:'N/A' },
  { name:'fact_transactions', layer:'gold', rows:30000, cols:16, size:'4.5 MB', quality:98.5, pii:{PII:0,CONFIDENTIAL:4,PUBLIC:12}, tags:['gold','fact','financial','transactional'], owner:'Payment Operations', refresh:'Near real-time', sla:'< 15 min' },
  { name:'fact_loan_payments', layer:'gold', rows:20486, cols:10, size:'2.0 MB', quality:97.4, pii:{CONFIDENTIAL:3,PUBLIC:7}, tags:['gold','fact','financial'], owner:'Loan Operations', refresh:'Daily', sla:'< 6 hours' },
  { name:'fact_credit_risk', layer:'gold', rows:1844, cols:16, size:'191 KB', quality:98.1, pii:{SPII:5,CONFIDENTIAL:3,PUBLIC:8}, tags:['gold','fact','risk','regulatory'], owner:'Risk Analytics', refresh:'Daily snapshot', sla:'< 6 hours' },
  { name:'digital_events', layer:'clickstream', rows:40000, cols:19, size:'6.2 MB', quality:97.9, pii:{PII:1,CONFIDENTIAL:2,PUBLIC:16}, tags:['clickstream','digital','fact'], owner:'Digital Banking', refresh:'Streaming', sla:'< 5 min' },
  { name:'fraud_alerts', layer:'fraud', rows:644, cols:16, size:'82 KB', quality:98.8, pii:{CONFIDENTIAL:5,PUBLIC:11}, tags:['fraud','compliance','aml'], owner:'Fraud Operations', refresh:'Real-time', sla:'< 500ms' },
  { name:'partner_performance', layer:'partners', rows:120, cols:14, size:'12 KB', quality:99.2, pii:{PUBLIC:14}, tags:['partners','partnership'], owner:'Partnership Team', refresh:'Monthly', sla:'< 24 hours' },
  { name:'hourly_metrics', layer:'realtime', rows:336, cols:15, size:'28 KB', quality:99.0, pii:{PUBLIC:15}, tags:['realtime','operational'], owner:'SRE Team', refresh:'Hourly', sla:'< 5 min' },
  { name:'mdm_match_pairs', layer:'mdm', rows:32, cols:14, size:'3.8 KB', quality:99.5, pii:{CONFIDENTIAL:4,PUBLIC:10}, tags:['mdm','data_quality'], owner:'Data Governance', refresh:'After Silver', sla:'< 1 hour' },
  { name:'core_banking_customers', layer:'bronze', rows:800, cols:13, size:'98 KB', quality:96.2, pii:{PII:5,SPII:2,CONFIDENTIAL:2,PUBLIC:4}, tags:['bronze','contains_pii','customer'], owner:'Data Engineering', refresh:'Every 4 hours', sla:'< 30 min' },
  { name:'salesforce_accounts', layer:'bronze', rows:1200, cols:13, size:'142 KB', quality:95.8, pii:{PII:5,SPII:1,CONFIDENTIAL:1,PUBLIC:6}, tags:['bronze','contains_pii','customer'], owner:'Data Engineering', refresh:'Every 2 hours', sla:'< 15 min' },
  { name:'fiserv_parties', layer:'bronze', rows:1000, cols:11, size:'108 KB', quality:94.2, pii:{PII:4,SPII:2,CONFIDENTIAL:1,PUBLIC:4}, tags:['bronze','contains_pii','customer'], owner:'Data Engineering', refresh:'Daily 02:00 UTC', sla:'< 1 hour' },
];

const COLUMNS_DETAIL = {
  'dim_customer': [
    {name:'customer_id',type:'identifier',pii:'CONFIDENTIAL',nullRate:0,distinct:2000,unique:true,quality:100,glossary:'Unique golden record identifier'},
    {name:'first_name',type:'string',pii:'PII',nullRate:0,distinct:100,unique:false,quality:100,glossary:null},
    {name:'last_name',type:'string',pii:'PII',nullRate:0,distinct:100,unique:false,quality:100,glossary:null},
    {name:'email',type:'email',pii:'PII',nullRate:0,distinct:2000,unique:true,quality:100,glossary:null},
    {name:'phone',type:'phone',pii:'PII',nullRate:0,distinct:2000,unique:true,quality:100,glossary:null},
    {name:'date_of_birth',type:'date',pii:'PII',nullRate:0,distinct:1876,unique:false,quality:100,glossary:null},
    {name:'ssn_hash',type:'identifier',pii:'SPII',nullRate:0,distinct:2000,unique:true,quality:100,glossary:null},
    {name:'segment',type:'string',pii:'PUBLIC',nullRate:0,distinct:5,unique:false,quality:100,topValues:[{v:'mass_market',p:40},{v:'mass_affluent',p:30},{v:'affluent',p:15},{v:'high_net_worth',p:10},{v:'ultra_hnw',p:5}],glossary:'Wealth-based segmentation'},
    {name:'risk_tier',type:'string',pii:'PUBLIC',nullRate:0,distinct:5,unique:false,quality:100,topValues:[{v:'prime',p:35},{v:'super_prime',p:25},{v:'near_prime',p:20},{v:'subprime',p:15},{v:'deep_subprime',p:5}],glossary:'Credit risk classification based on FICO'},
    {name:'fico_score',type:'integer',pii:'SPII',nullRate:0,distinct:551,unique:false,quality:100,min:300,max:850,mean:712,glossary:'Fair Isaac Corporation credit score (300-850)'},
    {name:'annual_income',type:'integer',pii:'SPII',nullRate:0,distinct:1987,unique:false,quality:100,min:30000,max:4982000,mean:198500,glossary:null},
    {name:'acquisition_channel',type:'string',pii:'PUBLIC',nullRate:0,distinct:7,unique:false,quality:100,topValues:[{v:'web',p:22},{v:'branch',p:18},{v:'mobile_app',p:18},{v:'partner_referral',p:15},{v:'phone',p:12},{v:'mail',p:10},{v:'social_media',p:5}],glossary:'Marketing channel of acquisition'},
    {name:'status',type:'string',pii:'PUBLIC',nullRate:0,distinct:4,unique:false,quality:100,topValues:[{v:'active',p:80},{v:'inactive',p:10},{v:'closed',p:8},{v:'suspended',p:2}]},
    {name:'digital_enrolled',type:'boolean',pii:'PUBLIC',nullRate:0,distinct:2,unique:false,quality:100,topValues:[{v:'True',p:75},{v:'False',p:25}],glossary:'Digital banking enrollment status'},
  ],
};

const LINEAGE_DATA = {
  nodes: [
    {id:'oracle',label:'Oracle Core Banking',layer:'source',x:50,y:80},{id:'sfdc',label:'Salesforce CRM',layer:'source',x:50,y:200},{id:'fiserv',label:'Fiserv SFTP',layer:'source',x:50,y:320},
    {id:'core_banking',label:'core_banking_customers',layer:'bronze',x:250,y:80},{id:'salesforce',label:'salesforce_accounts',layer:'bronze',x:250,y:200},{id:'fiserv_p',label:'fiserv_parties',layer:'bronze',x:250,y:320},
    {id:'mdm',label:'mdm_match_pairs',layer:'mdm',x:450,y:200},
    {id:'dim_customer',label:'dim_customer',layer:'gold',x:650,y:120},{id:'dim_account',label:'dim_account',layer:'gold',x:650,y:240},{id:'dim_product',label:'dim_product',layer:'gold',x:650,y:360},
    {id:'fact_txn',label:'fact_transactions',layer:'gold',x:850,y:80},{id:'fact_loan',label:'fact_loan_payments',layer:'gold',x:850,y:180},{id:'fact_risk',label:'fact_credit_risk',layer:'gold',x:850,y:280},{id:'digital',label:'digital_events',layer:'clickstream',x:850,y:380},{id:'fraud',label:'fraud_alerts',layer:'fraud',x:850,y:460},
  ],
  edges: [
    {from:'oracle',to:'core_banking'},{from:'sfdc',to:'salesforce'},{from:'fiserv',to:'fiserv_p'},
    {from:'core_banking',to:'mdm'},{from:'salesforce',to:'mdm'},{from:'fiserv_p',to:'mdm'},
    {from:'mdm',to:'dim_customer'},{from:'dim_customer',to:'dim_account'},
    {from:'dim_account',to:'fact_txn'},{from:'dim_account',to:'fact_loan'},{from:'dim_account',to:'fact_risk'},
    {from:'dim_customer',to:'digital'},{from:'fact_txn',to:'fraud'},{from:'dim_product',to:'dim_account'},
  ],
};

const GLOSSARY_DATA = [
  {term:'Customer Identifier',column:'customer_id',domain:'MDM',definition:'Unique golden record identifier for a customer entity, MDM-assigned after deduplication across source systems.',tables:['dim_customer','dim_account','fact_transactions','fact_loan_payments','fact_credit_risk','digital_events','fraud_alerts'],steward:'Data Governance Team'},
  {term:'FICO Score',column:'fico_score',domain:'Credit Risk',definition:'Fair Isaac Corporation credit score (300-850) indicating creditworthiness. Core Banking is authoritative source.',tables:['dim_customer','fact_credit_risk','core_banking_customers'],steward:'Risk Analytics'},
  {term:'Customer Segment',column:'segment',domain:'Marketing',definition:'Wealth-based segmentation: mass_market, mass_affluent, affluent, high_net_worth, ultra_hnw.',tables:['dim_customer','fact_credit_risk'],steward:'Customer Analytics'},
  {term:'Credit Risk Tier',column:'risk_tier',domain:'Credit Risk',definition:'Classification based on FICO: super_prime (750+), prime (700-749), near_prime (650-699), subprime (580-649), deep_subprime (<580).',tables:['dim_customer','fact_credit_risk'],steward:'Risk Analytics'},
  {term:'Days Past Due',column:'days_past_due',domain:'Collections',definition:'Number of days a payment is overdue. 30/60/90/120+ trigger escalating collection actions.',tables:['fact_credit_risk'],steward:'Collections Team'},
  {term:'Probability of Default',column:'probability_of_default',domain:'Credit Risk',definition:'Statistical likelihood (0-1) of default within 12 months. Basel II/III regulatory metric.',tables:['fact_credit_risk'],steward:'Risk Analytics'},
  {term:'MDM Composite Score',column:'composite_score',domain:'MDM',definition:'Weighted similarity score (0-1): ≥0.92=auto_merge, 0.75-0.92=review, <0.75=no_match.',tables:['mdm_match_pairs'],steward:'Data Governance Team'},
  {term:'Merchant Category Code',column:'mcc_code',domain:'Payments',definition:'ISO 18245 four-digit code classifying merchant business type for card transactions.',tables:['fact_transactions'],steward:'Merchant Services'},
  {term:'Rewards Earned',column:'rewards_earned',domain:'Loyalty',definition:'Dollar value of rewards/cashback earned per transaction, based on product reward rates.',tables:['fact_transactions'],steward:'Loyalty Program'},
  {term:'Fraud Flag',column:'fraud_flag',domain:'Fraud',definition:'Boolean flag from real-time fraud detection engine when ML model or rules trigger an alert.',tables:['fact_transactions'],steward:'Fraud Operations'},
  {term:'Interchange Revenue',column:'interchange_revenue',domain:'Finance',definition:'Fee earned per card transaction paid by merchant acquirer. Typically 1.5-3.5% of transaction value.',tables:['partner_performance'],steward:'Finance Operations'},
  {term:'Digital Enrollment',column:'digital_enrolled',domain:'Digital',definition:'Whether customer has activated digital banking (web or mobile app access).',tables:['dim_customer'],steward:'Digital Banking'},
  {term:'Acquisition Channel',column:'acquisition_channel',domain:'Marketing',definition:'Original marketing channel: branch, web, mobile_app, phone, mail, partner_referral, social_media.',tables:['dim_customer'],steward:'Customer Analytics'},
  {term:'Alert Type',column:'alert_type',domain:'Fraud',definition:'Classification: velocity_spike, geographic_anomaly, large_purchase, account_takeover, structuring_pattern.',tables:['fraud_alerts'],steward:'Fraud Operations'},
  {term:'Account Balance',column:'balance',domain:'Finance',definition:'Current outstanding balance. Credit cards: amount owed. Loans: remaining principal. Deposits: available funds.',tables:['dim_account'],steward:'Finance Operations'},
  {term:'Partner Identifier',column:'partner_id',domain:'Partnerships',definition:'Unique ID for co-brand partners, merchant networks, and digital partners in rewards ecosystem.',tables:['partner_performance'],steward:'Partnership Team'},
];

// ─── Components ───
const KPI = ({label,value,sub,color=C.blue}) => (
  <div className="bg-white rounded-lg p-3 shadow-sm border border-slate-100">
    <div className="text-xs font-medium text-slate-500 uppercase tracking-wide">{label}</div>
    <div className="text-xl font-bold mt-1" style={{color}}>{value}</div>
    {sub && <div className="text-xs text-slate-400 mt-0.5">{sub}</div>}
  </div>
);

const Badge = ({children,color=C.blue,sm}) => (
  <span className={`inline-block px-2 ${sm?'py-0':'py-0.5'} rounded-full ${sm?'text-[10px]':'text-xs'} font-semibold text-white`} style={{backgroundColor:color}}>{children}</span>
);

const LayerBadge = ({layer}) => <Badge color={LAYER_COLORS[layer]||C.med}>{layer}</Badge>;
const PIIBadge = ({level}) => <Badge color={PII_COLORS[level]||C.green} sm>{level}</Badge>;

const MiniBar = ({value,max=100,color=C.blue}) => (
  <div className="w-full bg-slate-100 rounded-full h-1.5">
    <div className="h-1.5 rounded-full" style={{width:`${Math.min((value/max)*100,100)}%`,backgroundColor:color}}/>
  </div>
);

const QualityDot = ({score}) => {
  const color = score >= 98 ? C.green : score >= 95 ? C.amber : C.red;
  return <span className="inline-flex items-center gap-1"><span className="w-2 h-2 rounded-full inline-block" style={{backgroundColor:color}}/><span className="text-xs font-bold" style={{color}}>{score}%</span></span>;
};

// ─── Tab: Catalogue Browser ───
const CatalogueBrowser = ({onSelect}) => {
  const [search,setSearch] = useState('');
  const [layerFilter,setLayerFilter] = useState('all');
  const filtered = TABLES.filter(t => {
    if (layerFilter !== 'all' && t.layer !== layerFilter) return false;
    if (search && !t.name.toLowerCase().includes(search.toLowerCase()) && !t.tags.some(tag => tag.includes(search.toLowerCase()))) return false;
    return true;
  });
  const layers = ['all',...new Set(TABLES.map(t=>t.layer))];
  
  const treemapData = TABLES.map(t => ({name:t.name, size:t.rows, layer:t.layer}));
  const piiAgg = {PII:0,SPII:0,CONFIDENTIAL:0,PUBLIC:0};
  TABLES.forEach(t => Object.entries(t.pii).forEach(([k,v]) => piiAgg[k]=(piiAgg[k]||0)+v));
  
  return <div>
    <div className="mb-5"><h2 className="text-lg font-bold text-slate-800">Data Catalogue</h2><p className="text-xs text-slate-500 mt-0.5">15 tables · 225 columns · 103,443 records across the Horizon Bank Holdings MDM Lakehouse</p></div>
    
    <div className="grid grid-cols-5 gap-3 mb-5">
      <KPI label="Tables" value="15" sub="across 7 layers" color={C.blue}/>
      <KPI label="Total Records" value="103.4K" sub="all layers" color={C.teal}/>
      <KPI label="Columns" value="225" sub="profiled" color={C.amber}/>
      <KPI label="Avg Quality" value="98.6%" sub="across all tables" color={C.green}/>
      <KPI label="Glossary Terms" value="17" sub="business definitions" color={C.purple}/>
    </div>
    
    <div className="grid grid-cols-3 gap-4 mb-5">
      <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-100">
        <div className="text-xs font-semibold text-slate-600 mb-2">Records by Table</div>
        <ResponsiveContainer width="100%" height={160}>
          <BarChart data={TABLES.sort((a,b)=>b.rows-a.rows).slice(0,8)} layout="vertical">
            <XAxis type="number" tick={{fontSize:9}} stroke="#94A3B8"/>
            <YAxis dataKey="name" type="category" tick={{fontSize:8}} width={100} stroke="#94A3B8"/>
            <Tooltip contentStyle={{fontSize:10}}/>
            <Bar dataKey="rows" name="Rows">{TABLES.sort((a,b)=>b.rows-a.rows).slice(0,8).map((t,i)=><Cell key={i} fill={LAYER_COLORS[t.layer]}/>)}</Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-100">
        <div className="text-xs font-semibold text-slate-600 mb-2">PII Classification (225 columns)</div>
        <ResponsiveContainer width="100%" height={160}>
          <PieChart><Pie data={Object.entries(piiAgg).map(([k,v])=>({name:k,value:v}))} cx="50%" cy="50%" outerRadius={60} innerRadius={30} dataKey="value" label={({name,percent})=>`${name} ${(percent*100).toFixed(0)}%`} labelLine={false}>
            {Object.keys(piiAgg).map((k,i)=><Cell key={i} fill={PII_COLORS[k]}/>)}
          </Pie><Tooltip contentStyle={{fontSize:10}}/></PieChart>
        </ResponsiveContainer>
      </div>
      <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-100">
        <div className="text-xs font-semibold text-slate-600 mb-2">Quality by Layer</div>
        <ResponsiveContainer width="100%" height={160}>
          <RadarChart data={Object.entries(
            TABLES.reduce((acc,t)=>{if(!acc[t.layer])acc[t.layer]={sum:0,count:0};acc[t.layer].sum+=t.quality;acc[t.layer].count++;return acc;},{})
          ).map(([k,v])=>({layer:k,score:Math.round(v.sum/v.count*10)/10}))}>
            <PolarGrid stroke="#E2E8F0"/><PolarAngleAxis dataKey="layer" tick={{fontSize:9}} stroke="#64748B"/>
            <PolarRadiusAxis angle={30} domain={[90,100]} tick={{fontSize:8}}/>
            <Radar dataKey="score" stroke={C.blue} fill={C.blue} fillOpacity={0.3}/>
          </RadarChart>
        </ResponsiveContainer>
      </div>
    </div>
    
    <div className="flex gap-2 mb-3 items-center">
      <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search tables, tags..." className="px-3 py-1.5 border border-slate-200 rounded-lg text-xs flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500"/>
      <div className="flex gap-1">{layers.map(l=><button key={l} onClick={()=>setLayerFilter(l)} className={`px-2 py-1 rounded text-xs font-medium ${layerFilter===l?'text-white':'text-slate-500 bg-white border border-slate-200'}`} style={layerFilter===l?{backgroundColor:LAYER_COLORS[l]||C.blue}:{}}>{l}</button>)}</div>
    </div>
    
    <div className="bg-white rounded-lg shadow-sm border border-slate-100 overflow-hidden">
      <table className="w-full text-xs">
        <thead><tr className="bg-slate-50 border-b border-slate-200">
          {['Table','Layer','Rows','Cols','Size','Quality','PII','Owner','Refresh'].map(h=><th key={h} className="text-left px-3 py-2 font-semibold text-slate-500">{h}</th>)}
        </tr></thead>
        <tbody>{filtered.map((t,i)=>
          <tr key={i} className="border-b border-slate-50 hover:bg-blue-50 cursor-pointer" onClick={()=>onSelect(t.name)}>
            <td className="px-3 py-2 font-semibold text-slate-800 font-mono text-[11px]">{t.name}</td>
            <td className="px-3 py-2"><LayerBadge layer={t.layer}/></td>
            <td className="px-3 py-2 text-slate-600">{t.rows.toLocaleString()}</td>
            <td className="px-3 py-2 text-slate-600">{t.cols}</td>
            <td className="px-3 py-2 text-slate-500">{t.size}</td>
            <td className="px-3 py-2"><QualityDot score={t.quality}/></td>
            <td className="px-3 py-2"><div className="flex gap-0.5">{Object.entries(t.pii).filter(([_,v])=>v>0).map(([k,v])=><span key={k} className="text-[9px] font-medium px-1 rounded text-white" style={{backgroundColor:PII_COLORS[k]}}>{v}</span>)}</div></td>
            <td className="px-3 py-2 text-slate-500">{t.owner}</td>
            <td className="px-3 py-2 text-slate-400">{t.refresh}</td>
          </tr>
        )}</tbody>
      </table>
    </div>
  </div>;
};

// ─── Tab: Table Detail ───
const TableDetail = ({tableName, onBack}) => {
  const table = TABLES.find(t=>t.name===tableName);
  const cols = COLUMNS_DETAIL[tableName] || COLUMNS_DETAIL['dim_customer'];
  if (!table) return <div>Table not found</div>;
  
  return <div>
    <button onClick={onBack} className="text-xs text-blue-600 hover:underline mb-3 inline-block">← Back to catalogue</button>
    <div className="flex items-center gap-3 mb-4">
      <LayerBadge layer={table.layer}/>
      <h2 className="text-lg font-bold text-slate-800 font-mono">{table.name}</h2>
      <QualityDot score={table.quality}/>
    </div>
    
    <div className="grid grid-cols-6 gap-3 mb-5">
      <KPI label="Rows" value={table.rows.toLocaleString()} color={C.blue}/>
      <KPI label="Columns" value={table.cols} color={C.teal}/>
      <KPI label="Size" value={table.size} color={C.amber}/>
      <KPI label="Quality" value={`${table.quality}%`} color={C.green}/>
      <KPI label="Refresh" value={table.refresh} color={C.purple}/>
      <KPI label="SLA" value={table.sla} color={C.cyan}/>
    </div>
    
    <div className="flex gap-1 mb-4">{table.tags.map(t=><span key={t} className="px-2 py-0.5 bg-slate-100 text-slate-600 rounded text-[10px] font-medium">{t}</span>)}</div>
    
    <div className="bg-white rounded-lg shadow-sm border border-slate-100 overflow-hidden">
      <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 font-semibold text-xs text-slate-600">Column Schema & Profiles</div>
      <table className="w-full text-xs">
        <thead><tr className="border-b border-slate-200">{['Column','Type','PII','Null %','Distinct','Unique','Quality','Distribution / Glossary'].map(h=><th key={h} className="text-left px-3 py-2 font-semibold text-slate-500">{h}</th>)}</tr></thead>
        <tbody>{cols.map((c,i)=>
          <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
            <td className="px-3 py-2 font-mono text-[11px] font-semibold text-slate-800">{c.name}</td>
            <td className="px-3 py-2 text-slate-500">{c.type}</td>
            <td className="px-3 py-2"><PIIBadge level={c.pii}/></td>
            <td className="px-3 py-2"><span className={c.nullRate>5?'text-red-600 font-bold':'text-slate-500'}>{c.nullRate}%</span></td>
            <td className="px-3 py-2 text-slate-600">{c.distinct.toLocaleString()}</td>
            <td className="px-3 py-2">{c.unique?<span className="text-green-600">✓</span>:'—'}</td>
            <td className="px-3 py-2"><QualityDot score={c.quality}/></td>
            <td className="px-3 py-2">{c.topValues ? <div className="flex gap-1 flex-wrap">{c.topValues.slice(0,4).map((tv,j)=><span key={j} className="text-[9px] px-1.5 py-0.5 bg-slate-100 rounded text-slate-600">{tv.v} <b>{tv.p}%</b></span>)}</div> : c.glossary ? <span className="text-[10px] text-blue-600 italic">{c.glossary}</span> : c.min !== undefined ? <span className="text-[10px] text-slate-500">{c.min.toLocaleString()} — {c.max.toLocaleString()} (μ {c.mean.toLocaleString()})</span> : '—'}</td>
          </tr>
        )}</tbody>
      </table>
    </div>
  </div>;
};

// ─── Tab: Lineage Map ───
const LineageMap = () => {
  const [hover,setHover] = useState(null);
  const layerX = {source:50,bronze:250,mdm:450,gold:650,clickstream:850,fraud:850};
  
  return <div>
    <div className="mb-4"><h2 className="text-lg font-bold text-slate-800">Data Lineage Map</h2><p className="text-xs text-slate-500 mt-0.5">Source Systems → Bronze → MDM → Gold — full dependency graph</p></div>
    
    <div className="flex gap-2 mb-4">{Object.entries(LAYER_COLORS).filter(([k])=>['source','bronze','mdm','gold','clickstream','fraud'].includes(k)).map(([k,v])=><div key={k} className="flex items-center gap-1"><span className="w-3 h-3 rounded" style={{backgroundColor:v}}/><span className="text-xs text-slate-600">{k}</span></div>)}</div>
    
    <div className="bg-white rounded-lg shadow-sm border border-slate-100 p-4 overflow-x-auto">
      <svg width="1050" height="520" viewBox="0 0 1050 520">
        <defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 Z" fill="#94A3B8"/></marker></defs>
        
        {LINEAGE_DATA.edges.map((e,i)=>{
          const from = LINEAGE_DATA.nodes.find(n=>n.id===e.from);
          const to = LINEAGE_DATA.nodes.find(n=>n.id===e.to);
          if(!from||!to) return null;
          const highlighted = hover===from.id || hover===to.id;
          return <line key={i} x1={from.x+80} y1={from.y+18} x2={to.x} y2={to.y+18} stroke={highlighted?C.amber:'#CBD5E1'} strokeWidth={highlighted?2.5:1.5} markerEnd="url(#arrow)" opacity={hover&&!highlighted?0.2:1}/>;
        })}
        
        {LINEAGE_DATA.nodes.map((n,i)=>(
          <g key={i} onMouseEnter={()=>setHover(n.id)} onMouseLeave={()=>setHover(null)} style={{cursor:'pointer'}}>
            <rect x={n.x} y={n.y} width={160} height={36} rx={6} fill={hover===n.id?LAYER_COLORS[n.layer]:'white'} stroke={LAYER_COLORS[n.layer]} strokeWidth={hover===n.id?2:1.5} opacity={hover&&hover!==n.id?0.4:1}/>
            <text x={n.x+80} y={n.y+22} textAnchor="middle" fontSize="9" fontWeight="600" fill={hover===n.id?'white':C.slate} fontFamily="monospace">{n.label}</text>
          </g>
        ))}
        
        {['Source Systems','Bronze Layer','MDM Layer','Gold Star Schema','Streaming'].map((label,i)=>{
          const xs = [50,250,450,650,850];
          return <text key={i} x={xs[i]+80} y={12} textAnchor="middle" fontSize="10" fontWeight="700" fill={C.med}>{label}</text>;
        })}
      </svg>
    </div>
  </div>;
};

// ─── Tab: Glossary ───
const GlossaryTab = () => {
  const [search,setSearch] = useState('');
  const filtered = GLOSSARY_DATA.filter(g => !search || g.term.toLowerCase().includes(search.toLowerCase()) || g.column.includes(search.toLowerCase()) || g.domain.toLowerCase().includes(search.toLowerCase()));
  const domains = [...new Set(GLOSSARY_DATA.map(g=>g.domain))];
  
  return <div>
    <div className="mb-4"><h2 className="text-lg font-bold text-slate-800">Business Glossary</h2><p className="text-xs text-slate-500 mt-0.5">{GLOSSARY_DATA.length} business terms mapped to columns across the Horizon Bank Holdings data model</p></div>
    
    <div className="flex gap-2 mb-3 flex-wrap">{domains.map(d=><span key={d} className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded-full text-[10px] font-medium">{d}</span>)}</div>
    
    <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search terms, columns, domains..." className="w-full px-3 py-1.5 border border-slate-200 rounded-lg text-xs mb-3 focus:outline-none focus:ring-2 focus:ring-blue-500"/>
    
    <div className="space-y-2">{filtered.map((g,i)=>(
      <div key={i} className="bg-white rounded-lg p-3 shadow-sm border border-slate-100 hover:border-blue-200">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-bold text-sm text-slate-800">{g.term}</span>
          <span className="font-mono text-[10px] text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded">{g.column}</span>
          <span className="text-[10px] text-purple-600 bg-purple-50 px-1.5 py-0.5 rounded font-medium">{g.domain}</span>
        </div>
        <p className="text-xs text-slate-600 mb-2">{g.definition}</p>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-slate-400">Found in:</span>
          <div className="flex gap-1 flex-wrap">{g.tables.map(t=><span key={t} className="text-[9px] font-mono px-1.5 py-0.5 bg-slate-100 rounded text-slate-600">{t}</span>)}</div>
          <span className="text-[10px] text-slate-400 ml-auto">Steward: {g.steward}</span>
        </div>
      </div>
    ))}</div>
  </div>;
};

// ─── Tab: Quality Dashboard ───
const QualityTab = () => {
  const dims = [{d:'Completeness',s:98.1},{d:'Accuracy',s:97.3},{d:'Consistency',s:95.8},{d:'Timeliness',s:96.4},{d:'Uniqueness',s:96.2},{d:'Validity',s:97.8}];
  const sorted = [...TABLES].sort((a,b)=>a.quality-b.quality);
  
  return <div>
    <div className="mb-4"><h2 className="text-lg font-bold text-slate-800">Data Quality Observatory</h2><p className="text-xs text-slate-500 mt-0.5">Automated quality profiling across all 15 tables · 34 DQ tests · 100% pass rate</p></div>
    
    <div className="grid grid-cols-5 gap-3 mb-5">
      <KPI label="Overall Score" value="98.6%" sub="across 15 tables" color={C.green}/>
      <KPI label="DQ Tests" value="34/34" sub="all passing" color={C.green}/>
      <KPI label="PII Columns" value="23" sub="auto-classified" color={C.red}/>
      <KPI label="Glossary Coverage" value="17 terms" sub="business definitions" color={C.purple}/>
      <KPI label="Lineage Mapped" value="15/15" sub="full dependency graph" color={C.blue}/>
    </div>
    
    <div className="grid grid-cols-2 gap-4 mb-4">
      <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-100">
        <div className="text-xs font-semibold text-slate-600 mb-3">Quality Dimensions</div>
        <ResponsiveContainer width="100%" height={200}>
          <RadarChart data={dims}><PolarGrid stroke="#E2E8F0"/><PolarAngleAxis dataKey="d" tick={{fontSize:10}} stroke="#64748B"/><PolarRadiusAxis angle={30} domain={[92,100]} tick={{fontSize:9}}/><Radar dataKey="s" stroke={C.blue} fill={C.blue} fillOpacity={0.3}/></RadarChart>
        </ResponsiveContainer>
      </div>
      <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-100">
        <div className="text-xs font-semibold text-slate-600 mb-3">Quality by Table (sorted lowest first)</div>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={sorted} layout="vertical">
            <XAxis type="number" domain={[90,100]} tick={{fontSize:9}} stroke="#94A3B8"/>
            <YAxis dataKey="name" type="category" tick={{fontSize:8}} width={120} stroke="#94A3B8"/>
            <Tooltip contentStyle={{fontSize:10}}/>
            <Bar dataKey="quality" name="Quality %">{sorted.map((t,i)=><Cell key={i} fill={t.quality>=98?C.green:t.quality>=96?C.teal:C.amber}/>)}</Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
    
    <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-100">
      <div className="text-xs font-semibold text-slate-600 mb-3">Quality Heatmap — All Tables</div>
      <div className="grid grid-cols-5 gap-2">{TABLES.map((t,i)=>{
        const bg = t.quality>=99?'#D1FAE5':t.quality>=98?'#CCFBF1':t.quality>=96?'#FEF3C7':'#FEE2E2';
        const tc = t.quality>=98?'#059669':t.quality>=96?'#0D9488':t.quality>=95?'#D97706':'#DC2626';
        return <div key={i} className="p-2 rounded-lg text-center" style={{backgroundColor:bg}}>
          <div className="font-mono text-[9px] font-semibold text-slate-700">{t.name}</div>
          <div className="text-lg font-bold mt-0.5" style={{color:tc}}>{t.quality}%</div>
          <div className="text-[9px] text-slate-500">{t.rows.toLocaleString()} rows</div>
        </div>;
      })}</div>
    </div>
  </div>;
};

// ─── Main App ───
const TABS = [
  {id:'browse',label:'📋 Catalogue',icon:'📋'},
  {id:'detail',label:'🔍 Table Detail',icon:'🔍'},
  {id:'lineage',label:'🔗 Lineage',icon:'🔗'},
  {id:'glossary',label:'📖 Glossary',icon:'📖'},
  {id:'quality',label:'✅ Quality',icon:'✅'},
];

export default function App() {
  const [tab,setTab] = useState('browse');
  const [selectedTable,setSelectedTable] = useState('dim_customer');
  
  const handleSelect = (name) => { setSelectedTable(name); setTab('detail'); };
  const handleBack = () => setTab('browse');
  
  return <div className="min-h-screen bg-slate-50">
    <div className="bg-slate-900 text-white px-6 py-3 flex items-center justify-between">
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center font-bold text-sm">DC</div>
        <div><div className="text-sm font-bold">Horizon Bank Holdings — Data Catalogue</div><div className="text-xs text-slate-400">Enterprise Metadata · Lineage · Quality · Glossary</div></div>
      </div>
      <div className="flex items-center gap-4 text-xs text-slate-400"><span>15 tables</span><span>225 columns</span><span>103K+ records</span><span>17 glossary terms</span><span className="text-emerald-400">● Profiled</span></div>
    </div>
    
    <div className="bg-white border-b border-slate-200 px-4 flex gap-0.5">
      {TABS.map(t=><button key={t.id} onClick={()=>setTab(t.id)} className={`px-3 py-2.5 text-xs font-medium whitespace-nowrap border-b-2 ${tab===t.id?'border-blue-600 text-slate-900 bg-blue-50':'border-transparent text-slate-500 hover:text-slate-700 hover:bg-slate-50'}`}>{t.label}</button>)}
    </div>
    
    <div className="p-6 max-w-7xl mx-auto">
      {tab==='browse' && <CatalogueBrowser onSelect={handleSelect}/>}
      {tab==='detail' && <TableDetail tableName={selectedTable} onBack={handleBack}/>}
      {tab==='lineage' && <LineageMap/>}
      {tab==='glossary' && <GlossaryTab/>}
      {tab==='quality' && <QualityTab/>}
    </div>
    
    <div className="text-center py-3 text-xs text-slate-400 border-t border-slate-100">Simultaneous — Data Catalogue · Built with Claude Opus 4.6 AI Agents</div>
  </div>;
}
