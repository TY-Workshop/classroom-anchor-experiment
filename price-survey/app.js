/* Adapted from the uploaded classroom-quiz app and the telephone experiment:
   static browser UI + encrypted MQTT classroom relay + local teacher storage. */
'use strict';
const $=id=>document.getElementById(id);
const BROKERS=['wss://test.mosquitto.org:8081/','wss://broker.emqx.io:8084/mqtt','wss://broker.hivemq.com:8884/mqtt'];
const STORE='classroom-psm-v1',TOPIC='cq-psm1';
const QUESTIONS=[
 '价格低到多少人民币时，你会觉得这个产品太便宜，以至于怀疑质量而不考虑购买？',
 '价格是多少人民币时，你会觉得这个产品便宜、划算，物有所值？',
 '价格达到多少人民币时，你会开始觉得这个产品贵，但仍可能考虑购买？',
 '价格达到多少人民币时，你会觉得这个产品太贵，从而不考虑购买？'
];
const DEFAULT={product:'无线机械键盘',description:'一款适合学习和办公使用的无线机械键盘，支持蓝牙连接，配有可充电电池和可调节背光。\n\n请根据上述产品信息，独立填写四个价格。',questions:QUESTIONS};
const POINTS={
 PMC:{name:'边际低价点',curves:'太便宜 × 不便宜',meaning:'可接受价格下限的参考。低于这里，“太便宜、担心质量”的比例更高。'},
 OPP:{name:'最优价格点',curves:'太便宜 × 太贵',meaning:'觉得“太便宜”与“太贵”的比例相等，是两端价格顾虑的平衡点。'},
 IPP:{name:'无差异价格点',curves:'便宜／划算 × 贵',meaning:'觉得“便宜／划算”与“贵”的比例相等，可参考为心理正常价格。'},
 PME:{name:'边际高价点',curves:'不贵 × 太贵',meaning:'可接受价格上限的参考。高于这里，“太贵、不考虑”的比例更高。'}
};
const CURVES={tooCheap:{name:'太便宜',color:'#3976ad'},cheap:{name:'便宜／划算',color:'#168679'},expensive:{name:'贵',color:'#c28b26'},tooExpensive:{name:'太贵',color:'#b74c64'},notCheap:{name:'不便宜（辅助）',color:'#168679',aux:true},notExpensive:{name:'不贵（辅助）',color:'#c28b26',aux:true}};
const params=new URLSearchParams(location.hash.slice(1)),isStudent=params.has('join');
const random=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('');
let host,room,secret,cryptoKey,client,brokerIdx=1,connected=false,config,currentRun,pending=null,studentDone=false,studentOpen=true,lastHost=0,chart,analysis,selectedPoint=null;
const fmt=n=>n.toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2});
const pointPrice=p=>!p?'—':p.interval?`¥${fmt(p.low)}–${fmt(p.high)}`:`¥${fmt(p.price)}`;
function status(s,state='warn'){$('connection').textContent=s;$('connection').dataset.state=state;}
function notify(s){$('toast').textContent=s;$('toast').style.display='block';clearTimeout(notify.timer);notify.timer=setTimeout(()=>$('toast').style.display='none',3000);}
function persist(){try{localStorage.setItem(STORE,JSON.stringify(host));return true;}catch{status('本机保存失败，请立即导出已有回答。新的提交暂不确认。');return false;}}
function validConfig(c){return c&&typeof c.product==='string'&&c.product.trim()&&c.product.length<=300&&typeof c.description==='string'&&c.description.length<=10000&&Array.isArray(c.questions)&&c.questions.length===4&&c.questions.every(q=>typeof q==='string'&&q.trim()&&q.length<=1000);}
function newHost(){return {room:random(),secret:random(),run:random(),broker:1,config:structuredClone(DEFAULT),rows:[],history:[],open:true};}
function participant(){const k=`psm-participant-${room}`;let id=sessionStorage.getItem(k)||localStorage.getItem(k);if(!id){id=random();localStorage.setItem(k,id);}sessionStorage.setItem(k,id);return id;}
const studentStore=()=>`psm-response-${room}`;
const b64=bytes=>btoa(String.fromCharCode(...new Uint8Array(bytes)));
const unb64=s=>Uint8Array.from(atob(s),c=>c.charCodeAt(0));
async function setupCrypto(){cryptoKey=await crypto.subtle.importKey('raw',Uint8Array.from(secret.match(/../g),h=>parseInt(h,16)),{name:'AES-GCM'},false,['encrypt','decrypt']);}
async function encode(data){const iv=crypto.getRandomValues(new Uint8Array(12));const bytes=await crypto.subtle.encrypt({name:'AES-GCM',iv},cryptoKey,new TextEncoder().encode(JSON.stringify(data)));return JSON.stringify({v:1,iv:b64(iv),data:b64(bytes)});}
async function decode(buf){const e=JSON.parse(buf.toString());if(e.v!==1)return null;const bytes=await crypto.subtle.decrypt({name:'AES-GCM',iv:unb64(e.iv)},cryptoKey,unb64(e.data));return JSON.parse(new TextDecoder().decode(bytes));}
async function send(leaf,data){if(!connected)return false;const c=client;try{const payload=await encode(data);if(c!==client||!c.connected)return false;c.publish(`${TOPIC}/${room}/${leaf}`,payload,{qos:1,retain:false});return true;}catch{return false;}}
function broadcast(){return send('state',{run:host.run,config:host.config,open:host.open});}
function connect(){
 connected=false;status('正在连接课堂…');if(client)client.end(true);
 const c=mqtt.connect(BROKERS[brokerIdx],{clientId:'psm_'+random(),keepalive:30,reconnectPeriod:2500,connectTimeout:8000,clean:true});client=c;
 c.on('connect',()=>{if(c!==client)return;const leaves=isStudent?['state',`ack/${participant()}`]:['hello','answer'];c.subscribe(leaves.map(l=>`${TOPIC}/${room}/${l}`),{qos:1},err=>{if(err){status('订阅失败，请重新连接。');return;}connected=true;status(isStudent?'已连接课堂':'课堂已连接 · 保持教师页面打开','ok');if(isStudent){send('hello',{id:participant()});if(pending)send('answer',pending);}else{updateJoin();broadcast();}});});
 c.on('message',async(topic,buf)=>{if(c!==client||buf.length>100000)return;try{const data=await decode(buf);if(data)await handle(topic.slice(`${TOPIC}/${room}/`.length),data);}catch{}});
 c.on('close',()=>{if(c!==client)return;connected=false;status('连接中断，正在重连；未确认的回答会自动重试。');});
 c.on('error',()=>{if(c===client)status('暂时无法连接，请检查网络或在教师端更换线路。');});
}
async function handle(leaf,d){
 if(!isStudent){
  if(leaf==='hello'){broadcast();return;}
  if(leaf!=='answer'||typeof d.id!=='string'||!/^\w{32}$/.test(d.id))return;
  if(!PSM.valid(d)){send(`ack/${d.id}`,{run:d.run,ok:false,reason:'invalid'});return;}
  if(host.rows.some(r=>r.id===d.id&&r.run===d.run)){send(`ack/${d.id}`,{run:d.run,ok:true});return;}
  if(d.run!==host.run){send(`ack/${d.id}`,{run:d.run,ok:false,reason:'new-run'});broadcast();return;}
  if(!host.open){send(`ack/${d.id}`,{run:d.run,ok:false,reason:'paused'});return;}
  const row={id:d.id,run:d.run,at:new Date().toISOString()};for(const k of PSM.keys)row[k]=d[k];host.rows.push(row);
  if(persist()){renderResults();send(`ack/${d.id}`,{run:d.run,ok:true});}else host.rows.pop();
  return;
 }
 if(leaf==='state'){
  if(!validConfig(d.config)||typeof d.run!=='string')return;
  lastHost=Date.now();status(d.open?'已连接课堂':'老师已暂停收集','ok');
  if(currentRun!==d.run){
   if(currentRun){pending=null;studentDone=false;PSM.keys.forEach(k=>$(k).value='');$('error').textContent='老师开始了新一轮，请重新填写。';}
   currentRun=d.run;const saved=JSON.parse(localStorage.getItem(studentStore())||'null');
   if(saved?.run===currentRun){PSM.keys.forEach(k=>$(k).value=saved[k]??'');studentDone=!!saved.done;pending=saved.pending?saved:null;}
  }
  config=d.config;studentOpen=d.open;renderStudent();return;
 }
 if(leaf===`ack/${participant()}`&&pending&&d.run===pending.run){
  if(d.ok){localStorage.setItem(studentStore(),JSON.stringify({...pending,pending:false,done:true}));pending=null;studentDone=true;$('error').textContent='';renderStudent();}
  else{pending=null;saveDraft();$('error').textContent=d.reason==='paused'?'老师已暂停收集，恢复后请再次提交。':d.reason==='invalid'?'请核对四个价格，金额应非负且按顺序不递减。':'老师已开始新一轮，请重新填写。';renderStudent();}
 }
}
function saveDraft(){if(!currentRun||pending||studentDone)return;const draft={run:currentRun};for(const k of PSM.keys)draft[k]=$(k).value;try{localStorage.setItem(studentStore(),JSON.stringify(draft));}catch{}}
function buildStudent(){for(let i=0;i<4;i++){const k=PSM.keys[i],box=document.createElement('div');box.className='question';box.id='field-'+k;const label=document.createElement('label');label.htmlFor=k;const number=document.createElement('span');number.className='question-number';number.textContent=`${i+1} / 4`;const text=document.createElement('span');text.id='label-'+k;label.append(number,text);const money=document.createElement('div');money.className='money-input';const prefix=document.createElement('span');prefix.textContent='人民币 ¥';const input=document.createElement('input');Object.assign(input,{id:k,type:'text',inputMode:'decimal',autocomplete:'off',required:true});input.setAttribute('aria-describedby','error-'+k);input.addEventListener('input',()=>{saveDraft();if(PSM.parseAmount(input.value)!==null){$('error-'+k).textContent='';box.classList.remove('invalid');}if(PSM.valid(Object.fromEntries(PSM.keys.map(key=>[key,PSM.parseAmount($(key).value)]))))$('error').textContent='';});const err=document.createElement('p');err.id='error-'+k;err.className='field-error';money.append(prefix,input);box.append(label,money,err);$('questions').append(box);}}
function renderStudent(){
 $('waiting').hidden=!!config;$('questionnaire').hidden=!config||studentDone;$('done').hidden=!studentDone;
 if(!config)return;$('product').textContent=config.product;$('description').textContent=config.description;PSM.keys.forEach((k,i)=>$('label-'+k).textContent=config.questions[i]);
 $('submit').disabled=!!pending||!studentOpen;$('submit').textContent=pending?'等待老师确认…':'提交';PSM.keys.forEach(k=>$(k).disabled=!!pending);
}
function validateStudent(){
 const values={};let ok=true;for(const k of PSM.keys){const n=PSM.parseAmount($(k).value);$('error-'+k).textContent=n===null?'请填写有效的非负人民币金额，最多两位小数。':'';$('field-'+k).classList.toggle('invalid',n===null);if(n===null)ok=false;values[k]=n;}
 if(!ok)return null;if(!PSM.valid(values)){$('error').textContent='请核对四个价格：太便宜 ≤ 便宜／划算 ≤ 贵但仍考虑 ≤ 太贵。你可以返回各题修改答案。';return null;}$('error').textContent='';return values;
}
function updateJoin(){const u=new URL(location.href);u.hash=new URLSearchParams({join:host.room,k:host.secret,b:String(brokerIdx)}).toString();$('joinLink').href=u.href;$('joinLink').textContent=u.href;$('qr').replaceChildren();new QRCode($('qr'),{text:u.href,width:768,height:768,correctLevel:QRCode.CorrectLevel.M});}
function chartOption(){
 const nodes=analysis.nodes,points=analysis.points,showAux=$('auxiliary').checked;
 const active=selectedPoint?PSM.pairs[selectedPoint]:null;
 const series=Object.entries(CURVES).map(([key,c])=>({name:c.name,type:'line',data:nodes.map(p=>[p.price,p[key]*100]),showSymbol:false,symbol:'none',animation:false,lineStyle:{color:c.color,width:active?.includes(key)?4:2.2,type:c.aux?'dashed':'solid',opacity:active&&!active.includes(key)?.2:1},itemStyle:{color:c.color},emphasis:{focus:'series'},z:c.aux?2:3}));
 const markerData=[],areaData=[],intervalLines=[];
 for(const [name,p]of Object.entries(points)){if(!p)continue;const isActive=!selectedPoint||selectedPoint===name;if(p.interval){areaData.push([{name:`${name} 重合区间`,xAxis:p.low,itemStyle:{color:'rgba(50,75,65,0.035)'},label:{show:false}},{xAxis:p.high}]);intervalLines.push([{coord:[p.low,p.y*100],name,lineStyle:{color:isActive?'#526d5e':'#a8b5ad',width:isActive?4:2,type:'solid',opacity:isActive?1:.35},label:{show:isActive,formatter:()=>`${name}（区间）`,position:'middle',color:'#263d33',fontWeight:700,distance:9}},{coord:[p.high,p.y*100]}]);}else{markerData.push({name,coord:[p.price,p.y*100],symbolSize:isActive?12:7,itemStyle:{color:isActive?'#263d33':'#a5b2aa'},label:{show:isActive,formatter:name,fontWeight:700,color:'#263d33',position:{PMC:'left',OPP:'bottom',IPP:'top',PME:'right'}[name],distance:12}});}}
 series.push({name:'交叉点',type:'line',data:[],markPoint:{data:markerData,symbol:'circle'},markLine:{silent:true,data:intervalLines,symbol:['circle','circle'],symbolSize:5},markArea:{silent:true,data:areaData},tooltip:{show:false},z:10});
 const legendSelected=Object.fromEntries(Object.values(CURVES).map(c=>[c.name,!c.aux||showAux||active?.includes(Object.entries(CURVES).find(([,v])=>v===c)[0])]));
 return {animation:false,color:Object.values(CURVES).map(c=>c.color),textStyle:{fontFamily:'-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif'},legend:{top:5,left:'center',itemWidth:22,itemHeight:10,textStyle:{fontSize:13},data:Object.values(CURVES).map(c=>c.name),selected:legendSelected},grid:{left:64,right:34,top:82,bottom:58},xAxis:{type:'value',min:0,max:analysis.maxPrice,name:'价格（人民币元）',nameLocation:'middle',nameGap:36,axisLabel:{hideOverlap:true},splitLine:{lineStyle:{color:'#edf1ee'}}},yAxis:{type:'value',min:0,max:100,interval:20,name:'受访者比例',nameGap:18,axisLabel:{formatter:'{value}%'},splitLine:{lineStyle:{color:'#e7ede9'}}},tooltip:{trigger:'axis',confine:true,axisPointer:{type:'line'},formatter:items=>{if(!items.length)return '';const x=items[0].value[0];const html=[`人民币 ¥${fmt(x)}`];for(const item of items){if(!Array.isArray(item.value))continue;html.push(`${item.marker}${item.seriesName}：${Number(item.value[1]).toFixed(1)}%`);}return html.join('<br>');}},series};
}
function renderChart(){if(!chart)chart=echarts.init($('chart'),null,{renderer:'svg'});chart.setOption(chartOption(),{notMerge:true});}
function focusPoint(name){selectedPoint=selectedPoint===name?null:name;if(selectedPoint&&PSM.pairs[selectedPoint].some(k=>CURVES[k].aux))$('auxiliary').checked=true;renderChart();}
function renderResults(){
 analysis=PSM.analyze(host.rows);$('resultProduct').textContent=host.config.product;$('responseCount').textContent=`已确认 ${host.rows.length} 份回答 · 有效 ${analysis.n} 份${analysis.excluded?` · 不计入 ${analysis.excluded} 份`:''}${host.open?' · 收集中':' · 已暂停'}`;$('pause').textContent=host.open?'暂停收集':'恢复收集';
 $('emptyMessage').textContent=!analysis.n?'等待学生提交。尚无数据时不显示虚构曲线或交叉价格。':analysis.n<10?'当前样本较少，交叉点可能是较宽的区间，并会随新回答变化。':'';
 $('pointRows').replaceChildren();for(const [name,meta]of Object.entries(POINTS)){const p=analysis.points[name],tr=document.createElement('tr');const tdName=document.createElement('td'),button=document.createElement('button');button.className='point-button';button.textContent=name;button.onclick=()=>focusPoint(name);const label=document.createElement('span');label.className='point-name';label.textContent=meta.name;tdName.append(button,label);const tdPrice=document.createElement('td');tdPrice.className='point-price';tdPrice.textContent=pointPrice(p);if(p?.interval){const note=document.createElement('span');note.className='point-note';note.textContent='曲线重合区间';tdPrice.append(note);}const tdCurves=document.createElement('td');tdCurves.textContent=meta.curves;const tdMeaning=document.createElement('td');tdMeaning.textContent=meta.meaning;tr.append(tdName,tdPrice,tdCurves,tdMeaning);$('pointRows').append(tr);}
 const {PMC,PME}=analysis.points;$('acceptableRange').textContent=!PMC||!PME?'可接受价格范围：等待有效回答':PMC.interval||PME.interval?`可接受价格范围参考：下限 PMC ${pointPrice(PMC)}；上限 PME ${pointPrice(PME)}`:`可接受价格范围参考：¥${fmt(PMC.price)}–${fmt(PME.price)}`;
 $('records').replaceChildren();host.rows.forEach((r,i)=>{const tr=document.createElement('tr');for(const v of [i+1,...PSM.keys.map(k=>fmt(r[k])),new Date(r.at).toLocaleTimeString('zh-CN')]){const td=document.createElement('td');td.textContent=v;tr.append(td);}$('records').append(tr);});renderChart();
}
function buildEditor(){PSM.keys.forEach((k,i)=>{const label=document.createElement('label');label.textContent=`第 ${i+1} 题 · ${['太便宜','便宜／划算','贵但仍考虑','太贵不考虑'][i]}`;const input=document.createElement('textarea');input.id='edit-'+k;input.rows=3;input.maxLength=1000;label.append(input);$('questionEditors').append(label);});}
function populateEditor(){$('editProduct').value=host.config.product;$('editDescription').value=host.config.description;PSM.keys.forEach((k,i)=>$('edit-'+k).value=host.config.questions[i]);}
function archiveRound(){if(host.rows.length)host.history.push({run:host.run,config:structuredClone(host.config),rows:host.rows});host.rows=[];host.run=random();host.open=true;selectedPoint=null;}
function currentRound(){return {run:host.run,config:host.config,rows:host.rows};}
function download(name,content,type='text/csv;charset=utf-8'){const u=URL.createObjectURL(new Blob([content],{type}));const a=document.createElement('a');a.href=u;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);}
const csv=lines=>'\uFEFF'+lines.map(row=>row.map(v=>'"'+String(v??'').replace(/"/g,'""')+'"').join(',')).join('\r\n');
function responsesCsv(rounds){const lines=[['轮次','产品名称','产品介绍','第1题','第2题','第3题','第4题','太便宜_人民币元','便宜_人民币元','贵_人民币元','太贵_人民币元','随机响应编号','提交时间','有效回答']];for(const r of rounds)for(const row of r.rows)lines.push([r.run,r.config.product,r.config.description,...r.config.questions,...PSM.keys.map(k=>row[k]),row.id,row.at,PSM.valid(row)?'是':'否']);return csv(lines);}
function pointsCsv(){const lines=[['交叉点','中文名称','交叉价格_人民币元','区间下界','区间上界','是否重合区间','交叉曲线','含义','有效回答数','口径']];for(const [name,meta]of Object.entries(POINTS)){const p=analysis.points[name];lines.push([name,meta.name,p?.price??'',p?.low??'',p?.high??'',p?p.interval?'是':'否':'暂无数据',meta.curves,meta.meaning,analysis.n,'原始六曲线，经验累计阶梯']);}return csv(lines);}
function bindTeacher(){
 $('copyLink').onclick=async()=>{try{await navigator.clipboard.writeText($('joinLink').href);notify('学生链接已复制');}catch{notify('请选中上方链接手动复制');}};
 $('downloadQR').onclick=()=>{const canvas=$('qr').querySelector('canvas');if(canvas){const a=document.createElement('a');a.download='四个价格问卷_学生二维码.png';a.href=canvas.toDataURL('image/png');a.click();}};
 $('project').onclick=()=>{document.body.classList.toggle('projection');$('project').textContent=document.body.classList.contains('projection')?'返回教师管理':'投影结果';requestAnimationFrame(()=>chart?.resize());};
 $('auxiliary').onchange=()=>{selectedPoint=null;renderChart();};$('showAll').onclick=()=>{selectedPoint=null;$('auxiliary').checked=true;renderChart();};
 $('pause').onclick=()=>{host.open=!host.open;persist();renderResults();broadcast();};
 $('newRound').onclick=()=>{if(!confirm('开始新一轮？本轮回答会保存到本机历史，学生将重新填写四个价格。'))return;archiveRound();persist();renderResults();broadcast();notify('新一轮已开始，本轮已存入历史记录');};
 $('saveConfig').onclick=()=>{const next={product:$('editProduct').value.trim(),description:$('editDescription').value.trim(),questions:PSM.keys.map(k=>$('edit-'+k).value.trim())};if(!validConfig(next)){$('editorStatus').textContent='请填写产品名称和全部四道问题。';return;}if(JSON.stringify(next)===JSON.stringify(host.config)){notify('题目没有变化');return;}if(host.rows.length&&!confirm('已有回答。保存修改会把本轮存入历史并开始新一轮，以免混合不同题目或产品的结果。'))return;archiveRound();host.config=next;persist();renderResults();broadcast();$('editorStatus').textContent='已保存，学生将看到更新后的四道问题。';};
 $('broker').value=brokerIdx;$('broker').onchange=()=>{brokerIdx=Number($('broker').value);host.broker=brokerIdx;persist();connect();updateJoin();};
 $('export').onclick=()=>download('四个价格问卷_本轮回答.csv',responsesCsv([currentRound()]));$('exportHistory').onclick=()=>download('四个价格问卷_历史回答.csv',responsesCsv([...host.history,currentRound()]));$('exportPoints').onclick=()=>download('四个价格问卷_交叉点.csv',pointsCsv());
 window.addEventListener('resize',()=>chart?.resize());
}
async function startTeacher(){
 try{const saved=JSON.parse(localStorage.getItem(STORE)||'null');host=saved&&validConfig(saved.config)&&Array.isArray(saved.rows)?saved:newHost();}catch{host=newHost();}
 room=host.room;secret=host.secret;brokerIdx=host.broker??1;if(!persist())return;
 $('teacher').hidden=false;buildEditor();populateEditor();bindTeacher();renderResults();updateJoin();await setupCrypto();connect();setInterval(()=>{if(connected)broadcast();},2500);
}
async function startStudent(){
 room=params.get('join');secret=params.get('k');brokerIdx=Number(params.get('b'))||0;$('student').hidden=false;
 if(!/^[a-f0-9]{32}$/.test(room)||!/^[a-f0-9]{32}$/.test(secret)||brokerIdx<0||brokerIdx>=BROKERS.length){$('waiting').textContent='链接无效，请重新扫描老师的二维码。';status('无法加入课堂');return;}
 buildStudent();$('questionnaire').onsubmit=e=>{e.preventDefault();const values=validateStudent();if(!values)return;if(!studentOpen){$('error').textContent='老师已暂停收集。';return;}pending={id:participant(),run:currentRun,...values,pending:true};try{localStorage.setItem(studentStore(),JSON.stringify(pending));}catch{pending=null;$('error').textContent='浏览器无法暂存回答，请允许此网页使用本机存储后再提交。';return;}$('error').textContent='正在发送，请保持本页打开。收到老师确认后会显示提交成功。';renderStudent();send('answer',pending);};
 await setupCrypto();connect();setInterval(()=>{if(connected){if(pending)send('answer',pending);if(Date.now()-lastHost>7000){status('正在等待老师连接，请保持本页打开…');send('hello',{id:participant()});}}},2500);
}
async function start(){
 if(!window.mqtt||!window.QRCode||!window.echarts||!window.PSM){status('程序组件加载失败，请刷新网页。');return;}
 if(!crypto.subtle){status('请通过 HTTPS 或本机 localhost 打开网页。');return;}
 if(isStudent){await startStudent();return;}
 if(navigator.locks){navigator.locks.request('psm-teacher-controller',{ifAvailable:true},async lock=>{if(!lock){status('教师端已在另一个标签页打开，请使用原来的页面。');return;}await startTeacher();await new Promise(()=>{});});}else await startTeacher();
}
start().catch(()=>status('打开失败，请刷新并允许此网页使用本机存储。'));
