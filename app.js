/* Adapted from classroom-quiz.zip: the same standalone HTML + three public
   MQTT-over-WebSocket relay architecture. Quiz scoring/timers are replaced
   with two-page responses and recalculable teacher-side group means. */
'use strict';
const $ = id => document.getElementById(id);
const BROKERS = ['wss://test.mosquitto.org:8081/','wss://broker.emqx.io:8084/mqtt','wss://broker.hivemq.com:8884/mqtt'];
const STORE = 'classroom-anchor-v1';
const DEFAULT = {q1:'请填写你的两位被试编号',help:'为便于整理本次实验，请用你常用电话号码的最后两位数字作为被试编号（00–99）。例如，号码以 07 结尾，就填写 07。\n只填写最后两位即可，请不要填写完整电话号码或姓名。单凭这两位数字无法识别你的身份。',product:'无线机械键盘',description:'一款适合学习和办公使用的无线机械键盘，支持蓝牙连接，配有可充电电池和可调节背光。\n\n请根据上述产品信息，独立作答。',q2:'你最多愿意花多少人民币购买这个产品？'};
let host, client, room, secret, cryptoKey, brokerIdx=0, connected=false, config, currentRun, viewStep=1, pending=null, heartbeat=0, lastHost=0, hostBusy=false;
let releaseTeacher;
const params=new URLSearchParams(location.hash.slice(1));
const isStudent=params.has('join');
const random=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('');
const validAnchor=v=>/^\d{1,2}$/.test(String(v).trim());
const parsePrice=v=>{const s=String(v).trim();if(!/^\d+(\.\d{1,2})?$/.test(s))return null;const n=Number(s);return Number.isFinite(n)&&n>=0&&n<=Number.MAX_SAFE_INTEGER/100?Math.round(n*100)/100:null;};
function groupStats(rows,mode,cuts){const ranges=mode===2?[[0,cuts[0]-1],[cuts[0],99]]:[[0,cuts[0]-1],[cuts[0],cuts[1]-1],[cuts[1],99]];return ranges.map(([lo,hi],i)=>{const rs=rows.filter(r=>r.anchor>=lo&&r.anchor<=hi);return {name:(mode===2?['低锚组','高锚组']:['低锚组','中锚组','高锚组'])[i],lo,hi,n:rs.length,mean:rs.length?rs.reduce((s,r)=>s+r.price,0)/rs.length:null};});}
function notify(s){$('toast').textContent=s;$('toast').style.display='block';clearTimeout(notify.timer);notify.timer=setTimeout(()=>$('toast').style.display='none',3000);}
function status(s,state='warn'){$('connection').textContent=s;$('connection').dataset.state=state;}
function persist(){try{localStorage.setItem(STORE,JSON.stringify(host));return true;}catch{status('本机保存失败，请立即导出数据；新的提交暂不确认。');return false;}}
function newHost(){return {room:random(),secret:random(),run:random(),broker:0,config:{...DEFAULT},rows:[],history:[],open:true,mode:2,cuts2:[50],cuts3:[33,67]};}
function validConfig(c){return c&&['q1','help','product','description','q2'].every(k=>typeof c[k]==='string'&&c[k].length<=10000)&&c.product.trim()&&c.q1.trim()&&c.q2.trim();}
async function setupCrypto(){cryptoKey=await crypto.subtle.importKey('raw',Uint8Array.from(secret.match(/../g),h=>parseInt(h,16)),{name:'AES-GCM'},false,['encrypt','decrypt']);}
function b64(bytes){return btoa(String.fromCharCode(...new Uint8Array(bytes)));}
function unb64(s){return Uint8Array.from(atob(s),c=>c.charCodeAt(0));}
async function encode(data){const iv=crypto.getRandomValues(new Uint8Array(12));const encrypted=await crypto.subtle.encrypt({name:'AES-GCM',iv},cryptoKey,new TextEncoder().encode(JSON.stringify(data)));return JSON.stringify({v:1,iv:b64(iv),data:b64(encrypted)});}
async function decode(buf){const e=JSON.parse(buf.toString());if(e.v!==1)return null;const plain=await crypto.subtle.decrypt({name:'AES-GCM',iv:unb64(e.iv)},cryptoKey,unb64(e.data));return JSON.parse(new TextDecoder().decode(plain));}
async function send(leaf,data){if(!connected)return false;const c=client;try{const payload=await encode(data);if(c!==client||!c.connected)return false;c.publish(`cq-anchor1/${room}/${leaf}`,payload,{qos:1,retain:false});return true;}catch{return false;}}
function connect(){
  connected=false;status('正在连接课堂…');if(client)client.end(true);
  const c=mqtt.connect(BROKERS[brokerIdx],{clientId:'anchor_'+random(),keepalive:30,reconnectPeriod:2500,connectTimeout:8000,clean:true});client=c;
  c.on('connect',()=>{if(c!==client)return;const leaves=isStudent?['state',`ack/${participant()}`]:['hello','answer'];c.subscribe(leaves.map(l=>`cq-anchor1/${room}/${l}`),{qos:1},err=>{if(err){status('订阅失败，正在重连…');return;}connected=true;status(isStudent?'已连接课堂':'课堂已连接 · 保持教师页面打开','ok');if(isStudent){send('hello',{id:participant()});if(pending)send('answer',pending);}else{updateJoin();broadcast();}});});
  c.on('message',async(topic,buf)=>{if(c!==client||buf.length>100000)return;try{const d=await decode(buf);const leaf=topic.slice(`cq-anchor1/${room}/`.length);if(d)await handle(leaf,d);}catch{/* Ignore unrelated or undecryptable relay messages. */}});
  c.on('close',()=>{if(c!==client)return;connected=false;status('连接中断，正在重连；未确认的回答会自动重试。');});
  c.on('error',()=>{if(c===client)status('暂时无法连接，请检查网络或更换线路。');});
}
function participant(){const k=`anchor-participant-${room}`;let id=sessionStorage.getItem(k)||localStorage.getItem(k);if(!id){id=random();localStorage.setItem(k,id);}sessionStorage.setItem(k,id);return id;}
function studentStore(){return `anchor-response-${room}`;}
function storeStudent(data){localStorage.setItem(studentStore(),JSON.stringify(data));}
function broadcast(){return send('state',{run:host.run,config:host.config,open:host.open,at:Date.now()});}
async function handle(leaf,d){
  if(!isStudent){
    if(leaf==='hello'){broadcast();return;}
    if(leaf!=='answer'||typeof d.id!=='string'||!/^\w{32}$/.test(d.id)||!validAnchor(d.anchor)||parsePrice(d.price)===null)return;
    const old=host.rows.find(r=>r.id===d.id&&r.run===d.run);
    if(old){send(`ack/${d.id}`,{run:d.run,ok:true});return;}
    if(d.run!==host.run){send(`ack/${d.id}`,{run:d.run,ok:false,reason:'new-run'});broadcast();return;}
    if(!host.open){send(`ack/${d.id}`,{run:d.run,ok:false,reason:'paused'});return;}
    host.rows.push({id:d.id,run:d.run,anchor:Number(d.anchor),price:parsePrice(d.price),at:new Date().toISOString()});
    if(persist()){renderStats();send(`ack/${d.id}`,{run:d.run,ok:true});}else host.rows.pop();
    return;
  }
  if(leaf==='state'){
    if(!validConfig(d.config)||typeof d.run!=='string')return;
    lastHost=Date.now();status(d.open?'已连接课堂':'老师已暂停收集','ok');
    if(currentRun!==d.run){
      if(currentRun){pending=null;$('anchor').value='';$('price').value='';$('error2').textContent='老师开始了新一轮，请重新作答。';viewStep=1;}
      currentRun=d.run;const saved=JSON.parse(localStorage.getItem(studentStore())||'null');
      if(saved?.run===currentRun){$('anchor').value=String(saved.anchor).padStart(2,'0');$('price').value=saved.price;viewStep=saved.done?3:2;pending=saved.done?null:saved;}
    }
    config=d.config;studentOpen=d.open;renderStudent();return;
  }
  if(leaf===`ack/${participant()}`&&pending&&d.run===pending.run){
    if(d.ok){storeStudent({...pending,done:true});pending=null;viewStep=3;$('error2').textContent='';renderStudent();}
    else{pending=null;localStorage.removeItem(studentStore());$('error2').textContent=d.reason==='paused'?'老师已暂停收集，恢复后请再次提交。':'老师已开始新一轮，请重新填写。';renderStudent();}
  }
}
let studentOpen=true;
function renderStudent(){
  $('waiting').hidden=!!config;$('page1').hidden=!config||viewStep!==1;$('page2').hidden=!config||viewStep!==2;$('done').hidden=viewStep!==3;
  if(!config)return;for(const [id,value] of Object.entries({q1:config.q1,q1help:config.help,product:config.product,productDesc:config.description,q2:config.q2}))$(id).textContent=value;
  $('submit').disabled=!!pending||!studentOpen;$('back').disabled=!!pending;$('submit').textContent=pending?'等待老师确认…':'提交';
}
function updateJoin(){const u=new URL(location.href);u.hash=new URLSearchParams({join:host.room,k:host.secret,b:String(brokerIdx)}).toString();$('joinLink').href=u.href;$('joinLink').textContent=u.href;$('qr').replaceChildren();new QRCode($('qr'),{text:u.href,width:768,height:768,correctLevel:QRCode.CorrectLevel.M});}
const fmt=n=>n===null?'—':n.toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2});
function renderStats(){
  const gs=groupStats(host.rows,host.mode,host.mode===2?host.cuts2:host.cuts3);
  $('two').classList.toggle('active',host.mode===2);$('three').classList.toggle('active',host.mode===3);$('cut2wrap').hidden=host.mode===2;
  $('resultProduct').textContent=host.config.product;$('responseCount').textContent=`已确认 ${host.rows.length} 份回答${host.open?' · 收集中':' · 已暂停'}`;$('pause').textContent=host.open?'暂停收集':'恢复收集';
  $('groupStats').className='stats'+(host.mode===2?' two':'');$('groupStats').replaceChildren();$('bars').replaceChildren();
  const max=Math.max(0,...gs.map(g=>g.mean??0));
  for(const g of gs){const stat=document.createElement('div');stat.className='stat';const title=document.createElement('h3');title.textContent=g.name;const range=document.createElement('p');range.textContent=`编号 ${String(g.lo).padStart(2,'0')}–${String(g.hi).padStart(2,'0')} · n = ${g.n}`;const mean=document.createElement('div');mean.className='mean';mean.textContent=g.n?`¥${fmt(g.mean)}`:'—';stat.append(title,range,mean);$('groupStats').append(stat);
    const row=document.createElement('div');row.className='bar-row';const label=document.createElement('span');label.textContent=g.name;const track=document.createElement('div');track.className='bar-track';const fill=document.createElement('div');fill.className='bar-fill';fill.style.width=(max?(g.mean??0)/max*100:0)+'%';track.append(fill);const value=document.createElement('span');value.className='bar-value';value.textContent=g.n?`¥${fmt(g.mean)}`:'暂无回答';row.append(label,track,value);$('bars').append(row);}
  $('records').replaceChildren();host.rows.forEach((r,i)=>{const tr=document.createElement('tr');for(const value of [i+1,String(r.anchor).padStart(2,'0'),fmt(r.price),new Date(r.at).toLocaleTimeString('zh-CN')]){const td=document.createElement('td');td.textContent=value;tr.append(td);}$('records').append(tr);});
}
function renderCuts(){const cuts=host.mode===2?host.cuts2:host.cuts3;$('cut1').value=cuts[0];$('cut2').value=cuts[1]??67;}
function populateEditor(){for(const [id,key] of Object.entries({editQ1:'q1',editHelp:'help',editProduct:'product',editDesc:'description',editQ2:'q2'}))$(id).value=host.config[key];}
function archiveRound(){if(host.rows.length)host.history.push({run:host.run,config:{...host.config},rows:host.rows,mode:host.mode,cuts:host.mode===2?[...host.cuts2]:[...host.cuts3]});host.rows=[];host.run=random();host.open=true;}
function download(name,content,type){const url=URL.createObjectURL(new Blob([content],{type}));const a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
function csv(rounds){const quote=v=>'"'+String(v??'').replace(/"/g,'""')+'"';const lines=[['轮次','产品名称','产品描述','第一页问题','第二页问题','被试编号','最高愿付价格_人民币元','分组方式','所属组','随机响应编号','提交时间']];for(const round of rounds){for(const r of round.rows){const g=groupStats([r],round.mode,round.cuts).find(g=>g.n);lines.push([round.run,round.config.product,round.config.description,round.config.q1,round.config.q2,String(r.anchor).padStart(2,'0'),r.price,round.mode+'组',g?.name,r.id,r.at]);}}return '\uFEFF'+lines.map(line=>line.map(quote).join(',')).join('\r\n');}
function currentRound(){return {run:host.run,config:host.config,rows:host.rows,mode:host.mode,cuts:host.mode===2?host.cuts2:host.cuts3};}
function bindTeacher(){
  $('copyLink').onclick=async()=>{try{await navigator.clipboard.writeText($('joinLink').href);notify('学生链接已复制');}catch{notify('请选中上方链接手动复制');}};
  $('downloadQR').onclick=()=>{const canvas=$('qr').querySelector('canvas');if(canvas){const a=document.createElement('a');a.download='电话号码锚定实验_学生二维码.png';a.href=canvas.toDataURL('image/png');a.click();}};
  $('project').onclick=()=>{document.body.classList.toggle('projection');$('project').textContent=document.body.classList.contains('projection')?'返回教师管理':'投影结果';};
  for(const [id,mode] of [['two',2],['three',3]])$(id).onclick=()=>{host.mode=mode;persist();renderCuts();renderStats();};
  $('applyCuts').onclick=()=>{const c1=Number($('cut1').value),c2=Number($('cut2').value);if(!Number.isInteger(c1)||c1<1||c1>99||host.mode===3&&(!Number.isInteger(c2)||c2<=c1||c2>99)){$('groupError').textContent='分界须为 1–99 的整数，第二分界须大于第一分界。';return;}$('groupError').textContent='';if(host.mode===2)host.cuts2=[c1];else host.cuts3=[c1,c2];persist();renderStats();};
  $('pause').onclick=()=>{host.open=!host.open;persist();renderStats();broadcast();};
  $('newRound').onclick=()=>{if(!confirm('开始新一轮？本轮回答会存入本机历史记录，学生将重新回答两道题。'))return;archiveRound();persist();renderStats();broadcast();notify('新一轮已开始，本轮已存入历史记录');};
  $('saveConfig').onclick=()=>{const next={q1:$('editQ1').value.trim(),help:$('editHelp').value.trim(),product:$('editProduct').value.trim(),description:$('editDesc').value.trim(),q2:$('editQ2').value.trim()};if(!validConfig(next)){$('editorStatus').textContent='请填写两道问题和产品名称。';return;}if(JSON.stringify(next)===JSON.stringify(host.config)){notify('题目没有变化');return;}if(host.rows.length&&!confirm('已有回答。保存修改会把本轮存入历史，并开始新一轮，避免混合不同题目的结果。'))return;archiveRound();host.config=next;persist();renderStats();broadcast();$('editorStatus').textContent='已保存，学生将看到更新后的两道题。';};
  $('broker').value=brokerIdx;$('broker').onchange=()=>{brokerIdx=Number($('broker').value);host.broker=brokerIdx;persist();connect();updateJoin();};
  $('export').onclick=()=>download('锚定实验_本轮.csv',csv([currentRound()]),'text/csv;charset=utf-8');$('exportHistory').onclick=()=>download('锚定实验_全部轮次.csv',csv([...host.history,currentRound()]),'text/csv;charset=utf-8');
}
async function startTeacher(){
  try{const saved=JSON.parse(localStorage.getItem(STORE)||'null');host=saved&&validConfig(saved.config)&&Array.isArray(saved.rows)?saved:newHost();}catch{host=newHost();}
  room=host.room;secret=host.secret;brokerIdx=host.broker||0;if(!persist())return;
  $('teacher').hidden=false;populateEditor();renderCuts();renderStats();bindTeacher();updateJoin();await setupCrypto();connect();heartbeat=setInterval(()=>{if(connected)broadcast();},2500);
}
async function startStudent(){
  room=params.get('join');secret=params.get('k');brokerIdx=Number(params.get('b'))||0;$('student').hidden=false;
  if(!/^[a-f0-9]{32}$/.test(room)||!/^[a-f0-9]{32}$/.test(secret)||brokerIdx<0||brokerIdx>=BROKERS.length){$('waiting').textContent='链接无效，请重新扫描老师的二维码。';status('无法加入课堂');return;}
  $('page1').onsubmit=e=>{e.preventDefault();if(!validAnchor($('anchor').value)){$('error1').textContent='请填写 00–99 的两位数字。';return;}$('anchor').value=String(Number($('anchor').value)).padStart(2,'0');$('error1').textContent='';viewStep=2;renderStudent();window.scrollTo(0,0);};
  $('back').onclick=()=>{viewStep=1;renderStudent();};
  $('page2').onsubmit=e=>{e.preventDefault();const price=parsePrice($('price').value);if(price===null){$('error2').textContent='请填写不小于 0 的人民币金额，最多两位小数。';return;}if(!studentOpen){$('error2').textContent='老师已暂停收集。';return;}pending={id:participant(),run:currentRun,anchor:Number($('anchor').value),price};try{storeStudent(pending);}catch{pending=null;$('error2').textContent='浏览器无法暂存回答，请允许此网页使用本机存储后再提交。';return;}$('error2').textContent='正在发送，请保持本页打开，收到老师确认后会显示提交成功。';renderStudent();send('answer',pending);};
  await setupCrypto();connect();setInterval(()=>{if(connected){if(pending)send('answer',pending);if(Date.now()-lastHost>7000){status('正在等待老师连接，请保持本页打开…');send('hello',{id:participant()});}}},2500);
}
async function start(){
  if(!window.mqtt||!window.QRCode){status('程序组件加载失败，请刷新网页。');return;}
  if(!crypto.subtle){status('请通过 HTTPS 或本机 localhost 打开网页。');return;}
  if(isStudent){await startStudent();return;}
  if(navigator.locks){navigator.locks.request('anchor-teacher-controller',{ifAvailable:true},async lock=>{if(!lock){status('教师端已在另一个标签页打开，请使用原来的页面。');return;}await startTeacher();await new Promise(resolve=>releaseTeacher=resolve);});}else await startTeacher();
}
window.AnchorLab={groupStats,validAnchor,parsePrice};
start().catch(()=>status('打开失败，请刷新网页并允许本机存储。'));
