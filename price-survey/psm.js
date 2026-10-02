/* Van Westendorp original six-curve convention.
   PMC: tooCheap × notCheap; OPP: tooCheap × tooExpensive;
   IPP: cheap × expensive; PME: notExpensive × tooExpensive.
   Exact empirical cumulative step curves, not a sampled price grid. */
(function(root){
 'use strict';
 const keys=['tooCheap','cheap','expensive','tooExpensive'];
 const pairs={PMC:['tooCheap','notCheap'],OPP:['tooCheap','tooExpensive'],IPP:['cheap','expensive'],PME:['notExpensive','tooExpensive']};
 function parseAmount(value){const s=String(value).trim();if(!/^\d+(\.\d{1,2})?$/.test(s))return null;const n=Number(s);return Number.isFinite(n)&&n>=0&&n<=Number.MAX_SAFE_INTEGER/100?Math.round(n*100)/100:null;}
 function valid(row){return !!row&&keys.every(k=>typeof row[k]==='number'&&parseAmount(row[k])!==null)&&row.tooCheap<=row.cheap&&row.cheap<=row.expensive&&row.expensive<=row.tooExpensive;}
 function levels(counts,n){const f=k=>counts[k]/n;return {tooCheap:1-f('tooCheap'),cheap:1-f('cheap'),expensive:f('expensive'),tooExpensive:f('tooExpensive'),notCheap:f('cheap'),notExpensive:1-f('expensive')};}
 function intersect(nodes,a,b){
   const parts=[];const eps=1e-10;
   for(let i=1;i<nodes.length;i++){
     const p=nodes[i-1],q=nodes[i],d0=p[a]-p[b],d1=q[a]-q[b];
     if(Math.abs(d0)<eps&&Math.abs(d1)<eps){parts.push({low:p.price,high:q.price,y:p[a]});}
     else if(Math.abs(d0)<eps){parts.push({low:p.price,high:p.price,y:p[a]});}
     else if(Math.abs(d1)<eps){parts.push({low:q.price,high:q.price,y:q[a]});}
     else if(d0*d1<0){const t=d0/(d0-d1);const price=p.price+t*(q.price-p.price);parts.push({low:price,high:price,y:p[a]+t*(q[a]-p[a])});}
   }
   if(!parts.length)return null;
   const low=Math.min(...parts.map(p=>p.low)),high=Math.max(...parts.map(p=>p.high));
   return {low,high,price:low===high?low:null,y:parts[0].y,interval:high-low>eps};
 }
 function analyze(input){
   const rows=input.filter(valid);const n=rows.length;
   if(!n)return {n:0,excluded:input.length,nodes:[],points:{PMC:null,OPP:null,IPP:null,PME:null},maxPrice:1};
   const events=new Map();
   for(const row of rows)for(const key of keys){const price=row[key];if(!events.has(price))events.set(price,{tooCheap:0,cheap:0,expensive:0,tooExpensive:0});events.get(price)[key]++;}
   const prices=[...events.keys()].sort((a,b)=>a-b),max=prices[prices.length-1];
   const maxPrice=max>0?max*1.05:1;
   const counts={tooCheap:0,cheap:0,expensive:0,tooExpensive:0};
   const nodes=[{price:0,...levels(counts,n)}];
   for(const price of prices){nodes.push({price,...levels(counts,n)});for(const key of keys)counts[key]+=events.get(price)[key];nodes.push({price,...levels(counts,n)});}
   nodes.push({price:maxPrice,...levels(counts,n)});
   const points=Object.fromEntries(Object.entries(pairs).map(([name,[a,b]])=>[name,intersect(nodes,a,b)]));
   return {n,excluded:input.length-n,nodes,points,maxPrice};
 }
 const api={keys,pairs,parseAmount,valid,analyze,intersect};
 if(typeof module==='object'&&module.exports)module.exports=api;
 else root.PSM=api;
})(typeof globalThis!=='undefined'?globalThis:this);
