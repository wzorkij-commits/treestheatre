// Ядро таблицы рекордов: хранение в Upstash Redis через REST (без npm-зависимостей).
const GAMES={suitcase:2000,run:100000,producer:3000,pong:5000,snake:400,fight:100000}; // максимум, выше — явный чит
const clean=v=>String(v||'').trim().replace(/^[A-Z_]+\s*=\s*/,'').replace(/^["']|["']$/g,'').trim(); // терпим вставку целой строки KEY="value"
function env(){const e=process.env;return{url:clean(e.KV_REST_API_URL||e.UPSTASH_REDIS_REST_URL),token:clean(e.KV_REST_API_TOKEN||e.UPSTASH_REDIS_REST_TOKEN),admin:clean(e.HS_ADMIN_TOKEN)}}
async function redis(cmds){const {url,token}=env();if(!url||!token)throw new Error('no-db');
  const r=await fetch(url.replace(/\/$/,'')+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify(cmds)});
  if(!r.ok)throw new Error('db '+r.status);return (await r.json()).map(x=>x.result)}
const parse=arr=>{const o=[];for(let i=0;i<(arr||[]).length;i+=2){const m=String(arr[i]);const name=m.slice(0,m.lastIndexOf('|'));o.push({name,score:Math.round(+arr[i+1])})}return o};
async function handle({method,query,body,ip}){
  try{
    if(method==='GET'){const n=Math.min(50,Math.max(1,+query.n||10));
      if(query.game==='all'){const ks=Object.keys(GAMES);const res=await redis(ks.map(g=>['ZREVRANGE','hs:'+g,0,n-1,'WITHSCORES']));const games={};ks.forEach((g,i)=>games[g]=parse(res[i]));return[200,{games}]}
      if(!GAMES[query.game])return[400,{error:'game'}];const [res]=await redis([['ZREVRANGE','hs:'+query.game,0,n-1,'WITHSCORES']]);return[200,{scores:parse(res)}]}
    if(method==='POST'){const {game,name,score}=body||{};
      if(!GAMES[game])return[400,{error:'game'}];
      const s=Math.floor(+score);if(!Number.isFinite(s)||s<=0||s>GAMES[game])return[400,{error:'score'}];
      const nm=String(name||'').replace(/[<>|\u0000-\u001f]/g,'').replace(/\s+/g,' ').trim().slice(0,16);if(!nm)return[400,{error:'name'}];
      const [ok]=await redis([['SET','rl:'+ip,'1','NX','EX','8']]);if(ok!=='OK')return[429,{error:'slow down'}];
      await redis([['ZADD','hs:'+game,s,nm+'|'+Date.now().toString(36)+Math.random().toString(36).slice(2,6)],['ZREMRANGEBYRANK','hs:'+game,0,-201]]);return[200,{ok:true}]}
    if(method==='DELETE'){ // модерация: DELETE /api/scores?game=snake&name=Вася  с заголовком x-admin: HS_ADMIN_TOKEN
      return[405,{error:'use admin'}]}
    return[405,{error:'method'}]
  }catch(e){return[503,{error:String(e.message||e)}]}}
async function admin({query,headers}){const {admin}=env();if(!admin||headers['x-admin']!==admin)return[403,{error:'forbidden'}];
  if(!GAMES[query.game])return[400,{error:'game'}];const [all]=await redis([['ZRANGE','hs:'+query.game,0,-1]]);const del=(all||[]).filter(m=>m.slice(0,m.lastIndexOf('|'))===query.name);
  if(del.length)await redis([['ZREM','hs:'+query.game,...del]]);return[200,{removed:del.length}]}

export default async(req,context)=>{const u=new URL(req.url);const query=Object.fromEntries(u.searchParams);let body={};if(req.method==='POST'){try{body=await req.json()}catch(e){}}
  const headers=Object.fromEntries(req.headers);const ip=context.ip||headers['x-nf-client-connection-ip']||'0';
  const [code,data]=req.method==='DELETE'?await admin({query,headers}):await handle({method:req.method,query,body,ip});
  return new Response(JSON.stringify(data),{status:code,headers:{'content-type':'application/json','cache-control':'no-store'}})};
export const config={path:'/api/scores'};
