
const COLORS=['#ee6262','#5699f6','#f1c847','#5bcf84'];
const TEAM=['赤','青','黄','緑'];
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const rnd=(a,b)=>a+Math.random()*(b-a);
export default {async fetch(request,env){const url=new URL(request.url);if(url.pathname.startsWith('/api/')){const id=env.GAME.idFromName('one-permanent-party-room');return env.GAME.get(id).fetch(request)}return env.ASSETS.fetch(request)}};
export class GameRoom {
 constructor(state,env){this.state=state;this.env=env;this.clients=new Map();this.players=new Map();this.ghosts=[];this.phase='setup';this.target=20;this.configured=true;this.startedAt=0;this.lastTick=Date.now();this.timer=null;this.nextId=1;this.offlineGraceMs=20000;this.hostId=null;this.hostOfflineAt=0;this.lastSaved=0;this.countdownAt=0;this.loser=null;this.endedAt=0;this.state.blockConcurrencyWhile(async()=>{let cfg=await this.state.storage.get('config');if(cfg){this.target=cfg.target||20;this.configured=true;}const saved=await this.state.storage.get('match');if(saved){this.phase=saved.phase||'setup';this.players=new Map((saved.players||[]).map(p=>[p.id,{...p,online:false,dx:0,dy:0,offlineAt:Date.now()}]));this.ghosts=saved.ghosts||[];this.nextId=saved.nextId||1;this.hostId=saved.hostId||null;this.hostOfflineAt=Date.now();this.countdownAt=saved.countdownAt||0;this.startedAt=saved.startedAt||0;this.loser=saved.loser??null;this.configured=true;this.target=saved.target||this.target;if(this.phase==='playing'||this.phase==='countdown')this.startTimer();}})}
 async fetch(req){const u=new URL(req.url);if(u.pathname==='/api/state')return Response.json(this.snapshot());if(u.pathname!=='/api/ws'||req.headers.get('Upgrade')?.toLowerCase()!=='websocket')return new Response('Not found',{status:404});const pair=new WebSocketPair();const client=pair[0],server=pair[1];server.accept();const cid=crypto.randomUUID();this.clients.set(cid,{ws:server,pid:null});server.addEventListener('message',e=>{try{const msg=JSON.parse(e.data);this.messageQueue=(this.messageQueue||Promise.resolve()).catch(()=>{}).then(()=>this.handle(cid,msg)).catch(err=>{console.error(err);this.send(cid,{type:'error',message:'操作を処理できませんでした'})})}catch(err){this.send(cid,{type:'error',message:'操作を処理できませんでした'})}});server.addEventListener('close',()=>this.disconnect(cid));server.addEventListener('error',()=>this.disconnect(cid));this.send(cid,{type:'hello',...this.snapshot()});return new Response(null,{status:101,webSocket:client})}
 send(cid,data){try{this.clients.get(cid)?.ws.send(JSON.stringify(data))}catch{}}
 broadcast(data){const s=JSON.stringify(data);for(const c of this.clients.values())try{c.ws.send(s)}catch{}}
 snapshot(){this.assignHost();return{phase:this.phase,target:this.target,configured:this.configured,joined:this.onlineCount(),hostId:this.hostId,hostName:this.players.get(this.hostId)?.name||null,players:[...this.players.values()].map(p=>({id:p.id,name:p.name,team:p.team,hp:p.hp,alive:p.alive,online:p.online,x:p.x,y:p.y,immune:p.immune})),ghosts:this.ghosts.map(g=>({x:g.x,y:g.y,idx:g.idx})),countdownAt:this.countdownAt,startedAt:this.startedAt,loser:this.loser,now:Date.now()}}
 async handle(cid,m){let c=this.clients.get(cid);if(!c)return;this.cleanupOffline();
 if(m.type==='setTarget'&&c.pid===this.hostId&&this.phase==='setup'){const n=Number(m.value);if(!Number.isInteger(n)||n<1||n>20||n<this.onlineCount()){this.send(cid,{type:'error',message:'現在の参加人数以上、1〜20人で設定してください'});return}this.target=n;this.configured=true;await this.persist();await this.saveMatch();this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='emergencyReset'&&this.phase==='setup'&&c.pid===this.hostId){await this.resetRoom();return}
 if(m.type==='leave'&&c.pid&&this.phase==='setup'){if(this.hostId===c.pid)this.hostId=null;this.players.delete(c.pid);c.pid=null;if(this.players.size===0){this.configured=true;this.target=20;await this.persist();}this.assignHost();await this.saveMatch();this.send(cid,{type:'removed'});this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='join'){const token=String(m.token||'');let p=[...this.players.values()].find(x=>x.token===token&&token);if(!p&&c.pid){p=this.players.get(c.pid);}if(!p){if(this.phase==='ended'||this.onlineCount()>=this.target){this.send(cid,{type:'error',message:'現在は参加できません'});return}const name=String(m.name||'').trim().slice(0,8);if(!name){this.send(cid,{type:'error',message:'名前を入力してください'});return}p={id:this.nextId++,token:crypto.randomUUID(),name,team:clamp(Number(m.team)||0,0,3),x:rnd(65,935),y:rnd(70,630),vx:0,vy:0,hp:3,alive:true,immune:2,online:true,dx:0,dy:0};this.players.set(p.id,p);}if(c.pid&&c.pid!==p.id){const previous=this.players.get(c.pid);if(previous){previous.online=false;previous.offlineAt=Date.now()}c.pid=null;}for(let [otherId,other] of this.clients)if(otherId!==cid&&other.pid===p.id){other.pid=null;try{other.ws.close(1000,'reconnected')}catch{}}c.pid=p.id;p.online=true;p.offlineAt=0;if(this.hostId===p.id)this.hostOfflineAt=0;this.assignHost();this.send(cid,{type:'joined',id:p.id,token:p.token});await this.saveMatch();this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='endGame'&&c.pid===this.hostId&&(this.phase==='playing'||this.phase==='countdown')){this.stopTimer();this.phase='setup';this.ghosts=[];this.loser=null;this.startedAt=0;this.countdownAt=0;for(const p of this.players.values()){p.hp=3;p.alive=true;p.immune=2;p.dx=p.dy=p.vx=p.vy=0;}await this.saveMatch();this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='start'&&!(c.pid===this.hostId&&this.phase==='setup'&&this.onlineCount()>=1)){this.send(cid,{type:'error',message:'開始できません。オーナー・参加状態・通信を確認してください'});return}
 if(m.type==='start'&&c.pid===this.hostId&&this.phase==='setup'&&this.onlineCount()>=1){this.phase='countdown';this.countdownAt=Date.now()+10000;await this.saveMatch();this.broadcast({type:'state',...this.snapshot()});this.startTimer();return}
 if(m.type==='reset'&&c.pid&&this.players.has(c.pid)&&this.phase==='ended'){await this.newRound();return}if(m.type==='reset'&&this.phase!=='ended'){this.send(cid,{type:'state',...this.snapshot()});return}
 if(m.type==='setTarget'||m.type==='endGame'||m.type==='emergencyReset'){this.send(cid,{type:'error',message:'オーナー権限または操作できる画面を確認してください'});return}
 if(m.type==='move'&&c.pid){let p=this.players.get(c.pid);if(p&&p.alive&&this.phase==='playing'){let x=Number(m.x)||0,y=Number(m.y)||0,len=Math.hypot(x,y);p.dx=len>0?x/Math.max(1,len):0;p.dy=len>0?y/Math.max(1,len):0}return}

 }
 async persist(){await this.state.storage.put('config',{target:this.target,configured:this.configured})}
 async saveMatch(){this.lastSaved=Date.now();await this.state.storage.put('match',{phase:this.phase,players:[...this.players.values()],ghosts:this.ghosts,nextId:this.nextId,hostId:this.hostId,countdownAt:this.countdownAt,startedAt:this.startedAt,loser:this.loser,configured:this.configured,target:this.target})}
 assignHost(){if(this.hostId){const old=this.players.get(this.hostId);if(old?.online){this.hostOfflineAt=0;return}if(old&&Date.now()-(old.offlineAt||this.hostOfflineAt||Date.now())<this.offlineGraceMs)return}const online=[...this.players.values()].find(p=>p.online);this.hostId=online?online.id:null;this.hostOfflineAt=0}
 async newRound(){this.stopTimer();this.phase='setup';this.ghosts=[];this.loser=null;this.countdownAt=0;this.startedAt=0;for(const p of this.players.values()){p.hp=3;p.alive=true;p.immune=2;p.dx=p.dy=p.vx=p.vy=0}this.assignHost();await this.saveMatch();this.broadcast({type:'state',...this.snapshot()})}
 async resetRoom(){this.stopTimer();this.broadcast({type:'resetDone'});this.players.clear();this.ghosts=[];this.phase='setup';this.configured=true;this.target=20;this.countdownAt=0;this.loser=null;this.endedAt=0;this.hostId=null;for(const c of this.clients.values())c.pid=null;await this.persist();await this.saveMatch();this.broadcast({type:'state',...this.snapshot()})}
 disconnect(cid){let c=this.clients.get(cid);if(!c)return;this.clients.delete(cid);if(c.pid){let p=this.players.get(c.pid);if(p&&!([...this.clients.values()].some(other=>other.pid===p.id))){p.online=false;p.offlineAt=Date.now();p.dx=p.dy=0}}this.assignHost();this.saveMatch().catch(()=>{});this.broadcast({type:'state',...this.snapshot()})}
 onlineCount(){return [...this.players.values()].filter(p=>p.online).length}
 dropOffline(){for(const [id,p] of this.players){if(!p.online)this.players.delete(id)}}
 cleanupOffline(){if(this.phase!=='setup')return;let changed=false;for(const [id,p] of this.players){if(!p.online&&Date.now()-(p.offlineAt||0)>this.offlineGraceMs){this.players.delete(id);changed=true}}if(changed)this.broadcast({type:'state',...this.snapshot()})}
 startTimer(){if(!this.timer){this.lastTick=Date.now();this.timer=setInterval(()=>{try{this.tick()}catch(e){console.error('tick',e)}},50)}}
 stopTimer(){if(this.timer)clearInterval(this.timer);this.timer=null}
 tick(){const now=Date.now(),dt=clamp((now-this.lastTick)/1000,0,.1);this.lastTick=now;if(this.phase==='playing'&&this.onlineCount()>0){for(const [id,p] of this.players){if(!p.online&&now-(p.offlineAt||now)>this.offlineGraceMs)this.players.delete(id)}}if(this.phase==='countdown'){if(this.onlineCount()===0){return}if(now>=this.countdownAt){this.phase='playing';this.startedAt=now;this.ghosts=[];for(let p of this.players.values()){p.hp=3;p.alive=true;p.immune=2;p.vx=p.vy=0;p.x=rnd(80,920);p.y=rnd(80,620)}this.saveMatch().catch(()=>{});this.broadcast({type:'state',...this.snapshot()})}else return}
 if(this.phase!=='playing')return;if(this.onlineCount()===0)return;let elapsed=(now-this.startedAt)/1000;while(this.ghosts.length<Math.min(11,1+Math.floor(elapsed/15))){let i=this.ghosts.length;const starts=[[55,55],[945,645],[945,55],[55,645],[500,55],[500,645],[55,350],[945,350],[250,55],[750,645],[500,350]];this.ghosts.push({x:starts[i][0],y:starts[i][1],idx:i,vx:0,vy:0,turn:Math.random()*6.28,wayX:rnd(90,910),wayY:rnd(90,610),nextTurn:now+rnd(250,1200)});this.broadcast({type:'ghost',idx:i})}
 for(let p of this.players.values()){if(!p.alive||!p.online)continue;p.immune=Math.max(0,p.immune-dt);let k=Math.min(1,dt*9);p.vx+=(p.dx*250-p.vx)*k;p.vy+=(p.dy*250-p.vy)*k;p.x=clamp(p.x+p.vx*dt,19,981);p.y=clamp(p.y+p.vy*dt,19,681)}
 for(let g of this.ghosts){let living=[...this.players.values()].filter(p=>p.alive&&p.online);if(!living.length)break;
 // Each ghost uses a different target preference, rather than every ghost chasing the nearest player.
 let ordered=[...living].sort((a,b)=>a.id-b.id);let p=ordered[g.idx%ordered.length];
 if(g.idx%3===1)p=ordered.reduce((a,b)=>Math.hypot(b.x-g.x,b.y-g.y)>Math.hypot(a.x-g.x,a.y-g.y)?b:a);
 if(g.idx%3===2)p=ordered.reduce((a,b)=>Math.hypot(b.x-g.x,b.y-g.y)<Math.hypot(a.x-g.x,a.y-g.y)?b:a);
 // Ghost 11 (last uploaded face) wanders unpredictably instead of chasing players.
 if(g.idx===10){
   if(now>=g.nextTurn||Math.hypot(g.wayX-g.x,g.wayY-g.y)<75){
     const a=rnd(0,Math.PI*2),distance=rnd(160,590);
     g.wayX=clamp(g.x+Math.cos(a)*distance,65,935);
     g.wayY=clamp(g.y+Math.sin(a)*distance,65,635);
     g.nextTurn=now+rnd(350,1800);
   }
   let dx=g.wayX-g.x,dy=g.wayY-g.y,d=Math.hypot(dx,dy)||1;
   let speed=135+35*Math.sin(elapsed*2.7),k=Math.min(1,dt*7);
   let ux=dx/d,uy=dy/d;
   for(let other of this.ghosts){if(other===g)continue;let sx=g.x-other.x,sy=g.y-other.y,dist=Math.hypot(sx,sy)||1;if(dist<130){ux+=sx/dist*(130-dist)/130*2;uy+=sy/dist*(130-dist)/130*2}}
   let norm=Math.hypot(ux,uy)||1;
   g.vx+=(ux/norm*speed-g.vx)*k;g.vy+=(uy/norm*speed-g.vy)*k;
   g.x=clamp(g.x+g.vx*dt,52,948);g.y=clamp(g.y+g.vy*dt,52,648);
 }else{
 let dx=p.x-g.x,dy=p.y-g.y,d=Math.hypot(dx,dy)||1;
 // Distinct curved paths and separation steering prevent overlapping ghost sprites.
 let lateral=Math.sin(elapsed*.85+g.idx*1.8)*(.24+(g.idx%3)*.12);
 let ux=dx/d-dy/d*lateral,uy=dy/d+dx/d*lateral;
 for(let other of this.ghosts){if(other===g)continue;let sx=g.x-other.x,sy=g.y-other.y,dist=Math.hypot(sx,sy)||1;if(dist<145){let force=(145-dist)/145*2.4;ux+=sx/dist*force;uy+=sy/dist*force}}
 let norm=Math.hypot(ux,uy)||1,speed=98+g.idx*9+Math.min(elapsed,180)*.25,k=Math.min(1,dt*3);
 g.vx+=(ux/norm*speed-g.vx)*k;g.vy+=(uy/norm*speed-g.vy)*k;
 g.x=clamp(g.x+g.vx*dt,52,948);g.y=clamp(g.y+g.vy*dt,52,648);
 }
 for(let q of living)if(q.immune<=0&&Math.hypot(q.x-g.x,q.y-g.y)<65){q.hp--;q.immune=2.5;q.dx=q.dy=0;q.vx=q.vy=0;if(q.hp<=0){q.alive=false;this.broadcast({type:'hit',id:q.id,hp:0})}else{q.x=rnd(100,900);q.y=rnd(100,600);this.broadcast({type:'hit',id:q.id,hp:q.hp})}}
 }
 let counts=[0,0,0,0];for(let p of this.players.values())if(p.alive&&p.online)counts[p.team]++;let teamsPresent=new Set([...this.players.values()].filter(p=>p.online).map(p=>p.team));let dead=[...teamsPresent].filter(t=>counts[t]===0);if(dead.length||([ ...this.players.values()].filter(p=>p.online&&p.alive).length===0&&teamsPresent.size>0)){this.phase='ended';this.endedAt=Date.now();this.loser=dead[0]??[...teamsPresent][0];this.broadcast({type:'state',...this.snapshot()});this.stopTimer();this.saveMatch().catch(()=>{});return}if(now-this.lastSaved>2000)this.saveMatch().catch(()=>{});this.broadcast({type:'frame',players:[...this.players.values()].map(p=>[p.id,Math.round(p.x),Math.round(p.y),p.hp,p.alive,p.immune>0]),ghosts:this.ghosts.map(g=>[Math.round(g.x),Math.round(g.y),g.idx]),elapsed:Math.floor(elapsed)})}
}
