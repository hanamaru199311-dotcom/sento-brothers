const COLORS=['#ee6262','#5699f6','#f1c847','#5bcf84'];
const TEAM=['赤','青','黄','緑'];
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const rnd=(a,b)=>a+Math.random()*(b-a);
export default {async fetch(request,env){const url=new URL(request.url);if(url.pathname.startsWith('/api/')){const id=env.GAME.idFromName('one-permanent-party-room');return env.GAME.get(id).fetch(request)}return env.ASSETS.fetch(request)}};
export class GameRoom {
 constructor(state,env){this.state=state;this.env=env;this.clients=new Map();this.players=new Map();this.ghosts=[];this.phase='setup';this.target=12;this.configured=false;this.startedAt=0;this.lastTick=Date.now();this.timer=null;this.nextId=1;this.countdownAt=0;this.loser=null;this.endedAt=0;this.state.blockConcurrencyWhile(async()=>{let cfg=await this.state.storage.get('config');if(cfg){this.target=cfg.target||12;this.configured=!!cfg.configured;}})}
 async fetch(req){const u=new URL(req.url);if(u.pathname==='/api/state')return Response.json(this.snapshot());if(u.pathname!=='/api/ws'||req.headers.get('Upgrade')?.toLowerCase()!=='websocket')return new Response('Not found',{status:404});if(this.phase==='ended' && this.clients.size===0){await this.resetRoom()}const pair=new WebSocketPair();const client=pair[0],server=pair[1];server.accept();const cid=crypto.randomUUID();this.clients.set(cid,{ws:server,pid:null});server.addEventListener('message',e=>{try{this.handle(cid,JSON.parse(e.data))}catch(err){this.send(cid,{type:'error',message:'操作を処理できませんでした'})}});server.addEventListener('close',()=>this.disconnect(cid));server.addEventListener('error',()=>this.disconnect(cid));this.send(cid,{type:'hello',...this.snapshot()});return new Response(null,{status:101,webSocket:client})}
 send(cid,data){try{this.clients.get(cid)?.ws.send(JSON.stringify(data))}catch{}}
 broadcast(data){const s=JSON.stringify(data);for(const c of this.clients.values())try{c.ws.send(s)}catch{}}
 snapshot(){return{phase:this.phase,target:this.target,configured:this.configured,joined:this.players.size,players:[...this.players.values()].map(p=>({id:p.id,name:p.name,team:p.team,hp:p.hp,alive:p.alive,online:p.online,x:p.x,y:p.y,immune:p.immune})),ghosts:this.ghosts.map(g=>({x:g.x,y:g.y,idx:g.idx})),countdownAt:this.countdownAt,startedAt:this.startedAt,loser:this.loser,now:Date.now()}}
 async handle(cid,m){let c=this.clients.get(cid);if(!c)return;
 if(m.type==='configure'&&this.phase==='setup'&&this.players.size===0){const n=Number(m.value);if(!Number.isInteger(n)||n<1||n>19){this.send(cid,{type:'error',message:'参加人数は1〜19人です'});return}this.target=n;this.configured=true;await this.persist();this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='leave'&&c.pid&&this.phase==='setup'){this.players.delete(c.pid);c.pid=null;if(this.players.size===0){this.configured=false;this.target=12;await this.persist();}this.send(cid,{type:'removed'});this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='join'){const token=String(m.token||'');let p=[...this.players.values()].find(x=>x.token===token&&token);if(!p){if(this.phase!=='setup'||!this.configured||this.players.size>=this.target){this.send(cid,{type:'error',message:'現在は参加できません'});return}const name=String(m.name||'').trim().slice(0,8);if(!name){this.send(cid,{type:'error',message:'名前を入力してください'});return}if([...this.players.values()].some(x=>x.name===name)){this.send(cid,{type:'error',message:'その名前は登録済みです'});return}p={id:this.nextId++,token:crypto.randomUUID(),name,team:clamp(Number(m.team)||0,0,3),x:rnd(65,935),y:rnd(70,630),vx:0,vy:0,hp:3,alive:true,immune:2,online:true,dx:0,dy:0};this.players.set(p.id,p)}if(c.pid&&c.pid!==p.id)this.disconnect(cid);for(let [otherId,other] of this.clients)if(otherId!==cid&&other.pid===p.id){other.pid=null;try{other.ws.close(1000,'reconnected')}catch{}}c.pid=p.id;p.online=true;this.send(cid,{type:'joined',id:p.id,token:p.token});this.broadcast({type:'state',...this.snapshot()});return}
 if(m.type==='start'&&c.pid&&this.phase==='setup'&&this.players.size>0&&this.onlineCount()===this.players.size){this.phase='countdown';this.countdownAt=Date.now()+10000;this.broadcast({type:'state',...this.snapshot()});this.startTimer();return}
 if(m.type==='reset'&&this.phase==='ended'){await this.resetRoom();return}
 if(m.type==='move'&&c.pid){let p=this.players.get(c.pid);if(p&&p.alive&&this.phase==='playing'){let x=Number(m.x)||0,y=Number(m.y)||0,len=Math.hypot(x,y);p.dx=len>0?x/Math.max(1,len):0;p.dy=len>0?y/Math.max(1,len):0}return}

 }
 async persist(){await this.state.storage.put('config',{target:this.target,configured:this.configured})}
 async resetRoom(){this.stopTimer();this.broadcast({type:'resetDone'});this.players.clear();this.ghosts=[];this.phase='setup';this.configured=false;this.target=12;this.countdownAt=0;this.loser=null;this.endedAt=0;for(const c of this.clients.values())c.pid=null;await this.persist();this.broadcast({type:'state',...this.snapshot()})}
 disconnect(cid){let c=this.clients.get(cid);if(!c)return;this.clients.delete(cid);if(c.pid){let p=this.players.get(c.pid);if(p){p.online=false;p.dx=p.dy=0}}this.broadcast({type:'state',...this.snapshot()});if(this.phase==='countdown'&&this.onlineCount()<this.players.size){this.phase='setup';this.countdownAt=0;this.stopTimer();this.broadcast({type:'state',...this.snapshot()})}}
 onlineCount(){return [...this.players.values()].filter(p=>p.online).length}
 startTimer(){if(!this.timer){this.lastTick=Date.now();this.timer=setInterval(()=>this.tick(),50)}}
 stopTimer(){if(this.timer)clearInterval(this.timer);this.timer=null}
 tick(){const now=Date.now(),dt=clamp((now-this.lastTick)/1000,0,.1);this.lastTick=now;if(this.phase==='countdown'){if(this.onlineCount()<this.players.size){this.phase='setup';this.countdownAt=0;this.stopTimer();this.broadcast({type:'state',...this.snapshot()});return}if(now>=this.countdownAt){this.phase='playing';this.startedAt=now;this.ghosts=[];for(let p of this.players.values()){p.hp=3;p.alive=true;p.immune=2;p.vx=p.vy=0;p.x=rnd(80,920);p.y=rnd(80,620)}this.broadcast({type:'state',...this.snapshot()})}else return}
 if(this.phase!=='playing')return;let elapsed=(now-this.startedAt)/1000;while(this.ghosts.length<Math.min(11,1+Math.floor(elapsed/15))){let i=this.ghosts.length;const starts=[[55,55],[945,645],[945,55],[55,645],[500,55],[500,645],[55,350],[945,350],[250,55],[750,645],[500,350]];this.ghosts.push({x:starts[i][0],y:starts[i][1],idx:i,vx:0,vy:0,turn:Math.random()*6.28,wayX:rnd(90,910),wayY:rnd(90,610),nextTurn:now+rnd(250,1200)});this.broadcast({type:'ghost',idx:i})}
 for(let p of this.players.values()){if(!p.alive)continue;p.immune=Math.max(0,p.immune-dt);let k=Math.min(1,dt*9);p.vx+=(p.dx*250-p.vx)*k;p.vy+=(p.dy*250-p.vy)*k;p.x=clamp(p.x+p.vx*dt,19,981);p.y=clamp(p.y+p.vy*dt,19,681)}
 for(let g of this.ghosts){let living=[...this.players.values()].filter(p=>p.alive);if(!living.length)break;
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
 let counts=[0,0,0,0];for(let p of this.players.values())if(p.alive)counts[p.team]++;let teamsPresent=new Set([...this.players.values()].map(p=>p.team));let dead=[...teamsPresent].filter(t=>counts[t]===0);if(dead.length){this.phase='ended';this.endedAt=Date.now();this.loser=dead[0];this.broadcast({type:'state',...this.snapshot()});this.stopTimer();return}this.broadcast({type:'frame',players:[...this.players.values()].map(p=>[p.id,Math.round(p.x),Math.round(p.y),p.hp,p.alive,p.immune>0]),ghosts:this.ghosts.map(g=>[Math.round(g.x),Math.round(g.y),g.idx]),elapsed:Math.floor(elapsed)})}
}
