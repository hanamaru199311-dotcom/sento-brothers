
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const rnd=(a,b)=>a+Math.random()*(b-a);

export default {
 async fetch(request,env){
  const url=new URL(request.url);
  if(url.pathname.startsWith('/api/')){
   const id=env.GAME.idFromName('one-permanent-party-room');
   return env.GAME.get(id).fetch(request);
  }
  return env.ASSETS.fetch(request);
 }
};

export class GameRoom {
 constructor(state,env){
  this.state=state;
  this.env=env;
  this.clients=new Map();
  this.players=new Map();
  this.ghosts=[];
  this.phase='setup';
  this.target=20;
  this.configured=false;
  this.hostId=null;
  this.nextId=1;
  this.countdownAt=0;
  this.startedAt=0;
  this.endedAt=0;
  this.loser=null;
  this.timer=null;
  this.lastTick=Date.now();
  this.lastSaved=0;
  this.offlineGraceMs=20000;

  this.state.blockConcurrencyWhile(async()=>{
   const cfg=await this.state.storage.get('config');
   const saved=await this.state.storage.get('match');

   if(cfg){
    this.target=cfg.target||20;
    this.configured=!!cfg.configured;
   }

   if(saved){
    this.phase=saved.phase||'setup';
    this.target=saved.target||this.target;
    this.configured=saved.configured??this.configured;
    this.hostId=saved.hostId??null;
    this.nextId=saved.nextId||1;
    this.countdownAt=saved.countdownAt||0;
    this.startedAt=saved.startedAt||0;
    this.endedAt=saved.endedAt||0;
    this.loser=saved.loser??null;
    this.ghosts=saved.ghosts||[];

    this.players=new Map(
     (saved.players||[]).map(p=>[
      p.id,{
       ...p,
       online:false,
       dx:0,
       dy:0,
       offlineAt:Date.now()
      }
     ])
    );
   }

   if(this.phase==='playing'||this.phase==='countdown'){
    this.startTimer();
   }
  });
 }

 async fetch(req){
  const url=new URL(req.url);

  if(url.pathname==='/api/state'){
   return Response.json(this.snapshot());
  }

  if(
   url.pathname!=='/api/ws'||
   req.headers.get('Upgrade')?.toLowerCase()!=='websocket'
  ){
   return new Response('Not found',{status:404});
  }

  const pair=new WebSocketPair();
  const client=pair[0];
  const server=pair[1];
  server.accept();

  const cid=crypto.randomUUID();
  this.clients.set(cid,{ws:server,pid:null});

  server.addEventListener('message',event=>{
   try{
    const message=JSON.parse(event.data);
    this.handle(cid,message).catch(()=>{
     this.send(cid,{
      type:'error',
      message:'操作を処理できませんでした'
     });
    });
   }catch{
    this.send(cid,{
     type:'error',
     message:'通信データを処理できませんでした'
    });
   }
  });

  server.addEventListener('close',()=>this.disconnect(cid));
  server.addEventListener('error',()=>this.disconnect(cid));

  this.send(cid,{type:'hello',...this.snapshot()});

  return new Response(null,{
   status:101,
   webSocket:client
  });
 }

 send(cid,data){
  try{
   this.clients.get(cid)?.ws.send(JSON.stringify(data));
  }catch{}
 }

 broadcast(data){
  const message=JSON.stringify(data);
  for(const client of this.clients.values()){
   try{client.ws.send(message)}catch{}
  }
 }

 onlineCount(){
  return [...this.players.values()].filter(p=>p.online).length;
 }

 assignHost(){
  const current=this.players.get(this.hostId);
  if(current?.online)return;
  const next=[...this.players.values()].find(p=>p.online);
  this.hostId=next?next.id:null;
 }

 snapshot(){
  this.assignHost();
  const host=this.players.get(this.hostId);

  return{
   phase:this.phase,
   target:this.target,
   configured:this.configured,
   joined:this.onlineCount(),
   hostId:this.hostId,
   hostName:host?.name||null,
   players:[...this.players.values()].map(p=>({
    id:p.id,name:p.name,team:p.team,
    hp:p.hp,alive:p.alive,online:p.online,
    x:p.x,y:p.y,immune:p.immune
   })),
   ghosts:this.ghosts.map(g=>({
    x:g.x,y:g.y,idx:g.idx
   })),
   countdownAt:this.countdownAt,
   startedAt:this.startedAt,
   loser:this.loser,
   now:Date.now()
  };
 }

 async handle(cid,m){
  const c=this.clients.get(cid);
  if(!c)return;

  this.cleanupOffline();

  // 待機室の緊急初期化
  if(
   m.type==='emergencyReset'&&
   this.phase!=='playing'&&
   this.phase!=='countdown'
  ){
   await this.resetRoom();
   return;
  }

  // 初回人数設定
  if(
   m.type==='configure'&&
   this.phase==='setup'&&
   this.onlineCount()===0
  ){
   const n=Number(m.value);
   if(!Number.isInteger(n)||n<2||n>20){
    this.send(cid,{
     type:'error',
     message:'参加人数は2〜20人です'
    });
    return;
   }

   this.target=n;
   this.configured=true;
   await this.persist();
   await this.saveMatch();
   this.broadcast({type:'state',...this.snapshot()});
   return;
  }

  // オーナーによる人数変更
  if(
   m.type==='setTarget'&&
   c.pid===this.hostId&&
   this.phase==='setup'
  ){
   const n=Number(m.value);

   if(
    !Number.isInteger(n)||
    n<2||n>20||
    n<this.onlineCount()
   ){
    this.send(cid,{
     type:'error',
     message:'現在の参加人数以上、20人以下にしてください'
    });
    return;
   }

   this.target=n;
   this.configured=true;
   await this.persist();
   await this.saveMatch();
   this.broadcast({type:'state',...this.snapshot()});
   return;
  }

  // 参加・再接続・途中参加
  if(m.type==='join'){
   const token=String(m.token||'');
   let p=[...this.players.values()].find(
    x=>x.token===token&&token
   );

   if(!p&&c.pid){
    p=this.players.get(c.pid);
   }

   if(!p){
    if(this.phase==='ended'){
     this.send(cid,{
      type:'error',
      message:'次のゲーム開始までお待ちください'
     });
     return;
    }

    if(this.onlineCount()>=this.target){
     this.send(cid,{
      type:'error',
      message:'満員です'
     });
     return;
    }

    const name=String(m.name||'').trim().slice(0,8);
    if(!name){
     this.send(cid,{
      type:'error',
      message:'名前を入力してください'
     });
     return;
    }

    p={
     id:this.nextId++,
     token:crypto.randomUUID(),
     name,
     team:clamp(Number(m.team)||0,0,3),
     x:rnd(80,920),
     y:rnd(80,620),
     vx:0,vy:0,dx:0,dy:0,
     hp:3,alive:true,immune:3,
     online:true,offlineAt:0
    };
    this.players.set(p.id,p);
   }

   if(c.pid&&c.pid!==p.id){
    const old=this.players.get(c.pid);
    if(old){
     old.online=false;
     old.offlineAt=Date.now();
     old.dx=old.dy=0;
    }
    c.pid=null;
   }

   for(const [otherId,other] of this.clients){
    if(otherId!==cid&&other.pid===p.id){
     other.pid=null;
     try{other.ws.close(1000,'reconnected')}catch{}
    }
   }

   c.pid=p.id;
   p.online=true;
   p.offlineAt=0;
   this.assignHost();

   this.send(cid,{
    type:'joined',
    id:p.id,
    token:p.token
   });

   await this.saveMatch();
   this.broadcast({type:'state',...this.snapshot()});
   return;
  }

  // 待機室から退出
  if(
   m.type==='leave'&&
   c.pid&&this.phase==='setup'
  ){
   const leavingId=c.pid;
   this.players.delete(leavingId);
   c.pid=null;

   if(this.hostId===leavingId)this.hostId=null;
   this.assignHost();

   this.send(cid,{type:'removed'});
   await this.saveMatch();
   this.broadcast({type:'state',...this.snapshot()});
   return;
  }

  // オーナーによる試合終了
  if(
   m.type==='endGame'&&
   c.pid===this.hostId&&
   (this.phase==='playing'||this.phase==='countdown')
  ){
   this.stopTimer();
   this.phase='setup';
   this.ghosts=[];
   this.loser=null;
   this.startedAt=0;
   this.countdownAt=0;
   this.endedAt=0;

   for(const p of this.players.values()){
    p.hp=3;
    p.alive=true;
    p.immune=3;
    p.dx=p.dy=p.vx=p.vy=0;
   }

   await this.saveMatch();
   this.broadcast({type:'state',...this.snapshot()});
   return;
  }

  // オーナーだけが開始
  if(m.type==='start'){
   this.assignHost();

   if(c.pid!==this.hostId){
    this.send(cid,{
     type:'error',
     message:'オーナーだけが開始できます'
    });
    return;
   }

   if(this.phase!=='setup')return;

   if(this.onlineCount()<2){
    this.send(cid,{
     type:'error',
     message:'2人以上で開始できます'
    });
    return;
   }

   this.phase='countdown';
   this.countdownAt=Date.now()+10000;
   await this.saveMatch();
   this.broadcast({type:'state',...this.snapshot()});
   this.startTimer();
   return;
  }

  // 再戦
  if(
   m.type==='reset'&&
   c.pid===this.hostId&&
   this.phase==='ended'
  ){
   await this.newRound();
   return;
  }

  // 移動
  if(m.type==='move'&&c.pid){
   const p=this.players.get(c.pid);
   if(!p||!p.online||!p.alive||
      this.phase!=='playing')return;

   const x=Number(m.x)||0;
   const y=Number(m.y)||0;
   const len=Math.hypot(x,y);

   p.dx=len>0?x/Math.max(1,len):0;
   p.dy=len>0?y/Math.max(1,len):0;
  }
 }

 async persist(){
  await this.state.storage.put('config',{
   target:this.target,
   configured:this.configured
  });
 }

 async saveMatch(){
  this.lastSaved=Date.now();
  await this.state.storage.put('match',{
   phase:this.phase,
   target:this.target,
   configured:this.configured,
   hostId:this.hostId,
   nextId:this.nextId,
   countdownAt:this.countdownAt,
   startedAt:this.startedAt,
   endedAt:this.endedAt,
   loser:this.loser,
   players:[...this.players.values()],
   ghosts:this.ghosts
  });
 }

 cleanupOffline(){
  if(this.phase!=='setup')return;
  let changed=false;
  const now=Date.now();

  for(const [id,p] of this.players){
   if(!p.online&&
      now-(p.offlineAt||0)>this.offlineGraceMs){
    this.players.delete(id);
    changed=true;
   }
  }

  if(changed){
   this.assignHost();
   this.saveMatch().catch(()=>{});
   this.broadcast({type:'state',...this.snapshot()});
  }
 }

 disconnect(cid){
  const c=this.clients.get(cid);
  if(!c)return;
  this.clients.delete(cid);

  if(c.pid){
   const p=this.players.get(c.pid);
   const stillConnected=[...this.clients.values()].some(
    other=>other.pid===c.pid
   );

   if(p&&!stillConnected){
    p.online=false;
    p.offlineAt=Date.now();
    p.dx=p.dy=0;
   }
  }

  this.assignHost();
  this.saveMatch().catch(()=>{});
  this.broadcast({type:'state',...this.snapshot()});
 }

 async resetRoom(){
  this.stopTimer();
  this.players.clear();
  this.ghosts=[];
  this.phase='setup';
  this.target=20;
  this.configured=false;
  this.hostId=null;
  this.nextId=1;
  this.countdownAt=0;
  this.startedAt=0;
  this.endedAt=0;
  this.loser=null;

  for(const c of this.clients.values())c.pid=null;

  await this.persist();
  await this.saveMatch();
  this.broadcast({type:'resetDone'});
  this.broadcast({type:'state',...this.snapshot()});
 }

 async newRound(){
  this.stopTimer();
  this.phase='setup';
  this.ghosts=[];
  this.loser=null;
  this.countdownAt=0;
  this.startedAt=0;
  this.endedAt=0;

  for(const [id,p] of this.players){
   if(!p.online){
    this.players.delete(id);
    continue;
   }
   p.hp=3;
   p.alive=true;
   p.immune=3;
   p.dx=p.dy=p.vx=p.vy=0;
  }

  this.assignHost();
  await this.saveMatch();
  this.broadcast({type:'state',...this.snapshot()});
 }

 startTimer(){
  if(this.timer)return;
  this.lastTick=Date.now();

  this.timer=setInterval(()=>{
   try{this.tick()}
   catch(e){console.error('Game tick error',e)}
  },50);
 }

 stopTimer(){
  if(this.timer)clearInterval(this.timer);
  this.timer=null;
 }

 tick(){
  const now=Date.now();
  const dt=clamp((now-this.lastTick)/1000,0,.1);
  this.lastTick=now;

  if(this.phase==='countdown'){
   if(now<this.countdownAt)return;

   this.phase='playing';
   this.startedAt=now;
   this.ghosts=[];

   for(const p of this.players.values()){
    p.hp=3;
    p.alive=true;
    p.immune=3;
    p.vx=p.vy=p.dx=p.dy=0;
    p.x=rnd(80,920);
    p.y=rnd(80,620);
   }

   this.saveMatch().catch(()=>{});
   this.broadcast({type:'state',...this.snapshot()});
  }

  if(this.phase!=='playing')return;
  if(this.onlineCount()===0)return;

  const elapsed=(now-this.startedAt)/1000;
  const starts=[
   [55,55],[945,645],[945,55],[55,645],
   [500,55],[500,645],[55,350],[945,350],
   [250,55],[750,645],[500,350]
  ];

  while(this.ghosts.length<
        Math.min(11,1+Math.floor(elapsed/15))){
   const i=this.ghosts.length;
   this.ghosts.push({
    x:starts[i][0],
    y:starts[i][1],
    idx:i,
    vx:0,vy:0,
    wayX:rnd(90,910),
    wayY:rnd(90,610),
    nextTurn:now+rnd(250,1200)
   });
   this.broadcast({type:'ghost',idx:i});
  }

  for(const p of this.players.values()){
   if(!p.alive||!p.online)continue;
   p.immune=Math.max(0,p.immune-dt);
   const k=Math.min(1,dt*9);
   p.vx+=(p.dx*250-p.vx)*k;
   p.vy+=(p.dy*250-p.vy)*k;
   p.x=clamp(p.x+p.vx*dt,19,981);
   p.y=clamp(p.y+p.vy*dt,19,681);
  }

  for(const g of this.ghosts){
   const living=[...this.players.values()].filter(
    p=>p.alive&&p.online
   );
   if(!living.length)break;

   const ordered=[...living].sort((a,b)=>a.id-b.id);
   let target=ordered[g.idx%ordered.length];

   if(g.idx%3===1){
    target=ordered.reduce((a,b)=>
     Math.hypot(b.x-g.x,b.y-g.y)>
     Math.hypot(a.x-g.x,a.y-g.y)?b:a
    );
   }

   if(g.idx%3===2){
    target=ordered.reduce((a,b)=>
     Math.hypot(b.x-g.x,b.y-g.y)<
     Math.hypot(a.x-g.x,a.y-g.y)?b:a
    );
   }

   if(g.idx===10){
    if(now>=g.nextTurn||
       Math.hypot(g.wayX-g.x,g.wayY-g.y)<75){
     const a=rnd(0,Math.PI*2);
     const distance=rnd(160,590);
     g.wayX=clamp(
      g.x+Math.cos(a)*distance,65,935
     );
     g.wayY=clamp(
      g.y+Math.sin(a)*distance,65,635
     );
     g.nextTurn=now+rnd(350,1800);
    }

    const dx=g.wayX-g.x;
    const dy=g.wayY-g.y;
    const d=Math.hypot(dx,dy)||1;
    const speed=135+35*Math.sin(elapsed*2.7);
    const k=Math.min(1,dt*7);

    g.vx+=(dx/d*speed-g.vx)*k;
    g.vy+=(dy/d*speed-g.vy)*k;
   }else{
    const dx=target.x-g.x;
    const dy=target.y-g.y;
    const d=Math.hypot(dx,dy)||1;
    const lateral=Math.sin(
     elapsed*.85+g.idx*1.8
    )*(.24+(g.idx%3)*.12);

    let ux=dx/d-dy/d*lateral;
    let uy=dy/d+dx/d*lateral;

    for(const other of this.ghosts){
     if(other===g)continue;
     const sx=g.x-other.x;
     const sy=g.y-other.y;
     const dist=Math.hypot(sx,sy)||1;

     if(dist<145){
      const force=(145-dist)/145*2.4;
      ux+=sx/dist*force;
      uy+=sy/dist*force;
     }
    }

    const norm=Math.hypot(ux,uy)||1;
    const speed=98+g.idx*9+
     Math.min(elapsed,180)*.25;
    const k=Math.min(1,dt*3);

    g.vx+=(ux/norm*speed-g.vx)*k;
    g.vy+=(uy/norm*speed-g.vy)*k;
   }

   g.x=clamp(g.x+g.vx*dt,52,948);
   g.y=clamp(g.y+g.vy*dt,52,648);

   for(const p of living){
    if(
     p.immune<=0&&
     Math.hypot(p.x-g.x,p.y-g.y)<65
    ){
     p.hp--;
     p.immune=2.5;
     p.dx=p.dy=p.vx=p.vy=0;

     if(p.hp<=0){
      p.alive=false;
      this.broadcast({
       type:'hit',id:p.id,hp:0
      });
     }else{
      p.x=rnd(100,900);
      p.y=rnd(100,600);
      this.broadcast({
       type:'hit',id:p.id,hp:p.hp
      });
     }
    }
   }
  }

  const counts=[0,0,0,0];
  for(const p of this.players.values()){
   if(p.alive&&p.online)counts[p.team]++;
  }

  const teamsPresent=new Set(
   [...this.players.values()]
    .filter(p=>p.online)
    .map(p=>p.team)
  );

  const dead=[...teamsPresent].filter(
   team=>counts[team]===0
  );

  if(dead.length){
   this.phase='ended';
   this.endedAt=now;
   this.loser=dead[0];
   this.broadcast({type:'state',...this.snapshot()});
   this.stopTimer();
   this.saveMatch().catch(()=>{});
   return;
  }

  if(now-this.lastSaved>2000){
   this.saveMatch().catch(()=>{});
  }

  this.broadcast({
   type:'frame',
   players:[...this.players.values()].map(p=>[
    p.id,
    Math.round(p.x),
    Math.round(p.y),
    p.hp,
    p.alive,
    p.immune>0
   ]),
   ghosts:this.ghosts.map(g=>[
    Math.round(g.x),
    Math.round(g.y),
    g.idx
   ]),
   elapsed:Math.floor(elapsed)
  });
 }
}
