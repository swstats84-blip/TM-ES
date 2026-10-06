(function(){
 'use strict';
 if(window.__webV368HardRouteInstalled)return;window.__webV368HardRouteInstalled=true;
 const direct=window.__fbFirebaseDirectV355;
 if(!direct||!window.firebase||!firebase.database||!firebase.auth){console.warn('v3.80 transport: Firebase direct facade 준비 전');return;}
 const DB=String(firebase.app().options&&firebase.app().options.databaseURL||'').replace(/\/+$/,'');
 const CLIENT_KEY='psuRelayClientIdV365';
 let CLIENT_ID='';
 try{CLIENT_ID=String(sessionStorage.getItem(CLIENT_KEY)||'');if(!CLIENT_ID){CLIENT_ID='web-'+(crypto.randomUUID?crypto.randomUUID():Date.now().toString(36)+'-'+Math.random().toString(36).slice(2));sessionStorage.setItem(CLIENT_KEY,CLIENT_ID)}}catch(_e){CLIENT_ID='web-'+Date.now().toString(36)}
 let mode='WAIT',relayURL='',serverVersion='',generation=0,hybrid=null,aliveES=null,aliveWatch=0,recoveryTimer=0,recoveryBusy=false,failureProbeBusy=false,cachedRelayURL='',lastDiscoveryAt=0,authRebindTimer=0;
 const routeWaiters=[];
 const ALIVE_TIMEOUT_MS=6500,HEALTH_CONFIRM_MS=1200,RECOVERY_MS=2000,REDISCOVERY_MS=60000;
 const logicalListeners=new Set(),relayStreams=new Map();
 const clean=p=>String(p||'').replace(/^\/+|\/+$/g,''),clone=v=>v===undefined?undefined:JSON.parse(JSON.stringify(v)),obj=v=>!!v&&typeof v==='object'&&!Array.isArray(v);
 function sdkGate(online){try{const db=firebase.database();if(online&&db&&db.goOnline)db.goOnline();else if(db&&db.goOffline)db.goOffline()}catch(_e){}}
 // Critical startup rule: business RTDB stays OFF until relayDiscovery/current resolves.
 sdkGate(false);
 async function user(){
   // v3.72: direct.auth()가 Firebase 첫 Auth 복원 상태를 기다린 뒤에만 익명 로그인을 허용한다.
   // transport가 profile/RTDB 복원 promise를 기다리면 route 결정과 순환대기가 생길 수 있으므로 Auth SDK gate만 사용한다.
   return await direct.auth()
 }
 async function token(force=false){const u=await user();return await u.getIdToken(!!force)}
 function externalRelayURL(v){try{v=String(v||'').replace(/\/+$/,'');if(!v)return'';const u=new URL(v);if(u.protocol!=='https:'||!u.hostname)return'';return v}catch(_e){return''}}
 function status(){return {mode,relayReady:mode==='RELAY',relayURL,directActive:mode==='DIRECT',leaseUntil:0,serverVersion,generation,label:mode==='RELAY'?'중계 ON':mode==='DIRECT'?'중계 OFF · FB Direct':'중계 상태 확인 중'}}
 function emit(){window.dispatchEvent(new CustomEvent('psu-transport-mode',{detail:status()}))}
 function resolveRouteWaiters(){if(mode==='WAIT')return;while(routeWaiters.length){const x=routeWaiters.shift();clearTimeout(x.t);x.resolve(status())}}
 function waitRoute(timeoutMs=8000){if(mode!=='WAIT')return Promise.resolve(status());return new Promise((resolve,reject)=>{const x={resolve,reject,t:0};x.t=setTimeout(()=>{const i=routeWaiters.indexOf(x);if(i>=0)routeWaiters.splice(i,1);reject(new Error('중계서버 상태 확인 시간 초과'))},Math.max(1000,Number(timeoutMs)||8000));routeWaiters.push(x)})}
 function refreshRelayAuthStreams(){if(mode!=='RELAY')return;for(const st of [...relayStreams.values()]){st.close(false);st.scheduleReopen(false)}}
 function scheduleAuthRebind(){clearTimeout(authRebindTimer);if(mode!=='RELAY')return;authRebindTimer=setTimeout(()=>{authRebindTimer=0;if(mode==='RELAY')refreshRelayAuthStreams()},120)}
 function closeAlive(){clearInterval(aliveWatch);aliveWatch=0;if(aliveES){try{aliveES.close()}catch(_e){}aliveES=null}}
 function switchMode(next,url='',sv=''){
   next=String(next||'WAIT').toUpperCase();if(!['WAIT','RELAY','DIRECT'].includes(next))next='WAIT';
   url=next==='RELAY'?externalRelayURL(url):'';if(next==='RELAY'&&!url)next='DIRECT';
   const changed=mode!==next||relayURL!==url;
   serverVersion=String(sv||serverVersion||'');
   if(!changed){sdkGate(mode==='DIRECT');emit();return}
   for(const s of relayStreams.values())s.close(false);relayStreams.clear();for(const l of logicalListeners)l.detach();
   mode=next;relayURL=url;generation++;if(hybrid)hybrid.mode=mode.toLowerCase();sdkGate(mode==='DIRECT');for(const l of logicalListeners)l.bind();resolveRouteWaiters();emit();
 }
 function qs(q){q=q||{};const x=new URLSearchParams();if(q.orderBy!==undefined)x.set('orderBy',JSON.stringify(q.orderBy));if(q.startAtSet)x.set('startAt',JSON.stringify(q.startAt));if(q.endAtSet)x.set('endAt',JSON.stringify(q.endAt));if(q.equalToSet)x.set('equalTo',JSON.stringify(q.equalTo));if(q.limitFirst!==undefined)x.set('limitToFirst',String(q.limitFirst));if(q.limitLast!==undefined)x.set('limitToLast',String(q.limitLast));return x}
 async function rawFirebaseREST(path,q={},opt={}){const sp=qs(q);sp.set('auth',await token(!!opt.forceToken));const r=await fetch(DB+'/'+clean(path)+'.json?'+sp.toString(),{method:opt.method||'GET',body:opt.body===undefined?undefined:JSON.stringify(opt.body),cache:'no-store',headers:Object.assign({'Cache-Control':'no-cache'},opt.body===undefined?{}:{'Content-Type':'application/json'},opt.headers||{})});const text=await r.text();let data=null;try{data=text.trim()?JSON.parse(text):null}catch(_e){data=text}return {res:r,data,text}}
 async function relayREST(path,q={},opt={}){if(mode!=='RELAY'||!relayURL)throw new Error('Relay 경로가 활성화되지 않았습니다.');const sp=qs(q);sp.set('auth',await token(!!opt.forceToken));sp.set('clientId',CLIENT_ID);sp.set('clientType','web');if((opt.method==='PUT'||opt.method==='PATCH'||opt.method==='POST'||opt.method==='DELETE')&&!opt.headers?.['If-Match'])sp.set('print','silent');const r=await fetch(relayURL+'/firebase/'+clean(path)+'.json?'+sp.toString(),{method:opt.method||'GET',body:opt.body===undefined?undefined:JSON.stringify(opt.body),cache:'no-store',headers:Object.assign({'Cache-Control':'no-cache','X-PSU-Client-ID':CLIENT_ID,'X-PSU-Client-Type':'web'},opt.body===undefined?{}:{'Content-Type':'application/json'},opt.headers||{})});const text=await r.text();let data=null;try{data=text.trim()?JSON.parse(text):null}catch(_e){data=text}return {res:r,data,text}}
 async function rest(path,q={},opt={}){if(mode==='WAIT')await waitRoute();const x=mode==='DIRECT'?await rawFirebaseREST(path,q,opt):await relayREST(path,q,opt);if(!x.res.ok)throw new Error((mode==='DIRECT'?'Firebase':'Relay')+' HTTP '+x.res.status+': '+x.text);return x}
 class Snap{constructor(v,key=''){this._v=clone(v);this.key=key||null}val(){return clone(this._v)}exists(){return this._v!==null&&this._v!==undefined}forEach(fn){if(!obj(this._v)&&!Array.isArray(this._v))return false;for(const k of Object.keys(this._v||{})){if(fn(new Snap(this._v[k],k))===true)return true}return false}child(k){return new Snap(this._v&&typeof this._v==='object'?this._v[k]:null,String(k))}}
 function directQuery(path,q){let r=firebase.database().ref(clean(path));if(q.orderBy==='$key')r=r.orderByKey();else if(q.orderBy!==undefined)r=r.orderByChild(q.orderBy);if(q.startAtSet)r=r.startAt(q.startAt);if(q.endAtSet)r=r.endAt(q.endAt);if(q.equalToSet)r=r.equalTo(q.equalTo);if(q.limitFirst!==undefined)r=r.limitToFirst(q.limitFirst);if(q.limitLast!==undefined)r=r.limitToLast(q.limitLast);return r}
 function setAt(root,parts,val){if(!parts.length)return clone(val);let out=(obj(root)||Array.isArray(root))?clone(root):{},cur=out;for(let i=0;i<parts.length-1;i++){const k=parts[i];if(!cur[k]||typeof cur[k]!=='object')cur[k]={};cur=cur[k]}const k=parts[parts.length-1];if(val===null||val===undefined){if(Array.isArray(cur)&&/^\d+$/.test(k))cur[+k]=null;else delete cur[k]}else cur[k]=clone(val);return out}
 function apply(root,path,kind,data){const pp=clean(path).split('/').filter(Boolean);if(kind==='put')return setAt(root,pp,data);let out=(obj(root)||Array.isArray(root))?clone(root):{};if(obj(data))for(const [k,v] of Object.entries(data))out=setAt(out,pp.concat(clean(k).split('/').filter(Boolean)),v);return out}
 function streamKey(path,q){return clean(path)+'?'+qs(q).toString()}
 function childMap(v){return(v&&typeof v==='object')?v:{}}
 class RelayStream{
   constructor(path,q){this.path=clean(path);this.q=q;this.key=streamKey(path,q);this.es=null;this.model=undefined;this.listeners=new Set();this.opening=false;this.reopenTimer=0}
   add(l){this.listeners.add(l);if(this.model!==undefined)this.initial(l);this.open()}
   del(l){this.listeners.delete(l);if(!this.listeners.size)this.close(true)}
   initial(l){if(l.event==='value')l.cb(new Snap(this.model,l.ref.key));else if(l.event==='child_added')for(const k of Object.keys(childMap(this.model)))l.cb(new Snap(this.model[k],k))}
   dispatch(before,after,initial){for(const l of [...this.listeners]){try{if(l.event==='value'){l.cb(new Snap(after,l.ref.key));continue}const a=childMap(before),b=childMap(after);if(initial){if(l.event==='child_added')for(const k of Object.keys(b))l.cb(new Snap(b[k],k));continue}if(l.event==='child_added'){for(const k of Object.keys(b))if(!(k in a))l.cb(new Snap(b[k],k))}else if(l.event==='child_removed'){for(const k of Object.keys(a))if(!(k in b))l.cb(new Snap(a[k],k))}else if(l.event==='child_changed'){for(const k of Object.keys(b))if(k in a&&JSON.stringify(a[k])!==JSON.stringify(b[k]))l.cb(new Snap(b[k],k))}}catch(e){if(l.err)l.err(e)}}}
   scheduleReopen(force){clearTimeout(this.reopenTimer);if(mode!=='RELAY'||!this.listeners.size)return;this.reopenTimer=setTimeout(()=>this.open(!!force),700)}
   async open(forceToken=false){if(this.es||this.opening||mode!=='RELAY'||!relayURL||!this.listeners.size)return;this.opening=true;try{const sp=qs(this.q);sp.set('auth',await token(forceToken));sp.set('clientId',CLIENT_ID);sp.set('clientType','web');if(mode!=='RELAY'||!relayURL||!this.listeners.size)return;const es=new EventSource(relayURL+'/firebase/'+this.path+'.json?'+sp.toString());this.es=es;const ev=(kind,e)=>{if(this.es!==es)return;try{const m=JSON.parse(e.data||'{}'),before=clone(this.model),first=this.model===undefined;this.model=apply(this.model,String(m.path||'/'),kind,m.data);this.dispatch(before,this.model,first)}catch(x){for(const l of this.listeners)if(l.err)l.err(x)}};es.addEventListener('put',e=>ev('put',e));es.addEventListener('patch',e=>ev('patch',e));es.addEventListener('auth_revoked',()=>{if(this.es!==es)return;this.close(false);this.scheduleReopen(true)});es.addEventListener('cancel',()=>{if(this.es!==es)return;/* v3.80: cancel은 현재 Auth 상태로 재연결한다. 강제 token refresh는 auth_revoked에서만 수행한다. */this.close(false);this.scheduleReopen(false)});es.onerror=()=>{if(this.es!==es)return;if(es.readyState===EventSource.CLOSED){this.close(false);this.scheduleReopen(true)}}}catch(_e){this.scheduleReopen(forceToken)}finally{this.opening=false}}
   close(remove){clearTimeout(this.reopenTimer);this.reopenTimer=0;if(this.es){try{this.es.close()}catch(_e){}this.es=null}if(remove)relayStreams.delete(this.key)}
 }
 function relayStream(path,q){const k=streamKey(path,q);let s=relayStreams.get(k);if(!s){s=new RelayStream(path,q);relayStreams.set(k,s)}return s}
 class Listener{constructor(ref,event,cb,err){this.ref=ref;this.event=event;this.cb=cb;this.err=err;this.directRef=null;this.stream=null;logicalListeners.add(this);this.bind()}bind(){this.detach();if(mode==='DIRECT'){this.directRef=directQuery(this.ref.path,this.ref.q);this.directRef.on(this.event,this.cb,this.err)}else if(mode==='RELAY'){this.stream=relayStream(this.ref.path,this.ref.q);this.stream.add(this)}}detach(){if(this.directRef){try{this.directRef.off(this.event,this.cb)}catch(_e){}this.directRef=null}if(this.stream){this.stream.del(this);this.stream=null}}close(){this.detach();logicalListeners.delete(this)}}
 class HRef{
   constructor(path,q={}){this.path=clean(path);this.q=q;this._ls=[]}
   get key(){const a=this.path.split('/');return a[a.length-1]||null}
   child(p){return new HRef([this.path,clean(p)].filter(Boolean).join('/'),Object.assign({},this.q))}
   orderByKey(){return new HRef(this.path,Object.assign({},this.q,{orderBy:'$key'}))}
   orderByChild(k){return new HRef(this.path,Object.assign({},this.q,{orderBy:String(k)}))}
   startAt(v){return new HRef(this.path,Object.assign({},this.q,{startAtSet:true,startAt:v}))}
   endAt(v){return new HRef(this.path,Object.assign({},this.q,{endAtSet:true,endAt:v}))}
   equalTo(v){return new HRef(this.path,Object.assign({},this.q,{equalToSet:true,equalTo:v}))}
   limitToFirst(n){return new HRef(this.path,Object.assign({},this.q,{limitFirst:+n}))}
   limitToLast(n){return new HRef(this.path,Object.assign({},this.q,{limitLast:+n}))}
   async once(ev){if(mode==='WAIT')await waitRoute();if(mode==='DIRECT')return directQuery(this.path,this.q).once(ev);if(ev!=='value')throw new Error('Relay once는 value만 지원합니다.');return new Snap((await rest(this.path,this.q,{method:'GET'})).data,this.key)}
   on(ev,cb,err){const l=new Listener(this,ev,cb,err);this._ls.push(l);return cb}
   off(ev,cb){this._ls=this._ls.filter(l=>{if((!ev||l.event===ev)&&(!cb||l.cb===cb)){l.close();return false}return true})}
   async set(v){if(mode==='WAIT')await waitRoute();if(mode==='DIRECT')return directQuery(this.path,{}).set(v);await rest(this.path,{}, {method:'PUT',body:v});return v}
   async update(v){if(mode==='WAIT')await waitRoute();if(mode==='DIRECT')return directQuery(this.path,{}).update(v);await rest(this.path,{}, {method:'PATCH',body:v});return v}
   async remove(){if(mode==='WAIT')await waitRoute();if(mode==='DIRECT')return directQuery(this.path,{}).remove();await rest(this.path,{}, {method:'DELETE'});return null}
   push(v){const key=firebase.database().ref(this.path).push().key,r=new HRef([this.path,key].filter(Boolean).join('/'));if(arguments.length)return r.set(v).then(()=>r);return r}
   async transaction(fn){if(mode==='WAIT')await waitRoute();if(mode==='DIRECT')return directQuery(this.path,{}).transaction(fn);for(let i=0;i<10;i++){const g=await relayREST(this.path,{}, {method:'GET',headers:{'X-Firebase-ETag':'true'},forceToken:i>0});if(!g.res.ok)throw new Error('Relay transaction GET '+g.res.status);const cur=g.data,etag=g.res.headers.get('ETag')||g.res.headers.get('etag'),next=fn(clone(cur));if(next===undefined)return {committed:false,snapshot:new Snap(cur,this.key)};const p=await relayREST(this.path,{}, {method:'PUT',body:next,headers:{'If-Match':etag||'*'},forceToken:i>0});if(p.res.status===412)continue;if(!p.res.ok)throw new Error('Relay transaction PUT '+p.res.status);return {committed:true,snapshot:new Snap(next,this.key)}}throw new Error('transaction retry exceeded')}
 }
 async function fetchRelayURL(force=false){const x=await rawFirebaseREST('relayDiscovery/current/currentUrl',{}, {method:'GET',forceToken:force});if(!x.res.ok)throw new Error('relay discovery HTTP '+x.res.status);const u=externalRelayURL(typeof x.data==='string'?x.data:'');lastDiscoveryAt=Date.now();if(u)cachedRelayURL=u;return u}
 async function probeRelayHealth(url,timeoutMs=2500){url=externalRelayURL(url);if(!url)return {ok:false,version:''};let timer=0,ctrl=null;try{if(typeof AbortController==='function'){ctrl=new AbortController();timer=setTimeout(()=>ctrl.abort(),Math.max(250,Number(timeoutMs)||2500))}const r=await fetch(url+'/health',{method:'GET',cache:'no-store',signal:ctrl?ctrl.signal:undefined,headers:{'Cache-Control':'no-cache','X-PSU-Client-ID':CLIENT_ID,'X-PSU-Client-Type':'web'}});if(!r.ok)return {ok:false,version:''};const x=await r.json();return {ok:x&&x.serverOnline===true&&x.relayAvailable===true&&x.usable===true,version:String(x&&x.version||'')}}catch(_e){return {ok:false,version:''}}finally{clearTimeout(timer)}}
 function scheduleRecovery(delay=RECOVERY_MS,forceDiscovery=false){clearTimeout(recoveryTimer);recoveryTimer=setTimeout(()=>recoveryTick(forceDiscovery),Math.max(0,delay))}
 async function confirmRelayFailure(forceDiscovery=true){if(mode!=='RELAY'){scheduleRecovery(0,forceDiscovery);return}if(failureProbeBusy)return;failureProbeBusy=true;const url=relayURL,gen=generation;closeAlive();try{const h=await probeRelayHealth(url,HEALTH_CONFIRM_MS);if(mode!=='RELAY'||generation!==gen||relayURL!==url)return;if(h.ok){serverVersion=String(h.version||serverVersion||'');startAlive();return}switchMode('DIRECT','',serverVersion);scheduleRecovery(0,forceDiscovery)}finally{failureProbeBusy=false}}
 function failRelay(forceDiscovery=true){confirmRelayFailure(forceDiscovery).catch(()=>{if(mode==='RELAY'){switchMode('DIRECT','',serverVersion);scheduleRecovery(0,forceDiscovery)}})}
 function startAlive(){closeAlive();if(mode!=='RELAY'||!relayURL)return;const url=relayURL,gen=generation;let last=Date.now();const sp=new URLSearchParams();sp.set('clientId',CLIENT_ID);sp.set('clientType','web');const es=new EventSource(url+'/alive?'+sp.toString());aliveES=es;es.onopen=()=>{if(aliveES===es&&generation===gen)last=Date.now()};es.onmessage=()=>{if(aliveES===es&&generation===gen)last=Date.now()};es.onerror=()=>{if(aliveES!==es||generation!==gen)return;if(es.readyState===EventSource.CLOSED)failRelay(true)};aliveWatch=setInterval(()=>{if(aliveES!==es||generation!==gen)return;if(Date.now()-last>ALIVE_TIMEOUT_MS)failRelay(true)},1000)}
 async function enterRelay(url,sv){url=externalRelayURL(url);if(!url)return false;cachedRelayURL=url;switchMode('RELAY',url,sv);startAlive();return true}
 async function recoveryTick(forceDiscovery=false){if(mode!=='DIRECT'||recoveryBusy)return;recoveryBusy=true;try{if(forceDiscovery||!cachedRelayURL||!lastDiscoveryAt||Date.now()-lastDiscoveryAt>=REDISCOVERY_MS){try{await fetchRelayURL(false)}catch(_e){lastDiscoveryAt=Date.now()}}if(cachedRelayURL){const h=await probeRelayHealth(cachedRelayURL);if(h.ok){await enterRelay(cachedRelayURL,h.version);return}}}finally{recoveryBusy=false}if(mode==='DIRECT')scheduleRecovery(RECOVERY_MS,false)}
 async function bootstrapRoute(){let discovered='';try{discovered=await fetchRelayURL(false)}catch(_e){}if(discovered){const h=await probeRelayHealth(discovered);if(h.ok){await enterRelay(discovered,h.version);return}}switchMode('DIRECT','',serverVersion);scheduleRecovery(RECOVERY_MS,false)}
 hybrid={mode:'wait',auth:(...a)=>user(...a),bump:(...a)=>direct.bump(...a),bumpScheduleDeltaV286:(...a)=>direct.bumpScheduleDeltaV286(...a),read:(...a)=>direct.read(...a),readRange:(...a)=>direct.readRange(...a),readFresh:async p=>(await rest(p,{}, {method:'GET'})).data,set:(...a)=>direct.set(...a),update:(...a)=>direct.update(...a),del:(...a)=>direct.del(...a),b64key:(...a)=>direct.b64key(...a),isAdmin:(...a)=>direct.isAdmin(...a),isMaster:(...a)=>direct.isMaster(...a),ref:p=>new HRef(p)};
 window.__psuDataTransportV357=hybrid;window.__psuDataTransportV355=hybrid;window.__fbDirectV187=hybrid;window.__psuHybridTransportV368={status,waitReady:()=>waitRoute(),reconnectStatus:()=>scheduleRecovery(0,true)};window.__psuHybridTransportV367=window.__psuHybridTransportV368;window.__psuHybridTransportV366=window.__psuHybridTransportV368;window.__psuHybridTransportV365=window.__psuHybridTransportV368;
 try{firebase.auth().onIdTokenChanged(()=>{if(mode==='RELAY')scheduleAuthRebind();else if(mode==='DIRECT')scheduleRecovery(80,!cachedRelayURL)})}catch(_e){}
 bootstrapRoute();emit();
})();
