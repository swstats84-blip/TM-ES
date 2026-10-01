// PSU 작업현황 v3.59 - PWA install + Firebase-triggered master/admin-work/schedule/ladder Web Push.
// 업무 데이터 캐시는 사용하지 않는다. Push 수신만 Firebase Messaging compat로 처리한다.
importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey:'AIzaSyD95LEbX8fJt621FapB-oM2iY3zbWY70dE',
  authDomain:'tm-es-psu-6981d.firebaseapp.com',
  databaseURL:'https://tm-es-psu-6981d-default-rtdb.asia-southeast1.firebasedatabase.app',
  projectId:'tm-es-psu-6981d',
  storageBucket:'tm-es-psu-6981d.firebasestorage.app',
  messagingSenderId:'668350567112',
  appId:'1:668350567112:web:ee63dd3d4bde2e90908bfd'
});
const messaging=firebase.messaging();

self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',event=>event.waitUntil(self.clients.claim()));
self.addEventListener('fetch',()=>{});

messaging.onBackgroundMessage(payload=>{
  const d=payload&&payload.data||{};
  const title=String(d.title||'PSU 알림');
  const options={
    body:String(d.body||''),
    icon:'./temaat_icon_192.png',
    badge:'./temaat_icon_192.png',
    tag:'psu-'+String(d.eventId||d.type||Date.now()),
    data:{url:String(d.url||'./')}
  };
  return self.registration.showNotification(title,options);
});

self.addEventListener('notificationclick',event=>{
  event.notification.close();
  const url=new URL(String(event.notification&&event.notification.data&&event.notification.data.url||'./'),self.location.origin).href;
  event.waitUntil(self.clients.matchAll({type:'window',includeUncontrolled:true}).then(list=>{
    for(const client of list){
      try{if(new URL(client.url).origin===self.location.origin){if('navigate'in client)client.navigate(url);return client.focus()}}catch(_e){}
    }
    return self.clients.openWindow?self.clients.openWindow(url):null;
  }));
});
