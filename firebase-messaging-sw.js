importScripts("https://www.gstatic.com/firebasejs/11.1.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/11.1.0/firebase-messaging-compat.js");

firebase.initializeApp({
  apiKey: "AIzaSyD9HtmP6EgriteFXerhnuOVC8DbtfGpSvY",
  authDomain: "urbgloriaweb-1af85.firebaseapp.com",
  projectId: "urbgloriaweb-1af85",
  storageBucket: "urbgloriaweb-1af85.firebasestorage.app",
  messagingSenderId: "965163716312",
  appId: "1:965163716312:web:59182bbf9aaf2bc3f051ad",
  databaseURL: "https://urbgloriaweb-1af85-default-rtdb.firebaseio.com"
});

const messaging = firebase.messaging();

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = event.notification && event.notification.data && event.notification.data.url ? event.notification.data.url : "/";
  event.waitUntil(self.clients.matchAll({ type: "window" }).then((clientList) => {
    for (const client of clientList) {
      if (client.url === url && "focus" in client) return client.focus();
    }
    if (self.clients.openWindow) return self.clients.openWindow(url);
  }));
});

messaging.onBackgroundMessage((payload) => {
  const title = (payload && payload.notification && payload.notification.title) || "Notificación";
  const options = {
    body: (payload && payload.notification && payload.notification.body) || "",
    icon: (payload && payload.notification && payload.notification.icon) || undefined,
    data: (payload && payload.data) || {}
  };
  self.registration.showNotification(title, options);
});

// Cache básico para PWA (estático esencial)
const CACHE_NAME = "gloria-v1";
const PRECACHE_URLS = ["./", "./index.html", "./manifest.webmanifest"];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(PRECACHE_URLS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((resp) => {
        const respClone = resp.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(req, respClone)).catch(() => {});
        return resp;
      }).catch(() => cached || Promise.reject("offline"));
    })
  );
});
