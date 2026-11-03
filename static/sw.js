// БИЗ БИРГЕБИЗ! service worker: барак офлайн ачылат, жарыялар дайыма серверден алынат.
const CACHE = "birgebiz-v8";
const SHELL = ["/", "/manifest.webmanifest", "/static/icon.svg", "/static/icon-192.png"];

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== "GET" || url.origin !== location.origin || url.pathname.startsWith("/api/")) return;
  // Network first: жаңы версия дароо келет, интернет жок болсо кэштен ачылат.
  const isPage = e.request.mode === "navigate" || url.pathname === "/";
  e.respondWith(
    fetch(e.request, isPage ? { cache: "no-store" } : undefined)
      .then((res) => {
        const copy = res.clone();
        caches.open(CACHE).then((c) => c.put(e.request, copy));
        return res;
      })
      .catch(() => caches.match(e.request).then((r) => r || caches.match("/")))
  );
});
