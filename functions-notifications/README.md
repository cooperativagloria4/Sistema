# Cloud Functions (borrador) — Envío de notificaciones

Este módulo propone una función que escucha `/notifications/outbox/{id}` en la **base principal** y envía mensajes FCM a los tokens registrados en:

- `socios/<uid>/fcmTokens/<token>`
- `admins/<uid>/fcmTokens/<token>`

## Pasos
1. `npm init -y && npm i firebase-admin firebase-functions`
2. En Google Cloud, configura variables de entorno o usa cuenta de servicio por defecto del proyecto.
3. Despliega: `firebase deploy --only functions`

## Archivo `index.js`
```js
const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();
const db = admin.database();

exports.dispatchNotifications = functions.database
  .ref('/notifications/outbox/{id}')
  .onCreate(async (snap, ctx) => {
    const data = snap.val() || {};
    const title = data.title || 'Recordatorio';
    const body = data.body || '';
    const audience = data.audience || 'all'; // 'all' | { role: 'socio' } | { uids: [...] }

    const tokens = new Set();
    async function collect(path) {
      const s = await db.ref(path).once('value');
      const v = s.val() || {};
      Object.entries(v).forEach(([tok, enabled]) => { if (enabled) tokens.add(tok); });
    }

    if (audience === 'all' || (audience && audience.role === 'socio')) {
      const socios = await db.ref('socios').once('value');
      socios.forEach(ch => {
        const t = ch.child('fcmTokens').val() || {};
        Object.entries(t).forEach(([tok, ok]) => { if (ok) tokens.add(tok); });
      });
    }
    if (audience === 'all' || (audience && audience.role === 'admin')) {
      const admins = await db.ref('admins').once('value');
      admins.forEach(ch => {
        const t = ch.child('fcmTokens').val() || {};
        Object.entries(t).forEach(([tok, ok]) => { if (ok) tokens.add(tok); });
      });
    }
    if (audience && Array.isArray(audience.uids)) {
      for (const uid of audience.uids) {
        await collect(`socios/${uid}/fcmTokens`);
        await collect(`admins/${uid}/fcmTokens`);
      }
    }

    if (!tokens.size) return null;
    const payload = {
      notification: { title, body },
      data: data.data || {},
    };
    const batches = Array.from(tokens);
    const res = await admin.messaging().sendEachForMulticast({ tokens: batches, ...payload });
    console.log(`Enviados: ${res.successCount}, Fallidos: ${res.failureCount}`);
    return db.ref(`/notifications/outbox/${ctx.params.id}`).remove();
  });
```
``` 
