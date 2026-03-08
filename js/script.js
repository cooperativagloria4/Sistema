        import { initializeApp } from "https://www.gstatic.com/firebasejs/11.1.0/firebase-app.js";
        import { getDatabase, ref, set, push, onValue, update, remove, get, runTransaction } from "https://www.gstatic.com/firebasejs/11.1.0/firebase-database.js";
        import { getAuth, setPersistence, inMemoryPersistence, browserLocalPersistence, signInWithEmailAndPassword, onAuthStateChanged, signOut, createUserWithEmailAndPassword, updatePassword, reauthenticateWithCredential, EmailAuthProvider, updateEmail, deleteUser, fetchSignInMethodsForEmail } from "https://www.gstatic.com/firebasejs/11.1.0/firebase-auth.js";
        
        // ========================================================
        // CREDENCIALES (Puestas aquí para compatibilidad con GitHub Pages)
        // ========================================================
        const firebaseConfigPrincipal = {
            apiKey: "AIzaSyD9HtmP6EgriteFXerhnuOVC8DbtfGpSvY",
            authDomain: "urbgloriaweb-1af85.firebaseapp.com",
            projectId: "urbgloriaweb-1af85",
            storageBucket: "urbgloriaweb-1af85.firebasestorage.app",
            messagingSenderId: "965163716312",
            appId: "1:965163716312:web:59182bbf9aaf2bc3f051ad",
            databaseURL: "https://urbgloriaweb-1af85-default-rtdb.firebaseio.com" 
        };
        const firebaseConfigCaja = {
            apiKey: "AIzaSyD7vyFwUTUsb01rHCJ5PvepT70WFiVCqJ4",
            authDomain: "urbgloriacaja.firebaseapp.com",
            projectId: "urbgloriacaja",
            storageBucket: "urbgloriacaja.firebasestorage.app",
            messagingSenderId: "141426832510",
            appId: "1:141426832510:web:61c7fcd68395f90075142f",
            databaseURL: "https://urbgloriacaja-default-rtdb.firebaseio.com" 
        };

        // ========================================================
        // INICIALIZACIÓN DE FIREBASE
        // ========================================================
        const appPrincipal = initializeApp(firebaseConfigPrincipal, "principal");
        const appCaja = initializeApp(firebaseConfigCaja, "caja");

        const db = getDatabase(appPrincipal);
        const dbCaja = getDatabase(appCaja);
        try { console.log('[Caja] Conectado a DB secundaria'); } catch(_) {}
        const auth = getAuth(appPrincipal);
        try { setPersistence(auth, inMemoryPersistence); } catch(_) {}
        const authCaja = getAuth(appCaja);
        try { setPersistence(authCaja, inMemoryPersistence); } catch(_) {}

        let currentUser = null;
        let sociosData = [];
        let cuotasData = []; 
        let asambleasData = [];
        let votacionesData = [];
        let allCajaMovs = []; 
        let socioCajaMovs = [];
        let cajaSocioUnsub = null;
        let configData = {};
        let adminsData = [];
        let inactivityTimer = null;
        let activityListenersSet = false;
        let cajaUnsub = null;
        let socioStatusUnsub = null;
        let chartFinanzas = null;
        let chartSocios = null;
        const INACTIVITY_MS = 10 * 60 * 1000; // 10 minutos

        // ========================================================
        // TOAST NOTIFICATIONS
        // ========================================================
        window.showToast = (message, type = 'info', duration = 3000) => {
            console.log(`[Toast] ${type.toUpperCase()}: ${message}`); // Log para depuración
            const container = document.getElementById('toast-container');
            if (!container) return;

            const toast = document.createElement('div');
            toast.className = `toast toast-${type}`;
            
            const icon = {
                success: 'fa-check-circle',
                error: 'fa-exclamation-circle',
                info: 'fa-info-circle',
                warning: 'fa-exclamation-triangle'
            }[type] || 'fa-info-circle';

            toast.innerHTML = `
                <i class="fas ${icon}"></i>
                <span>${message}</span>
            `;

            container.appendChild(toast);

            // Force reflow for transition
            toast.offsetHeight;
            toast.classList.add('show');

            setTimeout(() => {
                toast.classList.remove('show');
                setTimeout(() => toast.remove(), 300);
            }, duration);
        };

        // Función global para reemplazar alert de forma segura
        const originalAlert = window.alert;
        window.alert = (msg) => {
            if (msg && typeof msg === 'string') {
                // Si el mensaje parece un error, usar toast error, si no info
                const type = (msg.toLowerCase().includes('error') || msg.toLowerCase().includes('falló') || msg.toLowerCase().includes('inválid')) ? 'error' : 'info';
                window.showToast(msg, type);
            } else {
                originalAlert(msg);
            }
        };

        document.getElementById('cuotas-filter-month').value = new Date().toISOString().substring(0, 7);
        document.getElementById('caja-filter-month').value = new Date().toISOString().substring(0, 7);

        // --- AUTH ---
        async function findProfileAfterAuth(fbUser, loginUsuario) {
            if (!fbUser) return null;
            const uid = fbUser.uid;
            const rootSnap = await get(ref(db, 'admins/root'));
            if (rootSnap.exists()) {
                const data = rootSnap.val() || {};
                if ((data.usuario && String(data.usuario).toLowerCase() === String(loginUsuario).toLowerCase()) || loginUsuario.toLowerCase() === 'root' || data.role === 'root') {
                    if (!data.uid || data.uid !== uid) {
                        try { await update(ref(db, 'admins/root'), { uid }); } catch(_) {}
                    }
                    return { ...data, id: 'root', uid, email: fbUser.email || '', role: 'root', permisos: { padron: true, cuotas: true, caja: true, asambleas: true, votaciones: true } };
                }
            }
            const adminsSnap = await get(ref(db, 'admins'));
            const admins = adminsSnap.val() || {};
            for (let id in admins) {
                const a = admins[id];
                if (!a) continue;
                if (a.usuario && String(a.usuario).toLowerCase() === String(loginUsuario).toLowerCase()) {
                    if (!a.uid || a.uid !== uid) {
                        try { await update(ref(db, `admins/${id}`), { uid }); } catch(_) {}
                    }
                    const role = id === 'root' || a.role === 'root' ? 'root' : (a.role || 'admin');
                    const permisos = role === 'root' ? { padron: true, cuotas: true, caja: true, asambleas: true, votaciones: true } : (a.permisos || {});
                    return { ...a, id, uid, email: fbUser.email || '', role, permisos };
                }
            }
            const socioByUid = await get(ref(db, `socios/${uid}`));
            if (socioByUid.exists()) {
                const data = socioByUid.val() || {};
                if (data.estado === 'inactivo') throw new Error('ACCOUNT_INACTIVE');
                if (!data.uid || data.uid !== uid) {
                    try { await update(ref(db, `socios/${uid}`), { uid }); } catch(_) {}
                }
                return { ...data, id: uid, uid, email: fbUser.email || '', role: 'socio' };
            }
            const sociosSnap = await get(ref(db, 'socios'));
            const socios = sociosSnap.val() || {};
            for (let id in socios) {
                const s = socios[id];
                if (s && s.usuario && String(s.usuario).toLowerCase() === String(loginUsuario).toLowerCase()) {
                    if (s.estado === 'inactivo') throw new Error('ACCOUNT_INACTIVE');
                    if (!s.uid || s.uid !== uid) {
                        try { await update(ref(db, `socios/${id}`), { uid }); } catch(_) {}
                    }
                    return { ...s, id, uid, email: fbUser.email || '', role: 'socio' };
                }
            }
            return null;
        }

        async function obtenerPerfil(fbUser) {
            if (!fbUser) return null;
            const uid = fbUser.uid;
            const email = fbUser.email || '';
            console.log(`Buscando UID [${uid}] en Admins...`);
            // Intento directo por clave uid
            const directAdmin = await get(ref(db, `admins/${uid}`));
            if (directAdmin.exists()) {
                const a = directAdmin.val() || {};
                const role = a.role === 'root' ? 'root' : (a.role || 'admin');
                const permisos = role === 'root' ? { padron: true, cuotas: true, caja: true, asambleas: true, votaciones: true } : (a.permisos || {});
                const profile = { ...a, id: uid, uid, email, role, permisos };
                console.log(`Perfil encontrado: ${(a.nombre || a.nombres || a.usuario || email)} con Rol: ${role}`);
                return profile;
            }
            // Escaneo de admins/
            const adminsSnap = await get(ref(db, 'admins'));
            const adminsAll = adminsSnap.val() || {};
            if (adminsAll.root) {
                const a = adminsAll.root;
                if (a.uid && a.uid === uid) {
                    const role = 'root';
                    const profile = { ...a, id: 'root', uid, email, role, permisos: { padron: true, cuotas: true, caja: true, asambleas: true, votaciones: true } };
                    console.log(`Perfil encontrado: ${(a.nombre || a.nombres || a.usuario || email)} con Rol: ${role}`);
                    return profile;
                }
            }
            for (const [id, a] of Object.entries(adminsAll)) {
                if (!a) continue;
                if ((a.uid && a.uid === uid)) {
                    const role = id === 'root' || a.role === 'root' ? 'root' : (a.role || 'admin');
                    const permisos = role === 'root' ? { padron: true, cuotas: true, caja: true, asambleas: true, votaciones: true } : (a.permisos || {});
                    const profile = { ...a, id, uid, email, role, permisos };
                    console.log(`Perfil encontrado: ${(a.nombre || a.nombres || a.usuario || email)} con Rol: ${role}`);
                    return profile;
                }
            }
            console.log(`Buscando UID [${uid}] en Socios...`);
            // Intento directo por clave uid
            const socioDirect = await get(ref(db, `socios/${uid}`));
            if (socioDirect.exists()) {
                const s = socioDirect.val() || {};
                if (String(s.estado || '').toLowerCase() === 'inactivo') { throw new Error('ACCOUNT_INACTIVE'); }
                const profile = { ...s, id: uid, uid, email, role: 'socio' };
                console.log(`Perfil encontrado: ${(s.nombres || s.nombre || s.usuario || email)} con Rol: socio`);
                return profile;
            }
            // Escaneo de socios/
            const sociosSnap = await get(ref(db, 'socios'));
            const sociosAll = sociosSnap.val() || {};
            for (const [id, s] of Object.entries(sociosAll)) {
                if (!s) continue;
                if (s.uid && s.uid === uid) {
                    if (String(s.estado || '').toLowerCase() === 'inactivo') { throw new Error('ACCOUNT_INACTIVE'); }
                    const profile = { ...s, id, uid, email, role: 'socio' };
                    console.log(`Perfil encontrado: ${(s.nombres || s.nombre || s.usuario || email)} con Rol: socio`);
                    return profile;
                }
            }
            return null;
        }

        window.handleLogin = async () => {
            try { console.clear(); } catch(_) {}
            const raw = document.getElementById('login-user').value.trim();
            const pass = document.getElementById('login-pass').value.trim();
            const usuario = raw.toLowerCase();
            window.currentUser = null;
            currentUser = null;
            limpiarInterfaz();
            document.getElementById('login-error').classList.add('hidden');
            if (!usuario || !pass) {
                document.getElementById('login-error').classList.remove('hidden');
                return;
            }
            const email = `${usuario}@urbgloria.com`;
            try {
                await signInWithEmailAndPassword(auth, email, pass);
                try { await signInWithEmailAndPassword(authCaja, email, pass); } catch(_) {}
                const fbUser = auth.currentUser;
                console.log("Login exitoso para:", usuario);
                const perfil = await obtenerPerfil(fbUser);
                if (perfil) {
                    loginSuccess(perfil);
                } else {
                    const el = document.getElementById('login-error');
                    el.classList.remove('hidden');
                    el.innerText = "Usuario autenticado pero sin perfil asignado. Contacte al soporte";
                    await signOut(auth);
                    return;
                }
            } catch (e) {
                const el = document.getElementById('login-error');
                el.classList.remove('hidden');
                const inactive = e && ((e.message && e.message === 'ACCOUNT_INACTIVE') || (e.code && e.code === 'ACCOUNT_INACTIVE'));
                const tooMany = e && e.code === 'auth/too-many-requests';
                el.innerText = inactive
                    ? 'Acceso denegado: Tu cuenta se encuentra inactiva. Por favor, comunícate con la administración de la Cooperativa.'
                    : (tooMany ? 'Demasiados intentos de acceso. Espera unos minutos antes de volver a intentar.' : ((e && e.code) ? `Error de acceso: ${e.code}` : 'Credenciales inválidas o error de red'));
                try { await signOut(auth); } catch(_) {}
            }
        };

        onAuthStateChanged(auth, async (fbUser) => {
            if (fbUser) {
                try {
                    const perfil = await obtenerPerfil(fbUser);
                    if (perfil) loginSuccess(perfil);
                } catch (_) {}
            } else {
                window.currentUser = null;
                currentUser = null;
                const app = document.getElementById('app-content');
                const login = document.getElementById('login-screen');
                if (app) app.classList.add('hidden');
                if (login) login.classList.remove('hidden');
                const tbPadron = document.getElementById('tbody-padron');
                const tbCuotas = document.getElementById('tbody-cuotas');
                const tbCaja = document.getElementById('tbody-caja');
                if (tbPadron) tbPadron.innerHTML = '';
                if (tbCuotas) tbCuotas.innerHTML = '';
                if (tbCaja) tbCaja.innerHTML = '';
                const u = document.getElementById('login-user');
                const p = document.getElementById('login-pass');
                if (u) u.value = '';
                if (p) p.value = '';
            }
        });

        function limpiarInterfaz() {
            document.querySelectorAll('section').forEach(s => s.classList.add('hidden-section'));
            const adminNavIds = ['nav-padron','nav-cuotas','nav-caja','nav-asambleas','nav-votaciones','nav-sistema'];
            adminNavIds.forEach(id => { const el = document.getElementById(id); if (el) el.classList.add('hidden'); });
            const adminWidgets = document.getElementById('admin-dashboard-widgets');
            const socioWidgets = document.getElementById('socio-dashboard-widgets');
            if (adminWidgets) adminWidgets.classList.add('hidden');
            if (socioWidgets) socioWidgets.classList.add('hidden');
        }

        function removeAdminElements() {
            try {
                document.querySelectorAll('.admin-nav').forEach(el => el.parentNode && el.parentNode.removeChild(el));
                const ids = ['nav-padron','nav-cuotas','nav-caja','nav-asambleas','nav-votaciones','nav-sistema','admin-dashboard-widgets'];
                ids.forEach(id => { const el = document.getElementById(id); if (el && el.parentNode) el.parentNode.removeChild(el); });
                const sections = ['sec-padron','sec-cuotas','sec-caja','sec-asambleas','sec-sistema'];
                sections.forEach(id => { const el = document.getElementById(id); if (el && el.parentNode) el.parentNode.removeChild(el); });
            } catch(_) {}
        }

        function scheduleInactivity() {
            if (inactivityTimer) clearTimeout(inactivityTimer);
            inactivityTimer = setTimeout(() => {
                window.cerrarSesionCompleta && window.cerrarSesionCompleta('Sesión expirada por inactividad');
            }, INACTIVITY_MS);
        }
        function activityHandler() {
            if (!window.currentUser) return;
            scheduleInactivity();
        }
        function ensureActivityListeners() {
            if (activityListenersSet) return;
            ['mousemove','click','keydown','touchstart'].forEach(evt => {
                document.addEventListener(evt, activityHandler, { passive: true });
            });
            activityListenersSet = true;
        }
        async function waitForAuthUser(maxMs = 10000, intervalMs = 200) {
            const start = Date.now();
            return await new Promise((resolve) => {
                const timer = setInterval(() => {
                    if (auth.currentUser) {
                        clearInterval(timer);
                        resolve(auth.currentUser);
                    } else if (Date.now() - start >= maxMs) {
                        clearInterval(timer);
                        resolve(null);
                    }
                }, intervalMs);
            });
        }
        async function ensureCajaSubscription() {
            const fbUser = await waitForAuthUser();
            const allowed = !!(fbUser && currentUser && (currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.caja)));
            if (cajaUnsub) { try { cajaUnsub(); } catch(_) {} cajaUnsub = null; }
            if (!allowed) return;
            const qRef = ref(dbCaja, 'movimientos');
            console.log('[Caja] Conectado a DB secundaria');
            try {
                const snap = await get(qRef);
                console.log('[Caja] snapshot.exists():', snap.exists());
                if (snap.exists()) {
                    const data = snap.val();
                    allCajaMovs = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
                    const tb = document.getElementById('tbody-caja');
                    if (tb) tb.innerHTML = '';
                    try { renderCaja(); } catch(_) {}
                    try {
                        if (currentUser && (currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.cuotas))) {
                            renderCuotas();
                        }
                    } catch(_) {}
                }
            } catch (e) {
                console.error('[Caja] Error lectura inicial', e);
            }
            cajaUnsub = onValue(qRef, (snap) => {
                const data = snap.val();
                allCajaMovs = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
                const tb = document.getElementById('tbody-caja');
                if (tb) tb.innerHTML = '';
                try {
                    if (currentUser && (currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.caja))) {
                        renderCaja();
                        updateCharts();
                    }
                } catch(_) {}
                try {
                    if (currentUser && (currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.cuotas))) {
                        renderCuotas();
                        updateCharts();
                    }
                } catch(_) {}
            }, (err) => {
                console.error('[Caja] Error suscripción', err);
            });
        }

        function mostrarSeccionAdmin(user) {
            document.getElementById('admin-dashboard-widgets').classList.remove('hidden');
            document.getElementById('perfil-lote-container').classList.add('hidden');
            document.getElementById('perfil-piso-container').classList.add('hidden');
            if (user.role === 'root') {
                document.querySelectorAll('.admin-nav').forEach(el => el.classList.remove('hidden'));
                const ns = document.getElementById('nav-sistema'); if (ns) ns.classList.remove('hidden');
            } else {
                const p = user.permisos || {};
                if(p.padron) { const el = document.getElementById('nav-padron'); if (el) el.classList.remove('hidden'); }
                if(p.cuotas) { const el = document.getElementById('nav-cuotas'); if (el) el.classList.remove('hidden'); }
                if(p.caja) { const el = document.getElementById('nav-caja'); if (el) el.classList.remove('hidden'); }
                if(p.asambleas) { const el = document.getElementById('nav-asambleas'); if (el) el.classList.remove('hidden'); }
                if(p.votaciones) { const el = document.getElementById('nav-votaciones'); if (el) el.classList.remove('hidden'); }
            }
            showSection('dashboard');
        }

        function mostrarSeccionSocio() {
            removeAdminElements();
            const navPerfil = document.getElementById('nav-perfil'); if (navPerfil) navPerfil.classList.remove('hidden');
            document.getElementById('socio-dashboard-widgets').classList.remove('hidden');
            showSection('dashboard');
        }

        function loginSuccess(user) {
            try { console.clear(); } catch(_) {}
            currentUser = user;
            window.currentUser = user;
            document.getElementById('login-screen').classList.add('hidden');
            document.getElementById('app-content').classList.remove('hidden');
            document.getElementById('user-display').innerText = user.nombre || `${user.nombres || ''} ${user.apellidos || ''}`;
            console.log("Login exitoso para:", user.usuario || user.email || '');
            
            const badge = document.getElementById('role-badge');
            badge.innerText = user.role === 'root' ? 'Admin Raíz' : (user.role === 'admin' ? 'Administrador' : 'Socio');
            badge.className = (user.role === 'root' || user.role === 'admin')
                ? 'bg-red-600 text-white px-3 py-1 rounded-full text-xs font-bold'
                : 'bg-blue-600 text-white px-3 py-1 rounded-full text-xs font-bold';

            limpiarInterfaz();

            if (user.role === 'root' || user.role === 'admin') { 
                mostrarSeccionAdmin(user); 
                updateCharts();
            }
            else if (user.role === 'socio') { 
                mostrarSeccionSocio(); 
                ensureSocioStatusGuard(user);
            }

            window.scrollTo(0, 0);
            cargarDatosPerfil();
            initData();
            ensureActivityListeners();
            scheduleInactivity();
        }
        function ensureSocioStatusGuard(user) {
            try { if (socioStatusUnsub) { socioStatusUnsub(); socioStatusUnsub = null; } } catch(_) {}
            if (!user || user.role !== 'socio' || !user.id) return;
            socioStatusUnsub = onValue(ref(db, `socios/${user.id}`), (snap) => {
                const data = snap.val() || {};
                const estado = String(data.estado || '').toLowerCase();
                if (estado === 'inactivo') {
                    window.cerrarSesionCompleta && window.cerrarSesionCompleta('Acceso denegado: Tu cuenta se encuentra inactiva. Por favor, comunícate con la administración de la Cooperativa.');
                }
            }, (_) => {});
        }

        window.cerrarSesionCompleta = async (msg) => {
            try {
                const banner = document.createElement('div');
                banner.style.position = 'fixed';
                banner.style.top = '10px';
                banner.style.left = '50%';
                banner.style.transform = 'translateX(-50%)';
                banner.style.background = '#1f2937';
                banner.style.color = '#fff';
                banner.style.padding = '10px 16px';
                banner.style.borderRadius = '8px';
                banner.style.zIndex = '9999';
                banner.textContent = msg || 'Cerrando sesión...';
                document.body.appendChild(banner);
                setTimeout(() => { banner.remove(); }, 2000);
            } catch(_) {}
            try { if (inactivityTimer) clearTimeout(inactivityTimer); } catch(_) {}
            try { if (socioStatusUnsub) { socioStatusUnsub(); socioStatusUnsub = null; } } catch(_) {}
            try { await signOut(auth); } catch(_) {}
            window.currentUser = null;
            try { localStorage.clear(); sessionStorage.clear(); } catch(_) {}
            try { window.location.replace(window.location.href); } catch { window.location.reload(); }
        };
        window.logout = async () => {
            await window.cerrarSesionCompleta();
        };
        window.toggleSidebar = () => {
            const sidebar = document.getElementById('sidebar');
            if (!sidebar) return;
            sidebar.classList.toggle('hidden');
        };
        window.showSection = (id) => {
            document.querySelectorAll('section').forEach(s => s.classList.add('hidden-section'));
            const can = (perm) => currentUser && (currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos && currentUser.permisos[perm]));
            let allowed = true;
            if (id === 'padron') allowed = can('padron');
            if (id === 'cuotas') allowed = can('cuotas');
            if (id === 'caja') allowed = can('caja');
            if (id === 'asambleas') allowed = can('asambleas');
            if (id === 'votaciones') allowed = can('votaciones');
            if (id === 'sistema') allowed = currentUser && currentUser.role === 'root';
            if (!allowed) {
                document.getElementById('sec-dashboard').classList.remove('hidden-section');
                window.scrollTo(0, 0);
                return;
            }
            document.getElementById('sec-' + id).classList.remove('hidden-section');
            try {
                const menuButtons = document.querySelectorAll('#sidebar button');
                menuButtons.forEach(b => b.classList.remove('nav-active'));
                const activeBtn = document.getElementById('nav-' + id);
                if (activeBtn) activeBtn.classList.add('nav-active');
            } catch(_) {}
            if (id === 'padron') {
                try { ensurePadronFiltersBound(); } catch(_) {}
                try { renderPadron(); } catch(_) {}
            }
            if(id === 'caja') renderCaja();
            if(id === 'sistema') renderSistema();

            // Resetear scroll al cambiar de sección (especialmente en móvil)
            window.scrollTo(0, 0);

            // Cerrar el menú lateral automáticamente en móviles
            if (window.innerWidth < 768) {
                const sidebar = document.getElementById('sidebar');
                if (sidebar) sidebar.classList.add('hidden');
            }
        };

        // --- DATA INIT ---
        function initData() {
            onValue(ref(db, 'config'), (snap) => {
                configData = snap.val() || {};
                if(currentUser.role === 'socio') renderSocioDashboard();
            });
            onValue(ref(db, 'admins'), (snap) => {
                const data = snap.val();
                adminsData = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
                if(currentUser.role === 'root' && !document.getElementById('sec-sistema').classList.contains('hidden-section')) renderSistema();
            });
            onValue(ref(db, 'socios'), (snap) => {
                const data = snap.val();
                sociosData = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
                if(currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.padron)) {
                document.getElementById('dash-socios').innerText = sociosData.length;
                renderPadron();
                updateCharts();
            }
        });
        onValue(ref(db, 'cuotas'), (snap) => {
            const data = snap.val();
            cuotasData = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
            if(currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.cuotas)) renderCuotas();
            if(currentUser.role === 'socio') renderSocioDashboard();
            updateCharts();
        });
            onValue(ref(db, 'asambleas'), (snap) => {
                const data = snap.val();
                asambleasData = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
                if(currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.asambleas)) {
                    renderAsambleas();
                    const now = new Date();
                    const futuras = asambleasData
                        .map(a => {
                            const [yy, mm, dd] = String(a.fecha || '').split('-').map(n => parseInt(n, 10));
                            const [HH, MM] = String(a.hora || '00:00').split(':').map(n => parseInt(n, 10));
                            const dt = (isFinite(yy) && isFinite(mm) && isFinite(dd)) ? new Date(yy, (mm || 1) - 1, dd || 1, HH || 0, MM || 0) : null;
                            return { a, dt };
                        })
                        .filter(x => x.dt && x.dt.getTime() >= now.getTime())
                        .sort((x, y) => x.dt.getTime() - y.dt.getTime())
                        .map(x => x.a);
                    document.getElementById('dash-asamblea').innerText = futuras.length > 0 ? futuras[0].asunto : 'Sin programar';
                }
                if(currentUser.role === 'socio') renderSocioDashboard();
            });
            onValue(ref(db, 'votaciones'), (snap) => {
                const data = snap.val();
                votacionesData = data ? Object.entries(data).map(([id, val]) => ({ id, ...val })) : [];
                if(currentUser.role === 'root' || (currentUser.role === 'admin' && currentUser.permisos?.votaciones)) renderVotaciones();
                if(currentUser.role === 'socio') renderSocioDashboard();
            });
            ensureCajaSubscription();
            if (currentUser.role === 'socio') ensureCajaSubscriptionForSocio();
        }
        async function ensureCajaSubscriptionForSocio() {
            if (cajaSocioUnsub) { try { cajaSocioUnsub(); } catch(_) {} cajaSocioUnsub = null; }
            const qRef = ref(dbCaja, 'movimientos');
            try {
                const snap = await get(qRef);
                if (snap.exists()) {
                    const data = snap.val() || {};
                    const all = Object.entries(data).map(([id, val]) => ({ id, ...val }));
                    socioCajaMovs = all.filter(m => m && m.esCuota && m.cuotaOriginal && m.cuotaOriginal.socioId === (currentUser && currentUser.id));
                    renderSocioDashboard();
                } else {
                    socioCajaMovs = [];
                }
            } catch(_) {}
            cajaSocioUnsub = onValue(qRef, (snap) => {
                const data = snap.val() || {};
                const all = Object.entries(data).map(([id, val]) => ({ id, ...val }));
                socioCajaMovs = all.filter(m => m && m.esCuota && (
                    (m.cuotaOriginal && m.cuotaOriginal.socioId === (currentUser && currentUser.id)) ||
                    (m.socioId && m.socioId === (currentUser && currentUser.id))
                ));
                renderSocioDashboard();
            }, (_) => {});
        }

        window.syncSociosToAuth = async () => {
            const snap = await get(ref(db, 'socios'));
            if (!snap.exists()) {
                console.log("[0/0] No hay socios que sincronizar");
                return;
            }
            const all = snap.val() || {};
            const entries = Object.entries(all);
            const total = entries.length;
            let tempApp;
            let tempAuth;
            try { tempApp = initializeApp(firebaseConfigPrincipal, 'signupApp'); } catch(_) { tempApp = initializeApp(firebaseConfigPrincipal, 'signupApp2'); }
            tempAuth = getAuth(tempApp);
            try { await setPersistence(tempAuth, browserLocalPersistence); } catch(_) {}
            let idx = 0;
            for (const [id, datos] of entries) {
                idx++;
                const usuario = String((datos && datos.usuario) || '').trim().toLowerCase();
                if (!usuario) {
                    console.warn(`[${idx}/${total}] Sin usuario en registro ${id}, omitiendo`);
                    continue;
                }
                let pass = String((datos && datos.password) || '').trim();
                if (pass.length < 6) {
                    pass = pass.padEnd(6, '0');
                    try { 
                        await update(ref(db, `socios/${id}`), { password: pass }); 
                        console.log(`[Update DB] Password actualizado para ${usuario}.`);
                    } catch(_) {}
                    console.warn(`[${idx}/${total}] ${usuario}: contraseña corta. Ajustada a "${pass}"`);
                }
                const email = `${usuario}@urbgloria.com`;
                let uid = (datos && datos.uid) || null;
                if (!uid) {
                    try {
                        const cred = await createUserWithEmailAndPassword(tempAuth, email, pass);
                        uid = cred && cred.user ? cred.user.uid : null;
                    } catch (e) {
                        if (e && e.code === 'auth/email-already-in-use') {
                            try {
                                const cred2 = await signInWithEmailAndPassword(tempAuth, email, pass);
                                uid = cred2 && cred2.user ? cred2.user.uid : null;
                            } catch (e2) {
                                console.warn(`[${idx}/${total}] ${usuario}: email ya existe pero no se pudo obtener UID (${e2 && e2.code ? e2.code : 'desconocido'})`);
                            }
                        } else {
                            console.warn(`[${idx}/${total}] ${usuario}: error al crear (${e && e.code ? e.code : 'desconocido'})`);
                        }
                    }
                }
                if (uid) {
                    try { await update(ref(db, `socios/${id}`), { uid, email }); } catch(_) {}
                    console.log(`[${idx}/${total}] Sincronizando: ${usuario}... OK`);
                } else {
                    console.log(`[${idx}/${total}] Sincronizando: ${usuario}... SKIP`);
                }
            }
            try { await signOut(tempAuth); } catch(_) {}
        };

        // Herramienta temporal: corrige contraseñas cortas directamente en la base de datos
        window.fixShortPasswordsInDB = async () => {
            const snap = await get(ref(db, 'socios'));
            if (!snap.exists()) {
                console.log("[0/0] No hay socios que corregir");
                return;
            }
            const all = snap.val() || {};
            const entries = Object.entries(all);
            const total = entries.length;
            let idx = 0;
            for (const [id, datos] of entries) {
                idx++;
                const usuario = String((datos && datos.usuario) || '').trim().toLowerCase();
                let pass = String((datos && datos.password) || '').trim();
                if (!usuario) {
                    console.warn(`[${idx}/${total}] Sin usuario en registro ${id}, omitiendo`);
                    continue;
                }
                if (pass.length < 6) {
                    const newPass = pass.padEnd(6, '0');
                    try { 
                        await update(ref(db, `socios/${id}`), { password: newPass });
                        console.log(`[Update DB] Password actualizado para ${usuario}.`);
                    } catch (e) {
                        console.warn(`[${idx}/${total}] ${usuario}: error al actualizar password en DB (${e && e.code ? e.code : 'desconocido'})`);
                    }
                }
            }
            console.log("Corrección de contraseñas cortas finalizada.");
        };
        window.fixAdminUID = async () => {
            const fbUser = auth.currentUser;
            if (!fbUser) { showToast("No autenticado", "error"); return; }
            try {
                await update(ref(db, 'admins/root'), { uid: fbUser.uid });
                console.log(`[Root] UID actualizado: ${fbUser.uid}`);
                showToast("UID de administrador actualizado", "success");
            } catch (e) {
                console.error("[Root] Error actualizando UID", e);
                showToast("Error al actualizar UID de administrador", "error");
            }
        };

        // --- PERFIL: CAMBIO DE CONTRASEÑA (SOCIOS/ADMINS) ---
        // Uso: window.actualizarPassword(contraseñaActual, nuevaContraseña)
        window.actualizarPassword = async (passActual, passNueva) => {
            try {
                const user = auth.currentUser;
                if (!user) {
                    showToast("Sesión no encontrada", "error");
                    return;
                }
                if (!passNueva) { showToast("Ingresa la nueva contraseña", "warning"); return; }
                if (passNueva.length < 8) {
                    showToast("La contraseña debe tener al menos 8 caracteres.", "warning");
                    return;
                }
                const email = user.email || `${String((window.currentUser && window.currentUser.usuario) || '').toLowerCase()}@urbgloria.com`;
                try {
                    const cred = EmailAuthProvider.credential(email, passActual || '');
                    await reauthenticateWithCredential(user, cred);
                } catch (e) {
                    if (e && e.code === 'auth/requires-recent-login') {
                        showToast("Reautenticación necesaria por seguridad", "info");
                        return;
                    }
                    showToast("Contraseña actual incorrecta", "error");
                    return;
                }
                await updatePassword(user, passNueva);
                // Sincronizar contraseña en DB principal según rol
                if (window.currentUser && window.currentUser.id) {
                    const targetPath = (window.currentUser.role === 'root' || window.currentUser.role === 'admin') ? `admins/${window.currentUser.id}` : `socios/${window.currentUser.id}`;
                    try { await update(ref(db, targetPath), { password: passNueva }); } catch(_) {}
                }
                showToast("Contraseña actualizada correctamente", "success");
            } catch (e) {
                if (e && e.code === 'auth/requires-recent-login') {
                    showToast("Reautenticación necesaria por seguridad", "info");
                    return;
                }
                try { await signOut(auth); } catch(_) {}
                showToast("Contraseña actualizada, inicia sesión de nuevo", "success");
                location.reload();
            }
        };
        // UI handler para el formulario de cambio de contraseña
        window.guardarNuevaPassword = async () => {
            const msg = document.getElementById('cp-msg');
            const btn = document.getElementById('cp-btn');
            const a = (document.getElementById('cp-actual') || { value: '' }).value.trim();
            const n1 = (document.getElementById('cp-nueva') || { value: '' }).value.trim();
            const n2 = (document.getElementById('cp-confirm') || { value: '' }).value.trim();
            msg.innerText = '';
            msg.className = 'text-sm';
            if (!n1) { msg.innerText = "Ingresa la nueva contraseña."; msg.classList.add('text-red-600'); return; }
            if (n1.length < 6) n1 = n1.padEnd(6, '0');
            if (n1 !== n2) {
                msg.innerText = "Las contraseñas nuevas no coinciden.";
                msg.classList.add('text-red-600');
                return;
            }
            btn.disabled = true;
            btn.innerText = "Guardando...";
            try {
                await window.actualizarPassword(a, n1);
                // Si no hubo reload, mostrar éxito
                msg.innerText = "Contraseña actualizada.";
                msg.classList.remove('text-red-600');
                msg.classList.add('text-green-600');
                (document.getElementById('cp-actual') || {}).value = '';
                (document.getElementById('cp-nueva') || {}).value = '';
                (document.getElementById('cp-confirm') || {}).value = '';
            } catch (_) {
                msg.innerText = "No se pudo actualizar la contraseña. Intenta nuevamente.";
                msg.classList.add('text-red-600');
            } finally {
                btn.disabled = false;
                btn.innerText = "Guardar nueva contraseña";
            }
        };

        window.generarInformeCajaPDF = async (mesFiltro, btn) => {
            if (btn) btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Cargando...';
            let datos = [];
            try {
                const snap = await get(ref(dbCaja, 'movimientos'));
                if (snap.exists()) {
                    const raw = snap.val() || {};
                    datos = Object.entries(raw).map(([id, v]) => ({ id, ...v }));
                }
            } catch (_) {}
            const filtrados = datos.filter(m => m && m.fecha && String(m.fecha).startsWith(mesFiltro));
            let totalIngresos = 0, totalEgresos = 0;
            const ingresosCuota = [];
            const ingresosOtros = [];
            const egresosDetalle = [];
            filtrados.forEach(m => {
                const monto = Number(m.monto) || 0;
                const tipo = String(m.tipo || '').toLowerCase();
                if (tipo === 'ingreso') {
                    totalIngresos += monto;
                    const desc = (m.descripcion || '').trim();
                    const esCuota = !!m.esCuota || /^Cobro cuota:/i.test(desc);
                    if (esCuota) ingresosCuota.push(m);
                    else ingresosOtros.push(m);
                } else {
                    totalEgresos += monto;
                    egresosDetalle.push(m);
                }
            });
            const cuotasAgr = new Map();
            ingresosCuota.forEach(d => {
                const raw = (d.descripcion || '').trim();
                let concepto = '';
                const m = raw.match(/Cobro cuota:\s*([^-\n]+)/i);
                if (m && m[1]) concepto = m[1].trim();
                if (!concepto) concepto = 'Cuota';
                const key = concepto.toLowerCase();
                const prev = cuotasAgr.get(key) || { concepto, monto: 0 };
                prev.monto += Number(d.monto) || 0;
                cuotasAgr.set(key, prev);
            });
            const ingresosCuotaAgrupados = Array.from(cuotasAgr.values()).sort((a, b) => a.concepto.localeCompare(b.concepto));
            ingresosOtros.sort((a, b) => String(a.fecha).localeCompare(String(b.fecha)));
            egresosDetalle.sort((a, b) => String(a.fecha).localeCompare(String(b.fecha)));
            const saldoFinal = totalIngresos - totalEgresos;
            const now = new Date();
            const pad = (n) => String(n).padStart(2, '0');
            const fechaGen = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()}, ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
            const partes = String(mesFiltro || '').split('-');
            let periodoFmt = mesFiltro;
            if (partes.length >= 2) periodoFmt = `${pad(partes[1])}-${partes[0]}`;
            if (btn) btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generando...';

            const { jsPDF } = window.jspdf || {};
            if (!jsPDF) return;
            const doc = new jsPDF({ unit: 'pt', format: 'letter', orientation: 'portrait' });
            const margin = 36;
            const pageWidth = doc.internal.pageSize.getWidth();
            const pageHeight = doc.internal.pageSize.getHeight();
            let y = margin;
            doc.setFontSize(14);
            doc.text('COOPERATIVA GLORIA Nº 4', pageWidth / 2, y, { align: 'center' });
            y += 18;
            doc.setFontSize(12);
            doc.text('INFORME MENSUAL DE CAJA', pageWidth / 2, y, { align: 'center' });
            y += 14;
            doc.setFontSize(10);
            doc.text(`Periodo: ${periodoFmt}`, pageWidth / 2, y, { align: 'center' });
            y += 16;
            y += 12;
            doc.setFontSize(10);
            const labelIng = 'Total Ingresos:';
            const labelEgr = 'Total Egresos:';
            const labelSal = 'Saldo Final:';
            const valIng = `S/ ${totalIngresos.toFixed(2)}`;
            const valEgr = `S/ ${totalEgresos.toFixed(2)}`;
            const valSal = `S/ ${saldoFinal.toFixed(2)}`;
            const wLabelIng = doc.getTextWidth(labelIng);
            const wLabelEgr = doc.getTextWidth(labelEgr);
            const wLabelSal = doc.getTextWidth(labelSal);
            const wValIng = doc.getTextWidth(valIng);
            const wValEgr = doc.getTextWidth(valEgr);
            const wValSal = doc.getTextWidth(valSal);
            const spacing = 8;
            const lineWIng = wLabelIng + spacing + wValIng;
            const lineWEgr = wLabelEgr + spacing + wValEgr;
            const lineWSal = wLabelSal + spacing + wValSal;
            const maxLineW = Math.max(lineWIng, lineWEgr, lineWSal);
            const padBox = 8;
            const boxWidth = maxLineW + padBox * 2;
            const boxLeft = (pageWidth - boxWidth) / 2;
            const boxTop = y - 10;
            let lineXBase = boxLeft + padBox + (maxLineW - lineWIng) / 2;
            doc.setFont(undefined, 'normal');
            doc.text(labelIng, lineXBase, y);
            doc.setFont(undefined, 'bold');
            doc.setTextColor(40, 167, 69);
            doc.text(valIng, lineXBase + wLabelIng + spacing, y);
            doc.setTextColor(0, 0, 0);
            y += 14;
            lineXBase = boxLeft + padBox + (maxLineW - lineWEgr) / 2;
            doc.setFont(undefined, 'normal');
            doc.text(labelEgr, lineXBase, y);
            doc.setFont(undefined, 'bold');
            doc.setTextColor(220, 53, 69);
            doc.text(valEgr, lineXBase + wLabelEgr + spacing, y);
            doc.setTextColor(0, 0, 0);
            y += 14;
            lineXBase = boxLeft + padBox + (maxLineW - lineWSal) / 2;
            doc.setFont(undefined, 'normal');
            doc.text(labelSal, lineXBase, y);
            doc.setFont(undefined, 'bold');
            doc.setTextColor(0, 64, 133);
            doc.text(valSal, lineXBase + wLabelSal + spacing, y);
            doc.setTextColor(0, 0, 0);
            const boxHeight = (y + 6) - boxTop;
            doc.setLineWidth(0.8);
            doc.rect(boxLeft, boxTop, boxWidth, boxHeight);
            y += 18;
            doc.setFontSize(11);
            doc.text('Detalle de Ingresos', margin, y);
            y += 8;
            const headIng = [['Fecha','Descripción','Responsable','Monto']];
            const bodyIng = [
                ...ingresosCuotaAgrupados.map(d => ['-', d.concepto ? ('Cobro cuota: ' + d.concepto) : 'Cobro cuota', 'Consolidado Sistema', `S/ ${(Number(d.monto) || 0).toFixed(2)}`]),
                ...ingresosOtros.map(d => [d.fecha || '-', d.descripcion || '-', d.registradoPor || '-', `S/ ${(Number(d.monto) || 0).toFixed(2)}`])
            ];
            if (typeof doc.autoTable === 'function') {
                doc.autoTable({
                    head: headIng,
                    body: bodyIng,
                    startY: y,
                    styles: { fontSize: 9, cellPadding: 4, lineColor: [0,0,0], lineWidth: 0.4 },
                    headStyles: { fillColor: [15, 63, 34], textColor: 255 },
                    columnStyles: { 3: { halign: 'right' } },
                    theme: 'grid',
                    margin: { left: margin, right: margin }
                });
                y = (doc.lastAutoTable && doc.lastAutoTable.finalY) ? doc.lastAutoTable.finalY + 14 : y + 14;
            }
            doc.setFontSize(11);
            doc.text('Detalle de Egresos', margin, y);
            y += 8;
            const headEgr = [['Fecha','Descripción','Responsable','Monto']];
            const bodyEgr = egresosDetalle.map(d => [d.fecha || '-', d.descripcion || '-', d.registradoPor || '-', `S/ ${(Number(d.monto) || 0).toFixed(2)}`]);
            if (typeof doc.autoTable === 'function') {
                doc.autoTable({
                    head: headEgr,
                    body: bodyEgr,
                    startY: y,
                    styles: { fontSize: 9, cellPadding: 4, lineColor: [0,0,0], lineWidth: 0.4 },
                    headStyles: { fillColor: [15, 63, 34], textColor: 255 },
                    columnStyles: { 3: { halign: 'right' } },
                    theme: 'grid',
                    margin: { left: margin, right: margin }
                });
                y = (doc.lastAutoTable && doc.lastAutoTable.finalY) ? doc.lastAutoTable.finalY + 24 : y + 24;
            }
            y += 28;
            doc.setDrawColor(51,51,51);
            doc.line(pageWidth/2 - 120, y, pageWidth/2 + 120, y);
            y += 16;
            doc.setFontSize(10);
            doc.text('FIRMA DEL TESORERO / ADMINISTRADOR', pageWidth / 2, y, { align: 'center' });
            doc.setFont(undefined, 'normal');
            doc.setFontSize(9);
            const total = doc.internal.getNumberOfPages();
            const stInf = (configData && configData.informeCaja) ? configData.informeCaja : {};
            const publicadoTexto = (stInf && stInf.mes === mesFiltro && stInf.publicadoEn)
                ? `Publicado el: ${stInf.publicadoEn}`
                : 'Informe aún no publicado';
            const generadoTexto = `Generado el: ${fechaGen}`;
            for (let i = 1; i <= total; i++) {
                doc.setPage(i);
                doc.text(publicadoTexto, margin, pageHeight - 24);
                doc.text(`Página ${i} de ${total}`, pageWidth / 2, pageHeight - 24, { align: 'center' });
                doc.text(generadoTexto, pageWidth - margin, pageHeight - 24, { align: 'right' });
            }
            doc.save(`Informe_Caja_${mesFiltro}.pdf`);
        };

        window.descargarInformePDFSocio = async (event, mesFiltro) => {
            const btn = event && event.currentTarget ? event.currentTarget : null;
            const original = btn ? btn.innerHTML : '';
            try {
                if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Cargando...'; }
                await window.generarInformeCajaPDF(mesFiltro, btn);
            } finally {
                if (btn) { btn.disabled = false; btn.innerHTML = original || 'Descargar PDF'; }
            }
        };

        // --- DASHBOARD SOCIO ---
        function renderSocioDashboard() {
            if(currentUser.role !== 'socio') return;

            const misPendientes = cuotasData.filter(c => c.socioId === currentUser.id && (c.estado ? c.estado === 'PENDIENTE' : true));
            const deuda = misPendientes.reduce((acc, c) => acc + parseFloat(c.monto), 0);
            const isMoroso = deuda > 0;
            
            document.getElementById('socio-deuda').innerText = `S/ ${deuda.toFixed(2)}`;
            const pagos = (socioCajaMovs && socioCajaMovs.length > 0)
                ? socioCajaMovs
                : allCajaMovs.filter(m => m.esCuota && (
                    (m.cuotaOriginal && m.cuotaOriginal.socioId === currentUser.id) ||
                    (m.socioId && m.socioId === currentUser.id)
                ));
            let pendientesHTML = '';
            misPendientes.forEach(c => {
                const monto = Number(c.monto) || 0;
                const concepto = c.concepto || 'Cuota';
                const fecha = c.fecha || '-';
                pendientesHTML += `
                <div class="flex justify-between items-center border border-red-200 rounded-lg p-2 bg-red-50">
                    <div>
                        <p class="font-bold text-slate-700">${concepto}</p>
                        <p class="text-[10px] text-gray-500">Emitido: ${fecha}</p>
                    </div>
                    <span class="font-bold text-[10px] px-2 py-1 rounded-full bg-red-100 text-red-700">S/ ${monto.toFixed(2)} · PENDIENTE</span>
                </div>`;
            });
            document.getElementById('socio-pendientes').innerHTML = (pendientesHTML !== '') ? pendientesHTML : '<p class="text-xs text-gray-500 italic">No tienes cuotas pendientes.</p>';

            const infConfig = configData.informeCaja || {};
            const infWidget = document.getElementById('socio-informe-caja');
            if (infConfig.publicadoEn) {
                infWidget.classList.remove('hidden');
                document.getElementById('socio-informe-texto').innerText = `Mes publicado: ${infConfig.mes}`;
                
                if (isMoroso && !infConfig.permitirMorosos) {
                    document.getElementById('socio-informe-accion').innerHTML = `<span class="text-xs font-bold text-red-600 bg-red-100 px-3 py-2 rounded-lg"><i class="fas fa-lock"></i> Regularice su deuda para descargar</span>`;
                } else {
                    document.getElementById('socio-informe-accion').innerHTML = `<button onclick="descargarInformePDFSocio(event, '${infConfig.mes}')" class="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg text-sm font-bold shadow transition flex items-center gap-2"><i class="fas fa-file-pdf"></i> Descargar PDF</button>`;
                }
            } else {
                infWidget.classList.add('hidden');
            }

            let historialHTML = '';
            pagos.forEach(p => {
                const recibo = p.numeroRecibo || '-';
                historialHTML += `<div class="flex justify-between items-center border-b pb-2">
                    <div>
                        <p class="font-bold text-slate-700">${p.cuotaOriginal && p.cuotaOriginal.concepto ? p.cuotaOriginal.concepto : (p.descripcion || 'Cuota')}</p>
                        <p class="text-[10px] text-gray-500">Pagado el: ${p.fecha} · Recibo Nº: ${recibo}</p>
                    </div>
                    <span class="font-bold text-[10px] px-2 py-1 rounded-full bg-emerald-100 text-emerald-700">S/ ${parseFloat(p.monto).toFixed(2)} - PAGADO</span>
                </div>`;
            });
            document.getElementById('socio-historial').innerHTML = (historialHTML !== '') ? historialHTML : '<p class="text-xs text-gray-500 italic">No tienes cuotas pagadas.</p>';

            const now = new Date();
            const futuras = asambleasData
                .map(a => {
                    const [yy, mm, dd] = String(a.fecha || '').split('-').map(n => parseInt(n, 10));
                    const [HH, MM] = String(a.hora || '00:00').split(':').map(n => parseInt(n, 10));
                    const dt = (isFinite(yy) && isFinite(mm) && isFinite(dd)) ? new Date(yy, (mm || 1) - 1, dd || 1, HH || 0, MM || 0) : null;
                    return { a, dt };
                })
                .filter(x => x.dt && x.dt.getTime() >= now.getTime())
                .sort((x, y) => x.dt.getTime() - y.dt.getTime())
                .map(x => x.a);
            document.getElementById('socio-asambleas').innerHTML = futuras.length > 0 ? futuras.map(a =>
                `<div class="border-b border-amber-200 pb-2"><span class="font-bold text-amber-900 text-sm">${a.asunto}</span><br><span class="text-xs text-amber-700"><i class="far fa-calendar-alt"></i> ${a.fecha} a las ${a.hora} · <i class="fas fa-map-marker-alt"></i> ${a.lugar || '-'}</span></div>`
            ).join('') : '<p class="text-xs text-amber-700 italic">No hay asambleas próximas.</p>';

            const vContainer = document.getElementById('socio-votaciones');
            vContainer.innerHTML = '';
            if (votacionesData.length === 0) {
                vContainer.innerHTML = '<p class="text-xs text-indigo-700 italic">No hay votaciones activas.</p>';
            } else {
                votacionesData.forEach(v => {
                    const votos = v.votos || {};
                    const yaVoto = votos[currentUser.id];
                    let uiVoto = '';
                    if (yaVoto) uiVoto = `<div class="w-full text-center py-2 bg-indigo-100 text-indigo-800 font-bold rounded-lg border border-indigo-200 shadow-inner"><i class="fas fa-check-circle"></i> Voto Registrado (${yaVoto})</div>`;
                    else if (v.cerrada) uiVoto = `<div class="w-full text-center py-2 bg-red-100 text-red-700 font-bold rounded-lg border border-red-200 shadow-inner"><i class="fas fa-lock"></i> Votación cerrada</div>`;
                    else if ((configData.votaciones && configData.votaciones.restringirMorosos) && isMoroso) uiVoto = `<div class="space-y-2"><div class="flex gap-2"><button disabled class="flex-1 py-1.5 rounded border border-indigo-300 text-indigo-700 text-sm font-bold bg-white">SÍ</button><button disabled class="flex-1 py-1.5 rounded border border-rose-300 text-rose-700 text-sm font-bold bg-white">NO</button></div><div class="w-full text-center py-2 bg-red-100 text-red-700 font-bold rounded-lg border border-red-200 text-xs"><i class="fas fa-ban"></i> Voto restringido por cuotas pendientes. Regulariza tu situación para participar.</div></div>`;
                    else uiVoto = `<div class="flex gap-2"><button onclick="votar('${v.id}', 'SI')" class="flex-1 py-1.5 rounded border border-indigo-300 text-indigo-700 text-sm font-bold transition hover:bg-indigo-600 hover:text-white">SÍ</button><button onclick="votar('${v.id}', 'NO')" class="flex-1 py-1.5 rounded border border-rose-300 text-rose-700 text-sm font-bold transition hover:bg-rose-600 hover:text-white">NO</button></div>`;
                    vContainer.innerHTML += `<div class="border border-indigo-200 rounded-lg p-3 bg-white shadow-sm"><p class="font-bold text-sm mb-3 text-indigo-900">${v.pregunta}</p>${uiVoto}</div>`;
                });
            }
        }

        // --- PERFIL ---
        function cargarDatosPerfil() {
            document.getElementById('p-nom').value = currentUser.nombres || currentUser.nombre || '';
            document.getElementById('p-ape').value = currentUser.apellidos || '';
            document.getElementById('p-lote').value = currentUser.lote || '';
            document.getElementById('p-piso').value = currentUser.piso || '';
            document.getElementById('p-tel').value = currentUser.telefono || '';
            document.getElementById('p-email').value = currentUser.email || '';
            document.getElementById('p-usu').value = currentUser.usuario || '';
            const pp = document.getElementById('p-pass'); if (pp) pp.value = '';
            const isSocio = currentUser.role === 'socio';
            const pisoEl = document.getElementById('p-piso');
            const emailEl = document.getElementById('p-email');
            if (pisoEl) {
                if (isSocio) {
                    pisoEl.disabled = true;
                    pisoEl.classList.remove('bg-slate-50');
                    pisoEl.classList.add('bg-gray-100','cursor-not-allowed');
                    pisoEl.title = 'Contacte a la directiva para cambiar su piso';
                } else {
                    pisoEl.disabled = false;
                    pisoEl.classList.remove('bg-gray-100','cursor-not-allowed');
                    pisoEl.classList.add('bg-slate-50');
                    pisoEl.title = '';
                }
            }
            if (emailEl) {
                if (isSocio) {
                    emailEl.disabled = true;
                    emailEl.classList.remove('bg-slate-50');
                    emailEl.classList.add('bg-gray-100','cursor-not-allowed');
                    emailEl.title = 'Contacte a la directiva para cambiar su email';
                } else {
                    emailEl.disabled = false;
                    emailEl.classList.remove('bg-gray-100','cursor-not-allowed');
                    emailEl.classList.add('bg-slate-50');
                    emailEl.title = '';
                }
            }
        }

        window.guardarPerfil = async () => {
            const isSocio = currentUser.role === 'socio';
            const data = { telefono: document.getElementById('p-tel').value };
            if (!isSocio) data.email = document.getElementById('p-email').value;
            if (currentUser.role === 'root' || currentUser.role === 'admin') data.nombre = document.getElementById('p-nom').value;
            else { data.nombres = document.getElementById('p-nom').value; data.apellidos = document.getElementById('p-ape').value; }
            const targetPath = (currentUser.role === 'root' || currentUser.role === 'admin') ? `admins/${currentUser.id}` : `socios/${currentUser.id}`;
            const email = (currentUser.email && String(currentUser.email)) || `${String(currentUser.usuario || '').toLowerCase()}@urbgloria.com`;
            const passNuevaRaw = (document.getElementById('p-pass')?.value || '').trim();
            const passNueva = passNuevaRaw ? (passNuevaRaw.length < 6 ? passNuevaRaw.padEnd(6, '0') : passNuevaRaw) : '';
            try {
                await update(ref(db, targetPath), data);
                Object.assign(currentUser, data);
                document.getElementById('user-display').innerText = currentUser.nombre || `${currentUser.nombres} ${currentUser.apellidos}`;
            } catch(e) { showToast("Error al actualizar el perfil.", "error"); return; }
            if (!passNueva) { showToast("Perfil actualizado correctamente", "success"); return; }
            let tempApp, tempAuth;
            try { tempApp = initializeApp(firebaseConfigPrincipal, 'perfilResetAuth'); } catch(_) { tempApp = initializeApp(firebaseConfigPrincipal, 'perfilResetAuth2'); }
            tempAuth = getAuth(tempApp);
            try {
                let passActual = String(currentUser.password || '');
                if (!passActual) {
                    try { const snap = await get(ref(db, targetPath)); const val = snap.val() || {}; passActual = String(val.password || ''); } catch(_) {}
                }
                const u = auth.currentUser;
                if (u) {
                    try {
                        if (passActual) {
                            try { const cred = EmailAuthProvider.credential(email, passActual); await reauthenticateWithCredential(u, cred); } catch(_) {}
                        }
                        await deleteUser(u);
                    } catch(_) {}
                }
            } catch(_) {}
            let newUid = null;
            try {
                const cred = await createUserWithEmailAndPassword(tempAuth, email, passNueva);
                newUid = cred && cred.user ? cred.user.uid : null;
            } catch (e) {
                if (e && e.code === 'auth/email-already-in-use') {
                    try { const cred2 = await signInWithEmailAndPassword(tempAuth, email, passNueva); newUid = cred2 && cred2.user ? cred2.user.uid : null; } catch(_) {}
                }
            }
            try { await signOut(tempAuth); } catch(_) {}
            if (newUid) {
                try { await signInWithEmailAndPassword(auth, email, passNueva); } catch(_) {}
            }
            try { await update(ref(db, targetPath), { password: passNueva, ...(newUid ? { uid: newUid } : {}) }); } catch(_) {}
            const pp = document.getElementById('p-pass'); if (pp) pp.value = '';
            showToast("Datos de acceso actualizados", "success");
        };

        // --- PADRON ---
        function ensurePadronFiltersBound() {
            const s = document.getElementById('padron-search-socio');
            const m = document.getElementById('padron-search-manz');
            if (s && !s.dataset.bound) {
                s.dataset.bound = '1';
                s.addEventListener('input', () => { renderPadron(); });
            }
            if (m && !m.dataset.bound) {
                m.dataset.bound = '1';
                m.addEventListener('input', () => { m.value = m.value.toUpperCase(); renderPadron(); });
            }
        }
        function renderPadron() {
            ensurePadronFiltersBound();
            const tbody = document.getElementById('tbody-padron');
            tbody.innerHTML = '';
            const normalize = (str) => String(str || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
            const qSocio = normalize((document.getElementById('padron-search-socio')?.value || '').trim());
            const qManz = (document.getElementById('padron-search-manz')?.value || '').trim().toUpperCase().replace(/[^A-Z]/g, '');
            const parseLote = (txt) => {
                const t = String(txt || '').toUpperCase().trim();
                // Busca patrón de manzana y número: A-10, A 10, MZ A LOTE 5, etc.
                let manzana = '', numero = 0;
                const m1 = t.match(/([A-Z]+)\s*[- ]\s*(\d+)/); // A-10 o A 10
                const m2 = t.match(/MZ\s*([A-Z]+).*?(\d+)/);    // MZ A ... 5
                if (m1) { manzana = m1[1]; numero = parseInt(m1[2], 10) || 0; }
                else if (m2) { manzana = m2[1]; numero = parseInt(m2[2], 10) || 0; }
                else {
                    const m3 = t.match(/([A-Z]+)/); const m4 = t.match(/(\d+)/);
                    manzana = m3 ? m3[1] : ''; numero = m4 ? parseInt(m4[1], 10) || 0 : 0;
                }
                return { manzana, numero, raw: t };
            };
            let list = [...sociosData];
            list.sort((a,b) => {
                const A = parseLote(a.lote), B = parseLote(b.lote);
                if (A.manzana !== B.manzana) return A.manzana.localeCompare(B.manzana);
                return A.numero - B.numero;
            });
            if (qSocio) {
                list = list.filter(s => normalize(s.nombres).includes(qSocio) || normalize(s.apellidos).includes(qSocio));
            }
            if (qManz) {
                list = list.filter(s => {
                    const p = parseLote(s.lote);
                    return p.manzana.startsWith(qManz);
                });
            }
            list.forEach((s, index) => {
                const tr = document.createElement('tr');
                tr.className = "border-b hover:bg-gray-50 text-sm transition";
                tr.innerHTML = `<td class="p-4">${index + 1}</td><td class="p-4 font-bold text-slate-700">${s.apellidos}, ${s.nombres}</td><td class="p-4">${s.lote || 'N/A'}</td><td class="p-4">${s.piso || '-'}</td><td class="p-4"><span class="${s.estado === 'inactivo' ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'} px-2 py-1 rounded-full text-[10px] font-bold">${s.estado === 'inactivo' ? 'INACTIVO' : 'ACTIVO'}</span></td><td class="p-4"><button onclick="toggleEstadoSocio('${s.id}', '${s.estado}')" class="text-xs font-bold text-blue-600 hover:underline">Cambiar Estado</button></td><td class="p-4"><button onclick="resetCredSocio('${s.id}')" class="bg-amber-500 hover:bg-amber-600 text-white px-3 py-1.5 rounded text-xs font-bold transition shadow-sm"><i class="fas fa-rotate"></i> Resetear</button></td>`;
                tbody.appendChild(tr);
            });
        }
        window.toggleEstadoSocio = async (id, currentStatus) => await update(ref(db, `socios/${id}`), { estado: currentStatus === 'inactivo' ? 'activo' : 'inactivo' });
        window.resetCredSocio = async (id) => {
            const s = sociosData.find(x => x.id === id);
            if (!s) { showToast("Socio no encontrado", "error"); return; }
            const nom = String(s.nombres || '').trim();
            const ape = String(s.apellidos || '').trim();
            if (!nom || !ape) { showToast("Datos de socio incompletos", "warning"); return; }
            let usuarioNuevo = (nom.charAt(0) + ape.split(' ')[0]).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]/g, '');
            if (usuarioNuevo.length < 6) usuarioNuevo = usuarioNuevo.padEnd(6, '0');
            const emailNuevo = `${usuarioNuevo}@urbgloria.com`;
            const passNuevo = usuarioNuevo;
            const emailActual = `${String(s.usuario || '').toLowerCase()}@urbgloria.com`;
            const passActual = String(s.password || '').trim();
            const body = `<div class="space-y-3"><p class="text-sm">Se resetearán las credenciales del socio:</p><p class="text-sm font-bold">${s.apellidos}, ${s.nombres}</p><div class="grid grid-cols-1 gap-2 text-sm"><div class="bg-gray-50 p-2 rounded"><span class="font-semibold">Usuario nuevo:</span> <span class="font-mono">${usuarioNuevo}</span></div><div class="bg-gray-50 p-2 rounded"><span class="font-semibold">Email nuevo:</span> <span class="font-mono">${emailNuevo}</span></div><div class="bg-gray-50 p-2 rounded"><span class="font-semibold">Password nuevo:</span> <span class="font-mono">${passNuevo}</span></div></div></div>`;
            openModal("Resetear Credenciales", body, async () => {
                let tempApp, tempAuth;
                try { tempApp = initializeApp(firebaseConfigPrincipal, 'resetAuth'); } catch(_) { tempApp = initializeApp(firebaseConfigPrincipal, 'resetAuth2'); }
                tempAuth = getAuth(tempApp);
                let uid = null;
                let oldDeleted = false;
                let existsActual = false, existsNuevo = false;
                try { const m = await fetchSignInMethodsForEmail(tempAuth, emailActual); existsActual = (m && m.length > 0); } catch(_) {}
                try { const m2 = await fetchSignInMethodsForEmail(tempAuth, emailNuevo); existsNuevo = (m2 && m2.length > 0); } catch(_) {}
                const passIngresada = (document.getElementById('rc-pass-actual')?.value || '').trim();
                if (existsActual && !oldDeleted) {
                    const p = passIngresada || passActual || passNuevo;
                    if (p) {
                        try {
                            const cred = await signInWithEmailAndPassword(tempAuth, emailActual, p);
                            const u = cred && cred.user ? cred.user : null;
                            if (u) { await deleteUser(u); oldDeleted = true; }
                        } catch(_) {}
                    }
                }
                let created = false;
                if (!existsNuevo) {
                    try {
                        const credNew = await createUserWithEmailAndPassword(tempAuth, emailNuevo, passNuevo);
                        const uNew = credNew && credNew.user ? credNew.user : null;
                        if (uNew) { uid = uNew.uid; created = true; }
                    } catch(_) {}
                } else {
                    try {
                        const p2 = passIngresada || passNuevo || passActual;
                        const cred3 = await signInWithEmailAndPassword(tempAuth, emailNuevo, p2);
                        const u3 = cred3 && cred3.user ? cred3.user : null;
                        if (u3) { uid = u3.uid; created = true; }
                    } catch(_) {}
                }
                try { await update(ref(db, `socios/${id}`), { usuario: usuarioNuevo, password: passNuevo, email: emailNuevo, ...(uid ? { uid } : {}) }); } catch(_) {}
                try { await signOut(tempAuth); } catch(_) {}
                closeModal();
                if (created) {
                    showToast(oldDeleted ? "Usuario actualizado en Authentication." : "Nuevo usuario creado en Authentication.", "success");
                } else {
                    showToast("Credenciales actualizadas en la base de datos.", "info");
                }
            });
        };
        window.modalNuevoSocio = () => {
            const body = `<div class="grid grid-cols-2 gap-4"><div class="col-span-2 md:col-span-1"><label class="block text-xs font-bold uppercase mb-1">Nombres</label><input id="ns-nom" type="text" class="w-full border p-2 rounded outline-none focus:ring-2 focus:ring-blue-500"></div><div class="col-span-2 md:col-span-1"><label class="block text-xs font-bold uppercase mb-1">Apellidos</label><input id="ns-ape" type="text" class="w-full border p-2 rounded outline-none focus:ring-2 focus:ring-blue-500"></div><div><label class="block text-xs font-bold uppercase mb-1">Lote / Manzana</label><input id="ns-lote" type="text" class="w-full border p-2 rounded outline-none focus:ring-2 focus:ring-blue-500"></div><div><label class="block text-xs font-bold uppercase mb-1">Piso</label><input id="ns-piso" type="text" class="w-full border p-2 rounded outline-none focus:ring-2 focus:ring-blue-500"></div><p class="col-span-2 text-xs text-gray-500 mt-2"><i class="fas fa-info-circle"></i> El usuario y contraseña se generarán automáticamente.</p></div>`;
            openModal("Registrar Nuevo Socio", body, async () => {
                const nom = document.getElementById('ns-nom').value.trim(), ape = document.getElementById('ns-ape').value.trim();
                if(!nom || !ape) return showToast("Nombres y apellidos son obligatorios", "warning");
                let usuario = (nom.charAt(0) + ape.split(' ')[0]).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]/g, '');
                if (usuario.length < 6) usuario = usuario.padEnd(6, '0');
                const email = `${usuario}@urbgloria.com`;
                const pass = usuario;
                let tempApp, tempAuth;
                try { tempApp = initializeApp(firebaseConfigPrincipal, 'socioCreator'); } catch(_) { tempApp = initializeApp(firebaseConfigPrincipal, 'socioCreator2'); }
                tempAuth = getAuth(tempApp);
                try {
                    const cred = await createUserWithEmailAndPassword(tempAuth, email, pass);
                    const uid = cred && cred.user ? cred.user.uid : null;
                    if (!uid) throw new Error("No se obtuvo UID del nuevo socio.");
                    await push(ref(db, 'socios'), { uid, nombres: nom, apellidos: ape, lote: document.getElementById('ns-lote').value, piso: document.getElementById('ns-piso').value, estado: 'activo', usuario: usuario, password: pass });
                    showToast("Socio creado correctamente.", "success");
                    closeModal();
                } catch (e) {
                    const code = (e && e.code) || '';
                    if (code === 'auth/email-already-in-use') showToast("El usuario ya existe en Authentication.", "error");
                    else if (code === 'auth/weak-password') showToast("La contraseña es demasiado débil.", "warning");
                    else showToast("No se pudo crear el socio: " + (e.message || 'Error'), "error");
                } finally {
                    try { await signOut(tempAuth); } catch(_) {}
                }
            });
        };

        // --- CUOTAS ---
        window.renderCuotas = () => {
            const tbody = document.getElementById('tbody-cuotas');
            tbody.innerHTML = '';
            const filterMonth = document.getElementById('cuotas-filter-month').value;
            let pendientes = cuotasData;
            if(filterMonth) pendientes = pendientes.filter(c => c.fecha && c.fecha.startsWith(filterMonth));
            let pagadas = allCajaMovs.filter(m => m.esCuota);
            if(filterMonth) pagadas = pagadas.filter(m => m.fecha && m.fecha.startsWith(filterMonth));

            const combinadas = [
                ...pendientes.map(c => ({ ...c, estado: 'pendiente' })),
                ...pagadas.map(m => ({
                    id: m.id,
                    socioId: m.cuotaOriginal.socioId,
                    concepto: m.cuotaOriginal.concepto,
                    monto: m.monto,
                    fecha: m.cuotaOriginal.fechaEmision,
                    estado: 'pagado',
                    fechaPago: m.fecha,
                    numeroRecibo: m.numeroRecibo || '-'
                }))
            ].sort((a,b) => (a.fecha || '').localeCompare(b.fecha || ''));

            combinadas.forEach(c => {
                const socio = sociosData.find(s => s.id === c.socioId);
                const tr = document.createElement('tr');
                tr.className = "border-b hover:bg-gray-50 text-sm transition";
                tr.innerHTML = `<td class="p-4 text-xs text-gray-500">${c.fecha || '-'}</td><td class="p-4 font-bold">${socio ? socio.apellidos + ', ' + socio.nombres : 'Socio Eliminado'}</td><td class="p-4">${socio ? (socio.lote || '-') : '-'}</td><td class="p-4">${socio ? (socio.piso || '-') : '-'}</td><td class="p-4">${c.concepto}</td><td class="p-4 font-semibold">S/ ${parseFloat(c.monto).toFixed(2)}</td><td class="p-4">${c.estado === 'pagado' ? (c.numeroRecibo || '-') : '-'}</td><td class="p-4">${c.estado === 'pendiente' ? '<span class="bg-amber-100 text-amber-700 px-3 py-1 rounded-full text-[10px] font-bold uppercase">Pendiente</span>' : `<span class="bg-green-100 text-green-700 px-3 py-1 rounded-full text-[10px] font-bold uppercase" title="Pagado el ${c.fechaPago}">Pagado</span>`}</td><td class="p-4">${c.estado === 'pendiente' ? `<button onclick="marcarPagoCaja('${c.id}')" class="bg-emerald-500 hover:bg-emerald-600 text-white px-3 py-1.5 rounded text-xs font-bold transition shadow-sm"><i class="fas fa-check"></i> Pagar</button>` : `<span class="text-xs text-gray-400 italic">En Caja</span>`}</td>`;
                tbody.appendChild(tr);
            });
        };
        window.marcarPagoCaja = async (id) => {
            const cuota = cuotasData.find(x => x.id === id);
            const socio = sociosData.find(s => s.id === cuota.socioId);
            const registradoPor = (currentUser && (currentUser.nombre || `${currentUser.nombres || ''} ${currentUser.apellidos || ''}`.trim())) || 'Sistema';
            const fbUser = auth.currentUser;
            if (!fbUser) { showToast("No autenticado. Inicie sesión para registrar en Caja.", "error"); console.error("[Caja] Intento de registro sin auth en marcarPagoCaja"); return; }
            const adminUid = fbUser.uid;
            console.log(`[Caja] Registrando movimiento de cuota con UID ${adminUid}`);
            let sugerido = '';
            try {
                const confSnap = await get(ref(dbCaja, 'config/correlativos/recibosNext'));
                let confVal = 0;
                if (confSnap.exists()) {
                    const v = Number(confSnap.val());
                    if (isFinite(v) && v > 0) confVal = v;
                }
                let maxMovRemote = 0;
                try {
                    const movSnap = await get(ref(dbCaja, 'movimientos'));
                    if (movSnap.exists()) {
                        const raw = movSnap.val() || {};
                        Object.values(raw).forEach(m => {
                            const n = parseInt(String(m && m.numeroRecibo || '').trim(), 10);
                            if (isFinite(n) && n > maxMovRemote) maxMovRemote = n;
                        });
                    }
                } catch(_) {}
                const maxMovLocal = (Array.isArray(allCajaMovs) ? allCajaMovs : [])
                    .reduce((acc, m) => {
                        const n = parseInt(String(m && m.numeroRecibo || '').trim(), 10);
                        return (isFinite(n) && n > acc) ? n : acc;
                    }, 0);
                const storedNext = parseInt(String(localStorage.getItem('cajaReciboNext') || '0'), 10);
                const candidateBase = Math.max(confVal, maxMovRemote, maxMovLocal, isFinite(storedNext) && storedNext > 0 ? storedNext : 0);
                const candidate = (candidateBase > 0) ? (candidateBase + 1) : 1;
                sugerido = String(candidate > 0 ? candidate : 1);
                try {
                    if (candidate > confVal) {
                        await set(ref(dbCaja, 'config/correlativos/recibosNext'), candidate);
                    }
                } catch(_) {}
            } catch(_) { sugerido = '1'; }
            const body = `<div class="space-y-3">
                <div><label class="block text-xs font-bold uppercase mb-1">Número de Recibo Físico</label><input id="recibo-num" type="text" class="w-full border p-2 rounded outline-none focus:ring-2 focus:ring-emerald-500" value="${sugerido}" placeholder="Ingrese el número de recibo"></div>
                <div><label class="block text-xs font-bold uppercase mb-1">Fecha de pago</label><input id="recibo-fecha" type="date" class="w-full border p-2 rounded outline-none focus:ring-2 focus:ring-emerald-500" value="${new Date().toISOString().split('T')[0]}"></div>
            </div>`;
            openModal("Confirmar Pago de Cuota", body, async () => {
                const numeroRecibo = (document.getElementById('recibo-num').value || '').trim();
                if (!numeroRecibo) { showToast("Debe ingresar el número de recibo.", "warning"); return; }
                const fechaPago = (document.getElementById('recibo-fecha').value || new Date().toISOString().split('T')[0]);
                try {
                    const movSnap = await get(ref(dbCaja, 'movimientos'));
                    if (movSnap.exists()) {
                        const raw = movSnap.val() || {};
                        const dup = Object.values(raw).some(m => String(m && m.numeroRecibo) === numeroRecibo);
                        if (dup) { showToast("Número de recibo ya registrado. Use otro correlativo.", "error"); return; }
                    }
                } catch(_) {}
                try {
                    const newRef = push(ref(dbCaja, 'movimientos'));
                    await set(newRef, {
                        fecha: fechaPago,
                        descripcion: `Cobro cuota: ${cuota.concepto} - ${socio ? socio.apellidos : 'Socio Eliminado'}`,
                        monto: cuota.monto,
                        tipo: 'ingreso',
                        esCuota: true,
                        registradoPor,
                        adminUid,
                        numeroRecibo,
                        cuotaOriginal: {
                            socioId: cuota.socioId,
                            concepto: cuota.concepto,
                            fechaEmision: cuota.fecha || new Date().toISOString().split('T')[0]
                        }
                    });
                    const numInt = Number(numeroRecibo);
                    if (isFinite(numInt) && numInt > 0) {
                        try { await runTransaction(ref(dbCaja, 'config/correlativos/recibosNext'), (current) => {
                            const cur = Number(current);
                            return isFinite(cur) && cur > 0 ? (cur + 1) : (numInt + 1);
                        }); } catch(_) {
                            try { await set(ref(dbCaja, 'config/correlativos/recibosNext'), numInt + 1); } catch(__) {}
                        }
                        try { localStorage.setItem('cajaReciboNext', String(numInt + 1)); } catch(_) {}
                    }
                    const nombreSocio = socio ? `${socio.apellidos}, ${socio.nombres}` : 'Socio Eliminado';
                    const loteSocio = socio ? (socio.lote || '-') : '-';
                    const cuerpo = `
                        <div>Recibo Nº: ${numeroRecibo}</div>
                        <div>Fecha de pago: ${fechaPago}</div>
                        <div>Socio: ${nombreSocio}</div>
                        <div>Lote: ${loteSocio}</div>
                        <div>Concepto: ${cuota.concepto}</div>
                        <div>Monto: S/ ${parseFloat(cuota.monto).toFixed(2)}</div>
                        <div>Registrado por: ${registradoPor}</div>
                    `;
                    try { await window.generarPDFEstandar('RECIBO DE PAGO', cuerpo, `Recibo_${numeroRecibo}.pdf`); } catch(_) {}
                    await remove(ref(db, `cuotas/${id}`));
                    showToast("Pago registrado correctamente", "success");
                    closeModal();
                } catch(e) {
                    console.error(`[Caja] Error registrando movimiento de cuota. UID=${adminUid}`, e);
                    showToast("Error al registrar en Caja.", "error");
                }
            });
        };
        window.modalGenerarCuota = () => {
            const normalize = (str) => String(str || '').toUpperCase().trim();
            const parseLote = (txt) => {
                const t = normalize(txt);
                let manzana = '', numero = 0;
                const m1 = t.match(/([A-Z]+)\s*[- ]\s*(\d+)/);
                const m2 = t.match(/MZ\s*([A-Z]+).*?(\d+)/);
                if (m1) { manzana = m1[1]; numero = parseInt(m1[2], 10) || 0; }
                else if (m2) { manzana = m2[1]; numero = parseInt(m2[2], 10) || 0; }
                else { const m3 = t.match(/([A-Z]+)/); const m4 = t.match(/(\d+)/); manzana = m3 ? m3[1] : ''; numero = m4 ? parseInt(m4[1], 10) || 0 : 0; }
                return { manzana, numero };
            };
            const activosOrdenados = sociosData.filter(s => s.estado !== 'inactivo').sort((a,b) => {
                const A = parseLote(a.lote), B = parseLote(b.lote);
                if (A.manzana !== B.manzana) return A.manzana.localeCompare(B.manzana);
                return A.numero - B.numero;
            });
            const optionsSocios = activosOrdenados.map(s => `<option value="${s.id}">${s.apellidos}, ${s.nombres}</option>`).join('');
            const body = `<div class="space-y-4"><div><label class="block text-xs font-bold uppercase mb-1">Concepto</label><input id="q-concepto" type="text" class="w-full border p-2 rounded focus:ring-2 focus:ring-blue-500 outline-none"></div><div><label class="block text-xs font-bold uppercase mb-1">Monto (S/)</label><input id="q-monto" type="number" class="w-full border p-2 rounded focus:ring-2 focus:ring-blue-500 outline-none"></div><div><label class="block text-xs font-bold uppercase mb-1">Fecha de emisión</label><input id="q-fecha" type="date" class="w-full border p-2 rounded focus:ring-2 focus:ring-blue-500 outline-none" value="${new Date().toISOString().split('T')[0]}"></div><div><label class="block text-xs font-bold uppercase mb-1">Destinatario</label><select id="q-dest" class="w-full border p-2 rounded focus:ring-2 focus:ring-blue-500 outline-none"><option value="todos">Todos los Socios Activos</option>${optionsSocios}</select></div></div>`;
            openModal("Generar Nueva Cuota", body, async () => {
                const concepto = document.getElementById('q-concepto').value;
                const monto = document.getElementById('q-monto').value;
                const dest = document.getElementById('q-dest').value;
                const fechaSel = (document.getElementById('q-fecha').value || '').trim();
                const fecha = fechaSel || new Date().toISOString().split('T')[0];
                if(dest === 'todos') { for (const s of sociosData.filter(x => x.estado !== 'inactivo')) await push(ref(db, 'cuotas'), { socioId: s.id, concepto, monto, fecha }); } 
                else await push(ref(db, 'cuotas'), { socioId: dest, concepto, monto, fecha });
                closeModal();
            });
        };

        // --- CAJA ---
        window.renderCaja = async () => {
            const filterMonth = document.getElementById('caja-filter-month').value || new Date().toISOString().substring(0, 7);
            const filtrados = allCajaMovs.filter(m => m.fecha && m.fecha.startsWith(filterMonth));
            const tbody = document.getElementById('tbody-caja');
            tbody.innerHTML = '';
            let totalIn = 0, totalOut = 0;
            filtrados.forEach(m => {
                const tr = document.createElement('tr');
                tr.className = "border-b hover:bg-gray-50 text-sm transition";
                if(m.tipo === 'ingreso') totalIn += parseFloat(m.monto); else totalOut += parseFloat(m.monto);
                tr.innerHTML = `
                    <td class="p-4">${m.fecha}</td>
                    <td class="p-4 font-medium text-slate-700">${m.descripcion}</td>
                    <td class="p-4 text-sm text-slate-600">${m.registradoPor || '-'}</td>
                    <td class="p-4 font-bold ${m.tipo === 'ingreso' ? 'text-green-600' : 'text-red-600'}">S/ ${parseFloat(m.monto).toFixed(2)}</td>
                    <td class="p-4 uppercase text-[10px] font-black tracking-wider">${m.tipo}</td>
                    <td class="p-4">
                        ${m.esCuota
                            ? (currentUser && currentUser.role === 'root'
                                ? `<button onclick="revertirACuota('${m.id}')" class="text-xs font-bold text-red-500 hover:text-red-700 underline">Revertir a Pendiente</button>`
                                : '-')
                            : '-'}
                    </td>
                `;
                tbody.appendChild(tr);
            });
            const balance = totalIn - totalOut;
            document.getElementById('caja-total-in').innerText = "S/ " + totalIn.toFixed(2);
            document.getElementById('caja-total-out').innerText = "S/ " + totalOut.toFixed(2);
            document.getElementById('caja-balance-ui').innerText = "S/ " + balance.toFixed(2);
            if(document.getElementById('dash-caja')) document.getElementById('dash-caja').innerText = "S/ " + balance.toFixed(2);
        };
        window.revertirACuota = async (movId) => {
            if(!confirm("¿Desea revertir este pago? El registro se eliminará de Caja y volverá a Control de Cuotas como pendiente.")) return;
            const fbUser = auth.currentUser;
            if (!fbUser) { alert("Sesión inválida. Inicie sesión nuevamente."); return; }
            try {
                const rootSnap = await get(ref(db, 'admins/root'));
                const rootData = rootSnap.exists() ? rootSnap.val() : {};
                const isRootUid = !!rootData && rootData.uid && rootData.uid === fbUser.uid;
                if (!(currentUser && currentUser.role === 'root' && isRootUid)) {
                    alert("Acción no permitida. Solo el Admin Raíz puede revertir pagos de cuotas.");
                    return;
                }
            } catch(_) {
                alert("No se pudo validar permisos de administrador raíz. Inténtelo más tarde.");
                return;
            }
            const mov = allCajaMovs.find(m => m.id === movId);
            if(mov && mov.cuotaOriginal) {
                await push(ref(db, 'cuotas'), {
                    socioId: mov.cuotaOriginal.socioId,
                    concepto: mov.cuotaOriginal.concepto,
                    monto: mov.monto,
                    fecha: mov.cuotaOriginal.fechaEmision
                });
                await remove(ref(dbCaja, `movimientos/${movId}`));
                allCajaMovs = allCajaMovs.filter(x => x.id !== movId);
                renderCuotas();
                renderCaja();
            }
        };
        window.modalCajaMov = () => {
            const body = `<div class="space-y-4"><div><label class="block text-xs font-bold uppercase mb-1">Descripción</label><input id="c-desc" type="text" class="w-full border p-2 rounded focus:ring-2 focus:ring-green-500 outline-none"></div><div><label class="block text-xs font-bold uppercase mb-1">Monto (S/)</label><input id="c-monto" type="number" step="0.01" class="w-full border p-2 rounded focus:ring-2 focus:ring-green-500 outline-none"></div><div><label class="block text-xs font-bold uppercase mb-1">Tipo de Movimiento</label><select id="c-tipo" class="w-full border p-2 rounded focus:ring-2 focus:ring-green-500 outline-none"><option value="ingreso">Ingreso (+)</option><option value="egreso">Egreso (-)</option></select></div><div><label class="block text-xs font-bold uppercase mb-1">Fecha</label><input id="c-fecha" type="date" value="${new Date().toISOString().split('T')[0]}" class="w-full border p-2 rounded focus:ring-2 focus:ring-green-500 outline-none"></div></div>`;
            openModal("Registrar Movimiento de Caja", body, async () => {
                const registradoPor = (currentUser && (currentUser.nombre || `${currentUser.nombres || ''} ${currentUser.apellidos || ''}`.trim())) || 'Sistema';
                const fbUser = auth.currentUser;
                if (!fbUser) { showToast("No autenticado. Inicie sesión para registrar en Caja.", "error"); console.error("[Caja] Intento de registro sin auth en modalCajaMov"); return; }
                const adminUid = fbUser.uid;
                console.log(`[Caja] Registrando movimiento manual con UID ${adminUid}`);
                try {
                    await push(ref(dbCaja, 'movimientos'), {
                        descripcion: document.getElementById('c-desc').value,
                        monto: document.getElementById('c-monto').value,
                        tipo: document.getElementById('c-tipo').value,
                        fecha: document.getElementById('c-fecha').value,
                        registradoPor,
                        adminUid
                    });
                    showToast("Movimiento registrado en Caja", "success");
                    closeModal();
                } catch(e) {
                    console.error(`[Caja] Error registrando movimiento manual. UID=${adminUid}`, e);
                    showToast("No se pudo registrar el movimiento en Caja.", "error");
                }
            });
        };
        window.modalPublicarInforme = () => {
            const mesActual = document.getElementById('caja-filter-month').value || new Date().toISOString().substring(0, 7);
            const st = configData.informeCaja || {};
            const publicadoInfo = st.publicadoEn ? `<div class="text-xs text-emerald-700 font-bold bg-emerald-50 border border-emerald-200 rounded p-2">Publicado el: ${st.publicadoEn}</div>` : `<div class="text-xs text-amber-700 font-bold bg-amber-50 border border-amber-200 rounded p-2">Aún no publicado</div>`;
            const body = `<div class="space-y-4">
                <p class="text-sm text-gray-600 mb-1">Publica el informe para que los socios puedan descargar el PDF desde su panel.</p>
                ${publicadoInfo}
                <div>
                    <label class="block text-xs font-bold uppercase mb-1">Mes a Publicar</label>
                    <input id="inf-mes" type="month" value="${st.mes || mesActual}" class="w-full border p-2 rounded outline-none">
                </div>
                <label class="flex items-center gap-2 cursor-pointer p-2 border rounded hover:bg-slate-50">
                    <input type="checkbox" id="inf-morosos" class="w-4 h-4" ${st.permitirMorosos ? 'checked' : ''}>
                    <span class="text-sm font-bold text-slate-700">Permitir a socios morosos ver el informe</span>
                </label>
                ${st.publicadoEn ? `<div><button id="btn-despublicar-inf" class="w-full py-2 rounded-lg border border-red-300 text-red-700 font-bold hover:bg-red-600 hover:text-white transition">Despublicar informe</button></div>` : ``}
            </div>`;
            openModal("Publicar Informe Mensual de Caja", body, async () => {
                const pad = (n) => String(n).padStart(2, '0');
                const now = new Date();
                const publicadoEn = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()} a las ${pad(now.getHours())}:${pad(now.getMinutes())}`;
                const mesSel = document.getElementById('inf-mes').value;
                const permitirMorosos = document.getElementById('inf-morosos').checked;
                const fbUser = auth && auth.currentUser;
                const publicadoPor = (currentUser && (currentUser.nombre || `${currentUser.nombres || ''} ${currentUser.apellidos || ''}`.trim())) || (fbUser && fbUser.uid) || 'Sistema';
                await update(ref(db, 'config/informeCaja'), { mes: mesSel, permitirMorosos, activo: true, publicadoEn, publicadoPor });
                closeModal();
            });
            try { const btn = document.getElementById('modal-action-btn'); if (btn) btn.textContent = 'Publicar informe mensual de caja'; } catch(_) {}
            try {
                const btnDes = document.getElementById('btn-despublicar-inf');
                if (btnDes) {
                    btnDes.onclick = async () => {
                        await update(ref(db, 'config/informeCaja'), { activo: false, publicadoEn: null, publicadoPor: null });
                        closeModal();
                    };
                }
            } catch(_) {}
        };
        window.printInformeCaja = async (event, mesFiltro) => {
            const btn = event && event.currentTarget ? event.currentTarget : null;
            const original = btn ? btn.innerHTML : '';
            try {
                if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Cargando...'; }
                await window.generarInformeCajaPDF(mesFiltro, btn);
            } finally {
                if (btn) { btn.disabled = false; btn.innerHTML = original || 'Imprimir'; }
            }
        };

        // --- ASAMBLEAS E IMPRESIÓN DE ACTA ---
        function renderAsambleas() {
            const container = document.getElementById('asambleas-list');
            container.innerHTML = '';
            asambleasData.forEach(a => {
                const div = document.createElement('div');
                div.className = "bg-white p-6 rounded-xl shadow border flex justify-between items-center";
                div.innerHTML = `
                    <div><h4 class="font-bold text-lg text-slate-800">${a.asunto}</h4><p class="text-sm text-gray-500"><i class="far fa-calendar-alt"></i> ${a.fecha} | ${a.hora} | <i class="fas fa-map-marker-alt"></i> ${a.lugar || '-'}</p></div>
                    <div class="flex gap-2">
                        <button onclick="modalTomarAsistencia('${a.id}')" class="bg-indigo-600 text-white px-4 py-2 rounded-lg text-xs font-bold uppercase hover:bg-indigo-700">Asistencia</button>
                        <button onclick="imprimirActa('${a.id}')" class="bg-gray-800 text-white px-4 py-2 rounded-lg text-xs font-bold uppercase hover:bg-gray-900"><i class="fas fa-print"></i> Acta</button>
                    </div>
                `;
                container.appendChild(div);
            });
        }
        window.modalNuevaAsamblea = () => {
            const today = new Date().toISOString().split('T')[0];
            const body = `<div class="space-y-4">
                <div>
                    <label class="block text-xs font-bold uppercase mb-1">Asunto</label>
                    <input id="a-asu" placeholder="Asunto" class="w-full border p-2 rounded bg-slate-50 focus:bg-white outline-none focus:ring-2 focus:ring-blue-500">
                </div>
                <div>
                    <label class="block text-xs font-bold uppercase mb-1">Fecha</label>
                    <input id="a-fec" type="date" value="${today}" class="w-full border p-2 rounded bg-slate-50 focus:bg-white outline-none focus:ring-2 focus:ring-blue-500">
                </div>
                <div>
                    <label class="block text-xs font-bold uppercase mb-1">Hora</label>
                    <input id="a-hor" type="time" class="w-full border p-2 rounded bg-slate-50 focus:bg-white outline-none focus:ring-2 focus:ring-blue-500">
                </div>
                <div>
                    <label class="block text-xs font-bold uppercase mb-1">Lugar</label>
                    <input id="a-lug" placeholder="Lugar (ej. Local Comunal)" class="w-full border p-2 rounded bg-slate-50 focus:bg-white outline-none focus:ring-2 focus:ring-blue-500">
                </div>
            </div>`;
            openModal("Programar Asamblea", body, async () => { await push(ref(db, 'asambleas'), { asunto: document.getElementById('a-asu').value, fecha: document.getElementById('a-fec').value, hora: document.getElementById('a-hor').value, lugar: document.getElementById('a-lug').value, asistentes: {} }); closeModal(); });
        };
        window.modalTomarAsistencia = (id) => {
            const a = asambleasData.find(x => x.id === id); const asistentes = a.asistentes || {};
            const normalize = (str) => String(str || '').toUpperCase().trim();
            const parseLote = (txt) => {
                const t = normalize(txt);
                let manzana = '', numero = 0;
                const m1 = t.match(/([A-Z]+)\s*[- ]\s*(\d+)/);
                const m2 = t.match(/MZ\s*([A-Z]+).*?(\d+)/);
                if (m1) { manzana = m1[1]; numero = parseInt(m1[2], 10) || 0; }
                else if (m2) { manzana = m2[1]; numero = parseInt(m2[2], 10) || 0; }
                else { const m3 = t.match(/([A-Z]+)/); const m4 = t.match(/(\d+)/); manzana = m3 ? m3[1] : ''; numero = m4 ? parseInt(m4[1], 10) || 0 : 0; }
                return { manzana, numero };
            };
            const activosOrdenados = sociosData.filter(s => s.estado !== 'inactivo').sort((a,b) => {
                const A = parseLote(a.lote), B = parseLote(b.lote);
                if (A.manzana !== B.manzana) return A.manzana.localeCompare(B.manzana);
                return A.numero - B.numero;
            });
            const body = `<div class="space-y-2 max-h-[50vh] overflow-y-auto pr-2">${activosOrdenados.map(s => `<label class="flex items-center gap-3 p-3 hover:bg-slate-50 border rounded-lg cursor-pointer transition"><input type="checkbox" class="asis-chk w-4 h-4 text-blue-600" data-sid="${s.id}" ${asistentes[s.id] ? 'checked' : ''}><span class="text-sm font-bold uppercase">${s.apellidos}, ${s.nombres}</span> <span class="text-[10px] text-gray-500 ml-auto">${s.lote || '-'}</span></label>`).join('')}</div>`;
            openModal("Toma de Asistencia", body, async () => {
                const updatedAsis = {}; document.querySelectorAll('.asis-chk').forEach(chk => { if(chk.checked) updatedAsis[chk.getAttribute('data-sid')] = true; });
                await update(ref(db, `asambleas/${id}`), { asistentes: updatedAsis }); closeModal();
            });
        };
        window.imprimirActa = (id) => {
            const a = asambleasData.find(x => x.id === id);
            const asistentes = a.asistentes || {};
            const presentes = sociosData.filter(s => asistentes[s.id]);
            const presentesOrd = presentes.slice().sort((x, y) => window.compareByLote(x.lote || '', y.lote || ''));
            const filas = presentesOrd.map((s, i) => `
              <tr style="background:${i%2===0 ? '#ffffff' : '#f8fafc'}">
                <td style="padding:10px;border:1px solid #e5e7eb;text-align:center;">${i+1}</td>
                <td style="padding:10px;border:1px solid #e5e7eb;">${s.apellidos}, ${s.nombres}</td>
                <td style="padding:10px;border:1px solid #e5e7eb;text-align:center;">${s.lote || '-'}</td>
                <td style="padding:10px;border:1px solid #e5e7eb;text-align:center;">${s.piso || '-'}</td>
                <td style="padding:10px;border:1px solid #e5e7eb;"></td>
              </tr>`).join('');
            const body = `
              <div style="margin-bottom:12px;font-size:14px;">
                <div style="margin-bottom:4px;"><b>Asunto:</b> ${a.asunto}</div>
                <div><b>Fecha:</b> ${a.fecha}</div>
                <div><b>Hora:</b> ${a.hora}</div>
                <div><b>Lugar:</b> ${a.lugar || '-'}</div>
                <div><b>Total de Asistentes Registrados:</b> ${presentes.length}</div>
              </div>
              <table style="width:100%;border-collapse:separate;border-spacing:0;font-size:13px;margin-top:8px;">
                <thead>
                  <tr>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;width:56px;">Nº</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:left;">Socio</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;">Lote</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;">Piso</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:left;">Firma / Verificación</th>
                  </tr>
                </thead>
                <tbody>${filas}</tbody>
              </table>
              `;
            window.generarPDFEstandar('ACTA DE ASISTENCIA A ASAMBLEA', body, `Acta_Asamblea_${a.fecha}.pdf`);
        };

        // --- VOTACIONES E IMPRESIÓN DE INFORME ---
        function renderVotaciones() {
            const container = document.getElementById('votaciones-list');
            container.innerHTML = '';
            votacionesData.forEach(v => {
                const votos = v.votos || {};
                const total = Object.keys(votos).length;
                const div = document.createElement('div');
                div.className = "bg-white p-6 rounded-xl shadow border";
                div.innerHTML = `
                    <div class="flex justify-between items-start mb-2">
                        <h4 class="font-bold text-slate-800 pr-4">${v.pregunta}</h4>
                        <div class="flex items-center gap-2">
                            <span class="text-xs font-bold bg-blue-100 text-blue-800 px-2 py-1 rounded-full whitespace-nowrap">Votos: ${total}</span>
                            ${v.cerrada ? `<span class="text-xs font-bold bg-red-100 text-red-700 px-2 py-1 rounded-full whitespace-nowrap">CERRADA</span>` : ``}
                        </div>
                    </div>
                    ${(configData.votaciones && configData.votaciones.restringirMorosos) ? `<p class="text-xs text-red-600 font-bold mb-3"><i class="fas fa-ban"></i> Voto restringido a socios sin deuda</p>` : ''}
                    <div class="space-y-2 mb-4">
                        <div class="w-full text-left p-3 border rounded-lg flex justify-between bg-slate-50"><span class="font-bold text-slate-700">SÍ</span><span class="bg-blue-600 text-white px-2 py-0.5 rounded shadow-sm text-sm font-bold">${Object.values(votos).filter(x => x === 'SI').length}</span></div>
                        <div class="w-full text-left p-3 border rounded-lg flex justify-between bg-slate-50"><span class="font-bold text-slate-700">NO</span><span class="bg-red-600 text-white px-2 py-0.5 rounded shadow-sm text-sm font-bold">${Object.values(votos).filter(x => x === 'NO').length}</span></div>
                    </div>
                    <div class="flex flex-wrap gap-2 border-t pt-3">
                        <button onclick="exportTableToExcel('temp-votaciones-excel', 'Votacion_${v.id.substring(0,6)}')" class="text-xs font-bold bg-emerald-100 text-emerald-700 px-3 py-1.5 rounded hover:bg-emerald-200 transition"><i class="fas fa-file-excel"></i> Excel</button>
                        <button onclick="imprimirVotacion('${v.id}')" class="text-xs font-bold bg-gray-800 text-white px-3 py-1.5 rounded hover:bg-gray-900 transition"><i class="fas fa-file-pdf"></i> Informe</button>
                        <button onclick="closeVotacion('${v.id}')" class="text-xs font-bold bg-red-600 text-white px-3 py-1.5 rounded hover:bg-red-700 transition" ${v.cerrada ? 'disabled' : ''}><i class="fas fa-lock"></i> Cerrar votación</button>
                        <button onclick="deleteVotacion('${v.id}')" class="text-xs font-bold text-red-500 hover:text-red-700 uppercase ml-auto transition"><i class="fas fa-trash"></i> Eliminar</button>
                    </div>
                `;
                container.appendChild(div);
            });
        }
        window.votar = (id, opcion) => {
            if(currentUser.role !== 'socio') return;
            const restr = configData.votaciones && configData.votaciones.restringirMorosos;
            if (restr) {
                const tienePend = cuotasData.some(c => c.socioId === currentUser.id && (c.estado ? c.estado === 'PENDIENTE' : true));
                if (tienePend) {
                    alert('Voto restringido por cuotas pendientes. Regulariza tu situación para participar.');
                    return;
                }
            }
            const v = votacionesData.find(x => x.id === id);
            if (v && v.cerrada) { showToast("Esta votación está cerrada.", "warning"); return; }
            if(v && v.votos && v.votos[currentUser.id]) { showToast("Ya registraste tu voto.", "info"); return; }
            update(ref(db, `votaciones/${id}/votos`), { [currentUser.id]: opcion });
        };
        window.closeVotacion = async (id) => { await update(ref(db, `votaciones/${id}`), { cerrada: true }); };
        window.modalNuevaPropuesta = () => {
            const body = `<div class="space-y-4"><input id="v-pre" placeholder="Pregunta de la votación" class="w-full border p-2 rounded focus:ring-2 focus:ring-blue-500 outline-none"></div>`;
            openModal("Crear Nueva Votación", body, async () => { await push(ref(db, 'votaciones'), { pregunta: document.getElementById('v-pre').value, votos: {} }); closeModal(); });
        };
        window.deleteVotacion = (id) => { if(confirm("¿Eliminar votación definitivamente?")) remove(ref(db, `votaciones/${id}`)); };
        window.imprimirVotacion = (id) => {
            const v = votacionesData.find(x => x.id === id);
            const votos = v.votos || {};
            const total = Object.keys(votos).length;
            const si = Object.values(votos).filter(x => x === 'SI').length;
            const no = Object.values(votos).filter(x => x === 'NO').length;
            const items = Object.keys(votos).map(sid => {
                const socio = sociosData.find(s => s.id === sid);
                return {
                    nombre: socio ? `${socio.apellidos}, ${socio.nombres}` : 'Socio Eliminado',
                    lote: socio ? (socio.lote || '-') : '-',
                    piso: socio ? (socio.piso || '-') : '-',
                    voto: votos[sid]
                };
            }).sort((a, b) => window.compareByLote(a.lote, b.lote) || a.nombre.localeCompare(b.nombre));
            const rows = items.map((it, i) => `<tr><td style="text-align:center;">${i+1}</td><td>${it.nombre}</td><td style="text-align:center;">${it.lote}</td><td style="text-align:center;">${it.piso}</td><td style="text-align:center;font-weight:700;">${it.voto}</td></tr>`).join('');
            const body = `
              <div style="font-size:14px; margin-bottom:10px;">
                <div><b>Propuesta:</b> ${v.pregunta}</div>
                <div style="margin-top:6px;"><b>Total de Votos Emitidos:</b> ${total}</div>
                <div><b>Resultados:</b> SÍ ${si} | NO ${no}</div>
              </div>
              <table style="width:100%;border-collapse:collapse;font-size:13px;margin-top:10px;">
                <thead>
                  <tr>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;width:56px;">Nº</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:left;">Nombre del Socio</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;">Lote</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;">Piso</th>
                    <th style="background:#0f3f22;color:#fff;padding:10px;border:1px solid #e5e7eb;text-align:center;">Voto Emitido</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>`;
            window.generarPDFEstandar('INFORME DETALLADO DE RESULTADOS DE VOTACIÓN', body, `Informe_Votacion_${v.id.substring(0,6)}.pdf`);
        };

        // --- SISTEMA (Root) ---
        function renderSistema() {
            const mainSize = (JSON.stringify(sociosData).length + JSON.stringify(cuotasData).length + JSON.stringify(asambleasData).length + JSON.stringify(votacionesData).length + JSON.stringify(adminsData).length) / 1024;
            const cajaSize = JSON.stringify(allCajaMovs).length / 1024;
            document.getElementById('sys-db-principal').innerText = `${mainSize.toFixed(2)} KB`;
            document.getElementById('sys-db-caja').innerText = `${cajaSize.toFixed(2)} KB`;

            const tbody = document.getElementById('tbody-admins');
            tbody.innerHTML = '';
            adminsData.forEach(ad => {
                if(ad.id === 'root') return; 
                const tr = document.createElement('tr');
                tr.className = "border-b";
                tr.innerHTML = `<td class="p-2 font-bold">${ad.usuario}</td><td class="p-2">${ad.nombre}</td><td class="p-2"><button onclick="eliminarAdmin('${ad.id}')" class="text-xs text-red-500 font-bold hover:underline">Eliminar</button></td>`;
                tbody.appendChild(tr);
            });
            const chk = document.getElementById('cfg-voto-morosos');
            if (chk) chk.checked = !!(configData.votaciones && configData.votaciones.restringirMorosos);
        }
        window.toggleRestriccionVoto = async (checked) => { await update(ref(db, 'config/votaciones'), { restringirMorosos: checked }); };
        window.limpiarBasePrincipal = async () => {
            if(!confirm("¡PELIGRO! Esta acción eliminará permanentemente todas las cuotas pendientes, asambleas y votaciones. Los socios, administradores y movimientos de CAJA NO serán afectados.\n\n¿Estás completamente seguro de continuar?")) return;
            try { 
                await remove(ref(db, 'cuotas')); 
                await remove(ref(db, 'asambleas')); 
                await remove(ref(db, 'votaciones')); 
                showToast("Base de datos principal limpiada exitosamente", "success"); 
            } catch(e) { 
                showToast("Error al limpiar la base de datos", "error"); 
            }
        };
        window.syncCajaACLAll = async () => {
            if (!confirm("Esto sincronizará todos los administradores actuales con la ACL de Caja.\n\n¿Deseas continuar?")) return;
            try {
                const meCaja = (authCaja && authCaja.currentUser) ? authCaja.currentUser.uid : null;
                const mePrincipal = (auth && auth.currentUser) ? auth.currentUser.uid : null;
                try {
                    const aclSnap = await get(ref(dbCaja, 'aclAdmins'));
                    const acl = aclSnap.exists() ? (aclSnap.val() || {}) : {};
                    if (!meCaja || !acl[meCaja]) {
                        throw new Error('BOOTSTRAP_REQUIRED');
                    }
                } catch (preErr) {
                    const uidPrincipal = mePrincipal || 'desconocido';
                    const uidCaja = meCaja || 'desconocido';
                    alert(`No se pudo sincronizar la ACL de Caja.\n\nEs necesario inicializar la ACL con el UID del proyecto de Caja:\n1) En la base de datos de Caja, crea la ruta "aclAdmins/${uidCaja}" con valor true (UID Caja).\n2) Luego vuelve a pulsar "Reparar ACL de Caja".\n\nUID Principal: ${uidPrincipal}\nUID Caja: ${uidCaja}`);
                    return;
                }
                const snap = await get(ref(db, 'admins'));
                const data = snap.exists() ? snap.val() || {} : {};
                const updates = {};
                if (meCaja) updates[meCaja] = true;
                let created = 0;
                let tempAppCaja, tempAuthCaja;
                try { tempAppCaja = initializeApp(firebaseConfigCaja, 'cajaUserCreatorSync'); } catch(_) { tempAppCaja = initializeApp(firebaseConfigCaja, 'cajaUserCreatorSync2'); }
                tempAuthCaja = getAuth(tempAppCaja);
                for (const [id, a] of Object.entries(data)) {
                    if (!a || !a.usuario) continue;
                    const email = `${String(a.usuario).toLowerCase()}@urbgloria.com`;
                    let methods = [];
                    try { methods = await fetchSignInMethodsForEmail(authCaja, email); } catch(_) {}
                    if (!methods || methods.length === 0) {
                        let pass = String(a.password || '').trim();
                        if (!pass || pass.length < 6) pass = String(a.usuario).toLowerCase().replace(/[^a-z0-9]/g,'').padEnd(6,'0');
                        try {
                            const cred = await createUserWithEmailAndPassword(tempAuthCaja, email, pass);
                            const uidNew = cred && cred.user ? cred.user.uid : null;
                            if (uidNew) { updates[uidNew] = true; created++; }
                        } catch(_) {}
                    } else {
                        if (authCaja.currentUser && authCaja.currentUser.email === email && authCaja.currentUser.uid) {
                            updates[authCaja.currentUser.uid] = true;
                        }
                    }
                }
                if (Object.keys(updates).length > 0) await update(ref(dbCaja, 'aclAdmins'), updates);
                showToast(created > 0 ? `ACL de Caja sincronizada. Usuarios creados: ${created}` : "ACL de Caja sincronizada.", "success");
            } catch(e) {
                const uidTxt = (authCaja && authCaja.currentUser) ? authCaja.currentUser.uid : 'desconocido';
                const msg = (e && e.message) ? e.message : '';
                showToast(`Error al sincronizar ACL de Caja`, "error");
                console.error(`ACL Sync Error: ${msg}, UID: ${uidTxt}`);
            }
        };
        window.modalNuevoAdmin = () => {
            const body = `<div class="space-y-4"><div><label class="block text-xs font-bold uppercase mb-1">Nombre Completo</label><input id="ad-nom" type="text" class="w-full border p-2 rounded outline-none"></div><div><label class="block text-xs font-bold uppercase mb-1">Usuario</label><input id="ad-usu" type="text" class="w-full border p-2 rounded outline-none"></div><div><label class="block text-xs font-bold uppercase mb-1">Contraseña</label><input id="ad-pass" type="password" class="w-full border p-2 rounded outline-none"></div><div class="pt-2 border-t"><label class="block text-xs font-bold uppercase mb-2">Permisos de Acceso</label><div class="grid grid-cols-2 gap-2"><label class="flex items-center gap-2 text-sm"><input type="checkbox" id="ad-p-padron"> Padrón de Socios</label><label class="flex items-center gap-2 text-sm"><input type="checkbox" id="ad-p-cuotas"> Control de Cuotas</label><label class="flex items-center gap-2 text-sm"><input type="checkbox" id="ad-p-caja"> Caja</label><label class="flex items-center gap-2 text-sm"><input type="checkbox" id="ad-p-asambleas"> Asambleas</label><label class="flex items-center gap-2 text-sm"><input type="checkbox" id="ad-p-votaciones"> Votaciones</label></div></div></div>`;
            openModal("Crear Nuevo Administrador", body, async () => {
                const usu = document.getElementById('ad-usu').value.trim(), pass = document.getElementById('ad-pass').value.trim();
                if(!usu || !pass) return showToast("Usuario y contraseña obligatorios", "warning");
                const permisos = { padron: document.getElementById('ad-p-padron').checked, cuotas: document.getElementById('ad-p-cuotas').checked, caja: document.getElementById('ad-p-caja').checked, asambleas: document.getElementById('ad-p-asambleas').checked, votaciones: document.getElementById('ad-p-votaciones').checked };
                let tempApp, tempAuth;
                try { tempApp = initializeApp(firebaseConfigPrincipal, 'adminCreator'); } catch(_) { tempApp = initializeApp(firebaseConfigPrincipal, 'adminCreator2'); }
                tempAuth = getAuth(tempApp);
                try {
                    const email = `${usu.toLowerCase()}@urbgloria.com`;
                    const cred = await createUserWithEmailAndPassword(tempAuth, email, pass);
                    const uid = cred && cred.user ? cred.user.uid : null;
                    if (!uid) throw new Error("No se obtuvo UID del nuevo administrador");
                    // Crear usuario en Auth de Caja usando APP temporal para no afectar la sesión actual
                    let cajaUid = null;
                    let tempAppCaja, tempAuthCaja;
                    try { tempAppCaja = initializeApp(firebaseConfigCaja, 'adminCajaCreator'); } catch(_) { tempAppCaja = initializeApp(firebaseConfigCaja, 'adminCajaCreator2'); }
                    tempAuthCaja = getAuth(tempAppCaja);
                    try {
                        const credCaja = await createUserWithEmailAndPassword(tempAuthCaja, email, pass);
                        cajaUid = credCaja && credCaja.user ? credCaja.user.uid : null;
                    } catch(e) {
                        try {
                            await signInWithEmailAndPassword(tempAuthCaja, email, pass);
                            const u = tempAuthCaja.currentUser;
                            cajaUid = u && u.uid ? u.uid : null;
                        } catch(_) {}
                    } finally {
                        try { await signOut(tempAuthCaja); } catch(_) {}
                    }
                    await set(ref(db, `admins/${uid}`), { nombre: document.getElementById('ad-nom').value.trim(), usuario: usu.toLowerCase(), password: pass, permisos, role: 'admin', uid, ...(cajaUid ? { cajaUid } : {}) });
                    if (cajaUid) { try { await set(ref(dbCaja, `aclAdmins/${cajaUid}`), true); } catch(_) {} }
                    showToast("Administrador creado correctamente", "success");
                    closeModal();
                } catch(e) {
                    showToast((e && e.code) ? `Error: ${e.code}` : 'No se pudo crear el administrador', "error");
                } finally {
                    try { await signOut(tempAuth); } catch(_) {}
                }
            });
        };
        window.eliminarAdmin = async (id) => { 
            let adminData = null;
            try { const snap = await get(ref(db, `admins/${id}`)); if (snap.exists()) adminData = snap.val() || null; } catch(_) {}
            if (!adminData) { showToast("No se encontró el administrador.", "error"); return; }
            if (id === 'root' || (adminData.role && String(adminData.role).toLowerCase() === 'root')) {
                showToast("No se puede eliminar el administrador raíz.", "warning");
                return;
            }
            const usuario = (adminData.usuario || '').toLowerCase();
            const body = `<div class="space-y-3">
              <p class="text-sm">Esta acción eliminará al administrador en:</p>
              <ul class="text-sm list-disc ml-5"><li>Authentication y DB Principal</li><li>Authentication y ACL de Caja</li></ul>
              <div class="bg-amber-50 border border-amber-200 text-amber-800 text-xs p-2 rounded">Para borrar en Authentication es necesario iniciar sesión temporal con las credenciales de este usuario.</div>
              <div><label class="block text-xs font-bold uppercase mb-1">Usuario</label><input type="text" value="${usuario}" disabled class="w-full border p-2 rounded bg-gray-100"></div>
              <div><label class="block text-xs font-bold uppercase mb-1">Contraseña del usuario</label><input id="del-admin-pass" type="password" class="w-full border p-2 rounded outline-none" placeholder="Contraseña del admin"></div>
            </div>`;
            openModal("Eliminar Administrador", body, async () => {
                const inputPass = (document.getElementById('del-admin-pass')?.value || '').trim();
                await window.doDeleteAdmin(id, inputPass);
                closeModal();
            });
        };
        window.doDeleteAdmin = async (id, inputPass) => {
            let adminData = null;
            try { const snap = await get(ref(db, `admins/${id}`)); if (snap.exists()) adminData = snap.val() || null; } catch(_) {}
            if (!adminData) { alert("No se encontró el administrador."); return; }
            const usuario = (adminData.usuario || '').toLowerCase();
            const email = usuario ? `${usuario}@urbgloria.com` : null;
            const passPrincipal = (inputPass || adminData.password || '').trim();
            const passCaja = passPrincipal; // normalmente iguales
            const cajaUid = adminData && adminData.cajaUid ? adminData.cajaUid : null;
            let okPrincipal = false, okCaja = false;
            // Auth Principal
            if (email && passPrincipal) {
                let tApp, tAuth;
                try { tApp = initializeApp(firebaseConfigPrincipal, 'adminDelPrincipal'); } catch(_) { tApp = initializeApp(firebaseConfigPrincipal, 'adminDelPrincipal2'); }
                tAuth = getAuth(tApp);
                try { 
                    const cred = await signInWithEmailAndPassword(tAuth, email, passPrincipal); 
                    const u = cred && cred.user ? cred.user : null; 
                    if (u) { await deleteUser(u); okPrincipal = true; }
                } catch(_) {}
                try { await signOut(tAuth); } catch(_) {}
            }
            // Auth Caja
            if (email && passCaja) {
                let tAppC, tAuthC;
                try { tAppC = initializeApp(firebaseConfigCaja, 'adminDelCaja'); } catch(_) { tAppC = initializeApp(firebaseConfigCaja, 'adminDelCaja2'); }
                tAuthC = getAuth(tAppC);
                try { 
                    const cred2 = await signInWithEmailAndPassword(tAuthC, email, passCaja); 
                    const u2 = cred2 && cred2.user ? cred2.user : null; 
                    if (u2) { await deleteUser(u2); okCaja = true; }
                } catch(_) {}
                try { await signOut(tAuthC); } catch(_) {}
            }
            try { await remove(ref(db, `admins/${id}`)); } catch(_) {}
            if (cajaUid) { try { await remove(ref(dbCaja, `aclAdmins/${cajaUid}`)); } catch(_) {} }
            const msg = `Eliminación realizada.\n- Auth Principal: ${okPrincipal ? 'OK' : 'No eliminado'}\n- Auth Caja: ${okCaja ? 'OK' : 'No eliminado'}\n- DB Principal: OK\n- ACL Caja: ${cajaUid ? 'OK' : 'No encontrado'}`;
            alert(msg);
        };

        // --- UTILS ---
        window.openModal = (title, body, action) => {
            const titleEl = document.getElementById('modal-title');
            const bodyEl = document.getElementById('modal-body');
            const actionBtn = document.getElementById('modal-action-btn');
            if (titleEl) titleEl.innerText = title;
            if (bodyEl) bodyEl.innerHTML = body;
            if (actionBtn) {
                actionBtn.textContent = 'Confirmar';
                actionBtn.onclick = action;
            }
            document.getElementById('modal').classList.add('modal-active');
        };
        window.closeModal = () => document.getElementById('modal').classList.remove('modal-active');
        window.parseLoteGlobal = (txt) => {
            const t = String(txt || '').toUpperCase().trim();
            let manzana = '', numero = 0;
            const m1 = t.match(/([A-Z]+)\s*[- \/]?(\d+)/);
            const m2 = t.match(/MZ\s*([A-Z]+).*?(\d+)/);
            if (m1) { manzana = m1[1]; numero = parseInt(m1[2], 10) || 0; }
            else if (m2) { manzana = m2[1]; numero = parseInt(m2[2], 10) || 0; }
            else {
                const m3 = t.match(/([A-Z]+)/);
                const m4 = t.match(/(\d+)/);
                manzana = m3 ? m3[1] : '';
                numero = m4 ? parseInt(m4[1], 10) || 0 : 0;
            }
            return { manzana, numero };
        };
        window.compareByLote = (a, b) => {
            const A = window.parseLoteGlobal(a);
            const B = window.parseLoteGlobal(b);
            if (A.manzana !== B.manzana) return A.manzana.localeCompare(B.manzana);
            return A.numero - B.numero;
        };
        window.printAislado = (tableId, title) => {
            const original = document.getElementById(tableId);
            if (!original) return;
            const table = original.cloneNode(true);
            const headers = table.tHead && table.tHead.rows[0] ? Array.from(table.tHead.rows[0].cells) : [];
            const toRemove = [];
            const accionIdx = headers.findIndex(th => /Acción/i.test(th.textContent || ''));
            if (accionIdx >= 0) toRemove.push(accionIdx);
            const resetIdx = headers.findIndex(th => /Reset/i.test(th.textContent || ''));
            if (resetIdx >= 0) toRemove.push(resetIdx);
            if (toRemove.length) {
                toRemove.sort((a,b) => b - a);
                Array.from(table.rows).forEach(row => {
                    toRemove.forEach(idx => { if (row.cells.length > idx) row.deleteCell(idx); });
                });
            }
            const hasNumero = headers.length > 0 && /N[º°]/i.test((headers[0].textContent || '').trim());
            if (!hasNumero && table.tHead && table.tHead.rows[0]) {
                const th = document.createElement('th');
                th.textContent = 'Nº';
                th.style.padding = '10px';
                th.style.border = '1px solid #e5e7eb';
                th.style.background = '#0f3f22';
                th.style.color = '#fff';
                table.tHead.rows[0].insertBefore(th, table.tHead.rows[0].firstChild);
                Array.from(table.tBodies[0].rows).forEach((row, i) => {
                    const td = document.createElement('td');
                    td.textContent = String(i+1);
                    td.style.textAlign = 'center';
                    td.style.padding = '10px';
                    td.style.border = '1px solid #e5e7eb';
                    row.insertBefore(td, row.firstChild);
                });
            } else if (table.tBodies && table.tBodies[0]) {
                Array.from(table.tBodies[0].rows).forEach((row, i) => {
                    if (row.cells[0]) row.cells[0].textContent = String(i+1);
                });
            }
            if (tableId === 'table-padron') {
                const tbody = table.tBodies && table.tBodies[0];
                if (tbody) {
                    const rows = Array.from(tbody.rows);
                    const loteIdx = headers.findIndex(th => /Lote|Manzana/i.test(th.textContent || ''));
                    const parseLote = (t) => {
                        const m = String(t || '').trim().toUpperCase().match(/^([A-Z]+)\s*[-\/]?\s*(\d+)/);
                        if (!m) return { z: String(t || '').toUpperCase(), a: '', n: 0 };
                        return { z: m[1], a: m[1], n: parseInt(m[2], 10) || 0 };
                    };
                    if (loteIdx >= 0) {
                        rows.sort((r1, r2) => {
                            const v1 = parseLote(r1.cells[loteIdx] ? r1.cells[loteIdx].textContent : '');
                            const v2 = parseLote(r2.cells[loteIdx] ? r2.cells[loteIdx].textContent : '');
                            if (v1.a === v2.a) return v1.n - v2.n;
                            return v1.a.localeCompare(v2.a);
                        });
                        rows.forEach((r, i) => {
                            tbody.appendChild(r);
                            if (r.cells[0]) r.cells[0].textContent = String(i+1);
                        });
                    }
                }
            } else if (tableId === 'table-cuotas') {
                const tbody = table.tBodies && table.tBodies[0];
                if (tbody) {
                    const rows = Array.from(tbody.rows);
                    const headerCells = table.tHead && table.tHead.rows[0] ? Array.from(table.tHead.rows[0].cells) : [];
                    const loteIdx = headerCells.findIndex(th => /Lote/i.test((th.textContent || '').trim()));
                    if (loteIdx >= 0) {
                        rows.sort((r1, r2) => {
                            const t1 = r1.cells[loteIdx] ? r1.cells[loteIdx].textContent : '';
                            const t2 = r2.cells[loteIdx] ? r2.cells[loteIdx].textContent : '';
                            return window.compareByLote(t1, t2);
                        });
                        rows.forEach((r, i) => {
                            tbody.appendChild(r);
                            if (r.cells[0]) r.cells[0].textContent = String(i+1);
                        });
                    }
                }
            }
            const now = new Date();
            const pad = n => String(n).padStart(2,'0');
            const fechaGen = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()} a las ${pad(now.getHours())}:${pad(now.getMinutes())}`;
            const { jsPDF } = window.jspdf || {};
            if (!jsPDF) return;
            const doc = new jsPDF({ unit: 'pt', format: 'letter', orientation: 'portrait' });
            const margin = 36;
            const pageWidth = doc.internal.pageSize.getWidth();
            const pageHeight = doc.internal.pageSize.getHeight();
            let y = margin;
            doc.setFontSize(14);
            doc.text('COOPERATIVA GLORIA Nº 4', pageWidth / 2, y, { align: 'center' });
            y += 18;
            doc.setFontSize(12);
            doc.text(String(title || '').toUpperCase(), pageWidth / 2, y, { align: 'center' });
            y += 18;
            const headRow = table.tHead && table.tHead.rows[0] ? Array.from(table.tHead.rows[0].cells).map(c => (c.textContent || '').trim()) : [];
            const bodyRows = table.tBodies && table.tBodies[0] ? Array.from(table.tBodies[0].rows).map(r => Array.from(r.cells).map(c => (c.textContent || '').trim())) : [];
            if (typeof doc.autoTable === 'function') {
                doc.autoTable({
                    head: [headRow],
                    body: bodyRows,
                    startY: y,
                    styles: { fontSize: 9, cellPadding: 4, lineColor: [0,0,0], lineWidth: 0.4 },
                    headStyles: { fillColor: [15, 63, 34], textColor: 255 },
                    theme: 'grid',
                    margin: { left: margin, right: margin }
                });
                y = (doc.lastAutoTable && doc.lastAutoTable.finalY) ? doc.lastAutoTable.finalY + 8 : y;
            }
            doc.setFont(undefined, 'normal');
            doc.setFontSize(9);
            const total = doc.internal.getNumberOfPages();
            for (let i = 1; i <= total; i++) {
                doc.setPage(i);
                doc.text(`Generado el: ${fechaGen}`, margin, pageHeight - 24);
                doc.text(`Página ${i} de ${total}`, pageWidth / 2, pageHeight - 24, { align: 'center' });
            }
            doc.save(`${title}.pdf`);
        };
        window.generarPDFEstandar = async (titulo, cuerpoHtml, nombre) => {
            const now = new Date();
            const pad = n => String(n).padStart(2,'0');
            const fechaGen = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()} a las ${pad(now.getHours())}:${pad(now.getMinutes())}`;
            const { jsPDF } = window.jspdf || {};
            if (!jsPDF) return;
            const doc = new jsPDF({ unit: 'pt', format: 'letter', orientation: 'portrait' });
            const margin = 36;
            const pageWidth = doc.internal.pageSize.getWidth();
            const pageHeight = doc.internal.pageSize.getHeight();
            let y = margin;
            doc.setFontSize(14);
            doc.text('COOPERATIVA GLORIA Nº 4', pageWidth / 2, y, { align: 'center' });
            y += 18;
            doc.setFontSize(12);
            doc.text(String(titulo || '').toUpperCase(), pageWidth / 2, y, { align: 'center' });
            y += 18;
            const temp = document.createElement('div');
            temp.innerHTML = cuerpoHtml || '';
            const contentWidth = pageWidth - margin * 2;
            const ensureSpace = (add) => {
                if (y + add > pageHeight - 48) {
                    doc.addPage();
                    y = margin;
                }
            };
            const esActa = (String(titulo || '')).toUpperCase().includes('ACTA');
            const esRecibo = (String(titulo || '')).toUpperCase().includes('RECIBO');
            const splitLabelPairs = (s) => {
                const out = [];
                const text = String(s || '').replace(/\s+/g, ' ').trim();
                if (!text) return out;
                const re = /([^:]+):\s*/g;
                const starts = [];
                let m;
                while ((m = re.exec(text)) !== null) {
                    starts.push({ index: m.index, label: m[1].trim() });
                }
                if (starts.length === 0) {
                    out.push(text);
                    return out;
                }
                for (let i = 0; i < starts.length; i++) {
                    const segStart = starts[i].index;
                    const segEnd = (i + 1 < starts.length) ? starts[i + 1].index : text.length;
                    const seg = text.slice(segStart, segEnd).trim();
                    if (seg) out.push(seg);
                }
                return out;
            };
            const renderLabelLine = (text) => {
                const t = String(text || '').replace(/\s+/g, ' ').trim();
                if (!t) return;
                if (esActa) {
                    const idx = t.indexOf(':');
                    doc.setFontSize(10);
                    ensureSpace(16);
                    if (idx !== -1) {
                        const label = (t.slice(0, idx).trim() || '') + ':';
                        const value = t.slice(idx + 1).trim();
                        doc.setFont(undefined, 'bold');
                        doc.text(label, margin, y);
                        const lw = doc.getTextWidth(label + ' ');
                        doc.setFont(undefined, 'normal');
                        if (value) doc.text(value, margin + lw, y);
                    } else {
                        doc.setFont(undefined, 'normal');
                        doc.text(t, margin, y);
                    }
                    y += 16;
                    return;
                }
                const parts = splitLabelPairs(t);
                if (parts.length === 0) return;
                parts.forEach(pair => {
                    const m = pair.match(/^([^:]+):\s*(.*)$/);
                    const labelRaw = m ? m[1].trim() : '';
                    const valueRaw = m ? m[2] : pair;
                    const label = m ? `${labelRaw}:` : '';
                    const value = String(valueRaw || '').trim();
                    doc.setFontSize(10);
                    ensureSpace(16);
                    if (label) {
                        doc.setFont(undefined, 'bold');
                        doc.text(label, margin, y);
                        const lw = doc.getTextWidth(label + ' ');
                        doc.setFont(undefined, 'normal');
                        if (value) doc.text(value, margin + lw, y);
                    } else {
                        doc.setFont(undefined, 'normal');
                        doc.text(value, margin, y);
                    }
                    y += 16;
                });
            };
            Array.from(temp.childNodes).forEach(node => {
                if (!node) return;
                if (node.nodeType === 1 && node.tagName && node.tagName.toLowerCase() === 'table') {
                    const t = node;
                    const thead = t.querySelector('thead');
                    const tbody = t.querySelector('tbody');
                    const headRow = thead && thead.rows[0] ? Array.from(thead.rows[0].cells).map(c => (c.textContent || '').trim()) : [];
                    const bodyRows = tbody ? Array.from(tbody.rows).map(r => Array.from(r.cells).map(c => (c.textContent || '').trim())) : [];
                    if (typeof doc.autoTable === 'function') {
                        doc.autoTable({
                            head: [headRow],
                            body: bodyRows,
                            startY: y,
                            styles: { fontSize: 9, cellPadding: esActa ? 8 : 4, lineColor: [0,0,0], lineWidth: 0.4 },
                            headStyles: { fillColor: [15, 63, 34], textColor: 255 },
                            theme: 'grid',
                            margin: { left: margin, right: margin }
                        });
                        y = (doc.lastAutoTable && doc.lastAutoTable.finalY) ? doc.lastAutoTable.finalY + 10 : y;
                    }
                } else if (node.nodeType === 1) {
                    const text = (node.innerText || '');
                    // Si contiene múltiples items dentro, separarlos por saltos de línea existentes
                    const parts = text.split(/\n+/).map(s => s.trim()).filter(Boolean);
                    if (parts.length === 0) return;
                    parts.forEach(p => renderLabelLine(p));
                } else if (node.nodeType === 3) {
                    renderLabelLine(node.textContent || '');
                }
            });
            if (esActa || esRecibo) {
                ensureSpace(44);
                y += 28;
                doc.setDrawColor(51,51,51);
                doc.line(pageWidth/2 - 120, y, pageWidth/2 + 120, y);
                doc.setFontSize(10);
                doc.text(esActa ? 'FIRMA DEL PRESIDENTE' : 'FIRMA DEL TESORERO', pageWidth / 2, y + 16, { align: 'center' });
                y += 16;
            }
            doc.setFont(undefined, 'normal');
            doc.setFontSize(9);
            const total = doc.internal.getNumberOfPages();
            for (let i = 1; i <= total; i++) {
                doc.setPage(i);
                doc.text(`Generado el: ${fechaGen}`, margin, pageHeight - 24);
                doc.text(`Página ${i} de ${total}`, pageWidth / 2, pageHeight - 24, { align: 'center' });
            }
            doc.save(nombre || 'documento.pdf');
        };
        window.exportTableToExcel = (tableId, fileName) => {
            const table = document.getElementById(tableId);
            if (!table) return;
            if (tableId === 'table-padron') {
                const headCells = (table.tHead && table.tHead.rows[0]) ? Array.from(table.tHead.rows[0].cells).map(c => (c.textContent || '').trim()) : [];
                const idxNumero = headCells.findIndex(h => /^N[º°]?$|^N°$|^Nº$|^N$|^#$/i.test(h));
                const idxNombre = headCells.findIndex(h => /Nombre/i.test(h));
                const idxLote = headCells.findIndex(h => /Lote|Manzana/i.test(h));
                const idxPiso = headCells.findIndex(h => /Piso/i.test(h));
                const idxEstado = headCells.findIndex(h => /Estado/i.test(h));
                const header = ['N°', 'Apellidos', 'Nombres', headCells[idxLote] || 'Lote/Manzana', headCells[idxPiso] || 'Piso', headCells[idxEstado] || 'Estado'];
                const rows = (table.tBodies && table.tBodies[0]) ? Array.from(table.tBodies[0].rows) : [];
                const data = rows.map(r => {
                    const cells = Array.from(r.cells);
                    const numero = idxNumero >= 0 ? (cells[idxNumero]?.textContent || '').trim() : String(rows.indexOf(r) + 1);
                    const full = idxNombre >= 0 ? (cells[idxNombre]?.textContent || '') : '';
                    const parts = full.split(',');
                    const apellidos = (parts[0] || '').trim();
                    const nombres = (parts.slice(1).join(',') || '').trim();
                    const lote = idxLote >= 0 ? (cells[idxLote]?.textContent || '').trim() : '';
                    const piso = idxPiso >= 0 ? (cells[idxPiso]?.textContent || '').trim() : '';
                    const estado = idxEstado >= 0 ? (cells[idxEstado]?.textContent || '').trim() : '';
                    return [numero, apellidos, nombres, lote, piso, estado];
                });
                const ws = XLSX.utils.aoa_to_sheet([header, ...data]);
                const wb = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(wb, ws, "Datos");
                XLSX.writeFile(wb, `${fileName}.xlsx`);
                return;
            }
            const wb = XLSX.utils.table_to_book(table, { sheet: "Datos" });
            XLSX.writeFile(wb, `${fileName}.xlsx`);
        };
        window.reporteMorososPDF = async () => {
            const mes = document.getElementById('cuotas-filter-month').value || new Date().toISOString().substring(0,7);
            let pendientes = cuotasData;
            if(mes) pendientes = pendientes.filter(c => c.fecha && c.fecha.startsWith(mes));
            const porSocio = {};
            pendientes.forEach(c => {
                const sid = c.socioId;
                if(!porSocio[sid]) porSocio[sid] = [];
                porSocio[sid].push(c);
            });
            const sociosPend = Object.keys(porSocio)
                .map(sid => {
                    const s = sociosData.find(x => x.id === sid);
                    const nombre = s ? `${(s.nombres || '').toUpperCase()} ${(s.apellidos || '').toUpperCase()}`.trim() : 'SOCIO ELIMINADO';
                    const lote = s ? (s.lote || '-') : '-';
                    const items = porSocio[sid].map(q => ({ concepto: q.concepto || '-', monto: Number(q.monto) || 0, fecha: q.fecha || '' }));
                    return { sid, nombre, lote, items };
                })
                .sort((a,b) => window.compareByLote(a.lote, b.lote) || a.nombre.localeCompare(b.nombre));
            const now = new Date();
            const pad = n => String(n).padStart(2,'0');
            const fechaGen = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()}, ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
            const { jsPDF } = window.jspdf || {};
            if (!jsPDF) return;
            const doc = new jsPDF({ unit: 'pt', format: 'letter', orientation: 'portrait' });
            const margin = 36;
            const pageWidth = doc.internal.pageSize.getWidth();
            const pageHeight = doc.internal.pageSize.getHeight();
            let y = margin;
            doc.setFontSize(14);
            doc.text('COOPERATIVA GLORIA Nº 4', pageWidth / 2, y, { align: 'center' });
            y += 18;
            doc.setFontSize(12);
            doc.text('INFORME DETALLADO DE SOCIOS MOROSOS', pageWidth / 2, y, { align: 'center' });
            y += 16;
            doc.setFontSize(10);
            doc.text(`Periodo: ${mes}`, pageWidth / 2, y, { align: 'center' });
            y += 14;
            for (let idx = 0; idx < sociosPend.length; idx++) {
                const s = sociosPend[idx];
                const totalSocio = s.items.reduce((acc, it) => acc + it.monto, 0);
                doc.setFontSize(11);
                doc.text(`${idx+1}. ${s.nombre} - ${s.lote}`, margin, y);
                y += 10;
                const head = [['CONCEPTO','MONTO']];
                const body = s.items.map(it => [it.concepto, `S/ ${Number(it.monto || 0).toFixed(2)}`]);
                body.push([`Total de Cuotas Adeudadas: ${s.items.length}`, `S/ ${totalSocio.toFixed(2)}`]);
                if (typeof doc.autoTable === 'function') {
                doc.autoTable({
                        head,
                        body,
                        startY: y,
                    styles: { fontSize: 9, cellPadding: 4, lineColor: [0,0,0], lineWidth: 0.4 },
                        headStyles: { fillColor: [15, 63, 34], textColor: 255 },
                        columnStyles: { 1: { halign: 'right' } },
                        theme: 'grid',
                        margin: { left: margin, right: margin }
                    });
                    y = (doc.lastAutoTable && doc.lastAutoTable.finalY) ? doc.lastAutoTable.finalY + 10 : y;
                }
                if (y > pageHeight - 80) { doc.addPage(); y = margin; }
            }
            doc.setFont(undefined, 'normal');
            doc.setFontSize(9);
            const total = doc.internal.getNumberOfPages();
            for (let i = 1; i <= total; i++) {
                doc.setPage(i);
                doc.text(`Generado el: ${fechaGen}`, margin, pageHeight - 24);
                doc.text(`Página ${i} de ${total}`, pageWidth / 2, pageHeight - 24, { align: 'center' });
            }
            doc.save(`Reporte_Morosos_${mes}.pdf`);
        };
        window.reportePagadosPDF = async () => {
            const mes = document.getElementById('cuotas-filter-month').value || new Date().toISOString().substring(0,7);
            let pagadas = allCajaMovs.filter(m => m.esCuota);
            if(mes) pagadas = pagadas.filter(m => m.cuotaOriginal && m.cuotaOriginal.fechaEmision && m.cuotaOriginal.fechaEmision.startsWith(mes));
            const rows = pagadas.map(m => {
                const sid = m.cuotaOriginal && m.cuotaOriginal.socioId;
                const s = sociosData.find(x => x.id === sid);
                const nombre = s ? `${s.apellidos}, ${s.nombres}` : 'Socio Eliminado';
                const lote = s ? (s.lote || '-') : '-';
                const piso = s ? (s.piso || '-') : '-';
                return { nombre, lote, piso, recibo: m.numeroRecibo || '-', fecha: m.fecha || '-', monto: Number(m.monto) || 0 };
            }).sort((a,b) => window.compareByLote(a.lote, b.lote) || a.nombre.localeCompare(b.nombre));
            const now = new Date();
            const pad = n => String(n).padStart(2,'0');
            const fechaGen = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()}, ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
            const { jsPDF } = window.jspdf || {};
            if (!jsPDF) return;
            const doc = new jsPDF({ unit: 'pt', format: 'letter', orientation: 'portrait' });
            const margin = 36;
            const pageWidth = doc.internal.pageSize.getWidth();
            const pageHeight = doc.internal.pageSize.getHeight();
            let y = margin;
            doc.setFontSize(14);
            doc.text('COOPERATIVA GLORIA Nº 4 - REPORTE DE PAGADOS', pageWidth / 2, y, { align: 'center' });
            y += 16;
            doc.setFontSize(10);
            doc.text(`Periodo: ${mes}`, pageWidth / 2, y, { align: 'center' });
            y += 12;
            const head = [['N°','Nombre','Lote','Piso','N° Recibo','Fecha de Pago','Monto']];
            const body = rows.map((r, i) => [String(i+1), r.nombre, r.lote, r.piso, r.recibo, r.fecha, `S/ ${Number(r.monto||0).toFixed(2)}`]);
            if (typeof doc.autoTable === 'function') {
                doc.autoTable({
                    head,
                    body,
                    startY: y,
                    styles: { fontSize: 9, cellPadding: 4, lineColor: [0,0,0], lineWidth: 0.4 },
                    headStyles: { fillColor: [15, 63, 34], textColor: 255 },
                    columnStyles: { 0: { halign: 'center', cellWidth: 36 }, 6: { halign: 'right' } },
                    theme: 'grid',
                    margin: { left: margin, right: margin }
                });
            }
            doc.setFont(undefined, 'normal');
            doc.setFontSize(9);
            const total = doc.internal.getNumberOfPages();
            for (let i = 1; i <= total; i++) {
                doc.setPage(i);
                doc.text(`Generado el: ${fechaGen}`, margin, pageHeight - 24);
                doc.text(`Página ${i} de ${total}`, pageWidth / 2, pageHeight - 24, { align: 'center' });
            }
            doc.save(`Reporte_Pagados_${mes}.pdf`);
        };

        // ========================================================
        // GRÁFICOS ESTADÍSTICOS (Chart.js)
        // ========================================================
        function updateCharts() {
            if (currentUser && (currentUser.role === 'root' || currentUser.role === 'admin')) {
                renderFinanceChart();
                renderSociosChart();
            }
        }

        function renderFinanceChart() {
            const ctx = document.getElementById('chart-finanzas');
            if (!ctx) return;

            // Obtener últimos 6 meses
            const labels = [];
            const ingresos = [];
            const egresos = [];
            
            for (let i = 5; i >= 0; i--) {
                const d = new Date();
                d.setMonth(d.getMonth() - i);
                const monthStr = d.toISOString().substring(0, 7); // YYYY-MM
                labels.push(d.toLocaleDateString('es-ES', { month: 'short', year: '2-digit' }));
                
                const movsMes = allCajaMovs.filter(m => m.fecha && m.fecha.startsWith(monthStr));
                const totalIn = movsMes.filter(m => m.tipo === 'ingreso').reduce((acc, m) => acc + parseFloat(m.monto || 0), 0);
                const totalOut = movsMes.filter(m => m.tipo === 'egreso').reduce((acc, m) => acc + parseFloat(m.monto || 0), 0);
                
                ingresos.push(totalIn);
                egresos.push(totalOut);
            }

            if (chartFinanzas) chartFinanzas.destroy();

            chartFinanzas = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [
                        {
                            label: 'Ingresos',
                            data: ingresos,
                            backgroundColor: 'rgba(16, 185, 129, 0.7)',
                            borderColor: '#10B981',
                            borderWidth: 1,
                            borderRadius: 6
                        },
                        {
                            label: 'Egresos',
                            data: egresos,
                            backgroundColor: 'rgba(239, 68, 68, 0.7)',
                            borderColor: '#EF4444',
                            borderWidth: 1,
                            borderRadius: 6
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'bottom', labels: { usePointStyle: true, padding: 20 } },
                        tooltip: { 
                            backgroundColor: '#1e293b',
                            padding: 12,
                            callbacks: {
                                label: (context) => ` S/ ${context.parsed.y.toFixed(2)}`
                            }
                        }
                    },
                    scales: {
                        y: { 
                            beginAtZero: true, 
                            grid: { color: '#f1f5f9' },
                            ticks: { callback: (value) => 'S/ ' + value }
                        },
                        x: { grid: { display: false } }
                    }
                }
            });
        }

        function renderSociosChart() {
            const ctx = document.getElementById('chart-socios');
            if (!ctx) return;

            // Filtrar socios por estado (ignorando mayúsculas/minúsculas)
            const activos = sociosData.filter(s => String(s.estado || '').toLowerCase() === 'activo');
            const inactivosCount = sociosData.filter(s => String(s.estado || '').toLowerCase() === 'inactivo').length;
            
            // Determinar morosos: socios activos que tienen al menos una cuota no pagada en cuotasData
            const morososIds = new Set(cuotasData.filter(c => !c.pagada).map(c => c.socioId));
            
            let numMorosos = 0;
            let numAlDia = 0;

            activos.forEach(s => {
                if (morososIds.has(s.id)) {
                    numMorosos++;
                } else {
                    numAlDia++;
                }
            });

            if (chartSocios) chartSocios.destroy();

            chartSocios = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Al Día', 'Morosos', 'Inactivos'],
                    datasets: [{
                        data: [numAlDia, numMorosos, inactivosCount],
                        backgroundColor: [
                            '#10B981', // Verde (Al Día)
                            '#F59E0B', // Ámbar (Morosos)
                            '#94A3B8'  // Gris (Inactivos)
                        ],
                        hoverOffset: 10,
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    cutout: '70%',
                    plugins: {
                        legend: { position: 'bottom', labels: { usePointStyle: true, padding: 20 } },
                        tooltip: {
                            backgroundColor: '#1e293b',
                            padding: 12,
                            callbacks: {
                                label: (context) => ` ${context.label}: ${context.parsed} socios`
                            }
                        }
                    }
                }
            });
        }

        window.toggleLoginPass = (btn) => {
            const input = document.getElementById('login-pass');
            if (!input) return;
            const eye = btn.querySelector('i');
            if (input.type === 'password') {
                input.type = 'text';
                if (eye) { eye.classList.remove('fa-eye'); eye.classList.add('fa-eye-slash'); }
            } else {
                input.type = 'password';
                if (eye) { eye.classList.remove('fa-eye-slash'); eye.classList.add('fa-eye'); }
            }
        };

