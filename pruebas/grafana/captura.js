// Toma capturas de los dashboards de Grafana con Chrome sin ventana (Puppeteer).
// La autenticación se envía solo a Grafana: si se envía también a los servidores
// de mapas, el navegador bloquea las imágenes (CORS) y el mapa sale sin calles.
// Uso (desde la raíz del proyecto, PowerShell; las imágenes quedan en pruebas\grafana):
//   docker run --rm --user root -e PUPPETEER_CACHE_DIR=/home/pptruser/.cache/puppeteer --network monitoreo-sjs_default
//     -v "${PWD}\pruebas\grafana:/out" -w /out ghcr.io/puppeteer/puppeteer:24.22.3
//     node captura.js "admin:<GRAFANA_ADMIN_PASSWORD>" "centro-monitoreo=dash1=2250" "bandeja-instituciones=dash4=1100"
// Cada par es uid=nombre_archivo=alto_en_pixeles.
const puppeteer = require('puppeteer');
const [,, auth, ...pares] = process.argv;   // pares: uid=archivo=alto
(async () => {
  const browser = await puppeteer.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage();
  // El header de autenticación solo para Grafana (en otros orígenes provoca errores CORS)
  await page.setRequestInterception(true);
  page.on('request', r => r.url().startsWith('http://grafana:3000') ? r.continue({ headers: { ...r.headers(), Authorization: 'Basic ' + Buffer.from(auth).toString('base64') } }) : r.continue());
  for (const par of pares) {
    const [uid, archivo, alto] = par.split('=');
    await page.setViewport({ width: 1600, height: parseInt(alto) });
    await page.goto(`http://grafana:3000/d/${uid}?orgId=1&kiosk&refresh=1d`, { waitUntil: 'networkidle2', timeout: 60000 });
    await new Promise(r => setTimeout(r, 15000));
    await page.screenshot({ path: `/out/${archivo}.png` });
    console.log('captura', archivo);
  }
  await browser.close();
})();

