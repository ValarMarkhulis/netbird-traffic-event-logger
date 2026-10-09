// Headless dashboard login smoke test. Run via `make login-check`.
import { chromium } from 'playwright';

const base = process.env.DASHBOARD_URL || 'http://localhost:8080';
const browser = await chromium.launch();
const page = await browser.newPage();
const problems = [];
page.on('console', m => { if (m.type() === 'error') problems.push(`console: ${m.text()}`); });
page.on('requestfailed', r => problems.push(`failed: ${r.url()} ${r.failure()?.errorText}`));

await page.goto(base);
try {
  await page.waitForURL(/localhost:5556\/dex/, { timeout: 30000 });
} catch {
  console.log(`NO REDIRECT TO DEX. Page says:\n${(await page.innerText('body')).slice(0, 500)}`);
  for (const p of problems) console.log(p);
  process.exit(1);
}
await page.fill('input[name=login]', process.env.NB_USER || 'admin@netbird.local');
await page.fill('input[name=password]', process.env.NB_PASSWORD || 'password');
await page.click('button[type=submit]');
await page.waitForURL(u => u.origin === new URL(base).origin && !u.pathname.startsWith('/nb-auth'), { timeout: 30000 });
await page.waitForTimeout(5000);

const body = await page.innerText('body');
const failed = /error logging you in|Unauthenticated/i.test(body);
console.log(`final url: ${page.url()}`);
console.log(failed ? `LOGIN FAILED:\n${body.slice(0, 500)}` : 'LOGIN OK');
for (const p of problems) console.log(p);
await browser.close();
process.exit(failed ? 1 : 0);
