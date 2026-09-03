"""Flip a Markdown reference doc into an atlas-styled, theme-aware HTML page.

Reuses the exact CSS custom properties from the existing atlases so the new
page sits in the same visual family, then adds rules for the elements markdown
actually emits (h2/h3, tables, blockquotes, fenced code, inline code).
"""
import re
import sys
import markdown

src, out, title, subtitle = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
text = open(src, encoding="utf-8").read()

# Strip the H1 and the lead paragraph — they become the hero.
lines = text.splitlines()
h1 = next(l[2:].strip() for l in lines if l.startswith("# "))
body_md = "\n".join(lines[1:])

from markdown.extensions.toc import slugify


def ch_slug(value, separator):
    """Heading ids must be valid CSS selectors.

    The default slugify turns "## 1 · The four kinds" into id="1-the-four-kinds",
    and `document.querySelector('#1-the-four-kinds')` throws a SyntaxError
    because a CSS identifier may not start with a digit. Prefixing fixes it.
    """
    return "ch-" + slugify(value, separator)


md = markdown.Markdown(
    extensions=["tables", "fenced_code", "toc", "attr_list", "sane_lists"],
    extension_configs={"toc": {"slugify": ch_slug}},
)
html = md.convert(body_md)

# Build the chapter nav from the "## N · Title" headings.
# NB: [^<]+ would silently drop any heading containing inline markup — and
# chapters 3 and 4 end in a `code` span, so they vanished from the nav.
chapters = [(cid, re.sub(r"<[^>]+>", "", inner))
            for cid, inner in re.findall(r'<h2 id="([^"]+)">(.*?)</h2>', html, re.S)]


def nav_label(t):
    """Turn "5 · AWS compute — the three EMR models" into "5 AWS compute"."""
    import html as _html
    t = _html.unescape(t).strip()
    m = re.match(r"^(\d+)\s*[·.]\s*(.+)$", t)
    num, rest = (m.group(1), m.group(2)) if m else ("", t)
    rest = re.split(r"\s+[—–-]\s+", rest)[0].strip()          # drop the sub-clause
    rest = re.sub(r"^(the|a)\s+", "", rest, flags=re.I)
    if len(rest) > 20:
        rest = rest[:19].rstrip() + "…"
    return f"{num} {rest}".strip()


nav = "".join(
    f'<a href="#{cid}">{nav_label(ctitle)}</a>' for cid, ctitle in chapters
)

# Wrap each h2..next-h2 in a section so the scrollspy and rules match the atlases.
parts = re.split(r'(?=<h2 id=")', html)
lead, sections = parts[0], parts[1:]
body = f'<section class="lead"><div class="wrap">{lead}</div></section>'
for i, sec in enumerate(sections):
    cid = re.search(r'<h2 id="([^"]+)"', sec).group(1)
    last = ' style="border-bottom:none"' if i == len(sections) - 1 else ""
    body += f'<section id="{cid}"{last}><div class="wrap">{sec}</div></section>'

CSS = """
*{box-sizing:border-box}
:root{
--bg:#F4F6F8;--surface:#FFFFFF;--surface2:#E9EDF2;--sunken:#DFE5EC;
--ink:#111820;--muted:#5A6875;--line:#CFD8E1;--line2:#B3C0CD;
--accent:#0E7490;--accent-ink:#0A5A70;--accent-soft:rgba(14,116,144,.11);
--ok:#2E7D4F;--ok-soft:rgba(46,125,79,.12);
--warn:#B45309;--warn-soft:rgba(180,83,9,.12);
--bad:#B3261E;--bad-soft:rgba(179,38,30,.10);
--code-bg:#0E1720;--code-ink:#D6E2EA;--code-dim:#6D8494;
--hero-bg:#0E1720;--hero-ink:#E9F1F6;--hero-mute:#8FA5B3;--hero-line:#22323F;
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
--bg:#0A1119;--surface:#111A24;--surface2:#182430;--sunken:#0D151D;
--ink:#E3EDF4;--muted:#8CA0AF;--line:#22303C;--line2:#334656;
--accent:#38BDF8;--accent-ink:#7DD3FC;--accent-soft:rgba(56,189,248,.13);
--ok:#4ADE80;--ok-soft:rgba(74,222,128,.13);
--warn:#FBBF24;--warn-soft:rgba(251,191,36,.13);
--bad:#F87171;--bad-soft:rgba(248,113,113,.13);
--code-bg:#070D13;--code-ink:#D6E2EA;--code-dim:#6D8494;
--hero-bg:#070D13;--hero-ink:#E9F1F6;--hero-mute:#8FA5B3;--hero-line:#1B2732;
}}
:root[data-theme="dark"]{
--bg:#0A1119;--surface:#111A24;--surface2:#182430;--sunken:#0D151D;
--ink:#E3EDF4;--muted:#8CA0AF;--line:#22303C;--line2:#334656;
--accent:#38BDF8;--accent-ink:#7DD3FC;--accent-soft:rgba(56,189,248,.13);
--ok:#4ADE80;--ok-soft:rgba(74,222,128,.13);
--warn:#FBBF24;--warn-soft:rgba(251,191,36,.13);
--bad:#F87171;--bad-soft:rgba(248,113,113,.13);
--code-bg:#070D13;--code-ink:#D6E2EA;--code-dim:#6D8494;
--hero-bg:#070D13;--hero-ink:#E9F1F6;--hero-mute:#8FA5B3;--hero-line:#1B2732;
}
body{margin:0;background:var(--bg);color:var(--ink);
font:400 15px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,system-ui,sans-serif;
-webkit-font-smoothing:antialiased}
.wrap{max-width:1120px;margin:0 auto;padding:0 28px}
code,pre,.mono{font-family:"JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}

/* hero */
.hero{background:var(--hero-bg);color:var(--hero-ink);padding:64px 0 52px;
border-bottom:1px solid var(--hero-line)}
.hero .eyebrow{font:600 11px/1 "JetBrains Mono",ui-monospace,monospace;letter-spacing:.18em;
text-transform:uppercase;color:var(--accent);margin-bottom:18px}
.hero h1{margin:0 0 16px;font:700 clamp(30px,5vw,50px)/1.06 -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;
letter-spacing:-.028em}
.hero .thesis{margin:0 0 14px;font-size:19px;line-height:1.5;color:var(--hero-ink);max-width:74ch}
.hero .thesis em{color:var(--accent);font-style:normal;font-weight:600}
.hero .sub{margin:0;color:var(--hero-mute);font-size:15px;line-height:1.6;max-width:80ch}
.vbadge{display:inline-block;margin-top:24px;padding:7px 13px;border:1px solid var(--accent);
border-radius:4px;color:var(--accent);font:600 10.5px/1 "JetBrains Mono",ui-monospace,monospace;
letter-spacing:.11em}
.stats{display:flex;flex-wrap:wrap;gap:36px;margin-top:32px;padding-top:26px;
border-top:1px solid var(--hero-line)}
.stats div{display:flex;flex-direction:column;gap:5px}
.stats b{font:700 26px/1 -apple-system,system-ui,sans-serif;color:var(--accent)}
.stats span{font:500 11px/1.3 "JetBrains Mono",ui-monospace,monospace;color:var(--hero-mute);
letter-spacing:.05em;text-transform:uppercase}

/* nav */
nav{position:sticky;top:0;z-index:20;background:var(--surface);
border-bottom:1px solid var(--line);overflow-x:auto;-webkit-overflow-scrolling:touch}
nav .wrap{display:flex;gap:2px;white-space:nowrap;padding-top:0;padding-bottom:0}
nav a{padding:13px 11px;text-decoration:none;color:var(--muted);
font:500 12px/1 "JetBrains Mono",ui-monospace,monospace;border-bottom:2px solid transparent}
nav a:hover{color:var(--ink)}
nav a.on{color:var(--accent);border-bottom-color:var(--accent)}

/* sections */
section{padding:52px 0;border-bottom:1px solid var(--line)}
section.lead{padding:40px 0}
h2{margin:0 0 8px;font:700 clamp(22px,3vw,31px)/1.15 -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;
letter-spacing:-.02em;scroll-margin-top:64px}
h3{margin:34px 0 10px;font:600 17px/1.3 -apple-system,system-ui,sans-serif;letter-spacing:-.01em}
p{margin:0 0 14px;max-width:82ch}
ul,ol{margin:0 0 16px;padding-left:22px;max-width:82ch}
li{margin:5px 0}
hr{border:none;border-top:1px solid var(--line);margin:38px 0}
strong{font-weight:650}
a{color:var(--accent-ink)}

/* inline code */
p code,li code,td code,th code,h3 code,blockquote code{
background:var(--accent-soft);color:var(--accent-ink);padding:1.5px 5px;border-radius:3px;
font-size:12.5px;white-space:nowrap}

/* tables */
table{width:100%;border-collapse:collapse;margin:6px 0 22px;font-size:13.5px;
background:var(--surface);border:1px solid var(--line);border-radius:6px;overflow:hidden}
thead{background:var(--surface2)}
th{text-align:left;padding:11px 13px;font:600 11px/1.3 "JetBrains Mono",ui-monospace,monospace;
letter-spacing:.07em;text-transform:uppercase;color:var(--muted);
border-bottom:1px solid var(--line2);vertical-align:bottom}
td{padding:10px 13px;border-bottom:1px solid var(--line);vertical-align:top;line-height:1.5}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:var(--accent-soft)}
td:first-child{font-weight:550}
.tablewrap{overflow-x:auto;margin:6px 0 22px}
.tablewrap table{margin:0}

/* code blocks */
pre{background:var(--code-bg);color:var(--code-ink);padding:16px 18px;border-radius:6px;
overflow-x:auto;margin:6px 0 22px;font-size:12.5px;line-height:1.62;
border:1px solid var(--hero-line)}
pre code{background:none;color:inherit;padding:0;white-space:pre}

/* blockquote = the callout */
blockquote{margin:18px 0 22px;padding:14px 18px;background:var(--warn-soft);
border-left:3px solid var(--warn);border-radius:0 5px 5px 0;max-width:88ch}
blockquote p{margin:0 0 8px}
blockquote p:last-child{margin:0}

footer{padding:38px 0 60px;color:var(--muted);font-size:13px}
footer b{color:var(--ink)}
@media print{nav{display:none}section{break-inside:avoid;border-bottom:1px solid #ccc}}
"""

JS = """
(function(){var l=[].slice.call(document.querySelectorAll('nav a')),
s=l.map(function(a){return document.querySelector(a.getAttribute('href'))});
function spy(){var p=window.scrollY+140,i=0;
for(var k=0;k<s.length;k++){if(s[k]&&s[k].offsetTop<=p)i=k}
l.forEach(function(a,k){a.classList.toggle('on',k===i)})}
window.addEventListener('scroll',spy,{passive:true});spy();
// every table scrolls inside its own box rather than widening the page
[].forEach.call(document.querySelectorAll('table'),function(t){
if(t.parentNode.className==='tablewrap')return;
var w=document.createElement('div');w.className='tablewrap';
t.parentNode.insertBefore(w,t);w.appendChild(t);});})();
"""

page = f"""<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>{CSS}</style>
<header class="hero"><div class="wrap">
  <div class="eyebrow">Reference · generated from the installed packages</div>
  <h1>{h1}</h1>
  <p class="thesis">{subtitle}</p>
  <p class="sub">Chapters 5 to 10 and Appendix A were produced by walking
  <code>airflow.providers.amazon.aws</code> inside the running Airflow 3.3.1
  image with <code>pkgutil</code> and <code>inspect</code>. No class name here
  was recalled from memory. The verification log at the end says exactly what
  was checked and how.</p>
  <div class="vbadge">✓ AIRFLOW 3.3.1 · AMAZON PROVIDER 9.34.0 · 268 CLASSES ENUMERATED</div>
  <div class="stats">
    <div><b>268</b><span>AWS operator / sensor / transfer classes</span></div>
    <div><b>87</b><span>AWS modules</span></div>
    <div><b>~90</b><span>provider packages in the ecosystem</span></div>
    <div><b>4</b><span>kinds of integration</span></div>
    <div><b>5</b><span>enterprise DAGs that use them</span></div>
  </div>
</div></header>
<nav aria-label="Chapters"><div class="wrap">{nav}</div></nav>
{body}
<footer><div class="wrap">
  <b>{h1}</b> — reference companion to atlases 8 and 9. Surender Sara · NorthBay Solutions.<br>
  Source of truth: <code>4_reference/airflow_integration_catalog.md</code>.
  Working examples: <code>3_labs/airflow_enterprise/</code>.
</div></footer>
<script>{JS}</script>
"""
open(out, "w", encoding="utf-8").write(page)
print(f"wrote {out}: {len(page):,} bytes, {len(chapters)} chapters, "
      f"{page.count('<table>')} tables, {page.count('<pre>')} code blocks")
